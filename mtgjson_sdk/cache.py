"""Version-aware CDN download and local file cache manager."""

from __future__ import annotations

import gzip
import hashlib
import json
import logging
import os
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import httpx

from .config import CDN_BASE, JSON_FILES, META_URL, PARQUET_FILES, default_cache_dir

logger = logging.getLogger("mtgjson_sdk")

REMOTE_VERSION_TTL_SECONDS = 300.0
LOCK_TIMEOUT_SECONDS = 120.0
LOCK_POLL_INTERVAL_SECONDS = 0.05


class CacheManager:
    """Downloads and caches MTGJSON data files from the CDN.

    Checks Meta.json for version changes and re-downloads when stale.
    Individual files are downloaded lazily on first access.
    """

    def __init__(
        self,
        cache_dir: Path | str | None = None,
        *,
        offline: bool = False,
        timeout: float = 120.0,
        on_progress: Any | None = None,
    ) -> None:
        """Create a cache manager.

        Args:
            cache_dir: Directory for cached data files. Defaults to a
                platform-appropriate cache directory.
            offline: If True, never download from CDN (use cached files only).
            timeout: HTTP request timeout in seconds (default 120).
            on_progress: Optional callback
                ``(filename, bytes_downloaded, total_bytes)``
                called during file downloads.
        """
        self.cache_dir = Path(cache_dir) if cache_dir else default_cache_dir()
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.offline = offline
        self.timeout = timeout
        self._client: httpx.Client | None = None
        self._remote_version: str | None = None
        self._remote_version_checked_at: float | None = None
        self._on_progress = on_progress
        self._prefetch_threads: dict[str, threading.Thread] = {}
        self._prefetch_errors: dict[str, str] = {}
        self._prefetch_lock = threading.Lock()
        self._download_locks: dict[str, threading.Lock] = {}
        self._download_locks_lock = threading.Lock()

    @property
    def client(self) -> httpx.Client:
        """Lazy HTTP client, created on first use."""
        if self._client is None:
            self._client = httpx.Client(timeout=self.timeout, follow_redirects=True)
        return self._client

    def close(self) -> None:
        """Close the HTTP client, if open."""
        if self._client is not None:
            close_fn = getattr(self._client, "close", None)
            if callable(close_fn):
                close_fn()
            self._client = None

    def _local_version(self) -> str | None:
        return self._active_cache_snapshot()[1]

    def _read_local_version_file(self, version_file: Path) -> str | None:
        if version_file.exists():
            value = version_file.read_text(encoding="utf-8").strip()
            return value or None
        return None

    def _active_cache_snapshot(self) -> tuple[Path, str | None]:
        version_file = self.cache_dir / "version.txt"
        with self._coordinated_lock(f"version-marker:{version_file.resolve()}"):
            version = self._read_local_version_file(version_file)
            if version:
                version_root = self._version_root(version)
                if version_root.exists():
                    return version_root, version
            return self.cache_dir, version

    def _write_text_atomically(self, path: Path, value: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = path.with_suffix(
            f"{path.suffix}.{os.getpid()}.{threading.get_ident()}.tmp"
        )
        tmp_path.write_text(value, encoding="utf-8")
        tmp_path.replace(path)

    def _save_version(self, version: str) -> None:
        """Persist the active cache version and create its version root."""

        version_root = self._version_root(version)
        version_file = self.cache_dir / "version.txt"
        with self._coordinated_lock(f"version-marker:{version_file.resolve()}"):
            version_root.mkdir(parents=True, exist_ok=True)
            self._write_text_atomically(version_root / "version.txt", version)
            self._write_text_atomically(version_file, version)
        self._remote_version = version
        self._remote_version_checked_at = time.monotonic()

    def _version_root(self, version: str) -> Path:
        return self.cache_dir / "versions" / version

    def _active_root(self) -> Path:
        return self._active_cache_snapshot()[0]

    def _path_for_filename(self, filename: str) -> Path:
        return self._active_root() / filename

    def _path_for_version(self, filename: str, version: str) -> Path:
        return self._version_root(version) / filename

    def cache_token(self) -> str:
        """Return a stable identifier for the currently active cache root."""

        active_root, version = self._active_cache_snapshot()
        return f"{active_root}::{version or 'legacy'}"

    def _thread_lock_for(self, key: str) -> threading.Lock:
        with self._download_locks_lock:
            lock = self._download_locks.get(key)
            if lock is None:
                lock = threading.Lock()
                self._download_locks[key] = lock
            return lock

    def _lock_file_path(self, key: str) -> Path:
        digest = hashlib.sha1(key.encode("utf-8")).hexdigest()
        return self.cache_dir / ".locks" / f"{digest}.lock"

    @contextmanager
    def _coordinated_lock(
        self,
        key: str,
        *,
        timeout_seconds: float = LOCK_TIMEOUT_SECONDS,
        poll_seconds: float = LOCK_POLL_INTERVAL_SECONDS,
    ):
        """Coordinate cache writes across threads and processes."""

        thread_lock = self._thread_lock_for(key)
        with thread_lock:
            lock_path = self._lock_file_path(key)
            lock_path.parent.mkdir(parents=True, exist_ok=True)
            deadline = time.monotonic() + timeout_seconds
            fd: int | None = None

            while fd is None:
                try:
                    fd = os.open(
                        lock_path,
                        os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                    )
                    os.write(fd, str(os.getpid()).encode("ascii", errors="ignore"))
                except FileExistsError:
                    if time.monotonic() >= deadline:
                        raise TimeoutError(f"Timed out waiting for cache lock '{key}'")
                    time.sleep(poll_seconds)

            try:
                yield
            finally:
                if fd is not None:
                    os.close(fd)
                lock_path.unlink(missing_ok=True)

    def remote_version(self, *, force: bool = False) -> str | None:
        """Fetch the current MTGJSON version from Meta.json on the CDN.

        Returns:
            Version string (e.g. ``"5.2.2+20240101"``), or None if
            offline or the CDN is unreachable.
        """
        if (
            not force
            and self._remote_version_checked_at is not None
            and (
                time.monotonic() - self._remote_version_checked_at
                < REMOTE_VERSION_TTL_SECONDS
            )
        ):
            return self._remote_version
        if self.offline:
            return None
        try:
            resp = self.client.get(META_URL)
            resp.raise_for_status()
            data = resp.json()
            self._remote_version = data.get("data", {}).get("version") or data.get(
                "meta", {}
            ).get("version")
            self._remote_version_checked_at = time.monotonic()
            return self._remote_version
        except (httpx.HTTPError, KeyError, json.JSONDecodeError):
            self._remote_version = None
            self._remote_version_checked_at = time.monotonic()
            logger.warning("Failed to fetch MTGJSON version from CDN")
            return None

    def clear_remote_version_cache(self) -> None:
        self._remote_version = None
        self._remote_version_checked_at = None

    def is_stale(self, *, force_remote: bool = False) -> bool:
        """Check if local cache is out of date compared to the CDN.

        Returns:
            True if there is no local cache or the CDN has a newer version.
            False if up to date or if the CDN is unreachable.
        """
        local = self._local_version()
        if local is None:
            return True
        remote = self.remote_version(force=force_remote)
        if remote is None:
            return False
        return local != remote

    def _download_file(self, filename: str, dest: Path) -> None:
        """Download a single file from the CDN.

        Downloads to a temp file first and renames on success, so an
        interrupted download never leaves a corrupt partial file behind.
        Calls ``on_progress(filename, bytes_downloaded, total_bytes)``
        after each chunk if a progress callback was provided.
        """
        url = f"{CDN_BASE}/{filename}"
        logger.info("Downloading %s", url)
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp_dest = dest.with_suffix(dest.suffix + ".tmp")
        try:
            with self.client.stream("GET", url) as resp:
                resp.raise_for_status()
                total = int(resp.headers.get("content-length", 0)) or None
                downloaded = 0
                with open(tmp_dest, "wb") as f:
                    for chunk in resp.iter_bytes(chunk_size=65536):
                        f.write(chunk)
                        downloaded += len(chunk)
                        if self._on_progress:
                            self._on_progress(filename, downloaded, total)
            tmp_dest.replace(dest)
        except BaseException:
            tmp_dest.unlink(missing_ok=True)
            raise

    def parquet_path(self, view_name: str) -> Path:
        """Return the expected local parquet path for a logical view name."""

        return self._path_for_filename(PARQUET_FILES[view_name])

    def parquet_tmp_path(self, view_name: str) -> Path:
        """Return the temp-file path used while downloading a parquet view."""

        path = self.parquet_path(view_name)
        return path.with_suffix(path.suffix + ".tmp")

    def parquet_status(self, view_name: str) -> dict[str, Any]:
        """Return local, non-downloading cache status for a parquet view."""

        path = self.parquet_path(view_name)
        tmp_path = self.parquet_tmp_path(view_name)
        with self._prefetch_lock:
            thread = self._prefetch_threads.get(view_name)
            error = self._prefetch_errors.get(view_name)

        prefetching = bool(thread and thread.is_alive())
        cached = path.exists()
        partial = tmp_path.exists()

        if cached:
            status = "ready"
        elif prefetching:
            status = "in_progress"
        elif partial:
            status = "partial"
        elif error:
            status = "error"
        elif self.offline:
            status = "offline"
        else:
            status = "missing"

        return {
            "view_name": view_name,
            "path": str(path),
            "tmp_path": str(tmp_path),
            "status": status,
            "cached": cached,
            "partial": partial,
            "prefetching": prefetching,
            "size_bytes": path.stat().st_size if cached else None,
            "tmp_size_bytes": tmp_path.stat().st_size if partial else None,
            "error": error,
        }

    def json_path(self, name: str) -> Path:
        """Return the expected local JSON path for a logical cache name."""

        return self._path_for_filename(JSON_FILES[name])

    def _expected_relative_paths(self) -> list[str]:
        return sorted({*PARQUET_FILES.values(), *JSON_FILES.values()})

    def _is_version_complete(self, version: str) -> bool:
        root = self._version_root(version)
        if not root.exists():
            return False
        return all(
            (root / relpath).exists() for relpath in self._expected_relative_paths()
        )

    def _download_if_missing(self, filename: str, target_path: Path) -> None:
        lock_key = f"download:{target_path.resolve()}"
        with self._coordinated_lock(lock_key):
            if target_path.exists():
                return
            self._download_file(filename, target_path)

    def _upgrade_to_version(self, version: str) -> None:
        if not version:
            return
        with self._coordinated_lock(f"upgrade:{version}"):
            if self._is_version_complete(version):
                self._save_version(version)
                return
            root = self._version_root(version)
            root.mkdir(parents=True, exist_ok=True)
            for filename in self._expected_relative_paths():
                self._download_if_missing(filename, root / filename)
            self._save_version(version)

    def _ensure_file(self, filename: str, *, offline_label: str) -> Path:
        active_root, local_version = self._active_cache_snapshot()
        remote_version = self.remote_version()
        local_path = active_root / filename

        if self.offline:
            if local_path.exists():
                return local_path
            raise FileNotFoundError(
                f"{offline_label} file {filename} not cached "
                "and offline mode is enabled"
            )

        if remote_version and remote_version != local_version:
            self._upgrade_to_version(remote_version)
            return self._path_for_filename(filename)

        self._download_if_missing(filename, local_path)
        return local_path

    def prefetch_parquet(self, view_name: str) -> dict[str, Any]:
        """Start warming a parquet view in a background thread if needed."""

        current = self.parquet_status(view_name)
        if current["status"] in {"in_progress", "offline"}:
            return current
        if current["status"] == "ready" and not self.is_stale():
            return current

        def _worker() -> None:
            try:
                self.ensure_parquet(view_name)
            except BaseException as exc:
                with self._prefetch_lock:
                    self._prefetch_errors[view_name] = str(exc)
            else:
                with self._prefetch_lock:
                    self._prefetch_errors.pop(view_name, None)

        with self._prefetch_lock:
            thread = self._prefetch_threads.get(view_name)
            if thread and thread.is_alive():
                current = {
                    "view_name": view_name,
                    "path": str(self.parquet_path(view_name)),
                    "tmp_path": str(self.parquet_tmp_path(view_name)),
                    "status": "in_progress",
                    "cached": False,
                    "partial": self.parquet_tmp_path(view_name).exists(),
                    "prefetching": True,
                    "size_bytes": None,
                    "tmp_size_bytes": (
                        self.parquet_tmp_path(view_name).stat().st_size
                        if self.parquet_tmp_path(view_name).exists()
                        else None
                    ),
                    "error": self._prefetch_errors.get(view_name),
                }
                return current
            self._prefetch_errors.pop(view_name, None)
            thread = threading.Thread(
                target=_worker,
                name=f"mtgjson-prefetch-{view_name}",
                daemon=True,
            )
            self._prefetch_threads[view_name] = thread
            thread.start()

        return self.parquet_status(view_name)

    def ensure_parquet(self, view_name: str) -> Path:
        """Ensure a parquet file is cached locally, downloading if needed.

        Args:
            view_name: Logical view name (e.g. ``"cards"``, ``"sets"``).

        Returns:
            Local filesystem path to the cached parquet file.

        Raises:
            FileNotFoundError: If offline and the file is not cached.
            KeyError: If *view_name* is not a known parquet file.
        """
        filename = PARQUET_FILES[view_name]
        return self._ensure_file(filename, offline_label="Parquet")

    def ensure_json(self, name: str) -> Path:
        """Ensure a JSON file is cached locally, downloading if needed.

        Args:
            name: Logical file name (e.g. ``"meta"``, ``"all_prices_today"``).

        Returns:
            Local filesystem path to the cached JSON file.

        Raises:
            FileNotFoundError: If offline and the file is not cached.
            KeyError: If *name* is not a known JSON file.
        """
        filename = JSON_FILES[name]
        return self._ensure_file(filename, offline_label="JSON")

    def load_json(self, name: str) -> dict:
        """Load and parse a JSON file (handles .gz transparently).

        If the cached file is corrupt (truncated download, disk error),
        it is deleted automatically so the next call re-downloads a fresh file.

        Args:
            name: Logical file name (e.g. ``"meta"``, ``"keywords"``).

        Returns:
            Parsed JSON as a dict.

        Raises:
            FileNotFoundError: If the file is corrupt (removed) or not cached.
        """
        path = self.ensure_json(name)
        try:
            if path.suffix == ".gz":
                with gzip.open(path, "rt", encoding="utf-8") as f:
                    return json.load(f)
            return json.loads(path.read_text(encoding="utf-8"))
        except (
            gzip.BadGzipFile,
            EOFError,
            json.JSONDecodeError,
            OSError,
            UnicodeDecodeError,
        ) as e:
            logger.warning("Corrupt cache file %s: %s — removing", path.name, e)
            path.unlink(missing_ok=True)
            raise FileNotFoundError(
                f"Cache file '{path.name}' was corrupt and has been removed. "
                f"Retry to re-download. Original error: {e}"
            ) from e

    def clear(self) -> None:
        """Remove all cached files and recreate the cache directory."""
        import shutil

        if self.cache_dir.exists():
            shutil.rmtree(self.cache_dir)
            self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.clear_remote_version_cache()
        with self._prefetch_lock:
            self._prefetch_threads.clear()
            self._prefetch_errors.clear()
