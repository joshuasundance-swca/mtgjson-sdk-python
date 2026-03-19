"""Tests for the cache manager."""

import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

from mtgjson_sdk.cache import CacheManager
from mtgjson_sdk.config import JSON_FILES, PARQUET_FILES


def test_cache_dir_created(tmp_path):
    cache_dir = tmp_path / "test_cache"
    cache = CacheManager(cache_dir, offline=True)
    assert cache_dir.exists()
    cache.close()


def test_local_version_none(tmp_path):
    cache = CacheManager(tmp_path / "cache", offline=True)
    assert cache._local_version() is None
    cache.close()


def test_save_and_read_version(tmp_path):
    cache = CacheManager(tmp_path / "cache", offline=True)
    cache._save_version("5.2.2+20250101")
    assert cache._local_version() == "5.2.2+20250101"
    cache.close()


def test_stale_when_no_version(tmp_path):
    cache = CacheManager(tmp_path / "cache", offline=True)
    assert cache.is_stale() is True
    cache.close()


def test_not_stale_when_version_saved(tmp_path):
    cache = CacheManager(tmp_path / "cache", offline=True)
    cache._save_version("5.2.2")
    # Offline mode can't check remote, so assumes fresh
    assert cache.is_stale() is False
    cache.close()


def test_clear(tmp_path):
    cache_dir = tmp_path / "cache"
    cache = CacheManager(cache_dir, offline=True)
    cache._save_version("test")
    assert (cache_dir / "version.txt").exists()
    cache.clear()
    assert not (cache_dir / "version.txt").exists()
    assert cache_dir.exists()  # Dir recreated
    cache.close()


# === Corrupt file recovery tests ===


def test_load_json_corrupt_removed(tmp_path):
    """Corrupt JSON file is deleted and FileNotFoundError raised."""
    cache = CacheManager(tmp_path / "cache", offline=True)
    corrupt_path = cache.cache_dir / "Meta.json"
    corrupt_path.write_bytes(b"\x00\xff\xfe invalid json bytes")

    with pytest.raises(FileNotFoundError, match="corrupt"):
        cache.load_json("meta")

    # File should have been removed
    assert not corrupt_path.exists()
    cache.close()


def test_load_json_corrupt_gzip_removed(tmp_path):
    """Corrupt .gz file is deleted and FileNotFoundError raised."""
    cache = CacheManager(tmp_path / "cache", offline=True)
    # Use a JSON file that's still in JSON_FILES (prices moved to parquet)
    corrupt_path = cache.cache_dir / "Keywords.json"
    corrupt_path.write_bytes(b"this is not valid json at all")

    with pytest.raises(FileNotFoundError, match="corrupt"):
        cache.load_json("keywords")

    assert not corrupt_path.exists()
    cache.close()


def test_load_json_truncated_removed(tmp_path):
    """Truncated JSON file is deleted and FileNotFoundError raised."""
    cache = CacheManager(tmp_path / "cache", offline=True)
    truncated_path = cache.cache_dir / "Meta.json"
    truncated_path.write_text('{"data": {', encoding="utf-8")

    with pytest.raises(FileNotFoundError, match="corrupt"):
        cache.load_json("meta")

    assert not truncated_path.exists()
    cache.close()


def test_parquet_status_ready(tmp_path):
    cache = CacheManager(tmp_path / "cache", offline=True)
    path = cache.parquet_path("cards")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"parquet")

    status = cache.parquet_status("cards")

    assert status["status"] == "ready"
    assert status["cached"] is True
    assert status["size_bytes"] == len(b"parquet")
    cache.close()


def test_parquet_status_partial(tmp_path):
    cache = CacheManager(tmp_path / "cache", offline=False)
    tmp_path_file = cache.parquet_tmp_path("all_prices")
    tmp_path_file.parent.mkdir(parents=True, exist_ok=True)
    tmp_path_file.write_bytes(b"partial")

    status = cache.parquet_status("all_prices")

    assert status["status"] == "partial"
    assert status["partial"] is True
    assert status["tmp_size_bytes"] == len(b"partial")
    cache.close()


def test_remote_version_can_be_forced_to_refresh(tmp_path, monkeypatch):
    cache = CacheManager(tmp_path / "cache", offline=False)
    versions = iter(["5.0.0+old", "5.1.0+new"])

    class FakeResponse:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict:
            return {"data": {"version": next(versions)}}

    fake_client = type(
        "FakeClient",
        (),
        {"get": staticmethod(lambda _url: FakeResponse())},
    )()
    monkeypatch.setattr(cache, "_client", fake_client)

    assert cache.remote_version() == "5.0.0+old"
    assert cache.remote_version() == "5.0.0+old"
    assert cache.remote_version(force=True) == "5.1.0+new"
    cache.close()


def test_failed_parquet_upgrade_does_not_advance_active_version(tmp_path):
    cache = CacheManager(tmp_path / "cache", offline=False)
    cache._save_version("5.0.0+old")
    old_path = cache.parquet_path("cards")
    old_path.parent.mkdir(parents=True, exist_ok=True)
    old_path.write_bytes(b"old-cards")

    cache._remote_version = "5.1.0+new"
    cache._remote_version_checked_at = time.monotonic()

    def fail_download(_filename: str, _dest) -> None:
        raise RuntimeError("boom")

    cache._download_file = fail_download

    with pytest.raises(RuntimeError, match="boom"):
        cache.ensure_parquet("cards")

    assert cache._local_version() == "5.0.0+old"
    assert cache.parquet_path("cards").read_bytes() == b"old-cards"
    cache.close()


def test_failed_json_upgrade_does_not_advance_active_version(tmp_path):
    cache = CacheManager(tmp_path / "cache", offline=False)
    cache._save_version("5.0.0+old")
    old_path = cache.json_path("meta")
    old_path.parent.mkdir(parents=True, exist_ok=True)
    old_path.write_text('{"data":{"version":"5.0.0+old"}}', encoding="utf-8")

    cache._remote_version = "5.1.0+new"
    cache._remote_version_checked_at = time.monotonic()

    def fail_download(_filename: str, _dest) -> None:
        raise RuntimeError("boom")

    cache._download_file = fail_download

    with pytest.raises(RuntimeError, match="boom"):
        cache.ensure_json("meta")

    assert cache._local_version() == "5.0.0+old"
    assert cache.json_path("meta").read_text(encoding="utf-8") == (
        '{"data":{"version":"5.0.0+old"}}'
    )
    cache.close()


def test_successful_upgrade_downloads_full_dataset_before_activation(tmp_path):
    cache = CacheManager(tmp_path / "cache", offline=False)
    cache._save_version("5.0.0+old")
    old_cards = cache.parquet_path("cards")
    old_cards.parent.mkdir(parents=True, exist_ok=True)
    old_cards.write_bytes(b"old-cards")

    cache._remote_version = "5.1.0+new"
    cache._remote_version_checked_at = time.monotonic()
    downloaded: list[str] = []

    def fake_download(filename: str, dest) -> None:
        downloaded.append(filename)
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(filename, encoding="utf-8")

    cache._download_file = fake_download
    path = cache.ensure_parquet("cards")

    assert cache._local_version() == "5.1.0+new"
    assert cache.parquet_path("cards") == path
    assert set(downloaded) == {*PARQUET_FILES.values(), *JSON_FILES.values()}
    cache.close()


def test_concurrent_ensure_parquet_is_coordinated_across_cache_instances(tmp_path):
    cache_dir = tmp_path / "cache"
    caches = [CacheManager(cache_dir, offline=False) for _ in range(2)]
    for cache in caches:
        cache._save_version("5.0.0+coordinated")
        cache._remote_version = "5.0.0+coordinated"
        cache._remote_version_checked_at = time.monotonic()

    calls = 0
    calls_lock = threading.Lock()

    def fake_download(_filename: str, dest) -> None:
        nonlocal calls
        with calls_lock:
            calls += 1
        time.sleep(0.1)
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(b"parquet")

    for cache in caches:
        cache._download_file = fake_download

    results: list[str] = []

    def ensure(cache: CacheManager) -> None:
        results.append(str(cache.ensure_parquet("cards")))

    threads = [
        threading.Thread(target=ensure, args=(cache,))
        for cache in caches
        for _ in range(2)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert calls == 1
    assert len(set(results)) == 1

    for cache in caches:
        cache.close()


def test_concurrent_ensure_json_is_coordinated_across_cache_instances(tmp_path):
    cache_dir = tmp_path / "cache"
    caches = [CacheManager(cache_dir, offline=False) for _ in range(2)]
    for cache in caches:
        cache._save_version("5.0.0+coordinated")
        cache._remote_version = "5.0.0+coordinated"
        cache._remote_version_checked_at = time.monotonic()

    calls = 0
    calls_lock = threading.Lock()

    def fake_download(_filename: str, dest) -> None:
        nonlocal calls
        with calls_lock:
            calls += 1
        time.sleep(0.1)
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text('{"ok":true}', encoding="utf-8")

    for cache in caches:
        cache._download_file = fake_download

    results: list[str] = []

    def ensure(cache: CacheManager) -> None:
        results.append(str(cache.ensure_json("meta")))

    threads = [
        threading.Thread(target=ensure, args=(cache,))
        for cache in caches
        for _ in range(2)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert calls == 1
    assert len(set(results)) == 1

    for cache in caches:
        cache.close()


def test_multiprocess_ensure_parquet_is_coordinated(tmp_path):
    cache_dir = tmp_path / "cache"
    start_file = tmp_path / "start.flag"
    counter_file = tmp_path / "download-count.txt"
    repo_root = Path(__file__).resolve().parents[1]
    script = """
import sys
import time
from pathlib import Path

from mtgjson_sdk.cache import CacheManager

cache_dir = Path(sys.argv[1])
start_file = Path(sys.argv[2])
counter_file = Path(sys.argv[3])

cache = CacheManager(cache_dir, offline=False)
cache._save_version("5.0.0+coordinated")
cache._remote_version = "5.0.0+coordinated"
cache._remote_version_checked_at = time.monotonic()

def fake_download(_filename, dest):
    with cache._coordinated_lock(f"counter:{counter_file.resolve()}"):
        if counter_file.exists():
            current = int(counter_file.read_text(encoding="utf-8"))
        else:
            current = 0
        counter_file.write_text(str(current + 1), encoding="utf-8")
    time.sleep(0.25)
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(b"parquet")

cache._download_file = fake_download

while not start_file.exists():
    time.sleep(0.01)

print(cache.ensure_parquet("cards"))
"""
    procs = [
        subprocess.Popen(
            [
                sys.executable,
                "-c",
                script,
                str(cache_dir),
                str(start_file),
                str(counter_file),
            ],
            cwd=repo_root,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for _ in range(2)
    ]
    start_file.write_text("go", encoding="utf-8")

    try:
        outputs = [proc.communicate(timeout=20) for proc in procs]
    finally:
        for proc in procs:
            if proc.poll() is None:
                proc.kill()
                proc.communicate()

    for stdout, stderr in outputs:
        assert stderr == ""
        assert stdout.strip()

    assert counter_file.read_text(encoding="utf-8") == "1"
    assert len({stdout.strip() for stdout, _stderr in outputs}) == 1
