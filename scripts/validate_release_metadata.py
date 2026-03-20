#!/usr/bin/env python3
"""Validate the committed release metadata used for container and MCP publishing."""

from __future__ import annotations

import argparse
import json
import re
import sys
import tomllib
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

GITHUB_SERVER_NAME = re.compile(
    r"^io\.github\.(?P<owner>[A-Za-z0-9-]+)/(?P<repository>[A-Za-z0-9][A-Za-z0-9._-]*)$"
)
OCI_IDENTIFIER = re.compile(
    r"^(?P<registry>[^/]+)/(?P<namespace>[^/]+)/(?P<repository>[^:]+):(?P<tag>.+)$"
)


@dataclass(frozen=True)
class ReleaseMetadata:
    server_name: str
    server_version: str
    repository_url: str
    image_identifier: str
    image_repository: str
    image_tag: str
    transport_type: str
    github_owner: str
    github_repository_name: str
    github_repository: str


def _load_server_json(path: Path) -> dict[str, object]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ValueError(f"Missing {path}.") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(f"{path} is not valid JSON: {exc}.") from exc


def _load_pyproject_version(path: Path) -> str:
    try:
        payload = tomllib.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ValueError(f"Missing {path}.") from exc
    except tomllib.TOMLDecodeError as exc:
        raise ValueError(f"{path} is not valid TOML: {exc}.") from exc

    project = payload.get("project")
    if not isinstance(project, dict):
        raise ValueError(f"{path} is missing the [project] table.")

    version = project.get("version")
    if not isinstance(version, str) or not version.strip():
        raise ValueError(f"{path} is missing project.version.")
    return version


def _extract_release_metadata(server_json: dict[str, object]) -> ReleaseMetadata:
    server_name = server_json.get("name")
    if not isinstance(server_name, str) or not server_name.strip():
        raise ValueError("server.json is missing a non-empty name.")

    name_match = GITHUB_SERVER_NAME.fullmatch(server_name)
    if name_match is None:
        raise ValueError(
            "server.json name must use the GitHub namespace form "
            "'io.github.<owner>/<repository>'."
        )

    server_version = server_json.get("version")
    if not isinstance(server_version, str) or not server_version.strip():
        raise ValueError("server.json is missing a non-empty version.")

    repository = server_json.get("repository")
    if not isinstance(repository, dict):
        raise ValueError("server.json is missing the repository object.")

    repository_url = repository.get("url")
    if not isinstance(repository_url, str) or not repository_url.strip():
        raise ValueError("server.json repository.url must be a non-empty string.")

    parsed_url = urlparse(repository_url)
    if parsed_url.scheme != "https" or parsed_url.netloc != "github.com":
        raise ValueError("server.json repository.url must be an https://github.com URL.")

    repository_path = parsed_url.path.strip("/")
    try:
        repo_owner, repo_name = repository_path.split("/", maxsplit=1)
    except ValueError as exc:
        raise ValueError(
            "server.json repository.url must point to a GitHub owner/repository path."
        ) from exc

    if repo_owner != name_match.group("owner") or repo_name != name_match.group(
        "repository"
    ):
        raise ValueError(
            "server.json repository.url must match the owner/repository encoded in name."
        )

    packages = server_json.get("packages")
    if not isinstance(packages, list) or not packages:
        raise ValueError("server.json must include at least one package.")

    oci_packages = [
        package
        for package in packages
        if isinstance(package, dict) and package.get("registryType") == "oci"
    ]
    if len(oci_packages) != 1:
        raise ValueError(
            "server.json must include exactly one OCI package for this workflow."
        )

    package = oci_packages[0]
    image_identifier = package.get("identifier")
    if not isinstance(image_identifier, str) or not image_identifier.strip():
        raise ValueError("server.json packages[].identifier must be a non-empty string.")

    identifier_match = OCI_IDENTIFIER.fullmatch(image_identifier)
    if identifier_match is None:
        raise ValueError(
            "server.json packages[].identifier must be in "
            "'registry/namespace/repository:tag' format."
        )

    registry = identifier_match.group("registry")
    namespace = identifier_match.group("namespace")
    repository_name = identifier_match.group("repository")
    image_tag = identifier_match.group("tag")

    if registry != "ghcr.io":
        raise ValueError("server.json packages[].identifier must point at ghcr.io.")
    if namespace != name_match.group("owner"):
        raise ValueError(
            "The GHCR namespace in packages[].identifier must match server.json name."
        )
    if repository_name != name_match.group("repository"):
        raise ValueError(
            "The GHCR repository in packages[].identifier must match server.json name."
        )
    if image_tag != server_version:
        raise ValueError(
            "server.json packages[].identifier tag must exactly match server.json version."
        )

    transport = package.get("transport")
    if not isinstance(transport, dict):
        raise ValueError("server.json packages[].transport must be an object.")

    transport_type = transport.get("type")
    if transport_type != "stdio":
        raise ValueError("server.json packages[].transport.type must be 'stdio'.")

    return ReleaseMetadata(
        server_name=server_name,
        server_version=server_version,
        repository_url=repository_url,
        image_identifier=image_identifier,
        image_repository=image_identifier.rsplit(":", maxsplit=1)[0],
        image_tag=image_tag,
        transport_type=transport_type,
        github_owner=repo_owner,
        github_repository_name=repo_name,
        github_repository=f"{repo_owner}/{repo_name}",
    )


def _write_github_output(path: Path, metadata: ReleaseMetadata) -> None:
    outputs = {
        "server_name": metadata.server_name,
        "server_version": metadata.server_version,
        "repository_url": metadata.repository_url,
        "image_identifier": metadata.image_identifier,
        "image_repository": metadata.image_repository,
        "image_tag": metadata.image_tag,
        "transport_type": metadata.transport_type,
        "github_owner": metadata.github_owner,
        "github_repository_name": metadata.github_repository_name,
        "github_repository": metadata.github_repository,
    }
    with path.open("a", encoding="utf-8") as handle:
        for key, value in outputs.items():
            handle.write(f"{key}={value}\n")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate release metadata shared by the Docker and MCP publish flows."
    )
    parser.add_argument(
        "--server-json",
        type=Path,
        default=Path("server.json"),
        help="Path to server.json.",
    )
    parser.add_argument(
        "--pyproject",
        type=Path,
        default=Path("pyproject.toml"),
        help="Path to pyproject.toml.",
    )
    parser.add_argument(
        "--expected-version",
        help="Optional version that server.json and pyproject.toml must match.",
    )
    parser.add_argument(
        "--github-output",
        type=Path,
        help="Optional path to the GitHub Actions output file.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        server_json = _load_server_json(args.server_json)
        package_version = _load_pyproject_version(args.pyproject)
        metadata = _extract_release_metadata(server_json)
    except ValueError as exc:
        print(f"release metadata validation failed: {exc}", file=sys.stderr)
        return 1

    errors: list[str] = []
    if package_version != metadata.server_version:
        errors.append(
            "pyproject.toml project.version must match server.json version "
            f"({package_version!r} != {metadata.server_version!r})."
        )
    if args.expected_version and args.expected_version != metadata.server_version:
        errors.append(
            "The expected release version must match server.json version "
            f"({args.expected_version!r} != {metadata.server_version!r})."
        )

    if errors:
        for error in errors:
            print(f"release metadata validation failed: {error}", file=sys.stderr)
        return 1

    if args.github_output is not None:
        _write_github_output(args.github_output, metadata)

    print(
        "Validated release metadata for "
        f"{metadata.server_name} -> {metadata.image_identifier}",
        file=sys.stdout,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
