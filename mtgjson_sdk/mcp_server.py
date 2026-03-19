"""Compatibility shim for the package-local MTGJSON FastMCP server."""

from __future__ import annotations

from .mcp.server import (
    DEFAULT_HTTP_HOST,
    DEFAULT_HTTP_PATH,
    DEFAULT_HTTP_PORT,
    MCPServerSettings,
    build_arg_parser,
    create_mcp_server,
    main,
    mcp,
)

__all__ = [
    "DEFAULT_HTTP_HOST",
    "DEFAULT_HTTP_PATH",
    "DEFAULT_HTTP_PORT",
    "MCPServerSettings",
    "build_arg_parser",
    "create_mcp_server",
    "main",
    "mcp",
]
