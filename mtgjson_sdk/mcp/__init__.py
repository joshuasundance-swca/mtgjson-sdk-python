"""Public package for the MTGJSON FastMCP server."""

from .server import (
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
