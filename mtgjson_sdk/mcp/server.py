"""FastMCP server exposing MTGJSON SDK functionality to MCP clients."""

from __future__ import annotations

import argparse
import asyncio
import inspect
import json
import logging
import os
import re
import socket
import subprocess
import sys
import threading
import time
from collections import Counter
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as package_version
from pathlib import Path
from typing import Annotated, Any, Literal

import httpx
from pydantic import BaseModel, Field

try:
    from fastmcp import Context, FastMCP
    from fastmcp.server.lifespan import lifespan
    from mcp.types import ToolAnnotations
    from starlette.requests import Request
    from starlette.responses import JSONResponse
except ModuleNotFoundError as exc:
    _MCP_IMPORT_ERROR: ModuleNotFoundError | None = exc
    Context = Any
    FastMCP = Any
    Request = Any
    JSONResponse = Any

    def lifespan(func: Callable[..., Any]) -> Callable[..., Any]:
        return func

    @dataclass(frozen=True)
    class ToolAnnotations:
        readOnlyHint: bool | None = None
        idempotentHint: bool | None = None
        destructiveHint: bool | None = None

else:
    _MCP_IMPORT_ERROR = None

from ..client import MtgjsonSDK
from ..config import PARQUET_FILES
from ..queries.identifiers import KNOWN_ID_COLUMNS

logger = logging.getLogger("mtgjson_sdk")

DEFAULT_HTTP_HOST = "127.0.0.1"
DEFAULT_HTTP_PORT = 8000
DEFAULT_HTTP_PATH = "/mcp"
DEFAULT_DOCKER_HTTP_HOST = "0.0.0.0"
WARM_PROFILE_BASE = "base"
WARM_PROFILE_PRICES = "prices"
WARM_PROFILE_FULL = "full"
MCP_ENV_PREFIX = "MTGJSON_MCP_"
ENV_TRANSPORT = f"{MCP_ENV_PREFIX}TRANSPORT"
ENV_HOST = f"{MCP_ENV_PREFIX}HOST"
ENV_PORT = f"{MCP_ENV_PREFIX}PORT"
ENV_PATH = f"{MCP_ENV_PREFIX}PATH"
ENV_WARM_PROFILE = f"{MCP_ENV_PREFIX}WARM_PROFILE"
ENV_STATELESS_HTTP = f"{MCP_ENV_PREFIX}STATELESS_HTTP"
ENV_CACHE_DIR = f"{MCP_ENV_PREFIX}CACHE_DIR"
ENV_OFFLINE = f"{MCP_ENV_PREFIX}OFFLINE"
ENV_TIMEOUT = f"{MCP_ENV_PREFIX}TIMEOUT"
ENV_LOG_LEVEL = f"{MCP_ENV_PREFIX}LOG_LEVEL"
_TRUE_ENV_VALUES = {"1", "true", "yes", "on"}
_FALSE_ENV_VALUES = {"0", "false", "no", "off"}
_LOG_LEVELS = ("CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG")

DEFAULT_LIMIT = 25
MAX_LIMIT = 200
MAX_OFFSET = 10_000
MAX_UUID_BATCH = 200
MAX_RANDOM_CARDS = 50
MAX_SQL_ROWS = 500
MAX_BOOSTER_PACKS = 216

Limit = Annotated[
    int,
    Field(
        default=DEFAULT_LIMIT,
        ge=1,
        le=MAX_LIMIT,
        description="Maximum number of records to return.",
    ),
]
Offset = Annotated[
    int,
    Field(
        default=0,
        ge=0,
        le=MAX_OFFSET,
        description="Result offset for pagination.",
    ),
]
RandomCount = Annotated[
    int,
    Field(
        default=1,
        ge=1,
        le=MAX_RANDOM_CARDS,
        description="Number of random cards to return.",
    ),
]
SqlRowLimit = Annotated[
    int,
    Field(
        default=100,
        ge=1,
        le=MAX_SQL_ROWS,
        description="Maximum number of rows to return from raw SQL.",
    ),
]
BoosterPackCount = Annotated[
    int,
    Field(
        default=36,
        ge=1,
        le=MAX_BOOSTER_PACKS,
        description="Number of packs to simulate for a booster box.",
    ),
]

IdentifierType = Literal[
    "scryfall_id",
    "scryfall_oracle_id",
    "scryfall_illustration_id",
    "tcgplayer_product_id",
    "tcgplayer_etched_product_id",
    "mtgo_id",
    "mtgo_foil_id",
    "mtg_arena_id",
    "multiverse_id",
    "mcm_id",
    "mcm_meta_id",
    "card_kingdom_id",
    "card_kingdom_foil_id",
    "card_kingdom_etched_id",
    "cardsphere_id",
    "cardsphere_foil_id",
]
EnumCatalog = Literal["keywords", "card_types", "enum_values"]
LegalityStatus = Literal["legal", "banned", "restricted", "suspended", "not_legal"]
PriceExtreme = Literal["cheapest", "most_expensive"]
SkuLookupType = Literal["sku_id", "product_id"]

_IDENTIFIER_FIELDS: dict[IdentifierType, str] = {
    "scryfall_id": "scryfallId",
    "scryfall_oracle_id": "scryfallOracleId",
    "scryfall_illustration_id": "scryfallIllustrationId",
    "tcgplayer_product_id": "tcgplayerProductId",
    "tcgplayer_etched_product_id": "tcgplayerEtchedProductId",
    "mtgo_id": "mtgoId",
    "mtgo_foil_id": "mtgoFoilId",
    "mtg_arena_id": "mtgArenaId",
    "multiverse_id": "multiverseId",
    "mcm_id": "mcmId",
    "mcm_meta_id": "mcmMetaId",
    "card_kingdom_id": "cardKingdomId",
    "card_kingdom_foil_id": "cardKingdomFoilId",
    "card_kingdom_etched_id": "cardKingdomEtchedId",
    "cardsphere_id": "cardsphereId",
    "cardsphere_foil_id": "cardsphereFoilId",
}

_ENUM_SUBSET_PATHS: dict[str, tuple[str, ...]] = {
    "languages": ("card", "language"),
    "language": ("card", "language"),
    "rarity": ("card", "rarity"),
    "rarities": ("card", "rarity"),
    "sealed_categories": ("sealedProduct", "category"),
    "sealed_category": ("sealedProduct", "category"),
    "sealed_subtypes": ("sealedProduct", "subtype"),
    "sealed_subtype": ("sealedProduct", "subtype"),
}

_READ_ONLY_TOOL = ToolAnnotations(readOnlyHint=True, idempotentHint=True)
_READ_ONLY_NONDETERMINISTIC_TOOL = ToolAnnotations(
    readOnlyHint=True,
    idempotentHint=False,
)
_DESTRUCTIVE_TOOL = ToolAnnotations(
    destructiveHint=True,
    idempotentHint=False,
)
_RESOURCE_ANNOTATIONS = {"readOnlyHint": True, "idempotentHint": True}
_READ_ONLY_SQL_PREFIXES = ("select", "with", "show", "describe")
_FORBIDDEN_SQL = re.compile(
    r"\b("
    r"alter|attach|call|copy|create|delete|detach|drop|export|import|insert|"
    r"install|load|merge|pragma|replace|reset|set|truncate|update|vacuum"
    r")\b",
    flags=re.IGNORECASE,
)


class MissingMCPDependenciesError(RuntimeError):
    """Raised when optional FastMCP dependencies are unavailable."""


def _ensure_mcp_dependencies() -> None:
    if _MCP_IMPORT_ERROR is None:
        return
    raise MissingMCPDependenciesError(
        "MCP support requires optional dependencies. "
        'Install them with `pip install "mtgjson-sdk[mcp]"` '
        "or `uv sync --extra mcp --group dev`."
    ) from _MCP_IMPORT_ERROR


def _normalize_token(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.lower())


def _build_aliases(*values: str) -> dict[str, str]:
    aliases: dict[str, str] = {}
    for value in values:
        aliases[_normalize_token(value)] = value
    return aliases


_IDENTIFIER_TYPE_ALIASES: dict[str, IdentifierType] = {
    **{_normalize_token(key): key for key in _IDENTIFIER_FIELDS},
    **{_normalize_token(value): key for key, value in _IDENTIFIER_FIELDS.items()},
}
_SKU_LOOKUP_ALIASES: dict[str, SkuLookupType] = {
    **_build_aliases("sku_id", "product_id"),
    _normalize_token("skuId"): "sku_id",
    _normalize_token("productId"): "product_id",
}
_LEGALITY_STATUS_ALIASES: dict[str, LegalityStatus] = {
    **_build_aliases("legal", "banned", "restricted", "suspended", "not_legal"),
    _normalize_token("not legal"): "not_legal",
    _normalize_token("notLegal"): "not_legal",
}
_PRICE_EXTREME_ALIASES: dict[str, PriceExtreme] = {
    **_build_aliases("cheapest", "most_expensive"),
    _normalize_token("mostExpensive"): "most_expensive",
    _normalize_token("most expensive"): "most_expensive",
}
_ENUM_CATALOG_ALIASES: dict[str, EnumCatalog] = {
    **_build_aliases("keywords", "card_types", "enum_values"),
    _normalize_token("cardTypes"): "card_types",
    _normalize_token("card types"): "card_types",
    _normalize_token("types"): "card_types",
    _normalize_token("enumValues"): "enum_values",
    _normalize_token("enum values"): "enum_values",
    _normalize_token("values"): "enum_values",
}

_SUPPORTED_TOOL_PARAMETER_VALUES: dict[tuple[str, str], dict[str, Any]] = {
    ("find_cards_by_identifier", "id_type"): {
        "values": sorted(_IDENTIFIER_FIELDS),
        "aliases": {
            field_name: canonical
            for canonical, field_name in sorted(_IDENTIFIER_FIELDS.items())
        },
    },
    ("search_by_external_id", "id_type"): {
        "values": sorted(_IDENTIFIER_FIELDS),
        "aliases": {
            field_name: canonical
            for canonical, field_name in sorted(_IDENTIFIER_FIELDS.items())
        },
    },
    ("find_sku", "lookup_type"): {
        "values": ["product_id", "sku_id"],
        "aliases": {
            "productId": "product_id",
            "skuId": "sku_id",
        },
    },
    ("list_cards_by_format_status", "status"): {
        "values": ["legal", "banned", "restricted", "suspended", "not_legal"],
        "aliases": {
            "Banned": "banned",
            "Restricted": "restricted",
            "Suspended": "suspended",
            "not legal": "not_legal",
            "notLegal": "not_legal",
        },
    },
    ("list_price_extremes", "kind"): {
        "values": ["cheapest", "most_expensive"],
        "aliases": {
            "mostExpensive": "most_expensive",
            "most expensive": "most_expensive",
        },
    },
    ("get_enums", "catalog"): {
        "values": [
            "keywords",
            "card_types",
            "enum_values",
            *sorted(_ENUM_SUBSET_PATHS),
        ],
        "aliases": {
            "cardTypes": "card_types",
            "enumValues": "enum_values",
            "values": "enum_values",
            "languages": "enum_values.card.language",
        },
    },
}


@dataclass(slots=True, frozen=True)
class MCPServerSettings:
    """Runtime configuration for the MTGJSON MCP server."""

    cache_dir: Path | None = None
    offline: bool = False
    timeout: float = 120.0
    warm_profile: Literal["base", "prices", "full"] | None = None
    warm_views: tuple[str, ...] = ()
    stateless_http: bool = False


class ServerStatus(BaseModel):
    """Operational details about the current server instance."""

    package_version: str
    cache_dir: str
    offline: bool
    timeout_seconds: float
    registered_views: list[str]
    known_views: list[str]
    cached_parquet_views: list[str]
    warming_parquet_views: list[str]
    partial_parquet_views: list[str]


SDKFactory = Callable[[], MtgjsonSDK]


def _current_package_version() -> str:
    try:
        return package_version("mtgjson-sdk")
    except PackageNotFoundError:
        return "0.0.0"


def _normalize_http_path(path: str) -> str:
    normalized = path.strip() or DEFAULT_HTTP_PATH
    if not normalized.startswith("/"):
        normalized = f"/{normalized}"
    return normalized


def _env_text(name: str) -> str | None:
    value = os.getenv(name)
    if value is None:
        return None
    normalized = value.strip()
    return normalized or None


def _parse_env_bool(name: str, value: str) -> bool:
    normalized = value.strip().lower()
    if normalized in _TRUE_ENV_VALUES:
        return True
    if normalized in _FALSE_ENV_VALUES:
        return False
    raise ValueError(
        f"Invalid boolean value for {name}: {value!r}. "
        "Use one of: 1, 0, true, false, yes, no, on, off."
    )


def _env_bool(name: str) -> bool | None:
    value = _env_text(name)
    if value is None:
        return None
    return _parse_env_bool(name, value)


def _env_int(name: str) -> int | None:
    value = _env_text(name)
    if value is None:
        return None
    try:
        return int(value)
    except ValueError as exc:
        raise ValueError(f"Invalid integer value for {name}: {value!r}.") from exc


def _env_float(name: str) -> float | None:
    value = _env_text(name)
    if value is None:
        return None
    try:
        return float(value)
    except ValueError as exc:
        raise ValueError(f"Invalid float value for {name}: {value!r}.") from exc


def _env_choice(name: str, choices: Sequence[str]) -> str | None:
    value = _env_text(name)
    if value is None:
        return None
    aliases = {choice.lower(): choice for choice in choices}
    canonical = aliases.get(value.lower())
    if canonical is None:
        raise ValueError(
            f"Invalid value for {name}: {value!r}. Supported values: "
            f"{', '.join(choices)}."
        )
    return canonical


def _env_path(name: str) -> Path | None:
    value = _env_text(name)
    if value is None:
        return None
    return Path(value).expanduser()


def _cli_env_help() -> str:
    return (
        "Environment defaults (CLI flags take precedence):\n"
        f"  {ENV_TRANSPORT}, {ENV_HOST}, {ENV_PORT}, {ENV_PATH},\n"
        f"  {ENV_WARM_PROFILE}, {ENV_STATELESS_HTTP}, {ENV_CACHE_DIR},\n"
        f"  {ENV_OFFLINE}, {ENV_TIMEOUT}, {ENV_LOG_LEVEL}"
    )


_WARM_PROFILE_VIEWS: dict[str, tuple[str, ...]] = {
    WARM_PROFILE_BASE: (
        "cards",
        "sets",
        "tokens",
        "card_identifiers",
        "card_legalities",
    ),
    WARM_PROFILE_PRICES: (
        "cards",
        "sets",
        "tokens",
        "card_identifiers",
        "card_legalities",
        "all_prices_today",
        "all_prices",
        "tcgplayer_skus",
    ),
    WARM_PROFILE_FULL: tuple(sorted(PARQUET_FILES)),
}


def _normalize_warm_profile(
    warm_profile: Literal["base", "prices", "full"] | str | None,
) -> Literal["base", "prices", "full"] | None:
    if warm_profile is None:
        return None
    normalized = warm_profile.strip().lower()
    supported = set(_WARM_PROFILE_VIEWS)
    if normalized not in supported:
        raise ValueError(
            f"Unknown warm profile '{warm_profile}'. Supported values: "
            f"{', '.join(sorted(supported))}."
        )
    return normalized


def _resolve_required_views(settings: MCPServerSettings) -> list[str]:
    required: list[str] = []
    warm_profile = _normalize_warm_profile(settings.warm_profile)
    if warm_profile:
        required.extend(_WARM_PROFILE_VIEWS[warm_profile])
    if settings.warm_views:
        required.extend(settings.warm_views)
    return _validate_known_views(list(dict.fromkeys(required)))


def _jsonable(value: Any) -> Any:
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json")
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json")
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_jsonable(item) for item in value]
    return value


def _json_text(value: Any) -> str:
    return json.dumps(_jsonable(value), indent=2, sort_keys=True, ensure_ascii=True)


def _dataset_metadata(sdk: MtgjsonSDK | None) -> dict[str, Any]:
    if sdk is None:
        return {}

    meta = sdk.meta
    source = meta.get("data") if isinstance(meta, dict) else None
    if not isinstance(source, dict):
        source = meta if isinstance(meta, dict) else {}

    return {
        "dataset_version": source.get("version"),
        "data_freshness": source.get("date"),
    }


def _base_response(
    *,
    sdk: MtgjsonSDK | None = None,
    normalized_params: dict[str, Any] | None = None,
    warnings: Sequence[str] | None = None,
    reason: str | None = None,
    error_type: str | None = None,
    truncated: bool = False,
) -> dict[str, Any]:
    return {
        **_dataset_metadata(sdk),
        "normalized_params": _jsonable(normalized_params or {}),
        "warnings": list(warnings or []),
        "reason": reason,
        "error_type": error_type,
        "truncated": truncated,
    }


def _response(
    payload: dict[str, Any],
    *,
    sdk: MtgjsonSDK | None = None,
    normalized_params: dict[str, Any] | None = None,
    warnings: Sequence[str] | None = None,
    reason: str | None = None,
    error_type: str | None = None,
    truncated: bool = False,
) -> dict[str, Any]:
    return {
        **payload,
        **_base_response(
            sdk=sdk,
            normalized_params=normalized_params,
            warnings=warnings,
            reason=reason,
            error_type=error_type,
            truncated=truncated,
        ),
    }


def _items_result(
    items: Any,
    *,
    sdk: MtgjsonSDK | None = None,
    normalized_params: dict[str, Any] | None = None,
    warnings: Sequence[str] | None = None,
    reason: str | None = None,
    error_type: str | None = None,
    truncated: bool = False,
    **extra: Any,
) -> dict[str, Any]:
    data = _jsonable(items)
    if data is None:
        normalized_items: list[Any] = []
    elif isinstance(data, list):
        normalized_items = data
    else:
        normalized_items = [data]
    result_reason = (
        reason
        if reason is not None
        else ("no_results" if not normalized_items else None)
    )
    return _response(
        {
            "count": len(normalized_items),
            "result_count": len(normalized_items),
            "items": normalized_items,
            **extra,
        },
        sdk=sdk,
        normalized_params=normalized_params,
        warnings=warnings,
        reason=result_reason,
        error_type=error_type,
        truncated=truncated,
    )


def _item_result(
    key: str,
    value: Any,
    *,
    sdk: MtgjsonSDK | None = None,
    normalized_params: dict[str, Any] | None = None,
    warnings: Sequence[str] | None = None,
    reason: str | None = None,
    error_type: str | None = None,
    truncated: bool = False,
    **extra: Any,
) -> dict[str, Any]:
    data = _jsonable(value)
    found = data is not None
    result_reason = reason if reason is not None else (None if found else "no_results")
    return _response(
        {
            "found": found,
            "result_count": 1 if found else 0,
            key: data,
            **extra,
        },
        sdk=sdk,
        normalized_params=normalized_params,
        warnings=warnings,
        reason=result_reason,
        error_type=error_type,
        truncated=truncated,
    )


def _normalize_choice(
    value: str,
    *,
    field_name: str,
    aliases: dict[str, str],
    supported_values: Sequence[str],
) -> str:
    canonical = aliases.get(_normalize_token(value))
    if canonical is None:
        raise ValueError(
            f"Unsupported {field_name} {value!r}. Supported values: "
            f"{', '.join(sorted(supported_values))}."
        )
    return canonical


def _normalize_identifier_type(value: str) -> IdentifierType:
    canonical = _normalize_choice(
        value,
        field_name="id_type",
        aliases=_IDENTIFIER_TYPE_ALIASES,
        supported_values=sorted(_IDENTIFIER_FIELDS),
    )
    return canonical  # type: ignore[return-value]


def _normalize_sku_lookup_type(value: str) -> SkuLookupType:
    canonical = _normalize_choice(
        value,
        field_name="lookup_type",
        aliases=_SKU_LOOKUP_ALIASES,
        supported_values=["product_id", "sku_id"],
    )
    return canonical  # type: ignore[return-value]


def _normalize_legality_status(value: str) -> LegalityStatus:
    canonical = _normalize_choice(
        value,
        field_name="status",
        aliases=_LEGALITY_STATUS_ALIASES,
        supported_values=["legal", "banned", "restricted", "suspended", "not_legal"],
    )
    return canonical  # type: ignore[return-value]


def _normalize_price_extreme(value: str) -> PriceExtreme:
    canonical = _normalize_choice(
        value,
        field_name="kind",
        aliases=_PRICE_EXTREME_ALIASES,
        supported_values=["cheapest", "most_expensive"],
    )
    return canonical  # type: ignore[return-value]


def _normalize_enum_catalog(
    value: str,
) -> tuple[EnumCatalog, tuple[str, ...] | None, str]:
    subset = _ENUM_SUBSET_PATHS.get(value.strip().lower())
    if subset is not None:
        return "enum_values", subset, value.strip().lower()

    canonical = _normalize_choice(
        value,
        field_name="catalog",
        aliases=_ENUM_CATALOG_ALIASES,
        supported_values=[
            "keywords",
            "card_types",
            "enum_values",
            *sorted(_ENUM_SUBSET_PATHS),
        ],
    )
    return canonical, None, canonical


def _extract_nested_value(data: Any, path: Sequence[str]) -> Any:
    current = data
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _parameter_reference(tool_name: str, parameter_name: str) -> dict[str, Any] | None:
    return _SUPPORTED_TOOL_PARAMETER_VALUES.get((tool_name, parameter_name))


def _require_sdk(ctx: Context) -> MtgjsonSDK:
    sdk = ctx.lifespan_context.get("sdk")
    if not isinstance(sdk, MtgjsonSDK):
        raise RuntimeError("The MTGJSON SDK is not available in the server lifespan.")
    return sdk


def _require_settings(ctx: Context) -> MCPServerSettings:
    settings = ctx.lifespan_context.get("settings")
    if not isinstance(settings, MCPServerSettings):
        raise RuntimeError("Server settings are not available in the lifespan context.")
    return settings


def _required_views(ctx: Context) -> list[str]:
    required = ctx.lifespan_context.get("required_views")
    if isinstance(required, list):
        return [str(view_name) for view_name in required]
    return []


def _download_progress_snapshot(ctx: Context) -> dict[str, Any]:
    state = ctx.lifespan_context.get("download_progress_state")
    lock = ctx.lifespan_context.get("download_progress_lock")
    lock_type = type(threading.Lock())
    if not isinstance(state, dict) or not isinstance(lock, lock_type):
        return {}
    with lock:
        return json.loads(json.dumps(state))


def _view_is_ready(
    *,
    view_name: str,
    status: dict[str, Any],
    registered_views: set[str],
) -> bool:
    return bool(status.get("status") == "ready" or view_name in registered_views)


def _readiness_payload(
    sdk: MtgjsonSDK,
    *,
    warm_profile: str | None,
    required_views: Sequence[str],
    download_progress: dict[str, Any] | None = None,
) -> dict[str, Any]:
    registered = set(sdk.views)
    view_statuses = {
        view_name: sdk._cache.parquet_status(view_name) for view_name in required_views
    }
    missing_views = [
        view_name
        for view_name, status in view_statuses.items()
        if not _view_is_ready(
            view_name=view_name,
            status=status,
            registered_views=registered,
        )
    ]
    warming_views = [
        view_name
        for view_name in missing_views
        if view_statuses[view_name].get("status") in {"in_progress", "partial"}
    ]
    ready = len(missing_views) == 0
    return {
        "status": "ready" if ready else "warming",
        "ready": ready,
        "warm_profile": warm_profile,
        "required_views": list(required_views),
        "registered_views": sdk.views,
        "missing_views": missing_views,
        "warming_views": warming_views,
        "view_statuses": view_statuses,
        "download_progress": download_progress or {},
    }


async def _report_progress(
    ctx: Context,
    *,
    progress: float,
    total: float | None = None,
    message: str | None = None,
) -> None:
    reporter = getattr(ctx, "report_progress", None)
    if reporter is None:
        return
    maybe_awaitable = reporter(progress=progress, total=total, message=message)
    if inspect.isawaitable(maybe_awaitable):
        await maybe_awaitable


def _server_status(
    sdk: MtgjsonSDK,
    settings: MCPServerSettings,
    *,
    required_views: Sequence[str] = (),
    download_progress: dict[str, Any] | None = None,
) -> dict[str, Any]:
    cached: list[str] = []
    warming: list[str] = []
    partial: list[str] = []
    for view_name in sorted(PARQUET_FILES):
        status = sdk._cache.parquet_status(view_name)["status"]
        if status == "ready":
            cached.append(view_name)
        elif status == "in_progress":
            warming.append(view_name)
        elif status == "partial":
            partial.append(view_name)

    base = ServerStatus(
        package_version=_current_package_version(),
        cache_dir=str(sdk._cache.cache_dir),
        offline=settings.offline,
        timeout_seconds=settings.timeout,
        registered_views=sdk.views,
        known_views=sorted(PARQUET_FILES),
        cached_parquet_views=cached,
        warming_parquet_views=warming,
        partial_parquet_views=partial,
    ).model_dump(mode="json")
    readiness = _readiness_payload(
        sdk,
        warm_profile=settings.warm_profile,
        required_views=required_views,
        download_progress=download_progress,
    )

    sealed_unavailable = _sealed_data_unavailable_details(sdk)
    return _response(
        {
            **base,
            "warm_profile": settings.warm_profile,
            "required_views": list(required_views),
            "download_progress": download_progress or {},
            "readiness": readiness,
            "capabilities": {
                "sealed_products": {
                    "available": sealed_unavailable is None,
                    "details": sealed_unavailable,
                },
                "first_class_views": {
                    "card_foreign_data": True,
                    "card_identifiers": True,
                    "card_legalities": True,
                    "card_purchase_urls": True,
                    "card_rulings": True,
                    "set_translations": True,
                    "token_identifiers": True,
                },
            },
        },
        sdk=sdk,
    )


def _require_existing_item(value: Any, message: str) -> Any:
    if value is None:
        raise FileNotFoundError(message)
    return value


def _validate_known_views(view_names: list[str] | None) -> list[str]:
    if not view_names:
        return []
    unknown = sorted(set(view_names) - set(PARQUET_FILES))
    if unknown:
        supported = ", ".join(sorted(PARQUET_FILES))
        raise ValueError(
            f"Unknown view names: {', '.join(unknown)}. Supported values: {supported}."
        )
    return view_names


def _coerce_string_list(
    value: list[str] | str | None,
    *,
    field_name: str,
) -> list[str] | None:
    if value is None:
        return None
    if isinstance(value, list):
        return value
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a list of strings.")

    stripped = value.strip()
    if not stripped:
        return []

    try:
        parsed = json.loads(stripped)
    except json.JSONDecodeError:
        parsed = None

    if isinstance(parsed, list):
        return [str(item) for item in parsed]
    if parsed is not None:
        return [str(parsed)]
    return [part.strip() for part in stripped.split(",") if part.strip()]


def _coerce_query_params(params: list[Any] | str | None) -> list[Any] | None:
    if params is None:
        return None
    if isinstance(params, list):
        return params
    if not isinstance(params, str):
        raise ValueError("params must be a JSON array string or a list.")

    stripped = params.strip()
    if not stripped:
        return []

    parsed = json.loads(stripped)
    if isinstance(parsed, list):
        return parsed
    return [parsed]


def _view_has_column(sdk: MtgjsonSDK, view_name: str, column_name: str) -> bool:
    return sdk._conn.view_has_column(view_name, column_name)


def _sealed_data_unavailable_details(sdk: MtgjsonSDK) -> dict[str, Any] | None:
    """Return metadata explaining why sealed-product tools are unavailable."""

    if _view_has_column(sdk, "sets", "sealedProduct"):
        return None

    if "all_printings" in sdk.views:
        if _view_has_column(sdk, "all_printings", "sealedProduct"):
            return None
        return {
            "requires_view": "all_printings",
            "cache_status": sdk._cache.parquet_status("all_printings"),
            "message": (
                "The available all_printings view does not expose set-level "
                "sealed product data."
            ),
        }

    sealed_status = sdk._cache.parquet_status("all_printings")
    if sealed_status["status"] != "ready":
        if sealed_status["status"] not in {"offline", "in_progress"}:
            sealed_status = sdk._cache.prefetch_parquet("all_printings")
        return {
            "requires_view": "all_printings",
            "cache_status": sealed_status,
            "message": _warming_message(
                noun="Sealed product data",
                view_name="all_printings",
                status=sealed_status,
            ),
        }

    if _view_has_column(sdk, "all_printings", "sealedProduct"):
        return None

    return {
        "requires_view": "all_printings",
        "cache_status": sealed_status,
        "message": (
            "The cached all_printings view does not expose set-level sealed "
            "product data."
        ),
    }


def _view_data_unavailable_details(
    sdk: MtgjsonSDK,
    *,
    view_name: str,
    noun: str,
) -> dict[str, Any] | None:
    sdk._conn.ensure_views(view_name)
    if view_name in sdk.views:
        return None

    status = sdk._cache.parquet_status(view_name)
    if status["status"] not in {"offline", "in_progress"}:
        status = sdk._cache.prefetch_parquet(view_name)
    return {
        "requires_view": view_name,
        "cache_status": status,
        "message": _warming_message(noun=noun, view_name=view_name, status=status),
    }


def _card_suggestions(
    sdk: MtgjsonSDK,
    *,
    query: str,
    set_code: str | None = None,
    limit: int = 5,
) -> list[dict[str, Any]]:
    if not query:
        return []

    suggestions = sdk.cards.search(
        fuzzy_name=query,
        set_code=set_code,
        limit=limit,
        as_dict=True,
    )
    seen: set[str] = set()
    unique: list[dict[str, Any]] = []
    for row in suggestions:
        uuid = str(row.get("uuid", ""))
        if uuid and uuid not in seen:
            seen.add(uuid)
            unique.append(row)
    return unique


def _latest_price_snapshot_date(sdk: MtgjsonSDK, uuid: str) -> str | None:
    sdk._conn.ensure_views("all_prices_today")
    if "all_prices_today" not in sdk.views:
        return None
    latest_date = sdk._conn.execute_scalar(
        "SELECT MAX(date) FROM all_prices_today WHERE uuid = $1",
        [uuid],
    )
    if latest_date is None:
        return None
    if hasattr(latest_date, "isoformat"):
        return latest_date.isoformat()
    return str(latest_date)


def _price_history_uses_latest_snapshot(
    latest_date: str | None,
    *,
    date_from: str | None,
    date_to: str | None,
) -> bool:
    if latest_date is None or date_from is None:
        return False
    return date_from == latest_date and (date_to is None or date_to >= latest_date)


def _warming_message(
    *,
    noun: str,
    view_name: str,
    status: dict[str, Any],
) -> str:
    state = status.get("status")
    if state == "offline":
        return (
            f"{noun} is not cached locally yet, and offline mode is enabled. "
            f"Cache the {view_name} view first and retry."
        )
    if state == "error" and status.get("error"):
        return (
            f"{noun} could not be loaded because warming the {view_name} view failed: "
            f"{status['error']}"
        )
    if state in {"in_progress", "partial"}:
        return (
            f"{noun} is warming the {view_name} view in the background. Retry shortly."
        )
    return (
        f"{noun} requires the {view_name} view, which is not cached yet. "
        "The server started warming it in the background; retry shortly."
    )


def _price_snapshot_status(sdk: MtgjsonSDK) -> dict[str, Any] | None:
    sdk._conn.ensure_views("all_prices_today")
    if "all_prices_today" in sdk.views:
        return None

    status = sdk._cache.parquet_status("all_prices_today")
    if status["status"] not in {"offline", "in_progress"}:
        status = sdk._cache.prefetch_parquet("all_prices_today")
    return status


def _price_data_unavailable(
    status: dict[str, Any],
    *,
    sdk: MtgjsonSDK | None = None,
    noun: str,
    item_key: str | None = None,
    as_items: bool = False,
    normalized_params: dict[str, Any] | None = None,
    **extra: Any,
) -> dict[str, Any]:
    payload = {
        "price_data_available": False,
        "requires_view": "all_prices_today",
        "cache_status": status,
        "message": _warming_message(
            noun=noun,
            view_name="all_prices_today",
            status=status,
        ),
        **extra,
    }
    if as_items:
        return _items_result(
            [],
            sdk=sdk,
            normalized_params=normalized_params,
            reason="missing_view",
            error_type="missing_view",
            **payload,
        )
    if item_key is not None:
        return _item_result(
            item_key,
            None,
            sdk=sdk,
            normalized_params=normalized_params,
            reason="missing_view",
            error_type="missing_view",
            **payload,
        )
    return _response(
        payload,
        sdk=sdk,
        normalized_params=normalized_params,
        reason="missing_view",
        error_type="missing_view",
    )


def _price_trend_rows(
    sdk: MtgjsonSDK,
    *,
    uuid: str,
    provider: str | None = None,
    finish: str | None = None,
    price_type: str = "retail",
) -> tuple[list[dict[str, Any]], str]:
    history_status = sdk._cache.parquet_status("all_prices")
    if "all_prices" in sdk.views or history_status["status"] == "ready":
        return (
            sdk.prices.history(
                uuid,
                provider=provider,
                finish=finish,
                price_type=price_type,
                as_dict=True,
            ),
            "all_prices",
        )

    return (
        sdk.prices.today(
            uuid,
            provider=provider,
            finish=finish,
            price_type=price_type,
            as_dict=True,
        ),
        "all_prices_today",
    )


def _price_trend_result(
    sdk: MtgjsonSDK,
    *,
    uuid: str,
    provider: str | None = None,
    finish: str | None = None,
    price_type: str = "retail",
) -> tuple[dict[str, Any] | None, str]:
    rows, source = _price_trend_rows(
        sdk,
        uuid=uuid,
        provider=provider,
        finish=finish,
        price_type=price_type,
    )
    if not rows:
        return None, source

    prices = [float(row["price"]) for row in rows if row.get("price") is not None]
    if not prices:
        return None, source

    dates = [str(row["date"]) for row in rows if row.get("date") is not None]
    return (
        {
            "min_price": min(prices),
            "max_price": max(prices),
            "avg_price": round(sum(prices) / len(prices), 2),
            "first_date": min(dates) if dates else None,
            "last_date": max(dates) if dates else None,
            "data_points": len(prices),
        },
        source,
    )


def _resolve_card_candidates(
    sdk: MtgjsonSDK,
    *,
    name: str,
    set_code: str | None = None,
    number: str | None = None,
    language: str | None = None,
    limit: int = 10,
) -> list[dict[str, Any]]:
    rows = sdk.cards.get_by_name(name, set_code=set_code, as_dict=True)
    if language:
        rows = [row for row in rows if row.get("language") == language]
    if number:
        rows = [row for row in rows if str(row.get("number")) == str(number)]
    if rows:
        return rows

    return _card_suggestions(sdk, query=name, set_code=set_code, limit=min(limit, 5))


def _summarize_booster_box(box: list[list[dict[str, Any]]]) -> dict[str, Any]:
    all_cards = [card for pack in box for card in pack]
    rarity_counts = Counter(str(card.get("rarity", "unknown")) for card in all_cards)
    highlights = [
        {
            "uuid": card.get("uuid"),
            "name": card.get("name"),
            "setCode": card.get("setCode"),
            "number": card.get("number"),
            "rarity": card.get("rarity"),
            "isFoil": card.get("isFoil"),
        }
        for card in all_cards
        if card.get("rarity") in {"mythic", "rare", "special"} or card.get("isFoil")
    ]
    return {
        "total_cards": len(all_cards),
        "rarity_breakdown": dict(rarity_counts),
        "highlights": highlights[:20],
        "pack_summaries": [
            {
                "pack_number": index + 1,
                "card_count": len(pack),
                "highlights": [
                    {
                        "uuid": card.get("uuid"),
                        "name": card.get("name"),
                        "rarity": card.get("rarity"),
                        "isFoil": card.get("isFoil"),
                    }
                    for card in pack
                    if card.get("rarity") in {"mythic", "rare", "special"}
                    or card.get("isFoil")
                ],
            }
            for index, pack in enumerate(box)
        ],
    }


def _validate_read_only_sql(query: str) -> str:
    statement = query.strip()
    if not statement:
        raise ValueError("SQL query must not be empty.")
    statement = statement.rstrip(";").strip()
    if ";" in statement:
        raise ValueError("Only a single SQL statement is allowed.")

    first_token = statement.split(None, 1)[0].lower() if statement else ""
    if first_token not in _READ_ONLY_SQL_PREFIXES:
        raise ValueError(
            "Only read-only SELECT, WITH, SHOW, and DESCRIBE queries are allowed."
        )
    if _FORBIDDEN_SQL.search(statement):
        raise ValueError(
            "The SQL query contains a forbidden mutating or administrative keyword."
        )
    return statement


def _limited_sql_query(statement: str, max_rows: int) -> str:
    first_token = statement.split(None, 1)[0].lower()
    if first_token in {"select", "with"}:
        return f"SELECT * FROM ({statement}) AS mtgjson_mcp_query LIMIT {max_rows + 1}"
    return statement


def _get_enum_catalog(sdk: MtgjsonSDK, catalog: EnumCatalog) -> dict[str, Any]:
    if catalog == "keywords":
        return _jsonable(sdk.enums.keywords())
    if catalog == "card_types":
        return _jsonable(sdk.enums.card_types())
    return _jsonable(sdk.enums.enum_values())


def create_mcp_server(
    settings: MCPServerSettings | None = None,
    *,
    sdk_factory: SDKFactory | None = None,
) -> FastMCP:
    """Create a FastMCP server for the MTGJSON SDK."""

    _ensure_mcp_dependencies()

    settings = settings or MCPServerSettings()
    cache_dir = settings.cache_dir.expanduser() if settings.cache_dir else None
    warm_profile = _normalize_warm_profile(settings.warm_profile)
    settings = MCPServerSettings(
        cache_dir=cache_dir,
        offline=settings.offline,
        timeout=settings.timeout,
        warm_profile=warm_profile,
        warm_views=settings.warm_views,
        stateless_http=settings.stateless_http,
    )
    required_views = _resolve_required_views(settings)
    progress_lock = threading.Lock()
    download_progress_state: dict[str, dict[str, Any]] = {}
    runtime_state: dict[str, Any] = {"sdk": None}

    def _on_progress(filename: str, downloaded: int, total: int | None) -> None:
        percent = (downloaded / total * 100.0) if total else None
        with progress_lock:
            download_progress_state[filename] = {
                "downloaded_bytes": downloaded,
                "total_bytes": total,
                "percent": percent,
            }

    def _attach_progress_callback(sdk: MtgjsonSDK) -> None:
        existing_on_progress = sdk._cache._on_progress
        if existing_on_progress is _on_progress:
            return

        def _combined_progress(
            filename: str,
            downloaded: int,
            total: int | None,
        ) -> None:
            _on_progress(filename, downloaded, total)
            if callable(existing_on_progress):
                existing_on_progress(filename, downloaded, total)

        sdk._cache._on_progress = _combined_progress

    @lifespan
    async def app_lifespan(_: FastMCP) -> dict[str, Any]:
        sdk = (
            sdk_factory()
            if sdk_factory is not None
            else MtgjsonSDK(
                cache_dir=settings.cache_dir,
                offline=settings.offline,
                timeout=settings.timeout,
                on_progress=_on_progress,
            )
        )
        _attach_progress_callback(sdk)
        runtime_state["sdk"] = sdk
        for view_name in required_views:
            try:
                sdk._cache.prefetch_parquet(view_name)
            except BaseException as exc:  # pragma: no cover - defensive
                logger.warning("Failed to prefetch warm view %s: %s", view_name, exc)
        try:
            yield {
                "sdk": sdk,
                "settings": settings,
                "required_views": required_views,
                "download_progress_state": download_progress_state,
                "download_progress_lock": progress_lock,
            }
        finally:
            runtime_state["sdk"] = None
            sdk.close()

    mcp = FastMCP(
        name="mtgjson-sdk",
        instructions=(
            "This server exposes the local MTGJSON cache through lookup resources, "
            "search tools, booster simulation, price analytics, and a guarded "
            "read-only SQL escape hatch. Prefer the domain-specific tools first. "
            "Use mtgjson:// resources when you already have a stable UUID or set "
            "code, and use execute_read_only_sql only for narrow SELECT, WITH, "
            "SHOW, or DESCRIBE queries."
        ),
        version=_current_package_version(),
        website_url="https://mtgjson.com",
        strict_input_validation=True,
        list_page_size=50,
        lifespan=app_lifespan,
    )

    @mcp.custom_route("/health", methods=["GET"])
    async def health_check(_: Request) -> JSONResponse:
        return JSONResponse(
            {
                "status": "ok",
                "server": "mtgjson-sdk",
                "version": _current_package_version(),
            }
        )

    @mcp.custom_route("/ready", methods=["GET"])
    async def readiness_check(_: Request) -> JSONResponse:
        sdk = runtime_state.get("sdk")
        if not isinstance(sdk, MtgjsonSDK):
            return JSONResponse(
                {
                    "status": "warming",
                    "ready": False,
                    "warm_profile": settings.warm_profile,
                    "required_views": required_views,
                    "message": "Server startup is still in progress.",
                },
                status_code=503,
            )

        with progress_lock:
            download_progress = json.loads(json.dumps(download_progress_state))
        payload = _readiness_payload(
            sdk,
            warm_profile=settings.warm_profile,
            required_views=required_views,
            download_progress=download_progress,
        )
        return JSONResponse(payload, status_code=200 if payload["ready"] else 503)

    @mcp.resource(
        "mtgjson://server/config",
        mime_type="application/json",
        annotations=_RESOURCE_ANNOTATIONS,
    )
    def server_config_resource(ctx: Context) -> str:
        """Server runtime configuration and currently registered views."""

        sdk = _require_sdk(ctx)
        return _json_text(
            _server_status(
                sdk,
                _require_settings(ctx),
                required_views=_required_views(ctx),
                download_progress=_download_progress_snapshot(ctx),
            )
        )

    @mcp.resource(
        "mtgjson://meta",
        mime_type="application/json",
        annotations=_RESOURCE_ANNOTATIONS,
    )
    def meta_resource(ctx: Context) -> str:
        """Cached MTGJSON metadata from Meta.json."""

        return _json_text(_require_sdk(ctx).meta)

    @mcp.resource(
        "mtgjson://views",
        mime_type="application/json",
        annotations=_RESOURCE_ANNOTATIONS,
    )
    def views_resource(ctx: Context) -> str:
        """Currently registered DuckDB views in this server process."""

        return _json_text(_require_sdk(ctx).views)

    @mcp.resource(
        "mtgjson://cards/{uuid}",
        mime_type="application/json",
        annotations=_RESOURCE_ANNOTATIONS,
    )
    def card_resource(uuid: str, ctx: Context) -> str:
        """Card printing lookup by MTGJSON UUID."""

        card = _require_sdk(ctx).cards.get_by_uuid(uuid, as_dict=True)
        return _json_text(
            _require_existing_item(card, f"No card found for UUID {uuid}.")
        )

    @mcp.resource(
        "mtgjson://tokens/{uuid}",
        mime_type="application/json",
        annotations=_RESOURCE_ANNOTATIONS,
    )
    def token_resource(uuid: str, ctx: Context) -> str:
        """Token lookup by MTGJSON UUID."""

        token = _require_sdk(ctx).tokens.get_by_uuid(uuid, as_dict=True)
        return _json_text(
            _require_existing_item(token, f"No token found for UUID {uuid}.")
        )

    @mcp.resource(
        "mtgjson://sets/{code}",
        mime_type="application/json",
        annotations=_RESOURCE_ANNOTATIONS,
    )
    def set_resource(code: str, ctx: Context) -> str:
        """Set lookup by set code."""

        set_data = _require_sdk(ctx).sets.get(code, as_dict=True)
        return _json_text(
            _require_existing_item(set_data, f"No set found for code {code}.")
        )

    @mcp.resource(
        "mtgjson://identifiers/{uuid}",
        mime_type="application/json",
        annotations=_RESOURCE_ANNOTATIONS,
    )
    def identifiers_resource(uuid: str, ctx: Context) -> str:
        """All external identifiers known for a card UUID."""

        sdk = _require_sdk(ctx)
        data = sdk.identifiers.get_identifiers(uuid, as_dict=True)
        return _json_text(
            _item_result("identifiers", data, sdk=sdk, normalized_params={"uuid": uuid})
        )

    @mcp.resource(
        "mtgjson://prices/{uuid}",
        mime_type="application/json",
        annotations=_RESOURCE_ANNOTATIONS,
    )
    def prices_resource(uuid: str, ctx: Context) -> str:
        """Nested price data for a card UUID."""

        sdk = _require_sdk(ctx)
        price_status = _price_snapshot_status(sdk)
        if price_status is not None:
            return _json_text(
                _price_data_unavailable(
                    price_status,
                    sdk=sdk,
                    noun="Price data",
                    item_key="prices",
                    normalized_params={"uuid": uuid},
                )
            )

        data = sdk.prices.get(uuid)
        return _json_text(
            _item_result("prices", data, sdk=sdk, normalized_params={"uuid": uuid})
        )

    @mcp.resource(
        "mtgjson://skus/{uuid}",
        mime_type="application/json",
        annotations=_RESOURCE_ANNOTATIONS,
    )
    def sku_resource(uuid: str, ctx: Context) -> str:
        """TCGPlayer SKU rows for a card UUID."""

        sdk = _require_sdk(ctx)
        data = sdk.skus.get(uuid, as_dict=True)
        return _json_text(
            _items_result(data, sdk=sdk, normalized_params={"uuid": uuid})
        )

    @mcp.resource(
        "mtgjson://sealed/{uuid}",
        mime_type="application/json",
        annotations=_RESOURCE_ANNOTATIONS,
    )
    def sealed_resource(uuid: str, ctx: Context) -> str:
        """Sealed product lookup by UUID."""

        data = _require_sdk(ctx).sealed.get(uuid)
        return _json_text(
            _require_existing_item(data, f"No sealed product found for UUID {uuid}.")
        )

    @mcp.resource(
        "mtgjson://enums/{catalog}",
        mime_type="application/json",
        annotations=_RESOURCE_ANNOTATIONS,
    )
    def enum_resource(catalog: str, ctx: Context) -> str:
        """Static MTGJSON enum catalogs."""

        sdk = _require_sdk(ctx)
        canonical_catalog, subset_path, requested_catalog = _normalize_enum_catalog(
            catalog
        )
        data = _get_enum_catalog(sdk, canonical_catalog)
        if subset_path is not None:
            data = _extract_nested_value(data, subset_path)
        return _json_text(
            _response(
                {
                    "catalog": requested_catalog,
                    "resolved_catalog": canonical_catalog,
                    "result_count": 1,
                    "data": _jsonable(data),
                },
                sdk=sdk,
                normalized_params={"catalog": requested_catalog},
            )
        )

    @mcp.resource(
        "mtgjson://booster/{set_code}/{booster_type}/sheets/{sheet_name}",
        mime_type="application/json",
        annotations=_RESOURCE_ANNOTATIONS,
    )
    def booster_sheet_resource(
        set_code: str,
        booster_type: str,
        sheet_name: str,
        ctx: Context,
    ) -> str:
        """Booster sheet contents by set, booster type, and sheet name."""

        data = _require_sdk(ctx).booster.sheet_contents(
            set_code, booster_type, sheet_name
        )
        return _json_text(
            _require_existing_item(
                data,
                (
                    "No booster sheet data found for "
                    f"{set_code}/{booster_type}/{sheet_name}."
                ),
            )
        )

    @mcp.tool(annotations=_READ_ONLY_TOOL, tags={"cards", "search"})
    def search_cards(
        ctx: Context,
        name: str | None = None,
        fuzzy_name: str | None = None,
        localized_name: str | None = None,
        set_code: str | None = None,
        colors: list[str] | None = None,
        color_identity: list[str] | None = None,
        types: str | None = None,
        rarity: str | None = None,
        legal_in: str | None = None,
        mana_value: float | None = None,
        mana_value_lte: float | None = None,
        mana_value_gte: float | None = None,
        text: str | None = None,
        text_regex: str | None = None,
        power: str | None = None,
        toughness: str | None = None,
        artist: str | None = None,
        keyword: str | None = None,
        is_promo: bool | None = None,
        availability: str | None = None,
        language: str | None = None,
        layout: str | None = None,
        set_type: str | None = None,
        limit: Limit = DEFAULT_LIMIT,
        offset: Offset = 0,
    ) -> dict[str, Any]:
        """Search cards using the SDK's full composable filter set."""

        sdk = _require_sdk(ctx)
        normalized_params = {
            "name": name,
            "fuzzy_name": fuzzy_name,
            "localized_name": localized_name,
            "set_code": set_code.upper() if set_code else None,
            "colors": colors,
            "color_identity": color_identity,
            "types": types,
            "rarity": rarity,
            "legal_in": legal_in,
            "mana_value": mana_value,
            "mana_value_lte": mana_value_lte,
            "mana_value_gte": mana_value_gte,
            "text": text,
            "text_regex": text_regex,
            "power": power,
            "toughness": toughness,
            "artist": artist,
            "keyword": keyword,
            "is_promo": is_promo,
            "availability": availability,
            "language": language,
            "layout": layout,
            "set_type": set_type,
            "limit": limit,
            "offset": offset,
        }
        rows = sdk.cards.search(
            name=name,
            fuzzy_name=fuzzy_name,
            localized_name=localized_name,
            set_code=set_code,
            colors=colors,
            color_identity=color_identity,
            types=types,
            rarity=rarity,
            legal_in=legal_in,
            mana_value=mana_value,
            mana_value_lte=mana_value_lte,
            mana_value_gte=mana_value_gte,
            text=text,
            text_regex=text_regex,
            power=power,
            toughness=toughness,
            artist=artist,
            keyword=keyword,
            is_promo=is_promo,
            availability=availability,
            language=language,
            layout=layout,
            set_type=set_type,
            limit=limit,
            offset=offset,
            as_dict=True,
        )
        suggestions: list[dict[str, Any]] = []
        if not rows and (name or fuzzy_name):
            query = fuzzy_name or name or ""
            suggestions = _card_suggestions(
                sdk,
                query=query,
                set_code=set_code,
                limit=min(limit, 5),
            )
        return _items_result(
            rows,
            sdk=sdk,
            normalized_params=normalized_params,
            suggestions=suggestions,
            warnings=(
                ["No exact matches found; suggestions are fuzzy matches."]
                if suggestions and not rows
                else None
            ),
        )

    @mcp.tool(annotations=_READ_ONLY_TOOL, tags={"cards", "lookup"})
    def get_cards_by_uuids(ctx: Context, uuids: list[str]) -> dict[str, Any]:
        """Fetch multiple card printings by MTGJSON UUID."""

        if len(uuids) > MAX_UUID_BATCH:
            raise ValueError(
                f"At most {MAX_UUID_BATCH} UUIDs can be requested at once."
            )
        sdk = _require_sdk(ctx)
        rows = sdk.cards.get_by_uuids(uuids, as_dict=True)
        return _items_result(
            rows,
            sdk=sdk,
            normalized_params={"uuids": uuids},
        )

    @mcp.tool(annotations=_READ_ONLY_TOOL, tags={"cards", "lookup"})
    def get_atomic_card(ctx: Context, name: str) -> dict[str, Any]:
        """Fetch atomic/oracle-style card data by exact card or face name."""

        sdk = _require_sdk(ctx)
        rows = sdk.cards.get_atomic(name, as_dict=True)
        return _items_result(
            rows,
            sdk=sdk,
            normalized_params={"name": name},
        )

    @mcp.tool(annotations=_READ_ONLY_NONDETERMINISTIC_TOOL, tags={"cards", "utility"})
    def sample_random_cards(
        ctx: Context,
        count: RandomCount = 1,
    ) -> dict[str, Any]:
        """Return one or more random card printings from the dataset."""

        sdk = _require_sdk(ctx)
        rows = sdk.cards.random(count, as_dict=True)
        return _items_result(rows, sdk=sdk, normalized_params={"count": count})

    @mcp.tool(annotations=_READ_ONLY_TOOL, tags={"tokens", "search"})
    def search_tokens(
        ctx: Context,
        name: str | None = None,
        set_code: str | None = None,
        colors: list[str] | None = None,
        types: str | None = None,
        artist: str | None = None,
        limit: Limit = DEFAULT_LIMIT,
        offset: Offset = 0,
    ) -> dict[str, Any]:
        """Search token cards by name, set, color, type, or artist."""

        sdk = _require_sdk(ctx)
        rows = sdk.tokens.search(
            name=name,
            set_code=set_code,
            colors=colors,
            types=types,
            artist=artist,
            limit=limit,
            offset=offset,
            as_dict=True,
        )
        return _items_result(
            rows,
            sdk=sdk,
            normalized_params={
                "name": name,
                "set_code": set_code.upper() if set_code else None,
                "colors": colors,
                "types": types,
                "artist": artist,
                "limit": limit,
                "offset": offset,
            },
        )

    @mcp.tool(annotations=_READ_ONLY_TOOL, tags={"sets", "lookup"})
    def get_set(ctx: Context, code: str) -> dict[str, Any]:
        """Fetch set metadata by set code."""

        sdk = _require_sdk(ctx)
        canonical_code = code.upper()
        set_data = sdk.sets.get(canonical_code, as_dict=True)
        return _item_result(
            "set",
            set_data,
            sdk=sdk,
            normalized_params={"code": canonical_code},
        )

    @mcp.tool(annotations=_READ_ONLY_TOOL, tags={"sets", "search"})
    def search_sets(
        ctx: Context,
        name: str | None = None,
        set_type: str | None = None,
        block: str | None = None,
        release_year: int | None = None,
        limit: Limit = DEFAULT_LIMIT,
        offset: Offset = 0,
    ) -> dict[str, Any]:
        """Search sets by name, type, block, or release year."""

        sdk = _require_sdk(ctx)
        rows = sdk.sets.search(
            name=name,
            set_type=set_type,
            block=block,
            release_year=release_year,
            limit=limit + offset,
            as_dict=True,
        )
        return _items_result(
            rows[offset : offset + limit],
            sdk=sdk,
            normalized_params={
                "name": name,
                "set_type": set_type,
                "block": block,
                "release_year": release_year,
                "limit": limit,
                "offset": offset,
            },
            total_rows=len(rows),
        )

    @mcp.tool(annotations=_READ_ONLY_TOOL, tags={"sets", "prices"})
    def get_set_financial_summary(
        ctx: Context,
        set_code: str,
        provider: str = "tcgplayer",
        currency: str = "USD",
        finish: str = "normal",
        price_type: str = "retail",
    ) -> dict[str, Any]:
        """Aggregate the latest set-level price statistics for a set."""

        sdk = _require_sdk(ctx)
        canonical_code = set_code.upper()
        summary = sdk.sets.get_financial_summary(
            canonical_code,
            provider=provider,
            currency=currency,
            finish=finish,
            price_type=price_type,
        )
        return _item_result(
            "summary",
            summary,
            sdk=sdk,
            normalized_params={
                "set_code": canonical_code,
                "provider": provider,
                "currency": currency,
                "finish": finish,
                "price_type": price_type,
            },
        )

    @mcp.tool(annotations=_READ_ONLY_TOOL, tags={"identifiers", "lookup"})
    def find_cards_by_identifier(
        ctx: Context,
        id_type: str,
        value: str,
    ) -> dict[str, Any]:
        """Find cards using a supported external identifier type."""

        sdk = _require_sdk(ctx)
        canonical_id_type = _normalize_identifier_type(id_type)
        rows = sdk.identifiers.find_by(
            _IDENTIFIER_FIELDS[canonical_id_type],
            value,
            as_dict=True,
        )
        return _items_result(
            rows,
            sdk=sdk,
            normalized_params={"id_type": canonical_id_type, "value": value},
            id_type=canonical_id_type,
            value=value,
        )

    @mcp.tool(annotations=_READ_ONLY_TOOL, tags={"identifiers", "lookup"})
    def get_card_identifiers(ctx: Context, uuid: str) -> dict[str, Any]:
        """Return every external identifier known for a card UUID."""

        sdk = _require_sdk(ctx)
        identifiers = sdk.identifiers.get_identifiers(uuid, as_dict=True)
        return _item_result(
            "identifiers",
            identifiers,
            sdk=sdk,
            normalized_params={"uuid": uuid},
        )

    @mcp.tool(annotations=_READ_ONLY_TOOL, tags={"identifiers", "lookup"})
    def search_by_external_id(
        ctx: Context,
        value: str,
        id_type: str | None = None,
        limit: Limit = DEFAULT_LIMIT,
    ) -> dict[str, Any]:
        """Search cards by external identifier, with optional auto-detection."""

        sdk = _require_sdk(ctx)
        normalized_params = {"value": value, "id_type": id_type, "limit": limit}
        if id_type:
            canonical_id_type = _normalize_identifier_type(id_type)
            rows = sdk.identifiers.find_by(
                _IDENTIFIER_FIELDS[canonical_id_type],
                value,
                as_dict=True,
            )
            items = [
                {**row, "matched_fields": [canonical_id_type]} for row in rows[:limit]
            ]
            return _items_result(
                items,
                sdk=sdk,
                normalized_params={
                    **normalized_params,
                    "id_type": canonical_id_type,
                },
            )

        sdk._conn.ensure_views("card_identifiers")
        condition_sql: list[str] = []
        params: list[Any] = []
        for column in sorted(KNOWN_ID_COLUMNS):
            params.append(value)
            condition_sql.append(f"CAST({column} AS VARCHAR) = ${len(params)}")
        identifier_rows = sdk._conn.execute(
            "SELECT * FROM card_identifiers "
            f"WHERE {' OR '.join(condition_sql)} "
            f"LIMIT {limit}",
            params,
        )
        cards = {
            row["uuid"]: row
            for row in sdk.cards.get_by_uuids(
                [row["uuid"] for row in identifier_rows],
                as_dict=True,
            )
        }
        items = []
        for row in identifier_rows:
            matched_fields = [
                canonical
                for canonical, field in _IDENTIFIER_FIELDS.items()
                if str(row.get(field)) == value
            ]
            items.append(
                {**cards.get(row["uuid"], {}), "matched_fields": matched_fields}
            )
        return _items_result(items, sdk=sdk, normalized_params=normalized_params)

    @mcp.tool(annotations=_READ_ONLY_TOOL, tags={"cards", "lookup"})
    def list_printings(
        ctx: Context,
        name: str,
        set_code: str | None = None,
    ) -> dict[str, Any]:
        """List all printings for a named card, optionally narrowed to one set."""

        sdk = _require_sdk(ctx)
        canonical_set_code = set_code.upper() if set_code else None
        rows = sdk.cards.get_printings(name, as_dict=True)
        if canonical_set_code:
            rows = [row for row in rows if row.get("setCode") == canonical_set_code]
        return _items_result(
            rows,
            sdk=sdk,
            normalized_params={"name": name, "set_code": canonical_set_code},
        )

    @mcp.tool(annotations=_READ_ONLY_TOOL, tags={"cards", "lookup"})
    def resolve_card(
        ctx: Context,
        name: str,
        set_code: str | None = None,
        number: str | None = None,
        language: str | None = None,
    ) -> dict[str, Any]:
        """Resolve a card name to one specific printing, or return candidates."""

        sdk = _require_sdk(ctx)
        canonical_set_code = set_code.upper() if set_code else None
        exact_rows = sdk.cards.get_by_name(
            name,
            set_code=canonical_set_code,
            as_dict=True,
        )
        if language:
            exact_rows = [row for row in exact_rows if row.get("language") == language]
        if number:
            exact_rows = [
                row for row in exact_rows if str(row.get("number")) == str(number)
            ]

        normalized_params = {
            "name": name,
            "set_code": canonical_set_code,
            "number": number,
            "language": language,
        }
        if len(exact_rows) == 1:
            return _item_result(
                "card",
                exact_rows[0],
                sdk=sdk,
                normalized_params=normalized_params,
                resolved_by="exact_name",
            )
        if len(exact_rows) > 1:
            return _item_result(
                "card",
                None,
                sdk=sdk,
                normalized_params=normalized_params,
                reason="ambiguous",
                error_type="ambiguous",
                candidates=exact_rows[:10],
                warnings=[
                    "Multiple printings matched; provide set_code or number "
                    "to disambiguate."
                ],
            )

        suggestions = _card_suggestions(
            sdk,
            query=name,
            set_code=canonical_set_code,
            limit=5,
        )
        if len(suggestions) == 1:
            return _item_result(
                "card",
                suggestions[0],
                sdk=sdk,
                normalized_params=normalized_params,
                warnings=["Resolved via fuzzy card match."],
                resolved_by="fuzzy_name",
            )
        return _item_result(
            "card",
            None,
            sdk=sdk,
            normalized_params=normalized_params,
            suggestions=suggestions,
            warnings=(
                ["No exact printing matched; suggestions are fuzzy matches."]
                if suggestions
                else None
            ),
        )

    @mcp.tool(annotations=_READ_ONLY_TOOL, tags={"cards", "lookup", "prices"})
    def get_card_bundle(
        ctx: Context,
        uuid: str | None = None,
        name: str | None = None,
        set_code: str | None = None,
        number: str | None = None,
        language: str | None = None,
    ) -> dict[str, Any]:
        """Fetch a card plus identifiers, legalities, and current market data."""

        sdk = _require_sdk(ctx)
        normalized_params = {
            "uuid": uuid,
            "name": name,
            "set_code": set_code.upper() if set_code else None,
            "number": number,
            "language": language,
        }
        card: dict[str, Any] | None = None
        candidates: list[dict[str, Any]] = []
        if uuid:
            card = sdk.cards.get_by_uuid(uuid, as_dict=True)
        elif name:
            exact_rows = sdk.cards.get_by_name(
                name,
                set_code=set_code.upper() if set_code else None,
                as_dict=True,
            )
            if language:
                exact_rows = [
                    row for row in exact_rows if row.get("language") == language
                ]
            if number:
                exact_rows = [
                    row for row in exact_rows if str(row.get("number")) == str(number)
                ]
            if len(exact_rows) == 1:
                card = exact_rows[0]
            elif len(exact_rows) > 1:
                candidates = exact_rows[:10]
            else:
                candidates = _card_suggestions(
                    sdk,
                    query=name,
                    set_code=set_code.upper() if set_code else None,
                    limit=5,
                )
                if len(candidates) == 1:
                    card = candidates[0]
        else:
            raise ValueError("Provide uuid or name.")

        if card is None:
            return _item_result(
                "bundle",
                None,
                sdk=sdk,
                normalized_params=normalized_params,
                reason="ambiguous" if len(candidates) > 1 else "no_results",
                error_type="ambiguous" if len(candidates) > 1 else None,
                candidates=candidates,
                warnings=(
                    [
                        "Multiple cards matched; narrow the request with "
                        "set_code or number."
                    ]
                    if len(candidates) > 1 and name
                    else None
                ),
            )

        price_status = _price_snapshot_status(sdk)
        if price_status is None:
            current_prices = sdk.prices.today(card["uuid"], as_dict=True)
            trend, trend_source = _price_trend_result(sdk, uuid=card["uuid"])
            price_info: dict[str, Any] = {
                "price_data_available": True,
                "current_prices": current_prices,
                "trend": trend,
                "trend_source": trend_source,
            }
        else:
            price_info = {
                "price_data_available": False,
                "requires_view": "all_prices_today",
                "cache_status": price_status,
                "message": _warming_message(
                    noun="Price data",
                    view_name="all_prices_today",
                    status=price_status,
                ),
            }

        bundle = {
            "card": card,
            "identifiers": sdk.identifiers.get_identifiers(card["uuid"], as_dict=True),
            "legalities": sdk.legalities.formats_for_card(card["uuid"]),
            **price_info,
        }
        return _item_result(
            "bundle",
            bundle,
            sdk=sdk,
            normalized_params=normalized_params,
        )

    @mcp.tool(annotations=_READ_ONLY_TOOL, tags={"prices", "lookup"})
    def get_card_market_snapshot(
        ctx: Context,
        uuid: str | None = None,
        name: str | None = None,
        set_code: str | None = None,
    ) -> dict[str, Any]:
        """Resolve a card and return its current price rows plus trend summary."""

        sdk = _require_sdk(ctx)
        normalized_params = {
            "uuid": uuid,
            "name": name,
            "set_code": set_code.upper() if set_code else None,
        }
        card: dict[str, Any] | None = None
        if uuid:
            card = sdk.cards.get_by_uuid(uuid, as_dict=True)
        elif name:
            rows = sdk.cards.get_by_name(
                name,
                set_code=set_code.upper() if set_code else None,
                as_dict=True,
            )
            if len(rows) == 1:
                card = rows[0]
            elif len(rows) > 1:
                return _item_result(
                    "market_snapshot",
                    None,
                    sdk=sdk,
                    normalized_params=normalized_params,
                    reason="ambiguous",
                    error_type="ambiguous",
                    candidates=rows[:10],
                )
            else:
                suggestions = _card_suggestions(
                    sdk,
                    query=name,
                    set_code=set_code.upper() if set_code else None,
                    limit=5,
                )
                if len(suggestions) == 1:
                    card = suggestions[0]
                elif suggestions:
                    return _item_result(
                        "market_snapshot",
                        None,
                        sdk=sdk,
                        normalized_params=normalized_params,
                        reason="ambiguous",
                        error_type="ambiguous",
                        candidates=suggestions,
                        warnings=[
                            "No exact card matched; candidates are fuzzy matches."
                        ],
                    )
        else:
            raise ValueError("Provide uuid or name.")

        if card is None:
            return _item_result(
                "market_snapshot",
                None,
                sdk=sdk,
                normalized_params=normalized_params,
            )

        price_status = _price_snapshot_status(sdk)
        if price_status is not None:
            return _item_result(
                "market_snapshot",
                {
                    "card": card,
                    "price_data_available": False,
                    "requires_view": "all_prices_today",
                    "cache_status": price_status,
                    "message": _warming_message(
                        noun="Price data",
                        view_name="all_prices_today",
                        status=price_status,
                    ),
                },
                sdk=sdk,
                normalized_params=normalized_params,
            )

        trend, trend_source = _price_trend_result(sdk, uuid=card["uuid"])
        snapshot = {
            "card": card,
            "price_data_available": True,
            "current_prices": sdk.prices.today(card["uuid"], as_dict=True),
            "trend": trend,
            "trend_source": trend_source,
        }
        return _item_result(
            "market_snapshot",
            snapshot,
            sdk=sdk,
            normalized_params=normalized_params,
        )

    @mcp.tool(annotations=_READ_ONLY_TOOL, tags={"cards", "reference"})
    def get_card_rulings(
        ctx: Context,
        uuid: str,
        limit: Limit = DEFAULT_LIMIT,
        offset: Offset = 0,
    ) -> dict[str, Any]:
        """Return official card rulings for a card UUID."""

        sdk = _require_sdk(ctx)
        unavailable = _view_data_unavailable_details(
            sdk,
            view_name="card_rulings",
            noun="Card rulings",
        )
        if unavailable is not None:
            return _items_result(
                [],
                sdk=sdk,
                normalized_params={"uuid": uuid, "limit": limit, "offset": offset},
                reason="missing_view",
                error_type="missing_view",
                **unavailable,
            )

        rows = sdk._conn.execute(
            "SELECT uuid, date, text FROM card_rulings "
            "WHERE uuid = $1 ORDER BY date DESC "
            f"LIMIT {limit} OFFSET {offset}",
            [uuid],
        )
        return _items_result(
            rows,
            sdk=sdk,
            normalized_params={"uuid": uuid, "limit": limit, "offset": offset},
        )

    @mcp.tool(annotations=_READ_ONLY_TOOL, tags={"cards", "reference"})
    def get_card_purchase_urls(ctx: Context, uuid: str) -> dict[str, Any]:
        """Return known purchase URLs for a card UUID."""

        sdk = _require_sdk(ctx)
        unavailable = _view_data_unavailable_details(
            sdk,
            view_name="card_purchase_urls",
            noun="Card purchase URLs",
        )
        if unavailable is not None:
            return _item_result(
                "purchase_urls",
                None,
                sdk=sdk,
                normalized_params={"uuid": uuid},
                reason="missing_view",
                error_type="missing_view",
                **unavailable,
            )

        rows = sdk._conn.execute(
            "SELECT * FROM card_purchase_urls WHERE uuid = $1",
            [uuid],
        )
        return _item_result(
            "purchase_urls",
            rows[0] if rows else None,
            sdk=sdk,
            normalized_params={"uuid": uuid},
        )

    @mcp.tool(annotations=_READ_ONLY_TOOL, tags={"cards", "reference"})
    def get_card_foreign_data(
        ctx: Context,
        uuid: str,
        language: str | None = None,
        limit: Limit = DEFAULT_LIMIT,
        offset: Offset = 0,
    ) -> dict[str, Any]:
        """Return foreign-language card data for a card UUID."""

        sdk = _require_sdk(ctx)
        unavailable = _view_data_unavailable_details(
            sdk,
            view_name="card_foreign_data",
            noun="Foreign card data",
        )
        if unavailable is not None:
            return _items_result(
                [],
                sdk=sdk,
                normalized_params={
                    "uuid": uuid,
                    "language": language,
                    "limit": limit,
                    "offset": offset,
                },
                reason="missing_view",
                error_type="missing_view",
                **unavailable,
            )

        params: list[Any] = [uuid]
        sql = "SELECT * FROM card_foreign_data WHERE uuid = $1"
        if language:
            params.append(language)
            sql += f" AND language = ${len(params)}"
        sql += " ORDER BY language ASC"
        rows = sdk._conn.execute(sql, params)
        return _items_result(
            rows[offset : offset + limit],
            sdk=sdk,
            normalized_params={
                "uuid": uuid,
                "language": language,
                "limit": limit,
                "offset": offset,
            },
            total_rows=len(rows),
        )

    @mcp.tool(annotations=_READ_ONLY_TOOL, tags={"sets", "reference"})
    def get_set_translations(
        ctx: Context,
        code: str,
        language: str | None = None,
        limit: Limit = DEFAULT_LIMIT,
        offset: Offset = 0,
    ) -> dict[str, Any]:
        """Return translated set names for a set code."""

        sdk = _require_sdk(ctx)
        canonical_code = code.upper()
        unavailable = _view_data_unavailable_details(
            sdk,
            view_name="set_translations",
            noun="Set translations",
        )
        if unavailable is not None:
            return _items_result(
                [],
                sdk=sdk,
                normalized_params={
                    "code": canonical_code,
                    "language": language,
                    "limit": limit,
                    "offset": offset,
                },
                reason="missing_view",
                error_type="missing_view",
                **unavailable,
            )

        params: list[Any] = [canonical_code]
        sql = "SELECT * FROM set_translations WHERE code = $1"
        if language:
            params.append(language)
            sql += f" AND language = ${len(params)}"
        sql += " ORDER BY language ASC"
        rows = sdk._conn.execute(sql, params)
        return _items_result(
            rows[offset : offset + limit],
            sdk=sdk,
            normalized_params={
                "code": canonical_code,
                "language": language,
                "limit": limit,
                "offset": offset,
            },
            total_rows=len(rows),
        )

    @mcp.tool(annotations=_READ_ONLY_TOOL, tags={"tokens", "lookup"})
    def get_token_identifiers(ctx: Context, uuid: str) -> dict[str, Any]:
        """Return every external identifier known for a token UUID."""

        sdk = _require_sdk(ctx)
        unavailable = _view_data_unavailable_details(
            sdk,
            view_name="token_identifiers",
            noun="Token identifier data",
        )
        if unavailable is not None:
            return _item_result(
                "identifiers",
                None,
                sdk=sdk,
                normalized_params={"uuid": uuid},
                reason="missing_view",
                error_type="missing_view",
                **unavailable,
            )

        rows = sdk._conn.execute(
            "SELECT * FROM token_identifiers WHERE uuid = $1",
            [uuid],
        )
        return _item_result(
            "identifiers",
            rows[0] if rows else None,
            sdk=sdk,
            normalized_params={"uuid": uuid},
        )

    @mcp.tool(annotations=_READ_ONLY_TOOL, tags={"legalities", "lookup"})
    def get_card_legalities(ctx: Context, uuid: str) -> dict[str, Any]:
        """Return the format-to-status legality map for a card UUID."""

        sdk = _require_sdk(ctx)
        return _response(
            {
                "uuid": uuid,
                "result_count": 1,
                "legalities": sdk.legalities.formats_for_card(uuid),
            },
            sdk=sdk,
            normalized_params={"uuid": uuid},
        )

    @mcp.tool(annotations=_READ_ONLY_TOOL, tags={"legalities", "lookup"})
    def is_card_legal(
        ctx: Context,
        uuid: str,
        format_name: str,
    ) -> dict[str, Any]:
        """Check whether a card UUID is legal in a format."""

        sdk = _require_sdk(ctx)
        return _response(
            {
                "uuid": uuid,
                "format_name": format_name,
                "result_count": 1,
                "is_legal": sdk.legalities.is_legal(uuid, format_name),
            },
            sdk=sdk,
            normalized_params={"uuid": uuid, "format_name": format_name},
        )

    @mcp.tool(annotations=_READ_ONLY_TOOL, tags={"legalities", "search"})
    def list_cards_by_format_status(
        ctx: Context,
        format_name: str,
        status: str = "legal",
        limit: Limit = DEFAULT_LIMIT,
        offset: Offset = 0,
    ) -> dict[str, Any]:
        """List cards in a format for a specific legality status."""

        sdk = _require_sdk(ctx)
        canonical_status = _normalize_legality_status(status)
        if canonical_status == "legal":
            rows = sdk.legalities.legal_in(
                format_name,
                limit=limit,
                offset=offset,
                as_dict=True,
            )
        elif canonical_status == "banned":
            rows = sdk.legalities.banned_in(format_name, limit=limit, offset=offset)
        elif canonical_status == "restricted":
            rows = sdk.legalities.restricted_in(
                format_name,
                limit=limit,
                offset=offset,
            )
        elif canonical_status == "suspended":
            rows = sdk.legalities.suspended_in(
                format_name,
                limit=limit,
                offset=offset,
            )
        else:
            rows = sdk.legalities.not_legal_in(
                format_name,
                limit=limit,
                offset=offset,
            )
        return _items_result(
            rows,
            sdk=sdk,
            normalized_params={
                "format_name": format_name,
                "status": canonical_status,
                "limit": limit,
                "offset": offset,
            },
            format_name=format_name,
            status=canonical_status,
        )

    @mcp.tool(annotations=_READ_ONLY_TOOL, tags={"prices", "lookup"})
    def get_card_prices(ctx: Context, uuid: str) -> dict[str, Any]:
        """Return the nested price payload for a card UUID."""

        sdk = _require_sdk(ctx)
        normalized_params = {"uuid": uuid}
        price_status = _price_snapshot_status(sdk)
        if price_status is not None:
            return _price_data_unavailable(
                price_status,
                sdk=sdk,
                noun="Price data",
                item_key="prices",
                normalized_params=normalized_params,
            )
        return _item_result(
            "prices",
            sdk.prices.get(uuid),
            sdk=sdk,
            normalized_params=normalized_params,
        )

    @mcp.tool(annotations=_READ_ONLY_TOOL, tags={"prices", "lookup"})
    def get_price_today(
        ctx: Context,
        uuid: str,
        provider: str | None = None,
        finish: str | None = None,
        price_type: str | None = None,
    ) -> dict[str, Any]:
        """Return the current price rows for a card UUID."""

        sdk = _require_sdk(ctx)
        normalized_params = {
            "uuid": uuid,
            "provider": provider,
            "finish": finish,
            "price_type": price_type,
        }
        price_status = _price_snapshot_status(sdk)
        if price_status is not None:
            return _price_data_unavailable(
                price_status,
                sdk=sdk,
                noun="Current price data",
                as_items=True,
                normalized_params=normalized_params,
            )

        rows = sdk.prices.today(
            uuid,
            provider=provider,
            finish=finish,
            price_type=price_type,
            as_dict=True,
        )
        return _items_result(rows, sdk=sdk, normalized_params=normalized_params)

    @mcp.tool(annotations=_READ_ONLY_TOOL, tags={"prices", "history"})
    def get_price_history(
        ctx: Context,
        uuid: str,
        provider: str | None = None,
        finish: str | None = None,
        price_type: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        limit: Limit = DEFAULT_LIMIT,
        offset: Offset = 0,
    ) -> dict[str, Any]:
        """Return historical price rows for a card UUID."""

        sdk = _require_sdk(ctx)
        normalized_params = {
            "uuid": uuid,
            "provider": provider,
            "finish": finish,
            "price_type": price_type,
            "date_from": date_from,
            "date_to": date_to,
            "limit": limit,
            "offset": offset,
        }
        latest_date = _latest_price_snapshot_date(sdk, uuid)

        if latest_date and date_from and date_from > latest_date:
            return _items_result(
                [],
                sdk=sdk,
                normalized_params=normalized_params,
                total_rows=0,
                history_available=True,
                latest_cached_date=latest_date,
            )

        if _price_history_uses_latest_snapshot(
            latest_date,
            date_from=date_from,
            date_to=date_to,
        ):
            rows = sdk.prices.today(
                uuid,
                provider=provider,
                finish=finish,
                price_type=price_type,
                as_dict=True,
            )
            sliced = rows[offset : offset + limit]
            return _items_result(
                sliced,
                sdk=sdk,
                normalized_params=normalized_params,
                total_rows=len(rows),
                history_available=True,
                history_source="all_prices_today",
                latest_cached_date=latest_date,
            )

        history_status = sdk._cache.parquet_status("all_prices")
        if "all_prices" not in sdk.views and history_status["status"] != "ready":
            if history_status["status"] not in {"offline", "in_progress"}:
                history_status = sdk._cache.prefetch_parquet("all_prices")
            return _items_result(
                [],
                sdk=sdk,
                normalized_params=normalized_params,
                total_rows=0,
                history_available=False,
                latest_cached_date=latest_date,
                requires_view="all_prices",
                cache_status=history_status,
                reason="missing_view",
                error_type="missing_view",
                message=_warming_message(
                    noun="Historical price data",
                    view_name="all_prices",
                    status=history_status,
                ),
            )

        rows = sdk.prices.history(
            uuid,
            provider=provider,
            finish=finish,
            price_type=price_type,
            date_from=date_from,
            date_to=date_to,
            as_dict=True,
        )
        sliced = rows[offset : offset + limit]
        return _items_result(
            sliced,
            sdk=sdk,
            normalized_params=normalized_params,
            total_rows=len(rows),
            history_available=True,
            latest_cached_date=latest_date,
            history_source="all_prices",
        )

    @mcp.tool(annotations=_READ_ONLY_TOOL, tags={"prices", "history"})
    def get_price_trend(
        ctx: Context,
        uuid: str,
        provider: str | None = None,
        finish: str | None = None,
        price_type: str = "retail",
    ) -> dict[str, Any]:
        """Return min, max, average, and count trend statistics for a card UUID."""

        sdk = _require_sdk(ctx)
        normalized_params = {
            "uuid": uuid,
            "provider": provider,
            "finish": finish,
            "price_type": price_type,
        }
        price_status = _price_snapshot_status(sdk)
        if price_status is not None:
            return _price_data_unavailable(
                price_status,
                sdk=sdk,
                noun="Price trend data",
                item_key="trend",
                normalized_params=normalized_params,
            )

        trend, trend_source = _price_trend_result(
            sdk,
            uuid=uuid,
            provider=provider,
            finish=finish,
            price_type=price_type,
        )
        return _item_result(
            "trend",
            trend,
            sdk=sdk,
            normalized_params=normalized_params,
            trend_source=trend_source,
        )

    @mcp.tool(annotations=_READ_ONLY_TOOL, tags={"prices", "search"})
    def find_cheapest_printing(
        ctx: Context,
        name: str,
        provider: str = "tcgplayer",
        finish: str = "normal",
        price_type: str = "retail",
    ) -> dict[str, Any]:
        """Return the cheapest current printing for a named card."""

        sdk = _require_sdk(ctx)
        normalized_params = {
            "name": name,
            "provider": provider,
            "finish": finish,
            "price_type": price_type,
        }
        price_status = _price_snapshot_status(sdk)
        if price_status is not None:
            return _price_data_unavailable(
                price_status,
                sdk=sdk,
                noun="Price data",
                item_key="printing",
                normalized_params=normalized_params,
            )

        printing = sdk.prices.cheapest_printing(
            name,
            provider=provider,
            finish=finish,
            price_type=price_type,
        )
        suggestions: list[dict[str, Any]] = []
        warnings: list[str] = []
        if printing is None:
            suggestions = _card_suggestions(sdk, query=name)
            if suggestions:
                warnings.append(
                    "No priced printing matched exactly; suggestions are fuzzy "
                    "card matches."
                )
        return _item_result(
            "printing",
            printing,
            sdk=sdk,
            normalized_params=normalized_params,
            warnings=warnings,
            suggestions=suggestions,
        )

    @mcp.tool(annotations=_READ_ONLY_TOOL, tags={"prices", "search"})
    def list_price_extremes(
        ctx: Context,
        kind: str = "most_expensive",
        provider: str = "tcgplayer",
        finish: str = "normal",
        price_type: str = "retail",
        limit: Limit = DEFAULT_LIMIT,
        offset: Offset = 0,
    ) -> dict[str, Any]:
        """List the cheapest or most expensive printings across the dataset."""

        sdk = _require_sdk(ctx)
        canonical_kind = _normalize_price_extreme(kind)
        normalized_params = {
            "kind": canonical_kind,
            "provider": provider,
            "finish": finish,
            "price_type": price_type,
            "limit": limit,
            "offset": offset,
        }
        price_status = _price_snapshot_status(sdk)
        if price_status is not None:
            return _price_data_unavailable(
                price_status,
                sdk=sdk,
                noun="Price data",
                as_items=True,
                normalized_params=normalized_params,
                kind=canonical_kind,
            )

        if canonical_kind == "cheapest":
            rows = sdk.prices.cheapest_printings(
                provider=provider,
                finish=finish,
                price_type=price_type,
                limit=limit,
                offset=offset,
            )
        else:
            rows = sdk.prices.most_expensive_printings(
                provider=provider,
                finish=finish,
                price_type=price_type,
                limit=limit,
                offset=offset,
            )
        return _items_result(
            rows,
            sdk=sdk,
            normalized_params=normalized_params,
            kind=canonical_kind,
        )

    @mcp.tool(annotations=_READ_ONLY_TOOL, tags={"decks", "search"})
    def list_decks(
        ctx: Context,
        name: str | None = None,
        set_code: str | None = None,
        deck_type: str | None = None,
        limit: Limit = DEFAULT_LIMIT,
        offset: Offset = 0,
    ) -> dict[str, Any]:
        """List or search deck summaries from DeckList.json."""

        sdk = _require_sdk(ctx)
        if name:
            rows = sdk.decks.search(name=name, set_code=set_code, as_dict=True)
            if deck_type:
                rows = [deck for deck in rows if deck.get("type") == deck_type]
        else:
            rows = sdk.decks.list(
                set_code=set_code,
                deck_type=deck_type,
                as_dict=True,
            )
        return _items_result(
            rows[offset : offset + limit],
            sdk=sdk,
            normalized_params={
                "name": name,
                "set_code": set_code.upper() if set_code else None,
                "deck_type": deck_type,
                "limit": limit,
                "offset": offset,
            },
            total_rows=len(rows),
        )

    @mcp.tool(annotations=_READ_ONLY_TOOL, tags={"sealed", "search"})
    def list_sealed_products(
        ctx: Context,
        set_code: str | None = None,
        category: str | None = None,
        limit: Limit = DEFAULT_LIMIT,
        offset: Offset = 0,
    ) -> dict[str, Any]:
        """List sealed products such as booster packs, boxes, and bundles."""

        sdk = _require_sdk(ctx)
        canonical_set_code = set_code.upper() if set_code else None
        normalized_params = {
            "set_code": canonical_set_code,
            "category": category,
            "limit": limit,
            "offset": offset,
        }
        unavailable = _sealed_data_unavailable_details(sdk)
        if unavailable is not None:
            if (
                canonical_set_code
                and sdk.sets.get(canonical_set_code, as_dict=True) is None
            ):
                return _items_result([], sdk=sdk, normalized_params=normalized_params)
            return _items_result(
                [],
                sdk=sdk,
                normalized_params=normalized_params,
                sealed_data_available=False,
                reason="unsupported",
                error_type="missing_view",
                **unavailable,
            )

        rows = sdk.sealed.list(
            set_code=canonical_set_code,
            category=category,
            limit=limit + offset,
            as_dict=True,
        )
        return _items_result(
            rows[offset : offset + limit],
            sdk=sdk,
            normalized_params=normalized_params,
            sealed_data_available=True,
        )

    @mcp.tool(annotations=_READ_ONLY_TOOL, tags={"sealed", "lookup"})
    def get_sealed_product(ctx: Context, uuid: str) -> dict[str, Any]:
        """Fetch a sealed product by UUID."""

        sdk = _require_sdk(ctx)
        unavailable = _sealed_data_unavailable_details(sdk)
        if unavailable is not None:
            return _item_result(
                "sealed_product",
                None,
                sdk=sdk,
                normalized_params={"uuid": uuid},
                sealed_data_available=False,
                reason="unsupported",
                error_type="missing_view",
                **unavailable,
            )

        return _item_result(
            "sealed_product",
            sdk.sealed.get(uuid),
            sdk=sdk,
            normalized_params={"uuid": uuid},
            sealed_data_available=True,
        )

    @mcp.tool(annotations=_READ_ONLY_TOOL, tags={"skus", "lookup"})
    def get_skus_for_card(ctx: Context, uuid: str) -> dict[str, Any]:
        """Fetch TCGPlayer SKU rows for a card UUID."""

        sdk = _require_sdk(ctx)
        rows = sdk.skus.get(uuid, as_dict=True)
        return _items_result(rows, sdk=sdk, normalized_params={"uuid": uuid})

    @mcp.tool(annotations=_READ_ONLY_TOOL, tags={"skus", "lookup"})
    def find_sku(
        ctx: Context,
        lookup_type: str,
        value: int,
    ) -> dict[str, Any]:
        """Find TCGPlayer SKU data by SKU ID or product ID."""

        sdk = _require_sdk(ctx)
        canonical_lookup_type = _normalize_sku_lookup_type(lookup_type)
        normalized_params = {"lookup_type": canonical_lookup_type, "value": value}
        if canonical_lookup_type == "sku_id":
            return _item_result(
                "sku",
                sdk.skus.find_by_sku_id(value),
                sdk=sdk,
                normalized_params=normalized_params,
            )
        return _items_result(
            sdk.skus.find_by_product_id(value, as_dict=True),
            sdk=sdk,
            normalized_params=normalized_params,
            lookup_type=canonical_lookup_type,
            value=value,
        )

    @mcp.tool(annotations=_READ_ONLY_TOOL, tags={"enums", "reference"})
    def get_enums(ctx: Context, catalog: str) -> dict[str, Any]:
        """Return one of the static MTGJSON enum catalogs."""

        sdk = _require_sdk(ctx)
        canonical_catalog, subset_path, requested_catalog = _normalize_enum_catalog(
            catalog
        )
        data = _get_enum_catalog(sdk, canonical_catalog)
        if subset_path is not None:
            data = _extract_nested_value(data, subset_path)
        return _response(
            {
                "catalog": requested_catalog,
                "resolved_catalog": canonical_catalog,
                "result_count": 1,
                "data": _jsonable(data),
            },
            sdk=sdk,
            normalized_params={"catalog": requested_catalog},
        )

    @mcp.tool(annotations=_READ_ONLY_TOOL, tags={"reference", "discovery"})
    def describe_parameter_values(
        ctx: Context,
        tool_name: str,
        parameter_name: str,
    ) -> dict[str, Any]:
        """Return valid values and aliases for a known tool parameter."""

        sdk = _require_sdk(ctx)
        reference = _parameter_reference(tool_name, parameter_name)
        if reference is None:
            return _item_result(
                "reference",
                None,
                sdk=sdk,
                normalized_params={
                    "tool_name": tool_name,
                    "parameter_name": parameter_name,
                },
                reason="unsupported",
                error_type="unsupported",
                supported_parameters=sorted(
                    f"{tool}.{param}"
                    for tool, param in _SUPPORTED_TOOL_PARAMETER_VALUES
                ),
            )
        return _item_result(
            "reference",
            {
                "tool_name": tool_name,
                "parameter_name": parameter_name,
                **reference,
            },
            sdk=sdk,
            normalized_params={
                "tool_name": tool_name,
                "parameter_name": parameter_name,
            },
        )

    @mcp.tool(annotations=_READ_ONLY_TOOL, tags={"booster", "lookup"})
    def list_booster_types(ctx: Context, set_code: str) -> dict[str, Any]:
        """List available booster types for a set code."""

        sdk = _require_sdk(ctx)
        canonical_code = set_code.upper()
        return _response(
            {
                "set_code": canonical_code,
                "result_count": 1,
                "booster_types": sdk.booster.available_types(canonical_code),
            },
            sdk=sdk,
            normalized_params={"set_code": canonical_code},
        )

    @mcp.tool(
        annotations=_READ_ONLY_NONDETERMINISTIC_TOOL,
        tags={"booster", "simulation"},
    )
    def open_booster_pack(
        ctx: Context,
        set_code: str,
        booster_type: str = "draft",
    ) -> dict[str, Any]:
        """Simulate opening a single booster pack."""

        sdk = _require_sdk(ctx)
        canonical_code = set_code.upper()
        rows = sdk.booster.open_pack(
            canonical_code,
            booster_type=booster_type,
            as_dict=True,
        )
        return _items_result(
            rows,
            sdk=sdk,
            normalized_params={
                "set_code": canonical_code,
                "booster_type": booster_type,
            },
            set_code=canonical_code,
            booster_type=booster_type,
        )

    @mcp.tool(
        annotations=_READ_ONLY_NONDETERMINISTIC_TOOL,
        tags={"booster", "simulation"},
    )
    def open_booster_box(
        ctx: Context,
        set_code: str,
        booster_type: str = "draft",
        packs: BoosterPackCount = 36,
        summary: bool = False,
    ) -> dict[str, Any]:
        """Simulate opening many booster packs from the same set."""

        sdk = _require_sdk(ctx)
        canonical_code = set_code.upper()
        box = sdk.booster.open_box(
            canonical_code,
            booster_type=booster_type,
            packs=packs,
            as_dict=True,
        )
        payload = {
            "set_code": canonical_code,
            "booster_type": booster_type,
            "packs": len(box),
            "result_count": len(box),
            "cards_per_pack": [len(pack) for pack in box],
        }
        if summary:
            payload["summary"] = _summarize_booster_box(box)
        else:
            payload["box"] = _jsonable(box)
        return _response(
            payload,
            sdk=sdk,
            normalized_params={
                "set_code": canonical_code,
                "booster_type": booster_type,
                "packs": packs,
                "summary": summary,
            },
            warnings=(
                ["Summary mode omits the full card list for chat-friendly output."]
                if summary
                else None
            ),
        )

    @mcp.tool(annotations=_READ_ONLY_TOOL, tags={"booster", "lookup"})
    def get_booster_sheet_contents(
        ctx: Context,
        set_code: str,
        booster_type: str,
        sheet_name: str,
        expand: bool = False,
    ) -> dict[str, Any]:
        """Return booster sheet contents and weights for a set/type/sheet."""

        sdk = _require_sdk(ctx)
        canonical_code = set_code.upper()
        sheet = sdk.booster.sheet_contents(canonical_code, booster_type, sheet_name)
        payload: dict[str, Any] = {"sheet": sheet}
        if expand and isinstance(sheet, dict):
            uuids = list(sheet)
            cards = {
                row["uuid"]: row for row in sdk.cards.get_by_uuids(uuids, as_dict=True)
            }
            payload["expanded_sheet"] = [
                {
                    "uuid": uuid,
                    "weight": weight,
                    "name": cards.get(uuid, {}).get("name"),
                    "setCode": cards.get(uuid, {}).get("setCode"),
                    "number": cards.get(uuid, {}).get("number"),
                    "rarity": cards.get(uuid, {}).get("rarity"),
                }
                for uuid, weight in sheet.items()
            ]
        return _item_result(
            "sheet",
            payload["sheet"],
            sdk=sdk,
            normalized_params={
                "set_code": canonical_code,
                "booster_type": booster_type,
                "sheet_name": sheet_name,
                "expand": expand,
            },
            expanded_sheet=payload.get("expanded_sheet"),
        )

    @mcp.tool(annotations=_READ_ONLY_TOOL, tags={"system", "reference"})
    def get_server_status(ctx: Context) -> dict[str, Any]:
        """Return server configuration and currently registered views."""

        sdk = _require_sdk(ctx)
        return _server_status(
            sdk,
            _require_settings(ctx),
            required_views=_required_views(ctx),
            download_progress=_download_progress_snapshot(ctx),
        )

    @mcp.tool(annotations=_READ_ONLY_TOOL, tags={"system", "sql"})
    def execute_read_only_sql(
        ctx: Context,
        query: str,
        params: list[Any] | str | None = None,
        ensure_views: list[str] | str | None = None,
        max_rows: SqlRowLimit = 100,
    ) -> dict[str, Any]:
        """Run a guarded read-only SQL query against the local DuckDB dataset."""

        sdk = _require_sdk(ctx)
        sql_params = _coerce_query_params(params)
        views_to_ensure = _validate_known_views(
            _coerce_string_list(ensure_views, field_name="ensure_views")
        )
        if views_to_ensure:
            sdk._conn.ensure_views(*views_to_ensure)

        statement = _validate_read_only_sql(query)
        rows = sdk.sql(_limited_sql_query(statement, max_rows), params=sql_params)
        truncated = len(rows) > max_rows
        return _response(
            {
                "query": statement,
                "returned_rows": min(len(rows), max_rows),
                "result_count": min(len(rows), max_rows),
                "registered_views": sdk.views,
                "rows": rows[:max_rows],
            },
            sdk=sdk,
            normalized_params={
                "query": statement,
                "params": sql_params,
                "ensure_views": views_to_ensure,
                "max_rows": max_rows,
            },
            truncated=truncated,
        )

    @mcp.tool(tags={"system", "admin"})
    def refresh_cache(ctx: Context) -> dict[str, Any]:
        """Check MTGJSON for newer data and reset the SDK state when stale."""

        sdk = _require_sdk(ctx)
        refreshed = sdk.refresh()
        return _response(
            {
                "refreshed": refreshed,
                "result_count": 1,
                "meta": sdk.meta,
                "registered_views": sdk.views,
            },
            sdk=sdk,
        )

    @mcp.tool(tags={"system", "admin"})
    async def prefetch_views(
        ctx: Context,
        views: list[str] | str,
    ) -> dict[str, Any]:
        """Start warming one or more parquet views in the background."""

        sdk = _require_sdk(ctx)
        requested_views = _validate_known_views(
            _coerce_string_list(views, field_name="views")
        )
        statuses: dict[str, Any] = {}
        total = len(requested_views)
        for index, view_name in enumerate(requested_views, start=1):
            await _report_progress(
                ctx,
                progress=float(index - 1),
                total=float(total),
                message=f"Warming {view_name}",
            )
            statuses[view_name] = sdk._cache.prefetch_parquet(view_name)
        await _report_progress(
            ctx,
            progress=float(total),
            total=float(total),
            message="Warm requests queued.",
        )
        return _response(
            {
                "requested_views": requested_views,
                "result_count": len(requested_views),
                "view_statuses": statuses,
            },
            sdk=sdk,
            normalized_params={"views": requested_views},
        )

    @mcp.tool(annotations=_DESTRUCTIVE_TOOL, tags={"system", "admin"})
    async def export_duckdb(
        ctx: Context,
        path: str,
        overwrite: bool = False,
        include_views: list[str] | str | None = None,
    ) -> dict[str, Any]:
        """Export the currently loaded DuckDB views to a standalone .duckdb file."""

        sdk = _require_sdk(ctx)
        views_to_ensure = _validate_known_views(
            _coerce_string_list(include_views, field_name="include_views")
        )
        if views_to_ensure:
            total = len(views_to_ensure)
            for index, view_name in enumerate(views_to_ensure, start=1):
                await _report_progress(
                    ctx,
                    progress=float(index - 1),
                    total=float(total + 1),
                    message=f"Ensuring {view_name}",
                )
                sdk._conn.ensure_views(view_name)

        target = Path(path).expanduser()
        if target.suffix.lower() != ".duckdb":
            raise ValueError("Export path must end with .duckdb.")
        if target.exists() and not overwrite:
            raise FileExistsError(
                f"{target} already exists. Pass overwrite=true to replace it."
            )
        target.parent.mkdir(parents=True, exist_ok=True)
        exported_views = views_to_ensure or list(sdk.views)
        await _report_progress(
            ctx,
            progress=float(len(views_to_ensure or [])),
            total=float((len(views_to_ensure or [])) + 1),
            message=f"Exporting DuckDB file to {target}",
        )
        exported_path = sdk.export_db(target, views=exported_views)
        await _report_progress(
            ctx,
            progress=float((len(views_to_ensure or [])) + 1),
            total=float((len(views_to_ensure or [])) + 1),
            message="Export complete.",
        )
        return _response(
            {
                "path": str(exported_path),
                "exported_view_count": len(exported_views),
                "result_count": len(exported_views),
                "exported_views": exported_views,
            },
            sdk=sdk,
            normalized_params={
                "path": str(target),
                "overwrite": overwrite,
                "include_views": views_to_ensure,
            },
        )

    return mcp


def _server_cli_args(args: argparse.Namespace, *, transport: str) -> list[str]:
    cli_args = [
        "-m",
        "mtgjson_sdk.mcp.server",
        "--transport",
        transport,
        "--log-level",
        args.log_level,
    ]
    if args.cache_dir is not None:
        cli_args.extend(["--cache-dir", str(Path(args.cache_dir).expanduser())])
    if args.offline:
        cli_args.append("--offline")
    if args.timeout != 120.0:
        cli_args.extend(["--timeout", str(args.timeout)])
    if args.warm_profile:
        cli_args.extend(["--warm-profile", args.warm_profile])
    if args.show_banner:
        cli_args.append("--show-banner")
    if transport == "http":
        cli_args.extend(["--host", args.host, "--port", str(args.port)])
        cli_args.extend(["--path", _normalize_http_path(args.path)])
        if args.stateless_http:
            cli_args.append("--stateless-http")
    return cli_args


def _build_stdio_transport_config(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "mcpServers": {
            "mtgjson-local": {
                "type": "stdio",
                "command": sys.executable,
                "args": _server_cli_args(args, transport="stdio"),
                "cwd": str(Path.cwd()),
            }
        }
    }


async def _doctor_probe_client(client: Any) -> dict[str, Any]:
    status = await client.call_tool("get_server_status", {})
    search_sets = await client.call_tool(
        "search_sets",
        {"name": "Masters", "limit": 1},
    )
    sql = await client.call_tool(
        "execute_read_only_sql",
        {
            "query": "SELECT code, name FROM sets WHERE code = 'A25'",
            "ensure_views": ["sets"],
            "max_rows": 5,
        },
    )
    return {
        "dataset_version": status.data.get("dataset_version"),
        "offline": status.data.get("offline"),
        "registered_views": status.data.get("registered_views"),
        "search_set_codes": [
            item.get("code") for item in search_sets.data.get("items", [])
        ],
        "sql_rows": sql.data.get("rows", []),
    }


async def _run_stdio_doctor(args: argparse.Namespace) -> dict[str, Any]:
    from fastmcp import Client

    transport_config = _build_stdio_transport_config(args)
    async with Client(transport_config, timeout=15, init_timeout=15) as client:
        probe = await _doctor_probe_client(client)
    return {"transport": "stdio", "ok": True, "probe": probe}


def _pick_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind((DEFAULT_HTTP_HOST, 0))
        return int(sock.getsockname()[1])


async def _wait_for_http_server(base_url: str, timeout_seconds: float) -> None:
    deadline = time.monotonic() + timeout_seconds
    last_error: str | None = None
    async with httpx.AsyncClient(base_url=base_url, timeout=5.0) as client:
        while time.monotonic() < deadline:
            try:
                response = await client.get("/health")
                response.raise_for_status()
                return
            except Exception as exc:  # pragma: no cover - transient polling
                last_error = str(exc)
                await asyncio.sleep(0.1)
    raise RuntimeError(
        "Timed out waiting for HTTP MCP server to become healthy"
        + (f": {last_error}" if last_error else "")
    )


async def _run_http_doctor(args: argparse.Namespace) -> dict[str, Any]:
    from fastmcp import Client

    port = _pick_free_port()
    child_args = argparse.Namespace(**vars(args))
    child_args.host = DEFAULT_HTTP_HOST
    child_args.port = port
    child_args.path = _normalize_http_path(args.path)

    child = subprocess.Popen(
        [sys.executable, *_server_cli_args(child_args, transport="http")],
        cwd=str(Path.cwd()),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    base_url = f"http://{child_args.host}:{port}"
    endpoint = f"{base_url}{_normalize_http_path(child_args.path)}"
    try:
        await _wait_for_http_server(base_url, timeout_seconds=15.0)
        async with Client(endpoint, timeout=15, init_timeout=15) as client:
            probe = await _doctor_probe_client(client)
    finally:
        child.terminate()
        try:
            child.wait(timeout=5)
        except subprocess.TimeoutExpired:  # pragma: no cover - defensive cleanup
            child.kill()
            child.wait(timeout=5)
    return {
        "transport": "http",
        "ok": True,
        "endpoint": endpoint,
        "probe": probe,
    }


async def _run_transport_doctor(args: argparse.Namespace) -> dict[str, Any]:
    results: list[dict[str, Any]] = []
    if args.doctor in {"stdio", "both"}:
        results.append(await _run_stdio_doctor(args))
    if args.doctor in {"http", "both"}:
        results.append(await _run_http_doctor(args))

    parity_match = True
    if len(results) > 1:
        probes = [result["probe"] for result in results]
        parity_match = all(probe == probes[0] for probe in probes[1:])

    return {
        "ok": all(result["ok"] for result in results) and parity_match,
        "doctor": args.doctor,
        "transport_results": results,
        "parity_match": parity_match,
    }


def _run_transport_doctor_sync(args: argparse.Namespace) -> int:
    payload = asyncio.run(_run_transport_doctor(args))
    print(json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=True))
    return 0 if payload["ok"] else 1


def _resolve_cli_defaults(args: argparse.Namespace) -> argparse.Namespace:
    env_transport = _env_choice(ENV_TRANSPORT, ("stdio", "http"))
    env_host = _env_text(ENV_HOST)
    env_port = _env_int(ENV_PORT)
    env_timeout = _env_float(ENV_TIMEOUT)

    args.transport = args.transport or env_transport
    if args.transport is None:
        args.transport = "stdio"

    if args.host is None:
        if env_host is not None:
            args.host = env_host
        elif env_transport == "http" and args.transport == "http":
            args.host = DEFAULT_DOCKER_HTTP_HOST
        else:
            args.host = DEFAULT_HTTP_HOST
    args.port = args.port if args.port is not None else (
        env_port if env_port is not None else DEFAULT_HTTP_PORT
    )
    args.path = args.path or _env_text(ENV_PATH) or DEFAULT_HTTP_PATH
    args.warm_profile = args.warm_profile or _env_choice(
        ENV_WARM_PROFILE,
        (WARM_PROFILE_BASE, WARM_PROFILE_PRICES, WARM_PROFILE_FULL),
    )
    args.stateless_http = (
        args.stateless_http
        if args.stateless_http is not None
        else (_env_bool(ENV_STATELESS_HTTP) or False)
    )
    args.cache_dir = args.cache_dir or _env_path(ENV_CACHE_DIR)
    args.offline = (
        args.offline if args.offline is not None else (_env_bool(ENV_OFFLINE) or False)
    )
    args.timeout = args.timeout if args.timeout is not None else (
        env_timeout if env_timeout is not None else 120.0
    )
    args.log_level = args.log_level or _env_choice(ENV_LOG_LEVEL, _LOG_LEVELS)
    if args.log_level is None:
        args.log_level = "WARNING"

    if args.port < 0:
        raise ValueError(f"Invalid value for {ENV_PORT}: {args.port!r}. Use 0 or more.")
    if args.timeout <= 0:
        raise ValueError(
            f"Invalid value for {ENV_TIMEOUT}: {args.timeout!r}. Use a positive number."
        )
    args.path = _normalize_http_path(args.path)
    return args


def build_arg_parser() -> argparse.ArgumentParser:
    """Build the CLI parser for the MTGJSON MCP server."""

    parser = argparse.ArgumentParser(
        prog="mtgjson-mcp",
        description="Run the MTGJSON SDK as a FastMCP server over stdio or HTTP.",
        epilog=_cli_env_help(),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {_current_package_version()}",
    )
    parser.add_argument(
        "--transport",
        choices=("stdio", "http"),
        default=None,
        help="Use stdio for local MCP clients or HTTP for hosted access.",
    )
    parser.add_argument(
        "--host",
        default=None,
        help="HTTP host to bind when using --transport http.",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=None,
        help="HTTP port to bind when using --transport http.",
    )
    parser.add_argument(
        "--path",
        default=None,
        help="HTTP path for the MCP endpoint when using --transport http.",
    )
    parser.add_argument(
        "--warm-profile",
        choices=(WARM_PROFILE_BASE, WARM_PROFILE_PRICES, WARM_PROFILE_FULL),
        default=None,
        help=(
            "Optional startup cache warm profile. "
            "'base' warms core views, 'prices' adds pricing views, and "
            "'full' queues every parquet view."
        ),
    )
    parser.add_argument(
        "--stateless-http",
        action="store_true",
        default=None,
        help=("Enable stateless Streamable HTTP mode for multi-instance deployments."),
    )
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=None,
        help="Optional cache directory for MTGJSON data files.",
    )
    parser.add_argument(
        "--offline",
        action="store_true",
        default=None,
        help="Disable CDN downloads and only use cached files.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=None,
        help="HTTP timeout in seconds for MTGJSON downloads.",
    )
    parser.add_argument(
        "--log-level",
        choices=_LOG_LEVELS,
        default=None,
        help="Log level written to stderr.",
    )
    parser.add_argument(
        "--show-banner",
        action="store_true",
        help="Show the FastMCP startup banner.",
    )
    parser.add_argument(
        "--doctor",
        choices=("stdio", "http", "both"),
        default=None,
        help=(
            "Run a built-in round-trip MCP probe instead of serving forever. "
            "Useful for checking stdio and HTTP parity outside the editor."
        ),
    )
    return parser


def _configure_logging(log_level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.WARNING),
        stream=sys.stderr,
        format="%(levelname)s %(name)s: %(message)s",
    )


def create_asgi_app(
    settings: MCPServerSettings | None = None,
    *,
    sdk_factory: SDKFactory | None = None,
    path: str = DEFAULT_HTTP_PATH,
    stateless_http: bool | None = None,
) -> Any:
    """Create a FastMCP ASGI app for embedded framework deployments."""

    server = create_mcp_server(settings=settings, sdk_factory=sdk_factory)
    effective_stateless_http = (
        stateless_http
        if stateless_http is not None
        else (settings.stateless_http if settings is not None else None)
    )
    return server.http_app(
        path=_normalize_http_path(path),
        stateless_http=effective_stateless_http,
    )


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entrypoint for the MTGJSON MCP server."""

    parser = build_arg_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)
    try:
        args = _resolve_cli_defaults(args)
    except ValueError as exc:
        parser.error(str(exc))
    _configure_logging(args.log_level)

    try:
        _ensure_mcp_dependencies()
        if args.doctor is not None:
            return _run_transport_doctor_sync(args)

        server = create_mcp_server(
            MCPServerSettings(
                cache_dir=args.cache_dir,
                offline=args.offline,
                timeout=args.timeout,
                warm_profile=args.warm_profile,
                stateless_http=args.stateless_http,
            )
        )
    except MissingMCPDependenciesError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    if args.transport == "stdio":
        server.run(transport="stdio", show_banner=args.show_banner)
        return 0

    server.run(
        transport="http",
        host=args.host,
        port=args.port,
        path=args.path,
        stateless_http=args.stateless_http,
        show_banner=args.show_banner,
    )
    return 0


mcp = create_mcp_server() if _MCP_IMPORT_ERROR is None else None


if __name__ == "__main__":
    raise SystemExit(main())
