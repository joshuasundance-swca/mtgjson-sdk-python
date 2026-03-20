"""Tests for the mtgjson-sdk FastMCP server surface."""

from __future__ import annotations

import io
import json
import sys
from contextlib import redirect_stderr
from pathlib import Path
from urllib.parse import urlsplit

import httpx
import pytest
from fastmcp import Client
from fastmcp.utilities.tests import run_server_async
from starlette.applications import Starlette
from starlette.routing import Mount
from starlette.testclient import TestClient

from conftest import (
    SAMPLE_CARDS,
    SAMPLE_FOREIGN_DATA,
    SAMPLE_IDENTIFIERS,
    SAMPLE_LEGALITIES,
    SAMPLE_SETS,
    SAMPLE_TOKENS,
)
from mtgjson_sdk import MtgjsonSDK
from mtgjson_sdk.config import JSON_FILES
from mtgjson_sdk.mcp.server import MCPServerSettings, create_mcp_server

SAMPLE_PRICE_DATA = [
    {
        "uuid": "card-uuid-001",
        "source": "paper",
        "provider": "tcgplayer",
        "currency": "USD",
        "price_type": "retail",
        "finish": "normal",
        "date": "2024-01-01",
        "price": 1.50,
    },
    {
        "uuid": "card-uuid-001",
        "source": "paper",
        "provider": "tcgplayer",
        "currency": "USD",
        "price_type": "retail",
        "finish": "normal",
        "date": "2024-01-03",
        "price": 2.00,
    },
]


def test_create_mcp_server_requires_optional_mcp_dependencies(monkeypatch):
    import mtgjson_sdk.mcp.server as mcp_server

    monkeypatch.setattr(
        mcp_server,
        "_MCP_IMPORT_ERROR",
        ModuleNotFoundError("No module named 'fastmcp'"),
    )

    with pytest.raises(RuntimeError, match=r"mtgjson-sdk\[mcp\]"):
        mcp_server.create_mcp_server()


@pytest.mark.parametrize("argv", [[], ["--doctor", "both"]])
def test_main_prints_clean_missing_dependency_hint(monkeypatch, argv):
    import mtgjson_sdk.mcp.server as mcp_server

    monkeypatch.setattr(
        mcp_server,
        "_MCP_IMPORT_ERROR",
        ModuleNotFoundError("No module named 'fastmcp'"),
    )

    stderr = io.StringIO()
    with redirect_stderr(stderr):
        exit_code = mcp_server.main(argv)

    assert exit_code == 1
    assert "mtgjson-sdk[mcp]" in stderr.getvalue()
    assert "Traceback" not in stderr.getvalue()


def test_main_prints_version_without_loading_mcp_dependencies(monkeypatch, capsys):
    import mtgjson_sdk.mcp.server as mcp_server

    monkeypatch.setattr(
        mcp_server,
        "_MCP_IMPORT_ERROR",
        ModuleNotFoundError("No module named 'fastmcp'"),
    )

    with pytest.raises(SystemExit) as exc:
        mcp_server.main(["--version"])

    assert exc.value.code == 0
    captured = capsys.readouterr()
    assert captured.out.strip() == f"mtgjson-mcp {mcp_server._current_package_version()}"
    assert captured.err == ""


def test_top_level_mcp_server_shim_re_exports_public_api():
    import mtgjson_sdk.mcp.server as nested_mcp_server
    import mtgjson_sdk.mcp_server as shim_mcp_server

    assert shim_mcp_server.create_mcp_server is nested_mcp_server.create_mcp_server
    assert shim_mcp_server.MCPServerSettings is nested_mcp_server.MCPServerSettings
    assert shim_mcp_server.main is nested_mcp_server.main


SAMPLE_PRICE_HISTORY = [
    *SAMPLE_PRICE_DATA,
    {
        "uuid": "card-uuid-001",
        "source": "paper",
        "provider": "tcgplayer",
        "currency": "USD",
        "price_type": "retail",
        "finish": "normal",
        "date": "2024-01-02",
        "price": 1.75,
    },
]

SAMPLE_SKUS = [
    {
        "uuid": "card-uuid-001",
        "condition": "NEAR MINT",
        "language": "ENGLISH",
        "printing": "NON FOIL",
        "productId": 12345,
        "skuId": 555001,
        "finish": None,
    }
]

SAMPLE_CARD_RULINGS = [
    {
        "uuid": "card-uuid-001",
        "date": "2024-01-05",
        "text": "Lightning Bolt can target any target.",
    },
    {
        "uuid": "card-uuid-001",
        "date": "2024-01-06",
        "text": "Damage is dealt on resolution.",
    },
]

SAMPLE_CARD_PURCHASE_URLS = [
    {
        "uuid": "card-uuid-001",
        "cardKingdom": "https://example.invalid/cardkingdom/bolt",
        "tcgplayer": "https://example.invalid/tcgplayer/bolt",
    }
]

SAMPLE_SET_TRANSLATIONS = [
    {"code": "A25", "language": "French", "translation": "Maîtres 25"},
    {"code": "A25", "language": "German", "translation": "Masters 25 DE"},
]

SAMPLE_TOKEN_IDENTIFIERS = [
    {
        "uuid": "token-uuid-001",
        "scryfallId": "token-scryfall-001",
        "scryfallOracleId": "token-oracle-001",
        "scryfallIllustrationId": "token-illustration-001",
        "scryfallCardBackId": None,
        "mcmId": None,
        "mcmMetaId": None,
        "mtgArenaId": None,
        "mtgoId": None,
        "mtgoFoilId": None,
        "multiverseId": None,
        "tcgplayerProductId": None,
        "tcgplayerEtchedProductId": None,
        "tcgplayerAlternativeFoilProductId": None,
        "cardKingdomId": None,
        "cardKingdomFoilId": None,
        "cardKingdomEtchedId": None,
        "cardsphereId": None,
        "cardsphereFoilId": None,
        "deckboxId": None,
        "mtgjsonFoilVersionId": None,
        "mtgjsonNonFoilVersionId": None,
        "mtgjsonV4Id": "token-v4-001",
    }
]


def _create_sample_sdk(
    cache_dir: Path,
    *,
    set_rows: list[dict] | None = None,
    extra_tables: tuple[tuple[str, list[dict]], ...] = (),
) -> MtgjsonSDK:
    """Create an offline SDK seeded with the sample tables used in tests."""
    cache_dir.mkdir(parents=True, exist_ok=True)
    (cache_dir / "version.txt").write_text("5.0.0+test", encoding="utf-8")
    (cache_dir / JSON_FILES["meta"]).write_text(
        json.dumps({"data": {"version": "5.0.0+test", "date": "2026-03-19"}}),
        encoding="utf-8",
    )
    (cache_dir / JSON_FILES["keywords"]).write_text(
        json.dumps({"data": {"keywordAbilities": ["Flying", "Trample"]}}),
        encoding="utf-8",
    )
    (cache_dir / JSON_FILES["card_types"]).write_text(
        json.dumps({"data": {"creature": {"plural": "creatures"}}}),
        encoding="utf-8",
    )
    (cache_dir / JSON_FILES["enum_values"]).write_text(
        json.dumps(
            {
                "data": {
                    "card": {
                        "language": ["English", "French", "German"],
                        "rarity": ["common", "uncommon"],
                    },
                    "sealedProduct": {
                        "category": ["booster_box", "booster_pack"],
                    },
                }
            }
        ),
        encoding="utf-8",
    )

    sdk = MtgjsonSDK(cache_dir=cache_dir, offline=True)
    for table_name, rows in (
        ("cards", SAMPLE_CARDS),
        ("sets", set_rows or SAMPLE_SETS),
        ("tokens", SAMPLE_TOKENS),
        ("card_identifiers", SAMPLE_IDENTIFIERS),
        ("card_legalities", SAMPLE_LEGALITIES),
        ("card_foreign_data", SAMPLE_FOREIGN_DATA),
        ("tcgplayer_skus", SAMPLE_SKUS),
        ("card_rulings", SAMPLE_CARD_RULINGS),
        ("card_purchase_urls", SAMPLE_CARD_PURCHASE_URLS),
        ("set_translations", SAMPLE_SET_TRANSLATIONS),
        ("token_identifiers", SAMPLE_TOKEN_IDENTIFIERS),
        *extra_tables,
    ):
        sdk._conn.register_table_from_data(table_name, rows)
    return sdk


def _origin_from_mcp_url(url: str) -> str:
    parts = urlsplit(url)
    return f"{parts.scheme}://{parts.netloc}"


async def _collect_transport_probe(client: Client) -> dict[str, object]:
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
        "dataset_version": status.data["dataset_version"],
        "offline": status.data["offline"],
        "search_set_codes": [item["code"] for item in search_sets.data["items"]],
        "sql_rows": sql.data["rows"],
    }


@pytest.fixture
def mcp_test_server(tmp_path):
    """Build a FastMCP server backed by deterministic sample data."""
    cache_dir = tmp_path / "mcp-cache"
    settings = MCPServerSettings(cache_dir=cache_dir, offline=True)
    return create_mcp_server(
        settings=settings,
        sdk_factory=lambda: _create_sample_sdk(cache_dir),
    )


@pytest.mark.asyncio
async def test_resources_and_tools_in_memory(mcp_test_server):
    """Ensure key FastMCP resources and tools return sample data."""
    async with Client(mcp_test_server) as client:
        card_resource = await client.read_resource("mtgjson://cards/card-uuid-001")
        payload = json.loads(card_resource[0].text)
        assert payload["uuid"] == "card-uuid-001"
        assert payload["name"] == "Lightning Bolt"

        search_result = await client.call_tool(
            "search_cards",
            {"name": "Lightning Bolt", "limit": 5},
        )
        assert search_result.data["count"] == 1
        assert search_result.data["items"][0]["name"] == "Lightning Bolt"

        identifiers = await client.call_tool(
            "find_cards_by_identifier",
            {"id_type": "scryfall_id", "value": "scryfall-001"},
        )
        assert identifiers.data["count"] == 1
        assert identifiers.data["items"][0]["uuid"] == "card-uuid-001"

        legalities = await client.call_tool(
            "get_card_legalities",
            {"uuid": "card-uuid-001"},
        )
        assert legalities.data["legalities"]["modern"] == "Legal"

        sql_result = await client.call_tool(
            "execute_read_only_sql",
            {
                "query": "SELECT name FROM cards ORDER BY name",
                "max_rows": 2,
                "ensure_views": ["cards"],
            },
        )
        assert sql_result.data["returned_rows"] == 2
        assert [row["name"] for row in sql_result.data["rows"]] == [
            "Counterspell",
            "Fire // Ice",
        ]


@pytest.mark.asyncio
async def test_http_transport_smoke(tmp_path):
    """Smoke test the HTTP transport by hitting search_sets over Streamable HTTP."""
    cache_dir = tmp_path / "mcp-http-cache"
    settings = MCPServerSettings(cache_dir=cache_dir, offline=True)
    server = create_mcp_server(
        settings=settings,
        sdk_factory=lambda: _create_sample_sdk(cache_dir),
    )

    async with run_server_async(server) as url:
        async with Client(url) as client:
            response = await client.call_tool(
                "search_sets",
                {"name": "Masters", "limit": 1},
            )
            assert response.data["count"] == 1
            assert response.data["items"][0]["code"] == "A25"


@pytest.mark.asyncio
async def test_transport_parity_between_stdio_and_http(tmp_path):
    cache_dir = tmp_path / "mcp-parity-cache"
    _create_sample_sdk(cache_dir).close()
    tests_dir = Path(__file__).resolve().parent
    stdio_bootstrap = " ".join(
        [
            "from pathlib import Path;",
            "from mtgjson_sdk.mcp.server import (",
            "MCPServerSettings, create_mcp_server);",
            "from test_mcp_server import _create_sample_sdk;",
            f"cache_dir = Path(r'{cache_dir}');",
            "server = create_mcp_server(",
            "settings=MCPServerSettings(cache_dir=cache_dir, offline=True),",
            "sdk_factory=lambda: _create_sample_sdk(cache_dir)",
            ");",
            "server.run(transport='stdio')",
        ]
    )
    stdio_transport = {
        "mcpServers": {
            "mtgjson-local": {
                "type": "stdio",
                "command": sys.executable,
                "args": [
                    "-c",
                    stdio_bootstrap,
                ],
                "cwd": str(tests_dir),
            }
        }
    }
    http_server = create_mcp_server(
        settings=MCPServerSettings(cache_dir=cache_dir, offline=True),
        sdk_factory=lambda: _create_sample_sdk(cache_dir),
    )

    async with Client(stdio_transport, timeout=15, init_timeout=15) as stdio_client:
        stdio_probe = await _collect_transport_probe(stdio_client)

    async with run_server_async(http_server) as url:
        async with Client(url, timeout=15, init_timeout=15) as http_client:
            http_probe = await _collect_transport_probe(http_client)

    assert stdio_probe == http_probe


@pytest.mark.asyncio
async def test_mutating_sql_is_rejected(mcp_test_server):
    """The optional SQL escape hatch should reject mutating statements."""
    async with Client(mcp_test_server) as client:
        result = await client.call_tool(
            "execute_read_only_sql",
            {"query": "DELETE FROM cards"},
            raise_on_error=False,
        )

    assert result.is_error is True
    assert "Only read-only SELECT, WITH, SHOW, and DESCRIBE" in result.content[0].text


@pytest.mark.asyncio
async def test_execute_read_only_sql_accepts_json_string_params_and_views(tmp_path):
    cache_dir = tmp_path / "mcp-sql-cache"
    settings = MCPServerSettings(cache_dir=cache_dir, offline=True)
    server = create_mcp_server(
        settings=settings,
        sdk_factory=lambda: _create_sample_sdk(cache_dir),
    )

    async with Client(server) as client:
        result = await client.call_tool(
            "execute_read_only_sql",
            {
                "query": "SELECT code, name FROM sets WHERE code = $1",
                "params": '["A25"]',
                "ensure_views": '["sets"]',
                "max_rows": 1,
            },
        )

    assert result.data["returned_rows"] == 1
    assert result.data["rows"][0]["code"] == "A25"


@pytest.mark.asyncio
async def test_price_history_uses_today_snapshot_when_full_history_is_not_cached(
    tmp_path,
):
    cache_dir = tmp_path / "mcp-history-cache"
    settings = MCPServerSettings(cache_dir=cache_dir, offline=True)
    server = create_mcp_server(
        settings=settings,
        sdk_factory=lambda: _create_sample_sdk(
            cache_dir,
            extra_tables=(("all_prices_today", SAMPLE_PRICE_DATA),),
        ),
    )

    async with Client(server) as client:
        result = await client.call_tool(
            "get_price_history",
            {
                "uuid": "card-uuid-001",
                "provider": "tcgplayer",
                "finish": "normal",
                "price_type": "retail",
                "date_from": "2024-01-03",
                "date_to": "2024-01-03",
            },
        )

    assert result.data["history_available"] is True
    assert result.data["history_source"] == "all_prices_today"
    assert result.data["count"] == 1
    assert result.data["items"][0]["date"] == "2024-01-03"


@pytest.mark.asyncio
async def test_price_history_returns_guidance_when_full_history_is_not_cached(tmp_path):
    cache_dir = tmp_path / "mcp-history-guidance-cache"
    settings = MCPServerSettings(cache_dir=cache_dir, offline=True)
    server = create_mcp_server(
        settings=settings,
        sdk_factory=lambda: _create_sample_sdk(
            cache_dir,
            extra_tables=(("all_prices_today", SAMPLE_PRICE_DATA),),
        ),
    )

    async with Client(server) as client:
        result = await client.call_tool(
            "get_price_history",
            {
                "uuid": "card-uuid-001",
                "provider": "tcgplayer",
                "finish": "normal",
                "price_type": "retail",
                "date_from": "2024-01-01",
                "date_to": "2024-01-03",
            },
        )

    assert result.data["history_available"] is False
    assert result.data["requires_view"] == "all_prices"
    assert result.data["cache_status"]["status"] == "offline"
    assert "offline mode is enabled" in result.data["message"]


@pytest.mark.asyncio
async def test_price_history_handles_missing_today_view_without_crashing(tmp_path):
    cache_dir = tmp_path / "mcp-history-missing-today-cache"
    settings = MCPServerSettings(cache_dir=cache_dir, offline=True)
    server = create_mcp_server(
        settings=settings,
        sdk_factory=lambda: _create_sample_sdk(cache_dir),
    )

    async with Client(server) as client:
        result = await client.call_tool(
            "get_price_history",
            {"uuid": "card-uuid-001"},
        )

    assert result.data["history_available"] is False
    assert result.data["requires_view"] == "all_prices"
    assert result.data["latest_cached_date"] is None
    assert result.data["cache_status"]["status"] == "offline"


@pytest.mark.asyncio
async def test_price_surfaces_handle_missing_today_view_without_crashing(tmp_path):
    cache_dir = tmp_path / "mcp-price-missing-today-cache"
    settings = MCPServerSettings(cache_dir=cache_dir, offline=True)
    server = create_mcp_server(
        settings=settings,
        sdk_factory=lambda: _create_sample_sdk(cache_dir),
    )

    async with Client(server) as client:
        nested_prices = await client.call_tool(
            "get_card_prices",
            {"uuid": "card-uuid-001"},
        )
        latest_prices = await client.call_tool(
            "get_price_today",
            {"uuid": "card-uuid-001"},
        )
        trend = await client.call_tool(
            "get_price_trend",
            {"uuid": "card-uuid-001"},
        )
        cheapest = await client.call_tool(
            "find_cheapest_printing",
            {"name": "Lightning Bolt"},
        )
        extremes = await client.call_tool("list_price_extremes", {})
        price_resource = await client.read_resource("mtgjson://prices/card-uuid-001")

    assert nested_prices.data["price_data_available"] is False
    assert nested_prices.data["requires_view"] == "all_prices_today"
    assert latest_prices.data["price_data_available"] is False
    assert latest_prices.data["requires_view"] == "all_prices_today"
    assert trend.data["price_data_available"] is False
    assert trend.data["requires_view"] == "all_prices_today"
    assert cheapest.data["price_data_available"] is False
    assert cheapest.data["requires_view"] == "all_prices_today"
    assert extremes.data["price_data_available"] is False
    assert extremes.data["requires_view"] == "all_prices_today"

    resource_payload = json.loads(price_resource[0].text)
    assert resource_payload["price_data_available"] is False
    assert resource_payload["requires_view"] == "all_prices_today"


@pytest.mark.asyncio
async def test_sealed_tools_report_missing_heavy_source_without_timing_out(tmp_path):
    flat_sets = [
        {key: value for key, value in row.items() if key != "sealedProduct"}
        for row in SAMPLE_SETS
    ]
    cache_dir = tmp_path / "mcp-sealed-cache"
    settings = MCPServerSettings(cache_dir=cache_dir, offline=True)
    server = create_mcp_server(
        settings=settings,
        sdk_factory=lambda: _create_sample_sdk(cache_dir, set_rows=flat_sets),
    )

    async with Client(server) as client:
        list_result = await client.call_tool(
            "list_sealed_products",
            {"set_code": "A25"},
        )
        get_result = await client.call_tool(
            "get_sealed_product",
            {"uuid": "sealed-uuid-001"},
        )

    assert list_result.data["sealed_data_available"] is False
    assert list_result.data["requires_view"] == "all_printings"
    assert list_result.data["cache_status"]["status"] == "offline"
    assert get_result.data["sealed_data_available"] is False
    assert get_result.data["requires_view"] == "all_printings"


@pytest.mark.asyncio
async def test_sealed_tools_use_registered_all_printings_fallback_when_available(
    tmp_path,
):
    flat_sets = [
        {key: value for key, value in row.items() if key != "sealedProduct"}
        for row in SAMPLE_SETS
    ]
    cache_dir = tmp_path / "mcp-sealed-fallback-cache"
    settings = MCPServerSettings(cache_dir=cache_dir, offline=True)
    server = create_mcp_server(
        settings=settings,
        sdk_factory=lambda: _create_sample_sdk(
            cache_dir,
            set_rows=flat_sets,
            extra_tables=(("all_printings", SAMPLE_SETS),),
        ),
    )

    async with Client(server) as client:
        list_result = await client.call_tool(
            "list_sealed_products",
            {"set_code": "A25"},
        )
        get_result = await client.call_tool(
            "get_sealed_product",
            {"uuid": "sealed-uuid-001"},
        )

    assert list_result.data["sealed_data_available"] is True
    assert list_result.data["count"] == 2
    assert get_result.data["sealed_data_available"] is True
    assert get_result.data["found"] is True
    assert get_result.data["sealed_product"]["name"] == "Masters 25 Booster Box"


@pytest.mark.asyncio
async def test_sealed_tools_do_not_report_available_for_card_level_all_printings(
    tmp_path,
):
    flat_sets = [
        {key: value for key, value in row.items() if key != "sealedProduct"}
        for row in SAMPLE_SETS
    ]
    cache_dir = tmp_path / "mcp-sealed-live-shape-cache"
    settings = MCPServerSettings(cache_dir=cache_dir, offline=True)
    server = create_mcp_server(
        settings=settings,
        sdk_factory=lambda: _create_sample_sdk(
            cache_dir,
            set_rows=flat_sets,
            extra_tables=(("all_printings", SAMPLE_CARDS),),
        ),
    )

    async with Client(server) as client:
        list_result = await client.call_tool(
            "list_sealed_products",
            {"set_code": "A25"},
        )
        get_result = await client.call_tool(
            "get_sealed_product",
            {"uuid": "sealed-uuid-001"},
        )

    assert list_result.data["sealed_data_available"] is False
    assert list_result.data["count"] == 0
    assert (
        "does not expose set-level sealed product data" in list_result.data["message"]
    )
    assert get_result.data["sealed_data_available"] is False
    assert get_result.data["found"] is False
    assert "does not expose set-level sealed product data" in get_result.data["message"]


@pytest.mark.asyncio
async def test_normalized_identifier_and_sku_inputs_are_accepted(mcp_test_server):
    async with Client(mcp_test_server) as client:
        identifier_result = await client.call_tool(
            "find_cards_by_identifier",
            {"id_type": "scryfallId", "value": "scryfall-001"},
        )
        sku_result = await client.call_tool(
            "find_sku",
            {"lookup_type": "productId", "value": 12345},
        )
        status_result = await client.call_tool(
            "list_cards_by_format_status",
            {"format_name": "modern", "status": "Banned"},
        )

    assert identifier_result.data["count"] == 1
    assert identifier_result.data["normalized_params"]["id_type"] == "scryfall_id"
    assert sku_result.data["count"] == 1
    assert sku_result.data["normalized_params"]["lookup_type"] == "product_id"
    assert status_result.data["normalized_params"]["status"] == "banned"


@pytest.mark.asyncio
async def test_get_enums_supports_narrower_catalog_aliases(mcp_test_server):
    async with Client(mcp_test_server) as client:
        result = await client.call_tool(
            "get_enums",
            {"catalog": "languages"},
        )

    assert result.data["catalog"] == "languages"
    assert result.data["resolved_catalog"] == "enum_values"
    assert "English" in result.data["data"]


@pytest.mark.asyncio
async def test_describe_parameter_values_returns_alias_guidance(mcp_test_server):
    async with Client(mcp_test_server) as client:
        result = await client.call_tool(
            "describe_parameter_values",
            {
                "tool_name": "find_sku",
                "parameter_name": "lookup_type",
            },
        )

    assert result.data["found"] is True
    assert "productId" in result.data["reference"]["aliases"]
    assert "product_id" in result.data["reference"]["values"]


@pytest.mark.asyncio
async def test_hidden_view_tools_are_first_class(tmp_path):
    cache_dir = tmp_path / "mcp-hidden-tools-cache"
    settings = MCPServerSettings(cache_dir=cache_dir, offline=True)
    server = create_mcp_server(
        settings=settings,
        sdk_factory=lambda: _create_sample_sdk(cache_dir),
    )

    async with Client(server) as client:
        rulings = await client.call_tool("get_card_rulings", {"uuid": "card-uuid-001"})
        purchase_urls = await client.call_tool(
            "get_card_purchase_urls",
            {"uuid": "card-uuid-001"},
        )
        foreign_data = await client.call_tool(
            "get_card_foreign_data",
            {"uuid": "card-uuid-001", "language": "French"},
        )
        set_translations = await client.call_tool(
            "get_set_translations",
            {"code": "A25"},
        )
        token_identifiers = await client.call_tool(
            "get_token_identifiers",
            {"uuid": "token-uuid-001"},
        )

    assert rulings.data["count"] == 2
    assert purchase_urls.data["purchase_urls"]["tcgplayer"].endswith("/bolt")
    assert foreign_data.data["count"] == 1
    assert foreign_data.data["items"][0]["name"] == "Foudre"
    assert set_translations.data["count"] == 2
    assert token_identifiers.data["identifiers"]["scryfallId"] == "token-scryfall-001"


@pytest.mark.asyncio
async def test_workflow_tools_reduce_multi_hop_card_resolution(mcp_test_server):
    async with Client(mcp_test_server) as client:
        resolve_result = await client.call_tool(
            "resolve_card",
            {"name": "Lightning Bolt", "set_code": "A25"},
        )
        printings_result = await client.call_tool(
            "list_printings",
            {"name": "Lightning Bolt"},
        )
        bundle_result = await client.call_tool(
            "get_card_bundle",
            {"name": "Lightning Bolt", "set_code": "A25"},
        )
        market_snapshot = await client.call_tool(
            "get_card_market_snapshot",
            {"name": "Lightning Bolt", "set_code": "A25"},
        )

    assert resolve_result.data["found"] is True
    assert resolve_result.data["card"]["uuid"] == "card-uuid-001"
    assert printings_result.data["count"] == 1
    assert bundle_result.data["found"] is True
    assert bundle_result.data["bundle"]["card"]["uuid"] == "card-uuid-001"
    assert market_snapshot.data["found"] is True
    assert market_snapshot.data["market_snapshot"]["card"]["uuid"] == "card-uuid-001"


@pytest.mark.asyncio
async def test_search_by_external_id_auto_detects_matches(mcp_test_server):
    async with Client(mcp_test_server) as client:
        result = await client.call_tool(
            "search_by_external_id",
            {"value": "scryfall-001"},
        )

    assert result.data["count"] == 1
    assert result.data["items"][0]["matched_fields"] == ["scryfall_id"]


@pytest.mark.asyncio
async def test_price_trend_prefers_full_history_when_available(tmp_path):
    cache_dir = tmp_path / "mcp-price-trend-cache"
    settings = MCPServerSettings(cache_dir=cache_dir, offline=True)
    server = create_mcp_server(
        settings=settings,
        sdk_factory=lambda: _create_sample_sdk(
            cache_dir,
            extra_tables=(
                ("all_prices_today", SAMPLE_PRICE_DATA),
                ("all_prices", SAMPLE_PRICE_HISTORY),
            ),
        ),
    )

    async with Client(server) as client:
        trend = await client.call_tool(
            "get_price_trend",
            {
                "uuid": "card-uuid-001",
                "provider": "tcgplayer",
                "finish": "normal",
                "price_type": "retail",
            },
        )

    assert trend.data["trend_source"] == "all_prices"
    assert trend.data["trend"]["data_points"] == 3
    assert trend.data["trend"]["min_price"] == 1.5
    assert trend.data["trend"]["max_price"] == 2.0


@pytest.mark.asyncio
async def test_booster_tools_support_summary_and_hydration(tmp_path):
    booster_sets = [dict(row) for row in SAMPLE_SETS]
    booster_sets[0]["booster"] = {
        "draft": {
            "boosters": [{"contents": {"common": 1}, "weight": 1}],
            "boostersTotalWeight": 1,
            "sheets": {
                "common": {
                    "cards": {"card-uuid-001": 1, "card-uuid-002": 1},
                    "foil": False,
                    "totalWeight": 2,
                }
            },
            "sourceSetCodes": ["A25"],
        }
    }
    cache_dir = tmp_path / "mcp-booster-cache"
    settings = MCPServerSettings(cache_dir=cache_dir, offline=True)
    server = create_mcp_server(
        settings=settings,
        sdk_factory=lambda: _create_sample_sdk(cache_dir, set_rows=booster_sets),
    )

    async with Client(server) as client:
        box_result = await client.call_tool(
            "open_booster_box",
            {
                "set_code": "A25",
                "booster_type": "draft",
                "packs": 1,
                "summary": True,
            },
        )
        sheet_result = await client.call_tool(
            "get_booster_sheet_contents",
            {
                "set_code": "A25",
                "booster_type": "draft",
                "sheet_name": "common",
                "expand": True,
            },
        )

    assert "summary" in box_result.data
    assert "box" not in box_result.data
    assert "expanded_sheet" in sheet_result.data


@pytest.mark.asyncio
async def test_booster_tools_fall_back_when_flat_booster_tables_are_incomplete(
    tmp_path,
):
    booster_sets = [dict(row) for row in SAMPLE_SETS]
    booster_sets[0]["booster"] = {
        "draft": {
            "boosters": [{"contents": {"common": 1}, "weight": 1}],
            "boostersTotalWeight": 1,
            "sheets": {
                "common": {
                    "cards": {"card-uuid-001": 1, "card-uuid-002": 1},
                    "foil": False,
                    "totalWeight": 2,
                }
            },
            "sourceSetCodes": ["A25"],
        }
    }
    cache_dir = tmp_path / "mcp-booster-flat-fallback-cache"
    settings = MCPServerSettings(cache_dir=cache_dir, offline=True)
    server = create_mcp_server(
        settings=settings,
        sdk_factory=lambda: _create_sample_sdk(
            cache_dir,
            set_rows=booster_sets,
            extra_tables=(
                (
                    "set_booster_content_weights",
                    [
                        {
                            "setCode": "A25",
                            "boosterName": "draft",
                            "boosterIndex": 0,
                            "boosterWeight": 1,
                        }
                    ],
                ),
                (
                    "set_booster_contents",
                    [
                        {
                            "setCode": "A25",
                            "boosterName": "draft",
                            "boosterIndex": 0,
                            "sheetName": "common",
                            "sheetPicks": 1,
                        }
                    ],
                ),
                (
                    "set_booster_sheets",
                    [
                        {
                            "setCode": "A25",
                            "boosterName": "draft",
                            "sheetName": "common",
                            "sheetIsFoil": False,
                            "sheetHasBalanceColors": False,
                            "sheetTotalWeight": 2,
                        }
                    ],
                ),
                (
                    "set_booster_sheet_cards",
                    [
                        {
                            "setCode": "ZZZ",
                            "boosterName": "draft",
                            "sheetName": "common",
                            "cardUuid": "missing-card",
                            "cardWeight": 1,
                        }
                    ],
                ),
            ),
        ),
    )

    async with Client(server) as client:
        pack_result = await client.call_tool(
            "open_booster_pack",
            {
                "set_code": "A25",
                "booster_type": "draft",
            },
        )

    assert pack_result.data["count"] == 1
    assert pack_result.data["items"][0]["uuid"] in {"card-uuid-001", "card-uuid-002"}


@pytest.mark.asyncio
async def test_http_transport_supports_custom_mcp_path(tmp_path):
    cache_dir = tmp_path / "mcp-http-custom-path-cache"
    settings = MCPServerSettings(cache_dir=cache_dir, offline=True)
    server = create_mcp_server(
        settings=settings,
        sdk_factory=lambda: _create_sample_sdk(cache_dir),
    )

    async with run_server_async(server, path="/cards-mcp") as url:
        async with Client(url) as client:
            response = await client.call_tool(
                "search_sets",
                {"name": "Masters", "limit": 1},
            )
            assert response.data["count"] == 1
            assert response.data["items"][0]["code"] == "A25"


@pytest.mark.asyncio
async def test_health_and_ready_routes_are_exposed_over_http(tmp_path):
    cache_dir = tmp_path / "mcp-health-ready-cache"
    settings = MCPServerSettings(cache_dir=cache_dir, offline=True)
    server = create_mcp_server(
        settings=settings,
        sdk_factory=lambda: _create_sample_sdk(cache_dir),
    )

    async with run_server_async(server) as url:
        origin = _origin_from_mcp_url(url)
        async with httpx.AsyncClient(base_url=origin) as client:
            health = await client.get("/health")
            ready = await client.get("/ready")

    assert health.status_code == 200
    assert health.json()["status"] == "ok"
    assert ready.status_code == 200
    ready_payload = ready.json()
    assert ready_payload["status"] == "ready"
    assert "required_views" in ready_payload


@pytest.mark.asyncio
async def test_ready_route_is_not_ready_when_required_views_missing(tmp_path):
    cache_dir = tmp_path / "mcp-ready-missing-cache"
    settings = MCPServerSettings(
        cache_dir=cache_dir,
        offline=True,
        warm_profile="prices",
    )
    server = create_mcp_server(
        settings=settings,
        sdk_factory=lambda: _create_sample_sdk(cache_dir),
    )

    async with run_server_async(server) as url:
        origin = _origin_from_mcp_url(url)
        async with httpx.AsyncClient(base_url=origin) as client:
            ready = await client.get("/ready")

    assert ready.status_code == 503
    payload = ready.json()
    assert payload["status"] == "warming"
    assert payload["warm_profile"] == "prices"
    assert payload["ready"] is False
    assert payload["missing_views"]


@pytest.mark.asyncio
async def test_warm_profile_prefetches_views_during_startup(tmp_path):
    cache_dir = tmp_path / "mcp-warm-startup-cache"
    warmed_views: list[str] = []
    sdk = _create_sample_sdk(cache_dir)
    original_prefetch = sdk._cache.prefetch_parquet

    def _track_prefetch(view_name: str) -> dict:
        warmed_views.append(view_name)
        return original_prefetch(view_name)

    sdk._cache.prefetch_parquet = _track_prefetch
    settings = MCPServerSettings(
        cache_dir=cache_dir,
        offline=True,
        warm_profile="base",
    )
    server = create_mcp_server(settings=settings, sdk_factory=lambda: sdk)

    async with Client(server):
        pass

    assert warmed_views
    assert set(warmed_views) == {
        "card_identifiers",
        "card_legalities",
        "cards",
        "sets",
        "tokens",
    }


@pytest.mark.asyncio
async def test_get_server_status_exposes_warm_profile_and_progress(tmp_path):
    cache_dir = tmp_path / "mcp-status-warm-profile-cache"
    settings = MCPServerSettings(
        cache_dir=cache_dir,
        offline=True,
        warm_profile="base",
    )
    server = create_mcp_server(
        settings=settings,
        sdk_factory=lambda: _create_sample_sdk(cache_dir),
    )

    async with Client(server) as client:
        status = await client.call_tool("get_server_status", {})

    assert status.data["warm_profile"] == "base"
    assert set(status.data["required_views"]) == {
        "card_identifiers",
        "card_legalities",
        "cards",
        "sets",
        "tokens",
    }
    assert isinstance(status.data["download_progress"], dict)


@pytest.mark.asyncio
async def test_sdk_factory_progress_callback_is_chained_into_server_status(tmp_path):
    cache_dir = tmp_path / "mcp-progress-chain-cache"
    sdk = _create_sample_sdk(cache_dir)
    delegated_calls: list[tuple[str, int, int | None]] = []
    sdk._cache._on_progress = (
        lambda filename, downloaded, total: delegated_calls.append(
            (filename, downloaded, total)
        )
    )
    server = create_mcp_server(
        settings=MCPServerSettings(cache_dir=cache_dir, offline=True),
        sdk_factory=lambda: sdk,
    )

    async with Client(server) as client:
        assert sdk._cache._on_progress is not None
        sdk._cache._on_progress("Meta.json", 10, 20)
        status = await client.call_tool("get_server_status", {})

    assert delegated_calls == [("Meta.json", 10, 20)]
    assert status.data["download_progress"]["Meta.json"]["downloaded_bytes"] == 10
    assert status.data["download_progress"]["Meta.json"]["total_bytes"] == 20


def test_create_asgi_app_forwards_path_and_stateless_http(monkeypatch):
    import mtgjson_sdk.mcp.server as mcp_server

    seen: dict[str, object] = {}

    class _FakeServer:
        def http_app(self, **kwargs):
            seen.update(kwargs)
            return {"ok": True}

    monkeypatch.setattr(
        mcp_server,
        "create_mcp_server",
        lambda *args, **kwargs: _FakeServer(),
    )

    app = mcp_server.create_asgi_app(path="embedded", stateless_http=True)

    assert app == {"ok": True}
    assert seen["path"] == "/embedded"
    assert seen["stateless_http"] is True


def test_create_asgi_app_uses_settings_stateless_http_by_default(monkeypatch):
    import mtgjson_sdk.mcp.server as mcp_server

    seen: dict[str, object] = {}

    class _FakeServer:
        def http_app(self, **kwargs):
            seen.update(kwargs)
            return {"ok": True}

    monkeypatch.setattr(
        mcp_server,
        "create_mcp_server",
        lambda *args, **kwargs: _FakeServer(),
    )

    app = mcp_server.create_asgi_app(
        settings=mcp_server.MCPServerSettings(stateless_http=True),
        path="embedded",
    )

    assert app == {"ok": True}
    assert seen["path"] == "/embedded"
    assert seen["stateless_http"] is True


@pytest.mark.asyncio
async def test_mounted_asgi_app_serves_health_ready_and_tools(tmp_path):
    import mtgjson_sdk.mcp.server as mcp_server

    cache_dir = tmp_path / "mcp-mounted-asgi-cache"
    settings = MCPServerSettings(cache_dir=cache_dir, offline=True)
    mcp_app = mcp_server.create_asgi_app(
        settings=settings,
        sdk_factory=lambda: _create_sample_sdk(cache_dir),
        path="/",
    )
    root_app = Starlette(lifespan=mcp_app.lifespan, routes=[Mount("/mcp", mcp_app)])

    with TestClient(root_app, base_url="http://testserver") as http_client:
        health = http_client.get("/mcp/health")
        ready = http_client.get("/mcp/ready")
        mcp_rpc = http_client.post(
            "/mcp/",
            json={
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/list",
                "params": {},
            },
        )

    assert health.status_code == 200
    assert ready.status_code == 200
    assert mcp_rpc.status_code != 404


def test_main_http_passes_stateless_http_flag(monkeypatch):
    import mtgjson_sdk.mcp.server as mcp_server

    run_kwargs: dict[str, object] = {}

    class _FakeServer:
        def run(self, **kwargs):
            run_kwargs.update(kwargs)

    monkeypatch.setattr(
        mcp_server,
        "create_mcp_server",
        lambda *args, **kwargs: _FakeServer(),
    )

    exit_code = mcp_server.main(
        [
            "--transport",
            "http",
            "--path",
            "custom",
            "--stateless-http",
        ]
    )

    assert exit_code == 0
    assert run_kwargs["transport"] == "http"
    assert run_kwargs["path"] == "/custom"
    assert run_kwargs["stateless_http"] is True


def test_resolve_cli_defaults_uses_stdio_when_transport_is_unspecified(monkeypatch):
    import mtgjson_sdk.mcp.server as mcp_server

    for env_var in (
        mcp_server.ENV_TRANSPORT,
        mcp_server.ENV_HOST,
        mcp_server.ENV_PORT,
        mcp_server.ENV_PATH,
        mcp_server.ENV_WARM_PROFILE,
        mcp_server.ENV_STATELESS_HTTP,
        mcp_server.ENV_CACHE_DIR,
        mcp_server.ENV_OFFLINE,
        mcp_server.ENV_TIMEOUT,
        mcp_server.ENV_LOG_LEVEL,
    ):
        monkeypatch.delenv(env_var, raising=False)

    args = mcp_server.build_arg_parser().parse_args([])
    resolved = mcp_server._resolve_cli_defaults(args)

    assert resolved.transport == "stdio"
    assert resolved.host == mcp_server.DEFAULT_HTTP_HOST
    assert resolved.path == mcp_server.DEFAULT_HTTP_PATH


def test_main_doctor_short_circuits_server_run(monkeypatch):
    import mtgjson_sdk.mcp.server as mcp_server

    monkeypatch.setattr(
        mcp_server,
        "_run_transport_doctor_sync",
        lambda args: 17,
    )
    monkeypatch.setattr(
        mcp_server,
        "create_mcp_server",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("unreachable")),
    )

    exit_code = mcp_server.main(["--doctor", "both"])

    assert exit_code == 17


def test_main_reads_docker_friendly_env_defaults(monkeypatch, tmp_path):
    import mtgjson_sdk.mcp.server as mcp_server

    seen: dict[str, object] = {}
    cache_dir = tmp_path / "env-cache"

    class _FakeServer:
        def run(self, **kwargs):
            seen["run_kwargs"] = kwargs

    def _fake_create_mcp_server(settings):
        seen["settings"] = settings
        return _FakeServer()

    monkeypatch.setenv("MTGJSON_MCP_TRANSPORT", "http")
    monkeypatch.setenv("MTGJSON_MCP_CACHE_DIR", str(cache_dir))
    monkeypatch.setenv("MTGJSON_MCP_OFFLINE", "true")
    monkeypatch.setenv("MTGJSON_MCP_TIMEOUT", "45")
    monkeypatch.setenv("MTGJSON_MCP_WARM_PROFILE", "prices")
    monkeypatch.setenv("MTGJSON_MCP_PORT", "8765")
    monkeypatch.setenv("MTGJSON_MCP_PATH", "cards-mcp")
    monkeypatch.setenv("MTGJSON_MCP_STATELESS_HTTP", "true")
    monkeypatch.setenv("MTGJSON_MCP_LOG_LEVEL", "INFO")
    monkeypatch.setattr(mcp_server, "create_mcp_server", _fake_create_mcp_server)

    exit_code = mcp_server.main([])

    assert exit_code == 0
    settings = seen["settings"]
    assert isinstance(settings, mcp_server.MCPServerSettings)
    assert settings.cache_dir == cache_dir
    assert settings.offline is True
    assert settings.timeout == 45.0
    assert settings.warm_profile == "prices"
    assert settings.stateless_http is True

    run_kwargs = seen["run_kwargs"]
    assert run_kwargs["transport"] == "http"
    assert run_kwargs["host"] == "0.0.0.0"
    assert run_kwargs["port"] == 8765
    assert run_kwargs["path"] == "/cards-mcp"
    assert run_kwargs["stateless_http"] is True


def test_main_prefers_explicit_http_host_env_over_container_default(
    monkeypatch, tmp_path
):
    import mtgjson_sdk.mcp.server as mcp_server

    seen: dict[str, object] = {}
    cache_dir = tmp_path / "env-host-cache"

    class _FakeServer:
        def run(self, **kwargs):
            seen["run_kwargs"] = kwargs

    monkeypatch.setenv("MTGJSON_MCP_TRANSPORT", "http")
    monkeypatch.setenv("MTGJSON_MCP_HOST", "127.0.0.9")
    monkeypatch.setenv("MTGJSON_MCP_PORT", "8123")
    monkeypatch.setenv("MTGJSON_MCP_PATH", "/env-mcp")
    monkeypatch.setenv("MTGJSON_MCP_CACHE_DIR", str(cache_dir))
    monkeypatch.setattr(
        mcp_server,
        "create_mcp_server",
        lambda settings: (seen.setdefault("settings", settings), _FakeServer())[1],
    )

    exit_code = mcp_server.main([])

    assert exit_code == 0
    run_kwargs = seen["run_kwargs"]
    assert run_kwargs["transport"] == "http"
    assert run_kwargs["host"] == "127.0.0.9"
    assert run_kwargs["port"] == 8123
    assert run_kwargs["path"] == "/env-mcp"


def test_main_cli_flags_override_env_defaults(monkeypatch, tmp_path):
    import mtgjson_sdk.mcp.server as mcp_server

    seen: dict[str, object] = {}
    env_cache_dir = tmp_path / "env-cache"
    cli_cache_dir = tmp_path / "cli-cache"

    class _FakeServer:
        def run(self, **kwargs):
            seen["run_kwargs"] = kwargs

    def _fake_create_mcp_server(settings):
        seen["settings"] = settings
        return _FakeServer()

    monkeypatch.setenv("MTGJSON_MCP_TRANSPORT", "http")
    monkeypatch.setenv("MTGJSON_MCP_HOST", "10.0.0.5")
    monkeypatch.setenv("MTGJSON_MCP_PORT", "9000")
    monkeypatch.setenv("MTGJSON_MCP_PATH", "env-path")
    monkeypatch.setenv("MTGJSON_MCP_CACHE_DIR", str(env_cache_dir))
    monkeypatch.setenv("MTGJSON_MCP_TIMEOUT", "90")
    monkeypatch.setenv("MTGJSON_MCP_WARM_PROFILE", "prices")
    monkeypatch.setenv("MTGJSON_MCP_LOG_LEVEL", "ERROR")
    monkeypatch.setenv("MTGJSON_MCP_STATELESS_HTTP", "true")
    monkeypatch.setattr(mcp_server, "create_mcp_server", _fake_create_mcp_server)

    exit_code = mcp_server.main(
        [
            "--transport",
            "http",
            "--host",
            "127.0.0.7",
            "--port",
            "7000",
            "--path",
            "cli-path",
            "--cache-dir",
            str(cli_cache_dir),
            "--timeout",
            "5",
            "--warm-profile",
            "base",
            "--log-level",
            "DEBUG",
        ]
    )

    assert exit_code == 0
    settings = seen["settings"]
    assert settings.cache_dir == cli_cache_dir
    assert settings.timeout == 5.0
    assert settings.warm_profile == "base"
    assert settings.stateless_http is True

    run_kwargs = seen["run_kwargs"]
    assert run_kwargs["transport"] == "http"
    assert run_kwargs["host"] == "127.0.0.7"
    assert run_kwargs["port"] == 7000
    assert run_kwargs["path"] == "/cli-path"
    assert run_kwargs["stateless_http"] is True
