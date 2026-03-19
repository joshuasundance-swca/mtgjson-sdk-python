# mtgjson-sdk

[![PyPI](https://img.shields.io/pypi/v/mtgjson-sdk)](https://pypi.org/project/mtgjson-sdk/)
[![Python](https://img.shields.io/pypi/pyversions/mtgjson-sdk)](https://pypi.org/project/mtgjson-sdk/)
[![License](https://img.shields.io/pypi/l/mtgjson-sdk)](https://pypi.org/project/mtgjson-sdk/)

A high-performance, DuckDB-backed Python query client for [MTGJSON](https://mtgjson.com).

Unlike traditional SDKs that rely on rate-limited REST APIs, `mtgjson-sdk` implements a local data warehouse architecture. It synchronizes optimized Parquet data from the MTGJSON CDN to your local machine, utilizing DuckDB to execute complex analytics, fuzzy searches, and booster simulations with sub-millisecond latency.

## Key Features

*   **Vectorized Execution**: Powered by DuckDB for high-speed OLAP queries on the full MTG dataset.
*   **Offline-First**: Data is cached locally, allowing for full functionality without an active internet connection.
*   **Fuzzy Search**: Built-in Jaro-Winkler similarity matching to handle typos and approximate name lookups.
*   **Data Science Integration**: Native support for Polars DataFrames and Arrow-based zero-copy data transfer.
*   **Fully Async**: Thread-safe async wrapper designed for high-concurrency environments like FastAPI or Discord bots.
*   **Booster Simulation**: Accurate pack opening logic using official MTGJSON weights and sheet configurations.

## Install

Core SDK only:

```bash
pip install mtgjson-sdk
```

Install extras when you need additional features:

```bash
pip install mtgjson-sdk[mcp]      # MCP server / FastMCP support
pip install mtgjson-sdk[polars]   # Polars DataFrame support
pip install mtgjson-sdk[all]      # All optional runtime extras (mcp + polars + orjson)
```

Quick install guide:

```text
SDK only            -> mtgjson-sdk
SDK + MCP server    -> mtgjson-sdk[mcp]
SDK + Polars        -> mtgjson-sdk[polars]
Everything runtime  -> mtgjson-sdk[all]
```

## Quick Start

```python
from mtgjson_sdk import MtgjsonSDK

with MtgjsonSDK() as sdk:
    # Search for cards (returns Pydantic models)
    bolts = sdk.cards.search(name="Lightning Bolt")
    print(f"Found {len(bolts)} printings of Lightning Bolt")

    # Get set metadata
    mh3 = sdk.sets.get("MH3")
    print(f"{mh3.name} -- {mh3.totalSetSize} cards")

    # Check format legality
    if bolts:
        print(f"Modern legal: {sdk.legalities.is_legal(bolts[0].uuid, 'modern')}")

    # Find the cheapest printing
    cheapest = sdk.prices.cheapest_printing("Lightning Bolt")
    if cheapest:
        print(f"Cheapest: ${cheapest['price']} ({cheapest['setCode']})")

    # Performance tip: use as_dataframe=True for bulk analysis (1000+ rows)
    df = sdk.cards.search(set_code="MH3", as_dataframe=True)

    # Execute raw SQL with parameter binding
    rows = sdk.sql("SELECT name FROM cards WHERE manaValue = $1 LIMIT 5", [0])
```

## Architecture

By using DuckDB, the SDK leverages columnar storage and vectorized execution, making it significantly faster than SQLite or standard JSON parsing for MTG's relational dataset.

1.  **Synchronization**: On first use, the SDK lazily downloads Parquet and JSON files from the MTGJSON CDN to a platform-specific cache directory (`~/.cache/mtgjson-sdk` on Linux, `~/Library/Caches/mtgjson-sdk` on macOS, `AppData/Local/mtgjson-sdk` on Windows).
2.  **Virtual Schema**: DuckDB views are registered on-demand. Accessing `sdk.cards` registers the card view; accessing `sdk.prices` registers price data. You only pay the memory cost for the data you query.
3.  **Dynamic Adaptation**: The SDK introspects Parquet metadata to automatically handle schema changes, plural-column array conversion, and format legality unpivoting.
4.  **Materialization**: Queries return validated Pydantic models for individual record ergonomics, or Polars DataFrames for bulk processing.

## Use Cases

### Price Analytics

```python
with MtgjsonSDK() as sdk:
    # Find the cheapest printing of a card by name
    cheapest = sdk.prices.cheapest_printing("Ragavan, Nimble Pilferer")

    # Aggregate statistics (min, max, avg) for a specific card
    trend = sdk.prices.price_trend(
        cheapest["uuid"], provider="tcgplayer", finish="normal"
    )
    print(f"Range: ${trend['min_price']} - ${trend['max_price']}")
    print(f"Average: ${trend['avg_price']} over {trend['data_points']} data points")

    # Historical price lookup with date filtering
    history = sdk.prices.history(
        cheapest["uuid"],
        provider="tcgplayer",
        date_from="2024-01-01",
        date_to="2024-12-31",
    )

    # Top 10 most expensive printings across the entire dataset
    priciest = sdk.prices.most_expensive_printings(limit=10)
```

### Advanced Card Search

The `search()` method supports ~20 composable filters that can be combined freely:

```python
with MtgjsonSDK() as sdk:
    # Complex filters: Modern-legal red creatures with CMC <= 2
    aggro = sdk.cards.search(
        colors=["R"],
        types="Creature",
        mana_value_lte=2.0,
        legal_in="modern",
        limit=50,
    )

    # Typo-tolerant fuzzy search (Jaro-Winkler similarity)
    results = sdk.cards.search(fuzzy_name="Ligtning Bolt")  # still finds it

    # Rules text search using regular expressions
    burn = sdk.cards.search(text_regex=r"deals? \d+ damage to any target")

    # Search by keyword ability across formats
    flyers = sdk.cards.search(keyword="Flying", colors=["W", "U"], legal_in="standard")

    # Find cards by foreign-language name
    blitz = sdk.cards.search(localized_name="Blitzschlag")  # German for Lightning Bolt
```

<details>
<summary>All <code>search()</code> parameters</summary>

| Parameter | Type | Description |
|---|---|---|
| `name` | `str` | Name pattern (`%` = wildcard) |
| `fuzzy_name` | `str` | Typo-tolerant Jaro-Winkler match |
| `localized_name` | `str` | Foreign-language name search |
| `colors` | `list[str]` | Cards containing these colors |
| `color_identity` | `list[str]` | Color identity filter |
| `legal_in` | `str` | Format legality |
| `rarity` | `str` | Rarity filter |
| `mana_value` | `float` | Exact mana value |
| `mana_value_lte` | `float` | Mana value upper bound |
| `mana_value_gte` | `float` | Mana value lower bound |
| `text` | `str` | Rules text substring |
| `text_regex` | `str` | Rules text regex |
| `types` | `str` | Type line search |
| `artist` | `str` | Artist name |
| `keyword` | `str` | Keyword ability |
| `is_promo` | `bool` | Promo status |
| `availability` | `str` | `"paper"` or `"mtgo"` |
| `language` | `str` | Language filter |
| `layout` | `str` | Card layout |
| `set_code` | `str` | Set code |
| `set_type` | `str` | Set type (joins sets table) |
| `power` | `str` | Power filter |
| `toughness` | `str` | Toughness filter |
| `limit` / `offset` | `int` | Pagination |
| `as_dataframe` | `bool` | Return Polars DataFrame |

</details>

### Collection & Cross-Reference

```python
with MtgjsonSDK() as sdk:
    # Cross-reference by any external ID system
    cards = sdk.identifiers.find_by_scryfall_id("f7a21fe4-...")
    cards = sdk.identifiers.find_by_tcgplayer_id("12345")
    cards = sdk.identifiers.find_by_mtgo_id("67890")

    # Get all external identifiers for a card
    all_ids = sdk.identifiers.get_identifiers("card-uuid-here")
    # -> Scryfall, TCGPlayer, MTGO, Arena, Cardmarket, Card Kingdom, Cardsphere, ...

    # TCGPlayer SKU variants (foil, etched, etc.)
    skus = sdk.skus.get("card-uuid-here")

    # Export to a standalone DuckDB file for offline analysis
    sdk.export_db("my_collection.duckdb")
    # Now query with: duckdb my_collection.duckdb "SELECT * FROM cards LIMIT 5"
```

### Booster Simulation

```python
with MtgjsonSDK() as sdk:
    # See available booster types for a set
    types = sdk.booster.available_types("MH3")  # ["draft", "collector", ...]

    # Open a single draft pack using official set weights
    pack = sdk.booster.open_pack("MH3", "draft")
    for card in pack:
        print(f"  {card.name} ({card.rarity})")

    # Simulate opening a full box (36 packs)
    box = sdk.booster.open_box("MH3", "draft", packs=36)
    print(f"Opened {len(box)} packs, {sum(len(p) for p in box)} total cards")
```

## API Reference

### Core Data

```python
# Cards
sdk.cards.get_by_uuid("uuid")               # single card lookup
sdk.cards.get_by_uuids(["uuid1", "uuid2"])  # batch lookup
sdk.cards.get_by_name("Lightning Bolt")     # all printings of a name
sdk.cards.search(...)                       # composable filters (see above)
sdk.cards.get_printings("Lightning Bolt")   # all printings across sets
sdk.cards.get_atomic("Lightning Bolt")      # oracle data (no printing info)
sdk.cards.find_by_scryfall_id("...")        # cross-reference shortcut
sdk.cards.random(5)                         # random cards
sdk.cards.count()                           # total (or filtered with kwargs)

# Tokens
sdk.tokens.get_by_uuid("uuid")
sdk.tokens.get_by_name("Soldier Token")
sdk.tokens.search(name="%Token", set_code="MH3", colors=["W"])
sdk.tokens.for_set("MH3")

# Sets
sdk.sets.get("MH3")
sdk.sets.list(set_type="expansion")
sdk.sets.search(name="Horizons", release_year=2024)
```

### Playability

```python
# Legalities
sdk.legalities.formats_for_card("uuid")    # -> {"modern": "Legal", ...}
sdk.legalities.legal_in("modern")          # all modern-legal cards
sdk.legalities.is_legal("uuid", "modern")  # -> bool
sdk.legalities.banned_in("modern")         # also: restricted_in, suspended_in

# Decks & Sealed Products
sdk.decks.list(set_code="MH3")
sdk.decks.search(name="Eldrazi")
sdk.sealed.list(set_code="MH3")
sdk.sealed.get("uuid")
```

### Market & Identifiers

```python
# Prices
sdk.prices.get("uuid")                     # full nested price data
sdk.prices.today("uuid", provider="tcgplayer", finish="foil")
sdk.prices.history("uuid", provider="tcgplayer", date_from="2024-01-01")
sdk.prices.price_trend("uuid", provider="tcgplayer", finish="normal")
sdk.prices.cheapest_printing("Lightning Bolt")
sdk.prices.most_expensive_printings(limit=10)

# Identifiers (supports all major external ID systems)
sdk.identifiers.find_by_scryfall_id("...")
sdk.identifiers.find_by_tcgplayer_id("...")
sdk.identifiers.find_by_mtgo_id("...")
sdk.identifiers.find_by_mtg_arena_id("...")
sdk.identifiers.find_by_multiverse_id("...")
sdk.identifiers.find_by_mcm_id("...")
sdk.identifiers.find_by_card_kingdom_id("...")
sdk.identifiers.find_by("scryfallId", "...")  # generic lookup
sdk.identifiers.get_identifiers("uuid")       # all IDs for a card

# SKUs
sdk.skus.get("uuid")
sdk.skus.find_by_sku_id(123456)
sdk.skus.find_by_product_id(789)
```

### Booster & Enums

```python
sdk.booster.available_types("MH3")
sdk.booster.open_pack("MH3", "draft")
sdk.booster.open_box("MH3", packs=36)
sdk.booster.sheet_contents("MH3", "draft", "common")

sdk.enums.keywords()
sdk.enums.card_types()
sdk.enums.enum_values()
```

### System

```python
sdk.meta                                   # version and build date
sdk.views                                  # registered view names
sdk.refresh()                              # check CDN for new data -> bool
sdk.export_db("output.duckdb")             # export to persistent DuckDB file
sdk.sql(query, params)                     # raw parameterized SQL
sdk.close()                                # release resources
```

## Performance and Memory

When querying large datasets (thousands of cards), avoid returning Pydantic models. Instantiating tens of thousands of Python objects is CPU and memory intensive.

```python
# Returns a Polars DataFrame (zero-copy memory handoff from DuckDB)
df = sdk.cards.search(name="%", as_dataframe=True)

# Analysis runs in C++/Rust via Polars -- not Python
avg_cmc = df.select(pl.col("manaValue").mean())
```

## Advanced Usage

### Async Frameworks (FastAPI / Discord.py)

`AsyncMtgjsonSDK` wraps the sync client in a thread pool executor, making it safe to use from async frameworks without blocking the event loop. DuckDB releases the GIL during query execution, so thread pool concurrency works well.

```python
from mtgjson_sdk import AsyncMtgjsonSDK

async with AsyncMtgjsonSDK(max_workers=4) as sdk:
    cards = await sdk.run(sdk.inner.cards.search, name="Lightning%")
    count = await sdk.sql("SELECT COUNT(*) FROM cards")
```

### Auto-Refresh for Long-Running Services

```python
# In a scheduled task or health check:
if sdk.refresh():
    print("New MTGJSON data detected -- cache refreshed")
```

### Custom Cache Directory & Progress

```python
from pathlib import Path

def on_progress(filename: str, downloaded: int, total: int):
    pct = (downloaded / total * 100) if total else 0
    print(f"\r{filename}: {pct:.1f}%", end="", flush=True)

sdk = MtgjsonSDK(
    cache_dir=Path("/data/mtgjson-cache"),
    timeout=300.0,
    on_progress=on_progress,
)
```

### Raw SQL

All user input goes through DuckDB parameter binding (`$1`, `$2`, ...):

```python
with MtgjsonSDK() as sdk:
    # Ensure views are registered before querying
    _ = sdk.cards.count()

    # Parameterized queries
    rows = sdk.sql(
        "SELECT name, setCode, rarity FROM cards WHERE manaValue <= $1 AND rarity = $2",
        [2, "mythic"],
    )
```

## MCP Server

The project ships with a FastMCP server so agents can use the SDK over either `stdio` or HTTP.
`fastmcp` is optional and is not installed with the base SDK.
Install the MCP extra first:

```bash
pip install mtgjson-sdk[mcp]
```

If you try to run `mtgjson-mcp` without the extra, the server exits with an install hint.

### Start over stdio

```bash
uv run mtgjson-mcp
```

Example MCP client configuration:

```json
{
  "mcpServers": {
    "mtgjson": {
      "command": "uv",
      "args": ["run", "mtgjson-mcp"]
    }
  }
}
```

### VS Code workspace `.vscode/mcp.json`

VS Code workspace MCP configuration uses a different top-level shape than the
generic `mcpServers` example above. For local development, the least fragile
Windows setup is usually to point directly at the virtualenv executable.

```json
{
    "servers": {
        "mtgjson-local-stdio": {
            "type": "stdio",
            "command": "${workspaceFolder}\\.venv\\Scripts\\mtgjson-mcp.exe",
            "args": ["--log-level", "DEBUG"]
        },
        "mtgjson-local-http": {
            "type": "http",
            "url": "http://127.0.0.1:8765/mcp"
        }
    }
}
```

Use the `stdio` entry when you want VS Code to launch the server for you.
Use the `http` entry only if you already started `mtgjson-mcp --transport http`
separately.

If you prefer not to reference the virtualenv directly, this is a more portable
fallback:

```json
{
    "servers": {
        "mtgjson": {
            "type": "stdio",
            "command": "uv",
            "args": ["run", "mtgjson-mcp"]
        }
    }
}
```

The workspace `.vscode/mcp.json` file is a local editor convenience file. This
repository ignores `.vscode/` by default, so that file is normally not committed.

### Start over HTTP

```bash
uv run mtgjson-mcp --transport http --host 127.0.0.1 --port 8000 --path /mcp
```

The MCP endpoint is served at `/mcp`.
The server also exposes:

* `GET /health` for liveness (`status=ok` when the process is up)
* `GET /ready` for readiness (returns HTTP `200` only when required warm views are ready)

### Choosing stdio vs HTTP in clients

`stdio` is usually the best choice for local editor integrations. The client is
responsible for starting `mtgjson-mcp` as a local child process and passing a
command plus arguments.

`http` is usually the best choice when the server should run separately, be
shared across tools, or be hosted behind an ASGI/web deployment. In that model,
you start the server first and point clients at the MCP URL:

```bash
uv run mtgjson-mcp --transport http --host 127.0.0.1 --port 8000 --path /mcp
```

Different clients use different config shapes even when they connect to the same
server:

```text
stdio
- Best for local editor integrations.
- Client config usually needs a command plus args.
- The client owns process startup and shutdown.

HTTP
- Best when the server should run separately or be shared across tools.
- Client config usually needs only a URL.
- You start the server yourself.
```

Common config differences by client:

* VS Code workspace MCP uses `servers` and typically `"type": "stdio"` or `"type": "http"`.
* Generic MCP config files and MCP Inspector examples usually use `mcpServers`.
* Some clients call HTTP transport `http`, while others call it `streamable-http`.
    The URL is the same; only the config label changes.

Example generic HTTP config for clients that expect `mcpServers`:

```json
{
    "mcpServers": {
        "mtgjson-http": {
            "type": "streamable-http",
            "url": "http://127.0.0.1:8000/mcp"
        }
    }
}
```

### Warm Profiles and Readiness

Use a warm profile to make cold-start behavior predictable and to drive `/ready`:

```bash
# Warm core card/set/token views in the background during startup.
uv run mtgjson-mcp --transport http --warm-profile base

# Warm core + price views and use readiness to gate traffic.
uv run mtgjson-mcp --transport http --warm-profile prices
```

Supported profiles are `base`, `prices`, and `full`.
If no warm profile is set, `/ready` reports ready immediately (liveness-only mode).
Long-running admin tools (`prefetch_views`, `export_duckdb`) emit MCP progress updates, and
`get_server_status` / `mtgjson://server/config` include `download_progress` snapshots.

### Useful options

```bash
# Use an existing cache directory
uv run mtgjson-mcp --cache-dir ./data/mtgjson-cache

# Run without CDN access
uv run mtgjson-mcp --offline

# Enable stateless Streamable HTTP mode for multi-instance deployments
uv run mtgjson-mcp --transport http --stateless-http

# Show FastMCP's startup banner and raise log verbosity
uv run mtgjson-mcp --show-banner --log-level INFO
```

### Transport Doctor

Use the built-in doctor mode to verify round-trip MCP behavior outside editor tooling:

```bash
# Probe stdio only
uv run mtgjson-mcp --doctor stdio

# Probe HTTP only
uv run mtgjson-mcp --doctor http

# Probe both transports and compare the results
uv run mtgjson-mcp --doctor both
```

Doctor mode runs the same small MCP probe set across the selected transport(s)
and prints JSON output with parity results. This is useful when editor-side MCP
tooling is flaky and you need to separate client/session issues from server
behavior.

### Inspect with `@modelcontextprotocol/inspector`

The MCP Inspector is useful when you want to test tools and resources outside of
editor integration.

Inspect over stdio by letting the Inspector start `mtgjson-mcp` for you:

```bash
npx @modelcontextprotocol/inspector uv run mtgjson-mcp
```

Pass server flags after `--`:

```bash
npx @modelcontextprotocol/inspector -- uv run mtgjson-mcp --offline --log-level INFO
```

Inspect over HTTP in two terminals:

```bash
# Terminal 1: start the MCP server
uv run mtgjson-mcp --transport http --host 127.0.0.1 --port 8000 --path /mcp

# Terminal 2: start the Inspector UI
npx @modelcontextprotocol/inspector
```

Then connect in the Inspector UI with:

```text
transport: streamable-http
url: http://127.0.0.1:8000/mcp
```

You can also launch the Inspector from a config file:

```json
{
    "mcpServers": {
        "mtgjson": {
            "type": "streamable-http",
            "url": "http://127.0.0.1:8000/mcp"
        }
    }
}
```

```bash
npx @modelcontextprotocol/inspector --config ./mcp.json --server mtgjson
```

The Inspector uses `mcpServers` config files and labels HTTP transport as
`streamable-http`. If your editor uses `"type": "http"` instead, keep the same
URL and adapt only the client-specific config shape.

### ASGI Deployment Path (Hosted/Production)

For hosted deployments, middleware, and multi-worker ASGI serving, use the ASGI app factory:

```python
from fastapi import FastAPI

from mtgjson_sdk.mcp.server import create_asgi_app

mcp_app = create_asgi_app(path="/")
api = FastAPI(lifespan=mcp_app.lifespan)
api.mount("/mcp", mcp_app)
```

Run with Uvicorn/Gunicorn (`uvicorn yourmodule:api --host 0.0.0.0 --port 8000 --workers 4`).
For horizontally scaled HTTP, prefer stateless mode in CLI/server startup (`--stateless-http`).

### Exposed MCP surface

Resources:

* `mtgjson://server/config`
* `mtgjson://meta`
* `mtgjson://views`
* `mtgjson://cards/{uuid}`
* `mtgjson://tokens/{uuid}`
* `mtgjson://sets/{code}`
* `mtgjson://identifiers/{uuid}`
* `mtgjson://prices/{uuid}`
* `mtgjson://skus/{uuid}`
* `mtgjson://sealed/{uuid}`
* `mtgjson://enums/{catalog}`
* `mtgjson://booster/{set_code}/{booster_type}/sheets/{sheet_name}`

Representative tools:

* `search_cards`
* `search_tokens`
* `search_sets`
* `get_set_financial_summary`
* `find_cards_by_identifier`
* `get_card_legalities`
* `list_cards_by_format_status`
* `get_price_today`
* `get_price_history`
* `get_price_trend`
* `find_cheapest_printing`
* `list_price_extremes`
* `list_decks`
* `list_sealed_products`
* `get_skus_for_card`
* `list_booster_types`
* `open_booster_pack`
* `open_booster_box`
* `execute_read_only_sql`
* `get_server_status`
* `prefetch_views`
* `refresh_cache`
* `export_duckdb`

The SQL tool is intentionally restricted to single-statement `SELECT`, `WITH`, `SHOW`, and `DESCRIBE` queries, and it always enforces a server-side row cap.

## Development

```bash
git clone https://github.com/mtgjson/mtgjson-sdk-python.git
cd mtgjson-sdk-python
uv sync --group dev
uv run pytest
```

The `dev` group includes test and lint dependencies (including MCP test dependencies).
If you only want MCP runtime deps without full dev tooling, use:

```bash
uv sync --extra mcp
```

### Code Style

```bash
uv run ruff check mtgjson_sdk/ tests/
uv run ruff format mtgjson_sdk/ tests/
```

## License

MIT
