"""Tests for the MtgjsonSDK client."""

import duckdb
import pytest

from mtgjson_sdk import AsyncMtgjsonSDK, MtgjsonSDK


def test_sdk_repr(sdk_offline):
    assert "MtgjsonSDK" in repr(sdk_offline)


def test_context_manager(tmp_path):
    with MtgjsonSDK(cache_dir=tmp_path / "cache", offline=True) as sdk:
        assert sdk is not None


def test_sql_escape_hatch(sdk_offline):
    rows = sdk_offline.sql("SELECT COUNT(*) AS cnt FROM cards")
    assert rows[0]["cnt"] == 3


def test_raw_sql_with_params(sdk_offline):
    rows = sdk_offline.sql(
        "SELECT name FROM cards WHERE uuid = $1",
        ["card-uuid-001"],
    )
    assert len(rows) == 1
    assert rows[0]["name"] == "Lightning Bolt"


def test_refresh_not_stale(sdk_offline):
    """refresh() returns False when cache is not stale (offline + version set)."""
    # Write a version file so is_stale() returns False in offline mode
    sdk_offline._cache._save_version("5.0.0+test")
    result = sdk_offline.refresh()
    assert result is False


def test_refresh_clears_state(tmp_path):
    """refresh() resets DuckDB state while preserving lazy query handles."""
    sdk = MtgjsonSDK(cache_dir=tmp_path / "cache", offline=True)
    sdk._conn.register_table_from_data(
        "cards",
        [
            {
                "uuid": "test-001",
                "name": "Test Card",
                "type": "Instant",
                "types": ["Instant"],
                "subtypes": [],
                "supertypes": [],
                "colors": [],
                "colorIdentity": [],
                "manaCost": "{R}",
                "text": "Test",
                "layout": "normal",
                "manaValue": 1.0,
                "setCode": "TST",
                "number": "1",
                "borderColor": "black",
                "frameVersion": "2015",
                "availability": ["paper"],
                "finishes": ["nonfoil"],
                "language": "English",
                "rarity": "common",
            },
        ],
    )

    # Access a query to create the lazy instance
    cards = sdk.cards
    assert sdk._cards is cards
    assert "cards" in sdk._conn._registered_views
    original_conn = sdk._conn

    # Simulate staleness by forcing is_stale to return True
    sdk._cache.is_stale = lambda *, force_remote=False: True
    result = sdk.refresh()
    assert result is True
    assert sdk._cards is cards
    assert sdk._conn is original_conn
    assert len(sdk._conn._registered_views) == 0

    sdk.close()


def test_refresh_invalidates_existing_duckdb_state(tmp_path):
    sdk = MtgjsonSDK(cache_dir=tmp_path / "cache", offline=True)
    sdk._conn.register_table_from_data(
        "cards",
        [
            {
                "uuid": "test-001",
                "name": "Test Card",
                "type": "Instant",
                "types": ["Instant"],
                "subtypes": [],
                "supertypes": [],
                "colors": [],
                "colorIdentity": [],
                "manaCost": "{R}",
                "text": "Test",
                "layout": "normal",
                "manaValue": 1.0,
                "setCode": "TST",
                "number": "1",
                "borderColor": "black",
                "frameVersion": "2015",
                "availability": ["paper"],
                "finishes": ["nonfoil"],
                "language": "English",
                "rarity": "common",
            },
        ],
    )

    assert sdk.sql("SELECT COUNT(*) AS cnt FROM cards")[0]["cnt"] == 1

    sdk._cache.is_stale = lambda *, force_remote=False: True
    assert sdk.refresh() is True

    with pytest.raises(duckdb.Error):
        sdk.sql("SELECT COUNT(*) AS cnt FROM cards")

    sdk.close()


def test_refresh_preserves_existing_query_handles(tmp_path):
    sdk = MtgjsonSDK(cache_dir=tmp_path / "cache", offline=True)
    sdk._conn.register_table_from_data(
        "cards",
        [
            {
                "uuid": "test-001",
                "name": "Test Card",
                "type": "Instant",
                "types": ["Instant"],
                "subtypes": [],
                "supertypes": [],
                "colors": [],
                "colorIdentity": [],
                "manaCost": "{R}",
                "text": "Test",
                "layout": "normal",
                "manaValue": 1.0,
                "setCode": "TST",
                "number": "1",
                "borderColor": "black",
                "frameVersion": "2015",
                "availability": ["paper"],
                "finishes": ["nonfoil"],
                "language": "English",
                "rarity": "common",
            },
        ],
    )

    cards = sdk.cards
    conn = sdk._conn
    original_raw = conn.raw
    assert len(cards.search(name="Test Card")) == 1

    sdk._cache.is_stale = lambda *, force_remote=False: True
    assert sdk.refresh() is True

    sdk._conn.register_table_from_data(
        "cards",
        [
            {
                "uuid": "test-002",
                "name": "Refreshed Card",
                "type": "Instant",
                "types": ["Instant"],
                "subtypes": [],
                "supertypes": [],
                "colors": [],
                "colorIdentity": [],
                "manaCost": "{U}",
                "text": "Refreshed",
                "layout": "normal",
                "manaValue": 1.0,
                "setCode": "TST",
                "number": "2",
                "borderColor": "black",
                "frameVersion": "2015",
                "availability": ["paper"],
                "finishes": ["nonfoil"],
                "language": "English",
                "rarity": "common",
            },
        ],
    )

    assert sdk.cards is cards
    assert sdk._conn is conn
    assert conn.raw is not original_raw
    results = cards.search(name="Refreshed Card")
    assert len(results) == 1
    assert results[0].uuid == "test-002"

    sdk.close()


def test_refresh_reloads_cached_deck_query_when_cache_token_changes(
    tmp_path, monkeypatch
):
    sdk = MtgjsonSDK(cache_dir=tmp_path / "cache", offline=True)
    deck_payloads = iter(
        [
            {"data": [{"code": "A25", "name": "First Deck", "type": "Theme"}]},
            {"data": [{"code": "TDM", "name": "Second Deck", "type": "Commander"}]},
        ]
    )

    monkeypatch.setattr(sdk._cache, "load_json", lambda name: next(deck_payloads))

    decks = sdk.decks
    first = decks.list(as_dict=True)
    assert [deck["name"] for deck in first] == ["First Deck"]

    original_token = sdk._cache.cache_token()
    monkeypatch.setattr(
        sdk._cache,
        "cache_token",
        lambda: "refresh-token"
        if sdk._cache.is_stale(force_remote=True)
        else original_token,
    )
    sdk._cache.is_stale = lambda *, force_remote=False: force_remote

    assert sdk.refresh() is True
    second = decks.list(as_dict=True)
    assert [deck["name"] for deck in second] == ["Second Deck"]

    sdk.close()


# === execute_json tests ===


def test_execute_json_basic(sdk_offline):
    """execute_json returns a valid JSON string."""
    import json

    result = sdk_offline._conn.execute_json("SELECT name FROM cards ORDER BY name")
    assert isinstance(result, str)
    parsed = json.loads(result)
    assert len(parsed) == 3
    assert parsed[0]["name"] == "Counterspell"


def test_execute_json_empty(sdk_offline):
    """execute_json returns '[]' for empty results."""
    result = sdk_offline._conn.execute_json(
        "SELECT * FROM cards WHERE uuid = $1", ["nonexistent"]
    )
    assert result == "[]"


def test_execute_json_with_params(sdk_offline):
    """execute_json works with parameterized queries."""
    import json

    result = sdk_offline._conn.execute_json(
        "SELECT name FROM cards WHERE uuid = $1", ["card-uuid-001"]
    )
    parsed = json.loads(result)
    assert len(parsed) == 1
    assert parsed[0]["name"] == "Lightning Bolt"


def test_execute_json_dates(sdk_offline):
    """execute_json auto-converts dates to ISO strings."""
    import json

    result = sdk_offline._conn.execute_json(
        "SELECT releaseDate FROM cards WHERE uuid = $1", ["card-uuid-001"]
    )
    parsed = json.loads(result)
    assert parsed[0]["releaseDate"] == "2018-03-16"


def test_execute_json_arrays(sdk_offline):
    """execute_json preserves arrays as JSON arrays."""
    import json

    result = sdk_offline._conn.execute_json(
        "SELECT colors FROM cards WHERE uuid = $1", ["card-uuid-001"]
    )
    parsed = json.loads(result)
    assert parsed[0]["colors"] == ["R"]


# === export_db tests ===


def test_export_db(sdk_offline, tmp_path):
    """export_db creates a queryable DuckDB file."""
    out = tmp_path / "export.duckdb"
    result_path = sdk_offline.export_db(out)
    assert result_path == out
    assert out.exists()

    # Verify we can query the exported file independently
    conn = duckdb.connect(str(out))
    try:
        row = conn.execute("SELECT COUNT(*) FROM cards").fetchone()
        assert row[0] == 3
        row = conn.execute("SELECT COUNT(*) FROM sets").fetchone()
        assert row[0] == 2
    finally:
        conn.close()


def test_export_db_contains_all_views(sdk_offline, tmp_path):
    """export_db exports all registered views."""
    out = tmp_path / "export_all.duckdb"
    sdk_offline.export_db(out)

    conn = duckdb.connect(str(out))
    try:
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'main'"
            ).fetchall()
        }
        # All views registered in conftest
        for expected in ["cards", "sets", "tokens", "card_identifiers"]:
            assert expected in tables
    finally:
        conn.close()


def test_export_db_can_limit_exported_views(sdk_offline, tmp_path):
    """export_db can export only a requested subset of registered views."""
    out = tmp_path / "export_subset.duckdb"
    sdk_offline.export_db(out, views=["cards", "sets"])

    conn = duckdb.connect(str(out))
    try:
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'main'"
            ).fetchall()
        }
        assert tables == {"cards", "sets"}
    finally:
        conn.close()


def test_export_db_empty_view_subset_creates_empty_database(sdk_offline, tmp_path):
    out = tmp_path / "export_none.duckdb"
    sdk_offline.export_db(out, views=[])

    conn = duckdb.connect(str(out))
    try:
        tables = conn.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'main'"
        ).fetchall()
        assert tables == []
    finally:
        conn.close()


# === AsyncMtgjsonSDK tests ===


@pytest.mark.asyncio
async def test_async_sdk_sql(tmp_path):
    """AsyncMtgjsonSDK.sql runs queries without blocking."""
    async with AsyncMtgjsonSDK(cache_dir=tmp_path / "cache", offline=True) as sdk:
        from conftest import SAMPLE_CARDS

        sdk.inner._conn.register_table_from_data("cards", SAMPLE_CARDS)
        rows = await sdk.sql("SELECT COUNT(*) AS cnt FROM cards")
        assert rows[0]["cnt"] == 3


@pytest.mark.asyncio
async def test_async_sdk_run(tmp_path):
    """AsyncMtgjsonSDK.run wraps sync query methods."""
    async with AsyncMtgjsonSDK(cache_dir=tmp_path / "cache", offline=True) as sdk:
        from conftest import SAMPLE_CARDS

        sdk.inner._conn.register_table_from_data("cards", SAMPLE_CARDS)
        cards = await sdk.run(sdk.inner.cards.search, rarity="uncommon")
        assert len(cards) == 3


# === on_progress callback test ===


def test_on_progress_callback(tmp_path):
    """on_progress parameter is accepted and stored."""
    calls = []
    sdk = MtgjsonSDK(
        cache_dir=tmp_path / "cache",
        offline=True,
        on_progress=lambda f, d, t: calls.append((f, d, t)),
    )
    # Verify callback is stored (can't trigger download in offline mode)
    assert sdk._cache._on_progress is not None
    sdk.close()


# === execute_models (TypeAdapter fast path) test ===


def test_execute_models_returns_pydantic_instances(sdk_offline):
    """execute_models returns proper Pydantic model instances."""
    from pydantic import TypeAdapter

    from mtgjson_sdk.models.cards import CardSet

    adapter = TypeAdapter(list[CardSet])
    cards = sdk_offline._conn.execute_models(
        "SELECT * FROM cards ORDER BY name", adapter=adapter
    )
    assert len(cards) == 3
    assert all(isinstance(c, CardSet) for c in cards)
    assert cards[0].name == "Counterspell"


# === No-data / empty state tests ===


def test_sql_works_without_views(tmp_path):
    """Raw SQL works even with no views registered."""
    with MtgjsonSDK(cache_dir=tmp_path / "cache", offline=True) as sdk:
        rows = sdk.sql("SELECT 1 AS x")
        assert rows == [{"x": 1}]


def test_meta_returns_empty_when_missing(tmp_path):
    """meta property returns {} when Meta.json is not cached."""
    with MtgjsonSDK(cache_dir=tmp_path / "cache", offline=True) as sdk:
        assert sdk.meta == {}


def test_views_empty_initially(tmp_path):
    """views property returns [] with no data loaded."""
    with MtgjsonSDK(cache_dir=tmp_path / "cache", offline=True) as sdk:
        assert sdk.views == []


def test_export_db_no_views(tmp_path):
    """export_db with no views creates a valid empty DuckDB file."""
    with MtgjsonSDK(cache_dir=tmp_path / "cache", offline=True) as sdk:
        out = tmp_path / "empty.duckdb"
        result = sdk.export_db(out)
        assert result == out
        assert out.exists()

        conn = duckdb.connect(str(out))
        try:
            tables = conn.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'main'"
            ).fetchall()
            assert len(tables) == 0
        finally:
            conn.close()


def test_refresh_stale_with_no_version(tmp_path):
    """refresh() returns True when no version.txt exists (stale)."""
    with MtgjsonSDK(cache_dir=tmp_path / "cache", offline=True) as sdk:
        # No version.txt → is_stale() returns True
        result = sdk.refresh()
        assert result is True


def test_refresh_forces_a_fresh_remote_version_check(tmp_path):
    with MtgjsonSDK(cache_dir=tmp_path / "cache", offline=False) as sdk:
        sdk._cache._save_version("5.0.0+old")
        calls: list[bool] = []

        def fake_is_stale(*, force_remote: bool = False) -> bool:
            calls.append(force_remote)
            return force_remote

        sdk._cache.is_stale = fake_is_stale

        assert sdk.refresh() is True
        assert calls == [True]


def test_refresh_does_not_activate_new_version_before_download(tmp_path):
    with MtgjsonSDK(cache_dir=tmp_path / "cache", offline=False) as sdk:
        sdk._cache._save_version("5.0.0+old")
        sdk._cache.remote_version = lambda *, force=False: "5.1.0+new"
        sdk._cache.is_stale = lambda *, force_remote=False: True

        assert sdk.refresh() is True
        assert sdk._cache._local_version() == "5.0.0+old"
