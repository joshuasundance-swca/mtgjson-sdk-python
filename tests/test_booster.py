"""Tests for the booster simulator."""

from mtgjson_sdk.booster.simulator import BoosterSimulator, _pick_from_sheet, _pick_pack
from mtgjson_sdk.cache import CacheManager
from mtgjson_sdk.connection import Connection


def test_pick_pack_weighted():
    boosters = [
        {"contents": {"rare": 1, "common": 10}, "weight": 7},
        {"contents": {"mythic": 1, "common": 10}, "weight": 1},
    ]
    # Just verify it returns a valid pack
    pack = _pick_pack(boosters)
    assert "contents" in pack
    assert "weight" in pack


def test_pick_from_sheet_basic():
    sheet = {
        "cards": {"uuid-a": 10, "uuid-b": 5, "uuid-c": 1},
        "foil": False,
        "totalWeight": 16,
    }
    picked = _pick_from_sheet(sheet, 2)
    assert len(picked) == 2
    assert all(u in ("uuid-a", "uuid-b", "uuid-c") for u in picked)


def test_pick_from_sheet_no_duplicates():
    sheet = {
        "cards": {"uuid-a": 1, "uuid-b": 1, "uuid-c": 1},
        "foil": False,
        "totalWeight": 3,
    }
    picked = _pick_from_sheet(sheet, 3)
    assert len(picked) == 3
    assert len(set(picked)) == 3  # All unique


def test_pick_from_sheet_with_duplicates():
    sheet = {
        "cards": {"uuid-a": 1},
        "foil": False,
        "totalWeight": 1,
        "allowDuplicates": True,
    }
    picked = _pick_from_sheet(sheet, 3)
    assert len(picked) == 3
    assert all(u == "uuid-a" for u in picked)


def test_booster_simulator_clears_cached_configs_when_cache_changes(
    tmp_path,
    monkeypatch,
):
    cache = CacheManager(cache_dir=tmp_path, offline=True)
    conn = Connection(cache)
    simulator = BoosterSimulator(conn)
    calls: list[str] = []
    configs = [
        {
            "draft": {
                "boosters": [{"contents": {"common": 1}, "weight": 1}],
                "boostersTotalWeight": 1,
                "sheets": {
                    "common": {
                        "cards": {"uuid-a": 1},
                        "foil": False,
                        "totalWeight": 1,
                    }
                },
                "sourceSetCodes": ["A25"],
            }
        },
        {
            "draft": {
                "boosters": [{"contents": {"common": 1}, "weight": 1}],
                "boostersTotalWeight": 1,
                "sheets": {
                    "common": {
                        "cards": {"uuid-b": 1},
                        "foil": False,
                        "totalWeight": 1,
                    }
                },
                "sourceSetCodes": ["A25"],
            }
        },
    ]

    def _fake_flat(_: str):
        calls.append("called")
        return configs[min(len(calls) - 1, len(configs) - 1)]

    monkeypatch.setattr(simulator, "_get_config_from_flat", _fake_flat)
    monkeypatch.setattr(simulator, "_get_config_from_nested", lambda _: None)

    first = simulator._get_booster_config("A25")
    assert first is not None
    assert first["draft"]["sheets"]["common"]["cards"] == {"uuid-a": 1}

    monkeypatch.setattr(cache, "cache_token", lambda: "changed-cache-token")

    second = simulator._get_booster_config("A25")
    assert second is not None
    assert second["draft"]["sheets"]["common"]["cards"] == {"uuid-b": 1}
    assert len(calls) == 2
