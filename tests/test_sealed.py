"""Tests for the sealed product query module."""

import duckdb
import pytest

from conftest import SAMPLE_SETS


def test_sealed_list(sdk_offline):
    products = sdk_offline.sealed.list()
    assert len(products) == 3


def test_sealed_list_by_set(sdk_offline):
    products = sdk_offline.sealed.list(set_code="A25")
    assert len(products) == 2
    assert all(p["setCode"] == "A25" for p in products)


def test_sealed_list_by_category(sdk_offline):
    products = sdk_offline.sealed.list(category="booster_box")
    assert len(products) == 2


def test_sealed_get(sdk_offline):
    product = sdk_offline.sealed.get("sealed-uuid-001")
    assert product is not None
    assert product["name"] == "Masters 25 Booster Box"
    assert product["setCode"] == "A25"


def test_sealed_get_not_found(sdk_offline):
    product = sdk_offline.sealed.get("nonexistent-uuid")
    assert product is None


def test_sealed_falls_back_to_all_printings_when_sets_are_flat(sdk_offline):
    sets_without_sealed = [
        {key: value for key, value in row.items() if key != "sealedProduct"}
        for row in SAMPLE_SETS
    ]
    sdk_offline._conn.register_table_from_data("sets", sets_without_sealed)
    sdk_offline._conn.register_table_from_data("all_printings", SAMPLE_SETS)

    products = sdk_offline.sealed.list(set_code="A25")

    assert len(products) == 2
    assert all(product["setCode"] == "A25" for product in products)
    assert sdk_offline.sealed.get("sealed-uuid-001")["name"] == "Masters 25 Booster Box"


def test_sealed_list_as_dataframe_returns_empty_dataframe_when_sources_lack_sealed(
    sdk_offline,
    monkeypatch,
):
    sets_without_sealed = [
        {key: value for key, value in row.items() if key != "sealedProduct"}
        for row in SAMPLE_SETS
    ]
    sdk_offline._conn.register_table_from_data("sets", sets_without_sealed)
    sdk_offline._conn.register_table_from_data("all_printings", sets_without_sealed)
    sentinel = object()
    captured: dict[str, str] = {}

    def fake_execute_df(sql: str, params=None):
        captured["sql"] = sql
        return sentinel

    monkeypatch.setattr(sdk_offline._conn, "execute_df", fake_execute_df)

    result = sdk_offline.sealed.list(as_dataframe=True)

    assert result is sentinel
    assert "sealedProduct" in captured["sql"]
    assert "WHERE FALSE" in captured["sql"]


def test_sealed_list_falls_back_on_duckdb_error(sdk_offline, monkeypatch):
    monkeypatch.setattr(
        sdk_offline.sealed,
        "_source_has_sealed_product",
        lambda _: True,
    )

    def fake_list_from_source(source: str, **kwargs):
        if source == "sets":
            raise duckdb.Error("missing sealedProduct")
        return [{"uuid": "sealed-uuid-001", "setCode": "A25"}]

    monkeypatch.setattr(sdk_offline.sealed, "_list_from_source", fake_list_from_source)

    assert sdk_offline.sealed.list(set_code="A25") == [
        {"uuid": "sealed-uuid-001", "setCode": "A25"}
    ]


def test_sealed_list_propagates_unexpected_errors(sdk_offline, monkeypatch):
    monkeypatch.setattr(
        sdk_offline.sealed,
        "_source_has_sealed_product",
        lambda _: True,
    )
    monkeypatch.setattr(
        sdk_offline.sealed,
        "_list_from_source",
        lambda source, **kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
    )

    with pytest.raises(RuntimeError, match="boom"):
        sdk_offline.sealed.list(set_code="A25")


def test_sealed_get_falls_back_on_duckdb_error(sdk_offline, monkeypatch):
    monkeypatch.setattr(
        sdk_offline.sealed,
        "_source_has_sealed_product",
        lambda _: True,
    )

    def fake_get_from_source(source: str, uuid: str):
        if source == "sets":
            raise duckdb.Error("missing sealedProduct")
        return {"uuid": uuid, "name": "Fallback Product", "setCode": "A25"}

    monkeypatch.setattr(sdk_offline.sealed, "_get_from_source", fake_get_from_source)

    assert sdk_offline.sealed.get("sealed-uuid-001") == {
        "uuid": "sealed-uuid-001",
        "name": "Fallback Product",
        "setCode": "A25",
    }


def test_sealed_get_propagates_unexpected_errors(sdk_offline, monkeypatch):
    monkeypatch.setattr(
        sdk_offline.sealed,
        "_source_has_sealed_product",
        lambda _: True,
    )
    monkeypatch.setattr(
        sdk_offline.sealed,
        "_get_from_source",
        lambda source, uuid: (_ for _ in ()).throw(RuntimeError("boom")),
    )

    with pytest.raises(RuntimeError, match="boom"):
        sdk_offline.sealed.get("sealed-uuid-001")
