"""Tests for the sealed product query module."""

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
