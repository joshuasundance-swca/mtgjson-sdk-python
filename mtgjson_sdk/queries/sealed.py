"""Sealed product query module."""

from __future__ import annotations

from typing import Any

from .._sql import SQLBuilder
from ..connection import Connection


class SealedQuery:
    """Query interface for sealed product data (booster boxes, bundles, etc.).

    Sealed product data lives inside set-level rows as nested structs when the
    backing source exposes a ``sealedProduct`` column. Uses DuckDB's UNNEST for
    efficient server-side lookup.

    Example::

        products = sdk.sealed.list(set_code="MH3")
        product = sdk.sealed.get("uuid-here")
    """

    def __init__(self, conn: Connection) -> None:
        self._conn = conn

    def _ensure(self) -> None:
        self._conn.ensure_views("sets")

    def _source_has_sealed_product(self, source: str) -> bool:
        """Return whether a source view exposes the nested sealed-product data."""

        return self._conn.view_has_column(source, "sealedProduct")

    def _extract_products(
        self,
        rows: list[dict[str, Any]],
        *,
        category: str | None = None,
    ) -> list[dict]:
        """Expand nested sealed-product arrays into flat product dicts."""

        products: list[dict] = []
        for row in rows:
            sealed = row.get("sealedProduct")
            if sealed and isinstance(sealed, list):
                for sp in sealed:
                    if isinstance(sp, dict):
                        if category and sp.get("category") != category:
                            continue
                        sp["setCode"] = row.get("code")
                        products.append(sp)
        return products

    def _list_from_source(
        self,
        source: str,
        *,
        set_code: str | None = None,
        category: str | None = None,
        limit: int = 100,
        as_dataframe: bool = False,
    ) -> list[dict] | Any:
        """List sealed products from a specific set-level data source."""

        self._conn.ensure_views(source)
        q = SQLBuilder(source)
        q.select("code", "name AS setName", "sealedProduct")

        if set_code:
            q.where_eq("code", set_code.upper())

        q.limit(limit)
        sql, params = q.build()

        if as_dataframe:
            return self._conn.execute_df(sql, params)

        rows = self._conn.execute(sql, params)
        return self._extract_products(rows, category=category)

    def _get_from_source(self, source: str, uuid: str) -> dict | None:
        """Get a sealed product by UUID from a specific set-level source."""

        self._conn.ensure_views(source)
        sql = (
            "SELECT sub.code AS setCode, sub.sp "
            "FROM ("
            "  SELECT code, UNNEST(sealedProduct) AS sp "
            f"  FROM {source} WHERE sealedProduct IS NOT NULL"
            ") sub "
            "WHERE sub.sp.uuid = $1 "
            "LIMIT 1"
        )
        rows = self._conn.execute(sql, [uuid])
        if not rows:
            return None
        row = rows[0]
        product = row.get("sp", {})
        if isinstance(product, dict):
            product["setCode"] = row.get("setCode")
            return product
        return None

    def list(
        self,
        *,
        set_code: str | None = None,
        category: str | None = None,
        limit: int = 100,
        as_dict: bool = False,
        as_dataframe: bool = False,
    ) -> list[dict] | Any:
        """List sealed products.

        Note: Sealed products are nested within set data. This method
        queries set-level rows and extracts sealed product data.
        Requires a source that actually exposes the ``sealedProduct`` column.

        Args:
            set_code: Filter by set code (e.g. ``"MH3"``).
            category: Filter by product category.
            limit: Maximum number of sets to scan (default 100).
            as_dict: Return raw dicts (default behavior).
            as_dataframe: Return a Polars DataFrame.

        Returns:
            List of sealed product dicts with a ``setCode`` key added.
        """
        self._ensure()
        products: list[dict] | Any = []
        for source in ("sets", "all_printings"):
            try:
                if not self._source_has_sealed_product(source):
                    continue
                products = self._list_from_source(
                    source,
                    set_code=set_code,
                    category=category,
                    limit=limit,
                    as_dataframe=as_dataframe,
                )
            except Exception:
                # sealedProduct may be absent in flat sets.parquet; fall back.
                continue

            if as_dataframe or products or source == "all_printings":
                return products

        if as_dict:
            return products
        return products

    def get(self, uuid: str) -> dict | None:
        """Get a sealed product by UUID.

        Uses DuckDB UNNEST + struct field filtering for efficient
        server-side lookup instead of scanning all sets in Python.

        Args:
            uuid: The sealed product UUID.

        Returns:
            Product dict with a ``setCode`` key added, or None if not found.
        """
        self._ensure()
        for source in ("sets", "all_printings"):
            try:
                if not self._source_has_sealed_product(source):
                    continue
                product = self._get_from_source(source, uuid)
            except Exception:
                continue
            if product is not None or source == "all_printings":
                return product
        return None
