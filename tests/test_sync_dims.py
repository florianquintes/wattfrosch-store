"""Tests for the insert-only dimension sync (C2)."""

from __future__ import annotations

from typing import Self

import psycopg
import pytest
from psycopg.rows import dict_row
from wattfrosch_contracts import METRICS, REGIONS, SOURCES

from wattfrosch_store.migrate import run_init, run_migrations
from wattfrosch_store.sync_dims import sync_dims


class _StubCursor:
    def __init__(self, conn: _StubConnection) -> None:
        self._conn = conn

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> bool:
        return False

    def execute(self, query: str, params: object = None) -> None:
        self._conn.statements.append(query)
        self._conn.param_sets.append(params)
        if self._conn.rowcounts is not None:
            self._conn.rowcounts.append(self._conn.rowcounts.pop(0))

    @property
    def rowcount(self) -> int:
        return self._conn.rowcounts[-1] if self._conn.rowcounts else 0


class _StubConnection:
    def __init__(self, rowcounts: list[int] | None = None) -> None:
        self.statements: list[str] = []
        self.param_sets: list[object] = []
        self.rowcounts = rowcounts
        self.committed = False
        self.closed = False

    def cursor(self) -> _StubCursor:
        return _StubCursor(self)

    def commit(self) -> None:
        self.committed = True

    def rollback(self) -> None:
        pass

    def close(self) -> None:
        self.closed = True

    def transaction(self):
        import contextlib

        return contextlib.nullcontext()


def _rowcounts_for_catalog() -> list[int]:
    # one entry per INSERT: all succeed (rowcount 1)
    return [1] * (len(REGIONS) + len(METRICS) + len(SOURCES))


def test_sync_dims_sends_all_catalog_rows(monkeypatch):
    conn = _StubConnection(rowcounts=_rowcounts_for_catalog())

    inserted = sync_dims(conn)

    assert inserted == len(REGIONS) + len(METRICS) + len(SOURCES)
    region_sql = [s for s in conn.statements if "INSERT INTO raw.dim_region" in s]
    metric_sql = [s for s in conn.statements if "INSERT INTO raw.dim_metric" in s]
    source_sql = [s for s in conn.statements if "INSERT INTO raw.dim_source" in s]
    assert len(region_sql) == len(REGIONS)
    assert len(metric_sql) == len(METRICS)
    assert len(source_sql) == len(SOURCES)
    # insert-only: conflict targets, never UPDATE
    assert "ON CONFLICT (region_code, valid_from) DO NOTHING" in region_sql[0]
    assert "ON CONFLICT (metric_key, valid_from) DO NOTHING" in metric_sql[0]
    assert "ON CONFLICT (source_key) DO NOTHING" in source_sql[0]
    assert all(
        "UPDATE" not in s.upper().replace("DO NOTHING", "") for s in conn.statements
    )


def test_sync_dims_name_mapping(monkeypatch):
    conn = _StubConnection(rowcounts=_rowcounts_for_catalog())
    sync_dims(conn)

    region_params = [
        p for p in conn.param_sets if isinstance(p, dict) and "region_code" in p
    ]
    de_at_lu = [p for p in region_params if p["region_code"] == "DE-AT-LU"]
    assert de_at_lu[0]["valid_to"] is not None  # closed SCD-2 row

    metric_params = [
        p for p in conn.param_sets if isinstance(p, dict) and "metric_key" in p
    ]
    solar = [p for p in metric_params if p["metric_key"] == "generation.solar"]
    assert solar[0]["is_derived"] is False  # catalog 'derived' -> 'is_derived'
    assert solar[0]["plausible_min"] == 0.0
    assert solar[0]["plausible_max"] == 100_000.0

    source_params = [
        p for p in conn.param_sets if isinstance(p, dict) and "source_key" in p
    ]
    keys = {p["source_key"] for p in source_params}
    assert keys == set(SOURCES)  # catalog name -> source_key


def test_sync_dims_no_plausible_range_maps_to_nulls(monkeypatch):
    import wattfrosch_store.sync_dims as sync_module

    with_range = METRICS["generation.solar"]
    without_range = with_range.model_copy(
        update={"key": "test.without", "plausible_range": None}
    )
    monkeypatch.setattr(
        sync_module,
        "METRICS",
        {"test.with_range": with_range, "test.without": without_range},
    )
    conn = _StubConnection(rowcounts=[1, 1])

    sync_dims(conn)

    params = [p for p in conn.param_sets if isinstance(p, dict) and "metric_key" in p]
    without = next(p for p in params if p["metric_key"] == "test.without")
    assert without["plausible_min"] is None
    assert without["plausible_max"] is None


def test_sync_dims_returns_zero_when_all_skipped(monkeypatch):
    conn = _StubConnection(rowcounts=[0] * (len(REGIONS) + len(METRICS) + len(SOURCES)))

    assert sync_dims(conn) == 0


@pytest.mark.integration
def test_sync_dims_inserts_and_is_idempotent(clean_db):
    run_init(clean_db)
    run_migrations(clean_db)

    with psycopg.connect(clean_db.database_url, row_factory=dict_row) as conn:
        first = sync_dims(conn)
    assert first == len(REGIONS) + len(METRICS) + len(SOURCES)

    with psycopg.connect(clean_db.database_url, row_factory=dict_row) as conn:
        second = sync_dims(conn)
    assert second == 0  # re-run inserts nothing


@pytest.mark.integration
def test_sync_dims_scd2_rows_and_content(clean_db):
    run_init(clean_db)
    run_migrations(clean_db)
    with psycopg.connect(clean_db.database_url, row_factory=dict_row) as conn:
        sync_dims(conn)

        regions = conn.execute(
            "SELECT region_code, valid_from, valid_to FROM raw.dim_region "
            "ORDER BY region_code, valid_from"
        ).fetchall()
        codes = {r["region_code"] for r in regions}
        assert {"DE-AT-LU", "DE-LU"} <= codes  # both sides of the zone change

        de_at_lu = next(r for r in regions if r["region_code"] == "DE-AT-LU")
        assert de_at_lu["valid_to"] is not None  # closed window preserved

        # nothing was closed or overwritten by the re-run
        before = conn.execute(
            "SELECT region_code, name, valid_to FROM raw.dim_region ORDER BY 1"
        ).fetchall()
        sync_dims(conn)
        after = conn.execute(
            "SELECT region_code, name, valid_to FROM raw.dim_region ORDER BY 1"
        ).fetchall()
        assert before == after
