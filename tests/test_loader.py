"""Tests for loader dimension resolution (D1)."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Self

import psycopg
import pytest
from psycopg.rows import dict_row

from wattfrosch_store import StoreError
from wattfrosch_store.loader import (
    DimensionResolver,
    MetricNotFoundError,
    RegionNotFoundError,
    SourceNotFoundError,
)
from wattfrosch_store.migrate import run_init, run_migrations
from wattfrosch_store.sync_dims import sync_dims

TS_1 = datetime(2024, 6, 1, 10, 0, tzinfo=UTC)
TS_1_LATER = datetime(2024, 6, 1, 23, 0, tzinfo=UTC)  # same UTC date as TS_1
TS_2 = datetime(2024, 6, 2, 0, 0, tzinfo=UTC)  # different UTC date


class _StubCursor:
    def __init__(self, conn: _StubConnection) -> None:
        self._conn = conn
        self._rows: list[dict] = []

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> bool:
        return False

    def execute(self, query: str, params: object = None) -> None:
        self._conn.statements.append(query)
        self._conn.param_sets.append(params)
        self._rows = []
        for marker, rows in self._conn.results.items():
            if marker in query:
                self._rows = list(rows)
                return

    def fetchone(self) -> dict | None:
        return self._rows[0] if self._rows else None

    def fetchall(self) -> list[dict]:
        return self._rows


class _StubConnection:
    def __init__(self, results: dict[str, list[dict]] | None = None) -> None:
        self.statements: list[str] = []
        self.param_sets: list[object] = []
        self.results = results or {}

    def cursor(self) -> _StubCursor:
        return _StubCursor(self)


def _region_result(region_id: int = 5) -> dict[str, list[dict]]:
    return {
        "RETURNING time_id": [{"time_id": 11}],
        "FROM raw.dim_region": [{"region_id": region_id}],
        "FROM raw.dim_metric": [{"metric_id": 7}],
        "FROM raw.dim_source": [{"source_id": 3}],
    }


# --- time dimension ---


def test_time_upsert_uses_do_update_returning():
    conn = _StubConnection(_region_result())
    resolver = DimensionResolver(conn)

    time_id = resolver.time_id(TS_1)

    assert time_id == 11
    sql = conn.statements[0]
    assert "INSERT INTO raw.dim_time (ts_utc)" in sql
    assert "ON CONFLICT (ts_utc) DO UPDATE SET ts_utc = EXCLUDED.ts_utc" in sql
    assert "RETURNING time_id" in sql
    assert conn.param_sets[0] == {"ts": TS_1}


def test_time_upsert_cached_per_run():
    conn = _StubConnection(_region_result())
    resolver = DimensionResolver(conn)

    first = resolver.time_id(TS_1)
    second = resolver.time_id(TS_1)

    assert first == second
    assert len(conn.statements) == 1


def test_time_upsert_without_returning_row_raises():
    conn = _StubConnection({})  # stub returns no row for the RETURNING query
    resolver = DimensionResolver(conn)

    with pytest.raises(StoreError, match="dim_time upsert returned no row"):
        resolver.time_id(TS_1)


# --- region / metric (SCD-2, UTC date cast) ---


def test_region_resolution_sql_utc_cast():
    conn = _StubConnection(_region_result())
    resolver = DimensionResolver(conn)

    resolver.region_id("DE-LU", TS_1)

    sql = conn.statements[0]
    # never a bare timestamptz::date — session TimeZone must not matter
    assert "(%(ts)s AT TIME ZONE 'UTC')::date >= valid_from" in sql
    assert "(%(ts)s AT TIME ZONE 'UTC')::date < valid_to OR valid_to IS NULL" in sql
    assert conn.param_sets[0] == {"region_code": "DE-LU", "ts": TS_1}


def test_region_cached_per_utc_date():
    conn = _StubConnection(_region_result())
    resolver = DimensionResolver(conn)

    first = resolver.region_id("DE-LU", TS_1)
    same_date = resolver.region_id("DE-LU", TS_1_LATER)
    next_day = resolver.region_id("DE-LU", TS_2)

    assert first == same_date == next_day == 5
    region_queries = [s for s in conn.statements if "FROM raw.dim_region" in s]
    assert len(region_queries) == 2  # one per distinct UTC date, not per record


def test_metric_cached_per_utc_date():
    conn = _StubConnection(_region_result())
    resolver = DimensionResolver(conn)

    resolver.metric_id("generation.solar", TS_1)
    resolver.metric_id("generation.solar", TS_1)
    resolver.metric_id("generation.solar", TS_2)

    metric_queries = [s for s in conn.statements if "FROM raw.dim_metric" in s]
    assert len(metric_queries) == 2
    assert "FROM raw.dim_metric" in metric_queries[0]
    assert "(%(ts)s AT TIME ZONE 'UTC')::date >= valid_from" in metric_queries[0]


# --- source (flat lookup) ---


def test_source_lookup_plain_and_cached():
    conn = _StubConnection(_region_result())
    resolver = DimensionResolver(conn)

    first = resolver.source_id("energy-charts")
    second = resolver.source_id("energy-charts")

    assert first == second == 3
    source_queries = [s for s in conn.statements if "FROM raw.dim_source" in s]
    assert len(source_queries) == 1
    assert "valid_from" not in source_queries[0]
    assert conn.param_sets[0] == {"source_key": "energy-charts"}


# --- not-found errors ---


def test_region_not_found_raises_typed_error():
    conn = _StubConnection({"RETURNING time_id": [{"time_id": 11}]})
    resolver = DimensionResolver(conn)

    with pytest.raises(RegionNotFoundError) as exc_info:
        resolver.region_id("DE-AT-LU", TS_1)

    assert exc_info.value.code == "wattfrosch.store_region_not_found"
    assert exc_info.value.context == {
        "region_code": "DE-AT-LU",
        "ts_utc": TS_1.isoformat(),
    }


def test_metric_not_found_raises_typed_error():
    conn = _StubConnection({})
    resolver = DimensionResolver(conn)

    with pytest.raises(MetricNotFoundError) as exc_info:
        resolver.metric_id("generation.solar", TS_1)

    assert exc_info.value.code == "wattfrosch.store_metric_not_found"
    assert exc_info.value.context["metric_key"] == "generation.solar"


def test_source_not_found_raises_typed_error():
    conn = _StubConnection({})
    resolver = DimensionResolver(conn)

    with pytest.raises(SourceNotFoundError) as exc_info:
        resolver.source_id("unknown-source")

    assert exc_info.value.code == "wattfrosch.store_source_not_found"
    assert exc_info.value.context == {"source": "unknown-source"}


# --- integration (real DB) ---


@pytest.mark.integration
def test_integration_time_upsert_idempotent(clean_db):
    run_init(clean_db)
    run_migrations(clean_db)

    # autocommit: the resolver issues single statements without an explicit
    # transaction — the upsert must persist for the second connection's count
    with psycopg.connect(
        clean_db.database_url, row_factory=dict_row, autocommit=True
    ) as conn:
        sync_dims(conn)
        resolver = DimensionResolver(conn)
        first = resolver.time_id(TS_1)
        second = resolver.time_id(TS_1)
    assert first == second

    with psycopg.connect(clean_db.database_url, row_factory=dict_row) as conn:
        count = conn.execute("SELECT count(*) AS n FROM raw.dim_time").fetchone()
    assert count is not None and count["n"] == 1


@pytest.mark.integration
def test_integration_scd2_region_sides(clean_db):
    run_init(clean_db)
    run_migrations(clean_db)

    with psycopg.connect(clean_db.database_url, row_factory=dict_row) as conn:
        sync_dims(conn)
        resolver = DimensionResolver(conn)

        # DE-AT-LU: [2015-01-01, 2018-10-01)
        at_lu_id = resolver.region_id("DE-AT-LU", datetime(2018, 9, 30, 12, tzinfo=UTC))
        with pytest.raises(RegionNotFoundError):
            resolver.region_id("DE-AT-LU", datetime(2018, 10, 1, 0, 0, tzinfo=UTC))

        # DE-LU: [2018-10-01, open)
        lu_id = resolver.region_id("DE-LU", datetime(2018, 10, 1, 12, tzinfo=UTC))
        with pytest.raises(RegionNotFoundError):
            resolver.region_id("DE-LU", datetime(2018, 9, 30, 23, 59, tzinfo=UTC))

    assert at_lu_id != lu_id


@pytest.mark.integration
def test_integration_utc_cast_independent_of_session_timezone(clean_db):
    run_init(clean_db)
    run_migrations(clean_db)

    # 2018-09-30T23:30Z is already 2018-10-01 in Auckland: a bare ::date
    # would flip the UTC date and resolve the wrong (or no) region.
    ts = datetime(2018, 9, 30, 23, 30, tzinfo=UTC)
    with psycopg.connect(clean_db.database_url, row_factory=dict_row) as conn:
        conn.execute("SET TimeZone = 'Pacific/Auckland'")
        sync_dims(conn)
        resolver = DimensionResolver(conn)
        at_lu_id = resolver.region_id("DE-AT-LU", ts)
    with psycopg.connect(clean_db.database_url, row_factory=dict_row) as conn:
        sync_dims(conn)
        resolver = DimensionResolver(conn)
        at_lu_id_default_tz = resolver.region_id("DE-AT-LU", ts)
    assert at_lu_id == at_lu_id_default_tz


@pytest.mark.integration
def test_integration_metric_and_source_resolution(clean_db):
    run_init(clean_db)
    run_migrations(clean_db)

    with psycopg.connect(clean_db.database_url, row_factory=dict_row) as conn:
        sync_dims(conn)
        resolver = DimensionResolver(conn)
        metric_id = resolver.metric_id("generation.solar", TS_1)
        source_id = resolver.source_id("energy-charts")

        with pytest.raises(MetricNotFoundError):
            resolver.metric_id("generation.solar", datetime(2014, 1, 1, tzinfo=UTC))
        with pytest.raises(SourceNotFoundError):
            resolver.source_id("no-such-source")

    assert metric_id > 0
    assert source_id > 0
