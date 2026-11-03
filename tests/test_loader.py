"""Tests for loader dimension resolution and load_records (D1/D2)."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Self

import psycopg
import pytest
from psycopg.rows import dict_row
from wattfrosch_contracts import CONTRACT_VERSION
from wattfrosch_contracts.records import MeasurementRecord

import wattfrosch_store.loader as loader_module
import wattfrosch_store.sync_dims as sync_module
from wattfrosch_store import StoreError
from wattfrosch_store.config import Settings
from wattfrosch_store.loader import (
    DimensionResolver,
    MetricNotFoundError,
    RegionNotFoundError,
    SourceNotFoundError,
    load_records,
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
        self._rowcount = 0

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> bool:
        return False

    def execute(self, query: str, params: object = None) -> None:
        if self._conn.fail_execute_with is not None:
            raise self._conn.fail_execute_with
        self._conn.statements.append(query)
        self._conn.param_sets.append(params)
        self._rows = []
        self._rowcount = 1
        for marker, rows in self._conn.results.items():
            if marker in query:
                self._rows = list(rows)
                return

    def executemany(self, query: str, params_seq: object) -> None:
        self._conn.statements.append(query)
        params_list = list(params_seq)  # type: ignore[arg-type]
        self._conn.param_sets.append(params_list)
        self._conn.executemany_calls.append((query, params_list))
        self._rowcount = len(params_list)
        if (
            self._conn.fail_executemany_at is not None
            and len(self._conn.executemany_calls) >= self._conn.fail_executemany_at
        ):
            raise psycopg.Error("simulated batch failure")

    def fetchone(self) -> dict | None:
        return self._rows[0] if self._rows else None

    def fetchall(self) -> list[dict]:
        return self._rows

    @property
    def rowcount(self) -> int:
        return self._rowcount


class _StubConnection:
    def __init__(
        self,
        results: dict[str, list[dict]] | None = None,
        fail_executemany_at: int | None = None,
        fail_execute_with: BaseException | None = None,
    ) -> None:
        self.statements: list[str] = []
        self.param_sets: list[object] = []
        self.results = results or {}
        self.executemany_calls: list[tuple[str, list[dict]]] = []
        self.fail_executemany_at = fail_executemany_at
        self.fail_execute_with = fail_execute_with
        self.commits = 0
        self.rollbacks = 0

    def cursor(self) -> _StubCursor:
        return _StubCursor(self)

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1

    def transaction(self):
        import contextlib

        return contextlib.nullcontext()

    def close(self) -> None:
        pass


def _load_results() -> dict[str, list[dict]]:
    # markers must not match collect_divergences' "SELECT * FROM raw.dim_*"
    return {
        "FROM raw.schema_migration": [
            {"contract_version": CONTRACT_VERSION},
        ],
        "RETURNING time_id": [{"time_id": 11}],
        "SELECT region_id": [{"region_id": 5}],
        "SELECT metric_id": [{"metric_id": 7}],
        "SELECT source_id": [{"source_id": 3}],
    }


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


# --- load_records (D2) ---


def _record(
    i: int, region: str = "DE-LU", metric: str = "generation.solar"
) -> MeasurementRecord:
    return MeasurementRecord(
        ts_utc=datetime(2024, 6, 1, i % 24, 0, tzinfo=UTC),
        region=region,
        metric=metric,
        value=100.0 + i,
        source="energy-charts",
        resolution="PT1H",
    )


@pytest.fixture
def no_catalog_rows(monkeypatch):
    """Empty catalogs: the stub DB holds no dimension rows to compare."""
    monkeypatch.setattr(sync_module, "REGIONS", {})
    monkeypatch.setattr(sync_module, "METRICS", {})
    monkeypatch.setattr(sync_module, "SOURCES", {})


@pytest.fixture
def stub_connect(monkeypatch):
    """Route load_records' connect() to a stub connection."""

    def _patch(conn: _StubConnection) -> _StubConnection:
        monkeypatch.setattr(loader_module, "connect", lambda settings: conn)
        return conn

    return _patch


def test_load_records_batches_and_commits(no_catalog_rows, stub_connect):
    conn = stub_connect(_StubConnection(results=_load_results()))
    settings = Settings(batch_size=2)

    result = load_records([_record(i) for i in range(5)], settings)

    assert result.status == "ok"
    assert result.records_read == 5
    assert result.rows_written == 5
    assert result.batches_committed == 3
    assert result.batches_total == 3
    assert result.divergences == []
    assert result.error is None
    # 1 commit for the version-check read + one per batch (2/2/1)
    assert [len(rows) for _, rows in conn.executemany_calls] == [2, 2, 1]
    assert conn.commits == 4


def test_load_records_fact_upsert_params(no_catalog_rows, stub_connect):
    conn = stub_connect(_StubConnection(results=_load_results()))

    result = load_records([_record(0)], Settings(batch_size=10))

    assert result.status == "ok"
    upsert_sql, params = conn.executemany_calls[0]
    assert "INSERT INTO raw.fact_measurement" in upsert_sql
    assert (
        "ON CONFLICT (time_id, region_id, metric_id, source_id, resolution)"
        in upsert_sql
    )
    assert "DO UPDATE SET value = EXCLUDED.value, ingested_at = now()" in upsert_sql
    assert params[0] == {
        "time_id": 11,
        "region_id": 5,
        "metric_id": 7,
        "source_id": 3,
        "resolution": "PT1H",
        "value": 100.0,
    }


def test_load_records_empty_input(no_catalog_rows, stub_connect, caplog):
    conn = stub_connect(_StubConnection(results=_load_results()))

    with caplog.at_level(logging.WARNING, logger="wattfrosch_store.loader"):
        result = load_records([], Settings(batch_size=10))

    assert result.status == "empty"
    assert result.records_read == 0
    assert result.rows_written == 0
    assert result.batches_committed == 0
    assert result.batches_total == 0
    assert result.error is None
    assert conn.executemany_calls == []
    assert any("No records to load" in r.message for r in caplog.records)


def test_load_records_batch_failure_rolls_back_and_reports(
    no_catalog_rows, stub_connect
):
    conn = stub_connect(_StubConnection(results=_load_results(), fail_executemany_at=2))

    result = load_records([_record(i) for i in range(5)], Settings(batch_size=2))

    assert result.status == "failed"
    assert result.batches_committed == 1
    assert result.batches_total == 2
    assert result.rows_written == 2  # only the committed first batch
    assert result.records_read == 4  # 5th record was never consumed
    assert result.error is not None
    assert result.error.code == "wattfrosch.store_batch_failed"
    message = str(result.error)
    assert "Batch 2" in message
    assert "DE-LU" in message
    assert "generation.solar" in message
    assert "2024-06-01" in message
    assert "already committed" in message
    assert result.error.context["batch"] == 2
    assert conn.rollbacks == 1
    assert conn.commits == 2  # version-check read + first batch


def test_load_records_unresolvable_region(no_catalog_rows, stub_connect):
    results = _load_results()
    del results["SELECT region_id"]  # resolver finds nothing
    conn = stub_connect(_StubConnection(results=results))

    result = load_records([_record(0)], Settings(batch_size=10))

    assert result.status == "failed"
    assert result.error is not None
    assert result.error.code == "wattfrosch.store_region_not_found"
    assert result.error.context == {
        "region_code": "DE-LU",
        "ts_utc": "2024-06-01T00:00:00+00:00",
    }
    assert "DE-LU" in str(result.error)
    assert conn.executemany_calls == []  # failed before any fact was written
    assert conn.rollbacks == 1


def test_load_records_version_mismatch(no_catalog_rows, stub_connect):
    results = _load_results()
    results["FROM raw.schema_migration"] = [{"contract_version": "9.9.0"}]
    conn = stub_connect(_StubConnection(results=results))

    result = load_records([_record(0)], Settings(batch_size=10))

    assert result.status == "failed"
    assert result.error is not None
    assert result.error.code == "wattfrosch.store_contract_divergence"
    assert result.error.context == {
        "database_version": "9.9.0",
        "module_version": CONTRACT_VERSION,
    }
    assert not any("INSERT INTO raw.dim" in s for s in conn.statements)


def test_load_records_no_migrations_applied(no_catalog_rows, stub_connect):
    results = _load_results()
    results["FROM raw.schema_migration"] = []
    stub_connect(_StubConnection(results=results))

    result = load_records([_record(0)], Settings(batch_size=10))

    assert result.status == "failed"
    assert result.error is not None
    assert result.error.code == "wattfrosch.store_contract_divergence"
    assert "migrate" in str(result.error)


def test_load_records_connect_failure(no_catalog_rows, monkeypatch):
    monkeypatch.setattr(
        loader_module,
        "connect",
        lambda settings: (_ for _ in ()).throw(StoreError("not reachable")),
    )

    result = load_records([_record(0)], Settings(batch_size=10))

    assert result.status == "failed"
    assert result.error is not None
    assert "not reachable" in str(result.error)


def test_load_records_connect_psycopg_error(no_catalog_rows, monkeypatch):
    monkeypatch.setattr(
        loader_module,
        "connect",
        lambda settings: (_ for _ in ()).throw(psycopg.OperationalError("down")),
    )

    result = load_records([_record(0)], Settings(batch_size=10))

    assert result.status == "failed"
    assert result.error is not None
    assert "not reachable" in str(result.error).lower()


def test_load_records_missing_table_maps_to_schema_error(no_catalog_rows, stub_connect):
    conn = stub_connect(
        _StubConnection(fail_execute_with=psycopg.errors.UndefinedTable("missing"))
    )

    result = load_records([_record(0)], Settings(batch_size=10))

    assert result.status == "failed"
    assert result.error is not None
    assert "migrate" in str(result.error)
    assert conn.rollbacks == 1


def test_load_records_unexpected_db_error(no_catalog_rows, stub_connect):
    exc = psycopg.OperationalError("boom")
    exc.sqlstate = "XX000"  # not translated by db.py: passes through unchanged
    conn = stub_connect(_StubConnection(fail_execute_with=exc))

    result = load_records([_record(0)], Settings(batch_size=10))

    assert result.status == "failed"
    assert result.error is not None
    assert "Unexpected database error" in str(result.error)
    assert conn.rollbacks == 1


def test_load_records_missing_dim_row_aborts(monkeypatch, stub_connect):
    # one catalog row, empty DB table -> missing_in_db divergence -> abort
    from wattfrosch_contracts import REGIONS

    monkeypatch.setattr(sync_module, "REGIONS", {"DE-LU": REGIONS["DE-LU"]})
    monkeypatch.setattr(sync_module, "METRICS", {})
    monkeypatch.setattr(sync_module, "SOURCES", {})
    conn = stub_connect(
        _StubConnection(
            results={
                "FROM raw.schema_migration": [{"contract_version": CONTRACT_VERSION}],
                "SELECT * FROM raw.dim_region": [],
            }
        )
    )

    result = load_records([_record(0)], Settings(batch_size=10))

    assert result.status == "failed"
    assert result.error is not None
    assert result.error.code == "wattfrosch.store_contract_divergence"
    assert ("region", "DE-LU") in result.error.context["missing"]
    assert conn.executemany_calls == []  # aborted before any fact upsert


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


@pytest.mark.integration
def test_integration_load_twice_updates_values(clean_db):
    run_init(clean_db)
    run_migrations(clean_db)
    settings = Settings(batch_size=2)
    records = [_record(i) for i in range(5)]

    first = load_records(records, settings)
    assert first.status == "ok"
    assert first.dims_inserted > 0
    assert first.rows_written == 5
    assert first.batches_committed == 3
    assert first.batches_total == 3

    with psycopg.connect(clean_db.database_url, row_factory=dict_row) as conn:
        rows = conn.execute(
            "SELECT value, ingested_at FROM raw.fact_measurement ORDER BY time_id"
        ).fetchall()
    assert len(rows) == 5
    assert [r["value"] for r in rows] == [100.0, 101.0, 102.0, 103.0, 104.0]
    first_ingested = max(r["ingested_at"] for r in rows)

    updated = [
        _record(
            i,
        )
        for i in range(5)
    ]
    for i, rec in enumerate(updated):
        rec = rec.model_copy(update={"value": 200.0 + i})
        updated[i] = rec
    import time

    time.sleep(0.05)
    second = load_records(updated, settings)
    assert second.status == "ok"
    assert second.dims_inserted == 0  # sync re-run inserts nothing

    with psycopg.connect(clean_db.database_url, row_factory=dict_row) as conn:
        rows = conn.execute(
            "SELECT value, ingested_at FROM raw.fact_measurement ORDER BY time_id"
        ).fetchall()
    assert len(rows) == 5  # same row count, no duplicates
    assert [r["value"] for r in rows] == [200.0, 201.0, 202.0, 203.0, 204.0]
    assert max(r["ingested_at"] for r in rows) > first_ingested


@pytest.mark.integration
def test_integration_unresolvable_region_fails_without_fact_row(clean_db):
    run_init(clean_db)
    run_migrations(clean_db)

    with psycopg.connect(clean_db.database_url, row_factory=dict_row) as conn:
        sync_dims(conn)
        # hand-close the DB validity window well before the record's date:
        # sync is insert-only and will not repair it, the resulting
        # catalog-vs-DB divergence (valid_to differs) only warns
        conn.execute(
            "UPDATE raw.dim_region SET valid_to = '2019-06-01' "
            "WHERE region_code = 'DE-LU'"
        )

    result = load_records([_record(0)], Settings(batch_size=10))

    assert result.status == "failed"
    assert result.error is not None
    assert result.error.code == "wattfrosch.store_region_not_found"
    assert "DE-LU" in str(result.error)
    assert "2024-06-01" in str(result.error)
    with psycopg.connect(clean_db.database_url, row_factory=dict_row) as conn:
        count = conn.execute(
            "SELECT count(*) AS n FROM raw.fact_measurement"
        ).fetchone()
    assert count is not None and count["n"] == 0


@pytest.mark.integration
def test_integration_contract_version_mismatch_fails(clean_db):
    run_init(clean_db)
    run_migrations(clean_db)
    with psycopg.connect(
        clean_db.database_url, row_factory=dict_row, autocommit=True
    ) as conn:
        conn.execute("UPDATE raw.schema_migration SET contract_version = '9.9.0'")

    result = load_records([_record(0)], Settings(batch_size=10))

    assert result.status == "failed"
    assert result.error is not None
    assert result.error.code == "wattfrosch.store_contract_divergence"


@pytest.mark.integration
def test_integration_empty_input_is_success_with_warning(clean_db):
    run_init(clean_db)
    run_migrations(clean_db)

    result = load_records([], Settings(batch_size=10))

    assert result.status == "empty"
    assert result.rows_written == 0
    assert result.error is None


@pytest.mark.integration
def test_integration_content_divergence_warns_but_loads(clean_db):
    run_init(clean_db)
    run_migrations(clean_db)

    with psycopg.connect(
        clean_db.database_url, row_factory=dict_row, autocommit=True
    ) as conn:
        sync_dims(conn)
        conn.execute(
            "UPDATE raw.dim_metric SET display_name = 'WRONG' "
            "WHERE metric_key = 'generation.solar'"
        )

    result = load_records([_record(0)], Settings(batch_size=10))

    assert result.status == "ok"
    assert result.error is None
    assert any(
        d.dimension == "metric" and d.kind == "content_differs"
        for d in result.divergences
    )
