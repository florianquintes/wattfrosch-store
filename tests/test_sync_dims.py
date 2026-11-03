"""Tests for the insert-only dimension sync and divergence detection (C2/C3)."""

from __future__ import annotations

from datetime import date
from typing import Self

import psycopg
import pytest
from psycopg.rows import dict_row
from wattfrosch_contracts import METRICS, REGIONS, SOURCES

from wattfrosch_store.migrate import run_init, run_migrations
from wattfrosch_store.sync_dims import Divergence, collect_divergences, sync_dims


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
        if self._conn.rowcounts is not None:
            self._conn.rowcounts.append(self._conn.rowcounts.pop(0))
        if "SELECT * FROM raw." in query:
            table = query.split("SELECT * FROM raw.", 1)[1].split()[0]
            self._rows = list(self._conn.results.get(table, []))

    def fetchall(self) -> list[dict]:
        return self._rows

    @property
    def rowcount(self) -> int:
        return self._conn.rowcounts[-1] if self._conn.rowcounts else 0


class _StubConnection:
    def __init__(
        self,
        rowcounts: list[int] | None = None,
        results: dict[str, list[dict]] | None = None,
    ) -> None:
        self.statements: list[str] = []
        self.param_sets: list[object] = []
        self.rowcounts = rowcounts
        self.results = results or {}
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


def _empty_catalog_results() -> dict[str, list[dict]]:
    # stub DB knows every dimension table but holds no rows: sync inserts
    # everything, the comparison finds no divergences
    return {"dim_region": [], "dim_metric": [], "dim_source": []}


def _region_db_row(entry, **overrides):
    row = {
        "region_code": entry.code,
        "valid_from": entry.valid_from,
        "name": entry.name,
        "kind": entry.kind,
        "tz": entry.tz,
        "valid_to": entry.valid_to,
    }
    row.update(overrides)
    return row


def _metric_db_row(entry, **overrides):
    plausible_min, plausible_max = None, None
    if entry.plausible_range is not None:
        plausible_min, plausible_max = entry.plausible_range
    row = {
        "metric_key": entry.key,
        "valid_from": entry.valid_from,
        "unit": entry.unit,
        "category": entry.category,
        "aggregation": entry.aggregation,
        "is_derived": entry.derived,
        "is_renewable": entry.is_renewable,
        "can_be_negative": entry.can_be_negative,
        "plausible_min": plausible_min,
        "plausible_max": plausible_max,
        "display_name": entry.display_name,
        "description": entry.description,
        "valid_to": entry.valid_to,
    }
    row.update(overrides)
    return row


def _source_db_row(source_key, entry, **overrides):
    row = {
        "source_key": source_key,
        "name": entry.name,
        "license": entry.license,
        "url": entry.url,
    }
    row.update(overrides)
    return row


def test_sync_dims_sends_all_catalog_rows(monkeypatch):
    conn = _StubConnection(
        rowcounts=_rowcounts_for_catalog(), results=_empty_catalog_results()
    )

    result = sync_dims(conn)

    assert result.inserted == len(REGIONS) + len(METRICS) + len(SOURCES)
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

    assert sync_dims(conn).inserted == 0


# --- collect_divergences (C3) ---


def test_collect_divergences_equal_rows_yield_nothing(monkeypatch):
    import wattfrosch_store.sync_dims as sync_module

    entry = REGIONS["DE-LU"]
    monkeypatch.setattr(sync_module, "REGIONS", {"DE-LU": entry})
    monkeypatch.setattr(sync_module, "METRICS", {})
    monkeypatch.setattr(sync_module, "SOURCES", {})
    conn = _StubConnection(results={"dim_region": [_region_db_row(entry)]})

    assert collect_divergences(conn) == []


def test_collect_divergences_detects_content_diff(monkeypatch):
    import wattfrosch_store.sync_dims as sync_module

    entry = REGIONS["DE-LU"]
    monkeypatch.setattr(sync_module, "REGIONS", {"DE-LU": entry})
    monkeypatch.setattr(sync_module, "METRICS", {})
    monkeypatch.setattr(sync_module, "SOURCES", {})
    conn = _StubConnection(
        results={"dim_region": [_region_db_row(entry, name="WRONG NAME")]}
    )

    divergences = collect_divergences(conn)

    assert len(divergences) == 1
    assert divergences[0].dimension == "region"
    assert divergences[0].key == "DE-LU"
    assert divergences[0].valid_from == date(2018, 10, 1)
    assert divergences[0].kind == "content_differs"
    assert divergences[0].fields == {
        "name": (entry.name, "WRONG NAME"),
    }


def test_collect_divergences_detects_missing_row(monkeypatch):
    import wattfrosch_store.sync_dims as sync_module

    entry = REGIONS["DE-LU"]
    monkeypatch.setattr(sync_module, "REGIONS", {"DE-LU": entry})
    monkeypatch.setattr(sync_module, "METRICS", {})
    monkeypatch.setattr(sync_module, "SOURCES", {})
    conn = _StubConnection(results={"dim_region": []})

    divergences = collect_divergences(conn)

    assert divergences == [
        Divergence(
            dimension="region",
            key="DE-LU",
            valid_from=date(2018, 10, 1),
            kind="missing_in_db",
        )
    ]


def test_collect_divergences_metric_fields_and_mapping(monkeypatch):
    import wattfrosch_store.sync_dims as sync_module

    entry = METRICS["generation.solar"]
    monkeypatch.setattr(sync_module, "REGIONS", {})
    monkeypatch.setattr(sync_module, "METRICS", {"generation.solar": entry})
    monkeypatch.setattr(sync_module, "SOURCES", {})
    db_row = _metric_db_row(
        entry,
        plausible_max=42.0,
        display_name="OTHER",
        is_derived=True,  # DB says derived, catalog does not
    )
    conn = _StubConnection(results={"dim_metric": [db_row]})

    divergences = collect_divergences(conn)

    assert len(divergences) == 1
    fields = divergences[0].fields
    assert set(fields) == {"plausible_max", "display_name", "is_derived"}
    assert fields["plausible_max"] == (100_000.0, 42.0)
    assert fields["is_derived"] == (False, True)  # catalog 'derived' mapping


def test_collect_divergences_sources_keyed_without_valid_from(monkeypatch):
    import wattfrosch_store.sync_dims as sync_module

    monkeypatch.setattr(
        sync_module, "SOURCES", {"energy-charts": SOURCES["energy-charts"]}
    )
    monkeypatch.setattr(sync_module, "REGIONS", {})
    monkeypatch.setattr(sync_module, "METRICS", {})
    entry = SOURCES["energy-charts"]
    conn = _StubConnection(
        results={"dim_source": [_source_db_row("energy-charts", entry, url="X")]}
    )

    divergences = collect_divergences(conn)

    assert len(divergences) == 1
    assert divergences[0].dimension == "source"
    assert divergences[0].valid_from is None
    assert divergences[0].fields == {"url": (entry.url, "X")}


def test_collect_divergences_is_read_only(monkeypatch):
    import wattfrosch_store.sync_dims as sync_module

    monkeypatch.setattr(
        sync_module,
        "REGIONS",
        {"DE-LU": REGIONS["DE-LU"]},
    )
    monkeypatch.setattr(sync_module, "METRICS", {})
    monkeypatch.setattr(sync_module, "SOURCES", {})
    conn = _StubConnection(results={"dim_region": []})

    collect_divergences(conn)

    assert conn.statements
    assert all(s.lstrip().startswith("SELECT") for s in conn.statements)


def test_sync_dims_reports_divergences(monkeypatch):
    import wattfrosch_store.sync_dims as sync_module

    entry = REGIONS["DE-LU"]
    monkeypatch.setattr(sync_module, "REGIONS", {"DE-LU": entry})
    monkeypatch.setattr(sync_module, "METRICS", {})
    monkeypatch.setattr(sync_module, "SOURCES", {})
    conn = _StubConnection(
        rowcounts=[1],
        results={"dim_region": [_region_db_row(entry, tz="Europe/Vienna")]},
    )

    result = sync_dims(conn)

    assert result.inserted == 1
    assert len(result.divergences) == 1
    assert result.divergences[0].kind == "content_differs"
    assert result.divergences[0].fields == {"tz": ("Europe/Berlin", "Europe/Vienna")}


# --- integration (real DB) ---


@pytest.mark.integration
def test_sync_dims_inserts_and_is_idempotent(clean_db):
    run_init(clean_db)
    run_migrations(clean_db)

    with psycopg.connect(clean_db.database_url, row_factory=dict_row) as conn:
        first = sync_dims(conn)
    assert first.inserted == len(REGIONS) + len(METRICS) + len(SOURCES)
    assert first.divergences == []

    with psycopg.connect(clean_db.database_url, row_factory=dict_row) as conn:
        second = sync_dims(conn)
    assert second.inserted == 0  # re-run inserts nothing
    assert second.divergences == []


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


@pytest.mark.integration
def test_collect_divergences_clean_after_sync(clean_db):
    run_init(clean_db)
    run_migrations(clean_db)

    with psycopg.connect(
        clean_db.database_url, row_factory=dict_row, autocommit=True
    ) as conn:
        sync_dims(conn)
        assert collect_divergences(conn) == []


@pytest.mark.integration
def test_collect_divergences_detects_forced_changes(clean_db):
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
        divergences = collect_divergences(conn)
        metric_div = next(
            d
            for d in divergences
            if d.dimension == "metric" and d.key == "generation.solar"
        )
        assert metric_div.kind == "content_differs"
        assert metric_div.fields == {
            "display_name": (METRICS["generation.solar"].display_name, "WRONG")
        }

        conn.execute("DELETE FROM raw.dim_region WHERE region_code = 'DE-AT-LU'")
        divergences = collect_divergences(conn)
        missing = next(
            d for d in divergences if d.dimension == "region" and d.key == "DE-AT-LU"
        )
        assert missing.kind == "missing_in_db"
        assert missing.fields == {}

        # insert-only sync re-inserts the deleted row, but cannot fix content
        result = sync_dims(conn)
        assert result.inserted == 1
        keys = {(d.dimension, d.key) for d in result.divergences}
        assert ("metric", "generation.solar") in keys
        assert ("region", "DE-AT-LU") not in keys
