"""Tests for ``verify`` (D4) and the deferred T4 divergence exit-code tests."""

from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Self

import psycopg
import pytest
from psycopg.rows import dict_row
from wattfrosch_contracts import CONTRACT_VERSION, REGIONS

import wattfrosch_store.sync_dims as sync_module
import wattfrosch_store.verify as verify_module
from wattfrosch_store.config import Settings
from wattfrosch_store.migrate import run_init, run_migrations
from wattfrosch_store.verify import run_verify

REPO_ROOT = Path(__file__).resolve().parents[1]


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
    def __init__(self, results: dict[str, list[dict]]) -> None:
        self.results = results
        self.statements: list[str] = []

    def cursor(self) -> _StubCursor:
        return _StubCursor(self)

    def rollback(self) -> None:
        pass

    def close(self) -> None:
        pass


def _verify_results(
    *,
    contract_version: str = CONTRACT_VERSION,
    fact: int = 2,
    dim_time: int = 2,
    dim_region: int = 1,
    dim_metric: int = 1,
    dim_source: int = 1,
    dim_region_rows: list[dict] | None = None,
    outliers: list[dict] | None = None,
) -> dict[str, list[dict]]:
    return {
        "FROM raw.schema_migration": [{"contract_version": contract_version}],
        "SELECT * FROM raw.dim_region": dim_region_rows or [],
        "SELECT * FROM raw.dim_metric": [],
        "SELECT * FROM raw.dim_source": [],
        "count(*) AS n FROM raw.fact_measurement": [{"n": fact}],
        "count(*) AS n FROM raw.dim_time": [{"n": dim_time}],
        "count(*) AS n FROM raw.dim_region": [{"n": dim_region}],
        "count(*) AS n FROM raw.dim_metric": [{"n": dim_metric}],
        "count(*) AS n FROM raw.dim_source": [{"n": dim_source}],
        "GROUP BY m.metric_key": outliers or [],
    }


@pytest.fixture
def no_catalog_rows(monkeypatch):
    monkeypatch.setattr(sync_module, "REGIONS", {})
    monkeypatch.setattr(sync_module, "METRICS", {})
    monkeypatch.setattr(sync_module, "SOURCES", {})


@pytest.fixture
def stub_connect(monkeypatch):
    def _patch(conn: _StubConnection) -> _StubConnection:
        monkeypatch.setattr(verify_module, "connect", lambda settings: conn)
        return conn

    return _patch


def test_verify_ok_with_data(no_catalog_rows, stub_connect):
    stub_connect(_StubConnection(_verify_results()))

    result = run_verify(Settings(), check_values=False)

    assert result.ok
    assert not result.empty
    assert result.failures == []
    assert result.counts == {
        "fact_measurement": 2,
        "dim_time": 2,
        "dim_region": 1,
        "dim_metric": 1,
        "dim_source": 1,
    }
    assert result.outliers == {}


def test_verify_empty_database_is_ok_but_empty(no_catalog_rows, stub_connect):
    stub_connect(_StubConnection(_verify_results(fact=0, dim_time=0)))

    result = run_verify(Settings(), check_values=False)

    assert result.ok
    assert result.empty
    assert result.failures == []


def test_verify_fresh_db_ignores_missing_dimensions(stub_connect):
    # no catalog monkeypatch: the real catalogs are all missing_in_db on a
    # fresh database (no sync yet) — suppressed by the empty-DB rule
    stub_connect(
        _StubConnection(
            _verify_results(
                fact=0, dim_time=0, dim_region=0, dim_metric=0, dim_source=0
            )
        )
    )

    result = run_verify(Settings(), check_values=False)

    assert result.ok
    assert result.empty


def test_verify_content_diff_fails_even_when_empty(stub_connect):
    entry = REGIONS["DE-LU"]
    stub_connect(
        _StubConnection(
            _verify_results(
                fact=0,
                dim_time=0,
                dim_region_rows=[
                    {
                        "region_code": "DE-LU",
                        "valid_from": entry.valid_from,
                        "name": "WRONG NAME",
                        "kind": entry.kind,
                        "tz": entry.tz,
                        "valid_to": entry.valid_to,
                    }
                ],
            )
        )
    )

    result = run_verify(Settings(), check_values=False)

    assert not result.ok
    assert result.empty  # still counts as empty, but content is strict
    assert any("content_differs" in f for f in result.failures)


def test_verify_divergence_fails_with_data(stub_connect):
    entry = REGIONS["DE-LU"]
    stub_connect(
        _StubConnection(
            _verify_results(
                dim_region_rows=[
                    {
                        "region_code": "DE-LU",
                        "valid_from": entry.valid_from,
                        "name": "WRONG NAME",
                        "kind": entry.kind,
                        "tz": entry.tz,
                        "valid_to": entry.valid_to,
                    }
                ],
            )
        )
    )

    result = run_verify(Settings(), check_values=False)

    assert not result.ok
    assert any("DE-LU" in f and "content_differs" in f for f in result.failures)


def test_verify_missing_dimension_fails_with_data(monkeypatch, stub_connect):
    from wattfrosch_contracts import REGIONS as CATALOG

    monkeypatch.setattr(sync_module, "REGIONS", {"DE-LU": CATALOG["DE-LU"]})
    monkeypatch.setattr(sync_module, "METRICS", {})
    monkeypatch.setattr(sync_module, "SOURCES", {})
    # data present but the DE-LU row is gone: not empty, divergence is hard
    stub_connect(_StubConnection(_verify_results(dim_region=0)))

    result = run_verify(Settings(), check_values=False)

    assert not result.ok
    assert not result.empty
    assert any("missing_in_db" in f for f in result.failures)


def test_verify_empty_fact_table_fails(no_catalog_rows, stub_connect):
    stub_connect(_StubConnection(_verify_results(fact=0)))

    result = run_verify(Settings(), check_values=False)

    assert not result.ok
    assert any("fact_measurement is empty" in f for f in result.failures)


def test_verify_check_values_reports_outliers_without_failing(
    no_catalog_rows, stub_connect
):
    stub_connect(
        _StubConnection(
            _verify_results(
                outliers=[{"metric_key": "generation.solar", "outliers": 3}]
            )
        )
    )

    result = run_verify(Settings(), check_values=True)

    assert result.ok  # outliers are reported, never failures
    assert result.outliers == {"generation.solar": 3}


def test_verify_without_check_values_skips_scan(no_catalog_rows, stub_connect):
    conn = stub_connect(_StubConnection(_verify_results()))

    result = run_verify(Settings(), check_values=False)

    assert result.outliers == {}
    assert not any("GROUP BY m.metric_key" in s for s in conn.statements)


# --- T4: CLI-level divergence behaviour (deferred from C3) ---


def _run(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["uv", "run", "wattfrosch-store", *args],
        check=False,
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
        timeout=120,
    )


@pytest.mark.integration
def test_t4_load_with_divergence_warns_exit_0(clean_db):
    run_init(clean_db)
    run_migrations(clean_db)

    first = _run("load", "--from-file", "tests/fixtures/valid.ndjson")
    assert first.returncode == 0, first.stderr

    with psycopg.connect(
        clean_db.database_url, row_factory=dict_row, autocommit=True
    ) as conn:
        conn.execute(
            "UPDATE raw.dim_metric SET display_name = 'WRONG' "
            "WHERE metric_key = 'generation.solar'"
        )

    second = _run("load", "--from-file", "tests/fixtures/valid.ndjson")
    assert second.returncode == 0, second.stderr
    assert "catalog vs database divergences" in second.stderr
    assert "generation.solar" in second.stderr


@pytest.mark.integration
def test_t4_verify_with_divergence_exit_2(clean_db):
    run_init(clean_db)
    run_migrations(clean_db)
    assert _run("load", "--from-file", "tests/fixtures/valid.ndjson").returncode == 0

    with psycopg.connect(
        clean_db.database_url, row_factory=dict_row, autocommit=True
    ) as conn:
        conn.execute(
            "UPDATE raw.dim_metric SET display_name = 'WRONG' "
            "WHERE metric_key = 'generation.solar'"
        )

    result = _run("verify")
    assert result.returncode == 2, result.stderr
    assert "content_differs" in result.stderr


@pytest.mark.integration
def test_verify_fresh_database_exit_0_with_empty_notice(clean_db):
    run_init(clean_db)
    run_migrations(clean_db)

    result = _run("verify")
    assert result.returncode == 0, result.stderr
    assert "database is empty" in result.stderr


@pytest.mark.integration
def test_verify_after_load_exit_0(clean_db):
    run_init(clean_db)
    run_migrations(clean_db)
    assert _run("load", "--from-file", "tests/fixtures/valid.ndjson").returncode == 0

    result = _run("verify")
    assert result.returncode == 0, result.stderr
    assert "raw.fact_measurement: 2 row(s)" in result.stderr


@pytest.mark.integration
def test_verify_check_values_reports_outliers_exit_0(clean_db):
    run_init(clean_db)
    run_migrations(clean_db)
    assert _run("load", "--from-file", "tests/fixtures/valid.ndjson").returncode == 0

    with psycopg.connect(
        clean_db.database_url, row_factory=dict_row, autocommit=True
    ) as conn:
        conn.execute("UPDATE raw.fact_measurement SET value = 1e9")

    result = _run("verify", "--check-values")
    assert result.returncode == 0, result.stderr
    assert "generation.solar" in result.stderr
    assert "outlier" in result.stderr
