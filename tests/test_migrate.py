"""Tests for the migration runner (B2): T1 fresh apply, T2 idempotent, T7 lock."""

from datetime import timedelta
from pathlib import Path

import psycopg
import pytest
from psycopg.rows import dict_row
from test_db import _db_available
from wattfrosch_contracts import CONTRACT_VERSION

from wattfrosch_store import StoreError
from wattfrosch_store.config import load_settings
from wattfrosch_store.migrate import (
    MIGRATE_LOCK_KEY,
    MigrationApplyError,
    MigrationLockTimeoutError,
    MigrationStateError,
    _lock_timeout_literal,
    _plan,
    discover_migrations,
    run_migrations,
)


def _write_migrations(tmp_path: Path, names: list[str]) -> Path:
    for name in names:
        (tmp_path / name).write_text("-- empty migration\n")
    return tmp_path


def test_discover_sorts_numerically(tmp_path):
    migrations_dir = _write_migrations(
        tmp_path, ["0002_second.sql", "0010_tenth.sql", "0001_first.sql"]
    )
    files = discover_migrations(migrations_dir)
    assert [f.name for f in files] == [
        "0001_first.sql",
        "0002_second.sql",
        "0010_tenth.sql",
    ]


def test_discover_rejects_malformed_name(tmp_path):
    _write_migrations(tmp_path, ["fix_stuff.sql"])
    with pytest.raises(MigrationStateError, match="Invalid migration file name"):
        discover_migrations(tmp_path)


def test_discover_missing_directory(tmp_path):
    with pytest.raises(MigrationStateError, match="not found"):
        discover_migrations(tmp_path / "nope")


def test_plan_aborts_when_db_is_newer():
    files = (Path("0001_initial.sql"),)
    with pytest.raises(MigrationStateError, match="newer than this module"):
        _plan(files, {"0001_initial", "0002_gone"})


def test_plan_splits_pending_and_skipped():
    files = (Path("0001_initial.sql"), Path("0002_next.sql"))
    pending, skipped = _plan(files, {"0001_initial"})
    assert [f.stem for f in pending] == ["0002_next"]
    assert skipped == ("0001_initial",)


def test_lock_timeout_literal_formats():
    assert _lock_timeout_literal(timedelta(seconds=30)) == "30s"
    assert _lock_timeout_literal(timedelta(seconds=1)) == "1s"
    assert _lock_timeout_literal(timedelta(milliseconds=500)) == "0.5s"


def test_migration_errors_are_store_errors():
    for cls in (
        MigrationLockTimeoutError,
        MigrationStateError,
        MigrationApplyError,
    ):
        assert issubclass(cls, StoreError)


def test_discover_real_migrations_dir():
    files = discover_migrations()
    assert files[0].stem == "0001_initial"


@pytest.fixture
def clean_db():
    settings = load_settings()
    with psycopg.connect(settings.database_url, autocommit=True) as conn:
        conn.execute("DROP SCHEMA IF EXISTS raw CASCADE")
    yield settings


def _log_rows(settings):
    with psycopg.connect(
        settings.database_url, autocommit=True, row_factory=dict_row
    ) as conn:
        return conn.execute(
            "SELECT migration_id, contract_version "
            "FROM raw.schema_migration ORDER BY migration_id"
        ).fetchall()


def _table_names(settings, schema: str) -> set[str]:
    with psycopg.connect(
        settings.database_url, autocommit=True, row_factory=dict_row
    ) as conn:
        rows = conn.execute(
            "SELECT tablename FROM pg_tables WHERE schemaname = %s", (schema,)
        ).fetchall()
    return {row["tablename"] for row in rows}


@pytest.mark.integration
@pytest.mark.skipif(not _db_available(), reason="compose DB not running")
def test_t1_fresh_apply(clean_db):
    result = run_migrations(clean_db)

    assert result.applied == ("0001_initial",)
    assert result.skipped == ()
    rows = _log_rows(clean_db)
    assert rows == [
        {"migration_id": "0001_initial", "contract_version": CONTRACT_VERSION}
    ]
    tables = _table_names(clean_db, "raw")
    assert {
        "dim_time",
        "dim_region",
        "dim_metric",
        "dim_source",
        "fact_measurement",
        "schema_migration",
    } <= tables


@pytest.mark.integration
@pytest.mark.skipif(not _db_available(), reason="compose DB not running")
def test_t2_run_twice_is_idempotent(clean_db):
    run_migrations(clean_db)

    result = run_migrations(clean_db)
    assert result.applied == ()
    assert result.skipped == ("0001_initial",)
    assert len(_log_rows(clean_db)) == 1


@pytest.mark.integration
@pytest.mark.skipif(not _db_available(), reason="compose DB not running")
def test_t7_lock_timeout_aborts(clean_db, monkeypatch):
    settings = load_settings()
    with psycopg.connect(settings.database_url, autocommit=True) as holder:
        holder.execute("SELECT pg_advisory_lock(%s)", (MIGRATE_LOCK_KEY,))

        monkeypatch.setenv("MIGRATE_LOCK_TIMEOUT", "1s")
        with pytest.raises(MigrationLockTimeoutError, match="already running"):
            run_migrations(load_settings())

        assert _table_names(settings, "raw") == set()  # nothing applied

        holder.execute("SELECT pg_advisory_unlock(%s)", (MIGRATE_LOCK_KEY,))

    # the aborted run left the lock free: a normal run succeeds now
    result = run_migrations(settings)
    assert result.applied == ("0001_initial",)
