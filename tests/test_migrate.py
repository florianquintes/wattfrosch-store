"""Tests for the migration runner (B2): T1 fresh apply, T2 idempotent."""

from __future__ import annotations

import contextlib
from datetime import timedelta
from pathlib import Path
from typing import Self

import psycopg
import pytest
from psycopg.rows import dict_row
from wattfrosch_contracts import CONTRACT_VERSION

from wattfrosch_store import StoreError
from wattfrosch_store.config import load_settings
from wattfrosch_store.migrate import (
    MigrationApplyError,
    MigrationLockTimeoutError,
    MigrationStateError,
    _lock_timeout_literal,
    _plan,
    discover_migrations,
    run_init,
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
def test_t2_run_twice_is_idempotent(clean_db):
    run_migrations(clean_db)

    result = run_migrations(clean_db)
    assert result.applied == ()
    assert result.skipped == ("0001_initial",)
    assert len(_log_rows(clean_db)) == 1


# --- unit tests for run_migrations with a stub connection (no DB) ---


class _StubCursor:
    def __init__(self, conn: _StubConnection) -> None:
        self._conn = conn
        self.statements: list[str] = []
        self.rows: list[dict] = []

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> bool:
        return False

    def execute(self, query: str, params: object = None) -> None:
        self.statements.append(query)
        if self._conn.fail_on and self._conn.fail_on in query:
            raise self._conn.fail_error
        if "SELECT migration_id" in query:
            self.rows = [{"migration_id": m} for m in self._conn.applied_db]

    def fetchall(self) -> list[dict]:
        return self.rows


class _StubConnection:
    def __init__(
        self, applied_db: list[str] | None = None, fail_on: str | None = None
    ) -> None:
        self.applied_db = applied_db or []
        self.fail_on = fail_on
        self.fail_error: Exception = psycopg.errors.UndefinedTable("boom")
        self.commits = 0
        self.rollbacks = 0
        self.closed = False
        self.unlocked = False
        self.cursors: list[_StubCursor] = []

    def cursor(self) -> _StubCursor:
        cur = _StubCursor(self)
        self.cursors.append(cur)
        return cur

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1

    def close(self) -> None:
        self.closed = True

    def execute(self, query: str, params: object = None) -> None:
        if "pg_advisory_unlock" in query:
            self.unlocked = True

    def transaction(self):
        return contextlib.nullcontext()


def _run_with_stub(monkeypatch, conn: _StubConnection, files: tuple[Path, ...]):
    import wattfrosch_store.migrate as migrate_module

    monkeypatch.setattr(migrate_module, "connect", lambda settings: conn)
    monkeypatch.setattr(migrate_module, "discover_migrations", lambda: files)
    return run_migrations(load_settings())


# --- unit tests for run_init with stub connections (no DB) ---


class _StubMaintenanceCursor:
    def __init__(self, conn: _StubMaintenanceConnection) -> None:
        self._conn = conn

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> bool:
        return False

    def execute(self, query: str, params: object = None) -> None:
        self._conn.statements.append(query)
        if "pg_database" in query:
            self._conn.db_row = {"?column?": 1} if self._conn.db_exists else None
        elif self._conn.create_error is not None:
            raise self._conn.create_error

    def fetchone(self) -> dict | None:
        return self._conn.db_row


class _StubMaintenanceConnection:
    def __init__(self, db_exists: bool, create_error: Exception | None = None) -> None:
        self.db_exists = db_exists
        self.create_error = create_error
        self.db_row: dict | None = None
        self.statements: list[str] = []
        self.connect_dsn = ""
        self.connect_kwargs: dict = {}

    def cursor(self) -> _StubMaintenanceCursor:
        return _StubMaintenanceCursor(self)

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> bool:
        return False


def _run_init_with_stubs(
    monkeypatch,
    maintenance: _StubMaintenanceConnection,
    target: _StubConnection,
):
    import wattfrosch_store.migrate as migrate_module

    def fake_psycopg_connect(dsn, **kwargs):
        maintenance.connect_dsn = dsn
        maintenance.connect_kwargs = kwargs
        return maintenance

    monkeypatch.setattr(migrate_module.psycopg, "connect", fake_psycopg_connect)
    monkeypatch.setattr(migrate_module, "connect", lambda settings: target)
    return run_init(load_settings())


def test_init_creates_missing_database(monkeypatch):
    maintenance = _StubMaintenanceConnection(db_exists=False)
    target = _StubConnection()

    result = _run_init_with_stubs(monkeypatch, maintenance, target)

    assert result.db_created is True
    assert result.applied == ()
    assert result.pending == ("0001_initial",)
    assert "dbname=postgres" in maintenance.connect_dsn
    assert maintenance.connect_kwargs["autocommit"] is True
    assert len(maintenance.statements) == 2  # check + CREATE DATABASE
    assert target.closed


def test_init_skips_existing_database(monkeypatch):
    maintenance = _StubMaintenanceConnection(db_exists=True)
    target = _StubConnection(applied_db=["0001_initial"])

    result = _run_init_with_stubs(monkeypatch, maintenance, target)

    assert result.db_created is False
    assert result.applied == ("0001_initial",)
    assert result.pending == ()
    assert len(maintenance.statements) == 1  # check only, no CREATE DATABASE


def test_init_tolerates_duplicate_database(monkeypatch):
    maintenance = _StubMaintenanceConnection(
        db_exists=False,
        create_error=psycopg.errors.DuplicateDatabase("parallel init won"),
    )

    result = _run_init_with_stubs(monkeypatch, maintenance, _StubConnection())

    assert result.db_created is False


def test_init_missing_createdb_privilege(monkeypatch):
    maintenance = _StubMaintenanceConnection(
        db_exists=False,
        create_error=psycopg.errors.InsufficientPrivilege("no CREATEDB"),
    )

    with pytest.raises(StoreError, match="docker compose up -d db"):
        _run_init_with_stubs(monkeypatch, maintenance, _StubConnection())


def test_init_fails_when_db_newer(monkeypatch):
    maintenance = _StubMaintenanceConnection(db_exists=True)
    target = _StubConnection(applied_db=["0009_missing"])

    with pytest.raises(MigrationStateError, match="newer than this module"):
        _run_init_with_stubs(monkeypatch, maintenance, target)

    assert target.closed


def test_run_applies_pending_files(tmp_path, monkeypatch):
    files = (
        tmp_path / "0001_initial.sql",
        tmp_path / "0002_next.sql",
    )
    for path in files:
        path.write_text(f"-- migration {path.stem}\n")
    conn = _StubConnection()

    result = _run_with_stub(monkeypatch, conn, files)

    assert result.applied == ("0001_initial", "0002_next")
    assert result.skipped == ()
    assert conn.unlocked and conn.closed
    # file content was executed within the run
    statements = conn.cursors[0].statements
    assert any("-- migration 0001_initial" in s for s in statements)
    assert any("-- migration 0002_next" in s for s in statements)


def test_run_records_log_rows(tmp_path, monkeypatch):
    migration = tmp_path / "0001_initial.sql"
    migration.write_text("-- ok\n")
    files = (migration,)
    conn = _StubConnection()
    captured: list[tuple] = []

    import wattfrosch_store.migrate as migrate_module

    original_execute = _StubCursor.execute

    def spy_execute(self, query, params=None):
        original_execute(self, query, params)
        if params is not None and "INSERT INTO raw.schema_migration" in query:
            captured.append(params)

    monkeypatch.setattr(_StubCursor, "execute", spy_execute)
    monkeypatch.setattr(migrate_module, "connect", lambda settings: conn)
    monkeypatch.setattr(migrate_module, "discover_migrations", lambda: files)

    result = run_migrations(load_settings())

    assert result.applied == ("0001_initial",)
    assert captured == [("0001_initial", CONTRACT_VERSION)]
    assert conn.unlocked and conn.closed


def test_run_skips_already_applied(monkeypatch):
    files = (Path("0001_initial.sql"),)
    conn = _StubConnection(applied_db=["0001_initial"])

    result = _run_with_stub(monkeypatch, conn, files)

    assert result.applied == ()
    assert result.skipped == ("0001_initial",)


def test_run_aborts_when_db_newer(monkeypatch):
    conn = _StubConnection(applied_db=["0009_missing"])
    with pytest.raises(MigrationStateError, match="newer than this module"):
        _run_with_stub(monkeypatch, conn, (Path("0001_initial.sql"),))
    assert conn.closed


def test_run_wraps_apply_failure(tmp_path, monkeypatch, caplog):
    good = tmp_path / "0001_initial.sql"
    good.write_text("-- ok\n")
    bad = tmp_path / "0002_bad.sql"
    bad.write_text("-- boom marker\nBOOM;\n")
    conn = _StubConnection(applied_db=["0001_initial"], fail_on="BOOM")

    with (
        caplog.at_level("INFO"),
        pytest.raises(MigrationApplyError, match="0002_bad.sql failed"),
    ):
        _run_with_stub(monkeypatch, conn, (good, bad))

    assert conn.closed and conn.unlocked
    messages = [r.getMessage() for r in caplog.records]
    assert messages.index("Applying migration 0002_bad.sql") < len(messages)


def test_run_logs_pending_before_applying(tmp_path, monkeypatch, caplog):
    files = (
        tmp_path / "0001_initial.sql",
        tmp_path / "0002_next.sql",
    )
    for path in files:
        path.write_text("-- ok\n")
    conn = _StubConnection()

    with caplog.at_level("INFO"):
        _run_with_stub(monkeypatch, conn, files)

    messages = [r.getMessage() for r in caplog.records]
    pending_idx = next(
        i for i, m in enumerate(messages) if m.startswith("Pending migrations")
    )
    apply_idx = next(
        i for i, m in enumerate(messages) if m.startswith("Applying migration")
    )
    assert pending_idx < apply_idx


def test_run_lock_timeout_resets_and_closes(monkeypatch):
    conn = _StubConnection(fail_on="pg_advisory_lock")
    conn.fail_error = psycopg.errors.LockNotAvailable("held elsewhere")

    with pytest.raises(MigrationLockTimeoutError, match="already running"):
        _run_with_stub(monkeypatch, conn, (Path("0001_initial.sql"),))

    assert conn.rollbacks >= 1
    assert conn.closed
    # lock_timeout was set before locking and reset to 0 afterwards
    lock_timeout_calls = [s for s in conn.cursors[0].statements if "lock_timeout" in s]
    assert len(lock_timeout_calls) >= 2


def test_run_swallows_unlock_failure(tmp_path, monkeypatch):
    migration = tmp_path / "0001_initial.sql"
    migration.write_text("-- ok\n")
    conn = _StubConnection()

    def broken_execute(query: str, params: object = None) -> None:
        raise psycopg.errors.OperationalError("connection gone")

    monkeypatch.setattr(conn, "execute", broken_execute)

    result = _run_with_stub(monkeypatch, conn, (migration,))

    assert result.applied == ("0001_initial",)
    assert conn.closed  # teardown completed despite the failed unlock


# --- integration tests for run_init (C1) ---


@pytest.mark.integration
def test_init_fresh_and_idempotent(clean_db):
    first = run_init(clean_db)

    assert first.db_created is False  # compose DB exists via POSTGRES_DB
    assert first.applied == ()
    assert first.pending == ("0001_initial",)

    second = run_init(clean_db)
    assert second == first  # idempotent


@pytest.mark.integration
def test_init_migrate_flow(clean_db):
    run_init(clean_db)
    run_migrations(clean_db)

    status = run_init(clean_db)
    assert status.applied == ("0001_initial",)
    assert status.pending == ()
