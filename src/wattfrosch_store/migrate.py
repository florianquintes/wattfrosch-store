"""Migration runner with advisory lock.

Applies ``sql/migrations/*.sql`` in numeric order, one transaction per file,
logging each application in ``raw.schema_migration``. The session-level
advisory lock (``MIGRATE_LOCK_KEY``) is taken **before** the applied list is
read — that ordering, not the lock alone, protects against concurrent runs.

Used only by the ``migrate`` CLI command; the pipeline never calls it, so
exceptions may cross this module's boundary.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import LiteralString, cast

import psycopg
import psycopg.sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from psycopg.rows import dict_row
from wattfrosch_contracts import CONTRACT_VERSION

from wattfrosch_store import StoreError
from wattfrosch_store.config import Settings
from wattfrosch_store.db import BOOTSTRAP_SQL, connect, translate_db_errors

logger = logging.getLogger(__name__)

MIGRATE_LOCK_KEY = 0x77617474  # "watt", fixed documented constant

# v1 runs the CLI from the repo (uv run); the pipeline never calls this.
MIGRATIONS_DIR = Path(__file__).resolve().parents[2] / "sql" / "migrations"

_MIGRATION_NAME_RE = re.compile(r"^\d{4}_[a-z0-9_]+\.sql$")


class MigrationLockTimeoutError(StoreError):
    """Another process holds the migration advisory lock."""


class MigrationStateError(StoreError):
    """The migration state is inconsistent (e.g. database newer than module)."""


class MigrationApplyError(StoreError):
    """A migration file failed to apply; its transaction was rolled back."""


@dataclass(frozen=True)
class MigrationResult:
    applied: tuple[str, ...]  # applied during this run, in file order
    skipped: tuple[str, ...]  # already applied before the run


@dataclass(frozen=True)
class InitResult:
    db_created: bool
    # Already applied *before* this init — not "applied by init" (init never
    # applies star-schema DDL; that is migrate's job, see MigrationResult).
    applied: tuple[str, ...]
    pending: tuple[str, ...]  # waiting for ``migrate``


def discover_migrations(directory: Path | None = None) -> tuple[Path, ...]:
    """Return the migration files in ``sql/migrations`` in numeric order."""
    migrations_dir = directory if directory is not None else MIGRATIONS_DIR
    if not migrations_dir.is_dir():
        raise MigrationStateError(f"Migrations directory not found: {migrations_dir}")
    files = []
    for path in migrations_dir.glob("*.sql"):
        if not _MIGRATION_NAME_RE.fullmatch(path.name):
            raise MigrationStateError(
                f"Invalid migration file name '{path.name}': "
                "expected 'NNNN_name.sql' (e.g. '0001_initial.sql')"
            )
        files.append(path)
    files.sort(key=lambda p: (int(p.name[:4]), p.name))
    return tuple(files)


def _plan(
    files: tuple[Path, ...], applied_db: set[str]
) -> tuple[list[Path], tuple[str, ...]]:
    """Split files into pending and skipped; abort if the DB is newer."""
    known = {f.stem for f in files}
    unknown = applied_db - known
    if unknown:
        raise MigrationStateError(
            "Database is newer than this module; applied migrations missing "
            f"on disk: {', '.join(sorted(unknown))}. Applied migrations are "
            "never edited — ship or restore the missing files instead."
        )
    pending = [f for f in files if f.stem not in applied_db]
    skipped = tuple(f.stem for f in files if f.stem in applied_db)
    return pending, skipped


def _lock_timeout_literal(timeout: timedelta) -> str:
    return f"{timeout.total_seconds():g}s"


def _acquire_lock(
    conn: psycopg.Connection[dict],
    cur: psycopg.Cursor[dict],
    settings: Settings,
) -> None:
    timeout = _lock_timeout_literal(settings.migrate_lock_timeout)
    cur.execute("SELECT set_config('lock_timeout', %s, false)", (timeout,))
    conn.commit()
    try:
        cur.execute("SELECT pg_advisory_lock(%s)", (MIGRATE_LOCK_KEY,))
    except psycopg.errors.LockNotAvailable as exc:
        raise MigrationLockTimeoutError(
            f"Migration already running in another process (waited {timeout}). "
            "Run aborted, nothing applied."
        ) from exc
    finally:
        conn.rollback()
        cur.execute("SELECT set_config('lock_timeout', '0', false)")
        conn.commit()


def run_migrations(settings: Settings) -> MigrationResult:
    """Apply all pending migrations; idempotent via ``raw.schema_migration``.

    Lock first, then read the applied list. One transaction per file. The
    advisory lock is released on every path (explicitly, or via close).
    """
    files = discover_migrations()
    conn = connect(settings)
    locked = False
    try:
        with conn.cursor() as cur:
            _acquire_lock(conn, cur, settings)
            locked = True

            cur.execute(BOOTSTRAP_SQL)
            conn.commit()

            cur.execute("SELECT migration_id FROM raw.schema_migration")
            applied_db = {row["migration_id"] for row in cur.fetchall()}
            conn.commit()  # close the read transaction so each file gets its own
            pending, skipped = _plan(files, applied_db)
            if pending:
                logger.info(
                    "Pending migrations: %s", ", ".join(f.name for f in pending)
                )

            for path in pending:
                logger.info("Applying migration %s", path.name)
                try:
                    with conn.transaction():
                        cur.execute(
                            cast("LiteralString", path.read_text(encoding="utf-8"))
                        )
                        cur.execute(
                            "INSERT INTO raw.schema_migration "
                            "(migration_id, contract_version) VALUES (%s, %s)",
                            (path.stem, CONTRACT_VERSION),
                        )
                except psycopg.Error as exc:
                    raise MigrationApplyError(
                        f"Migration {path.name} failed: {exc}"
                    ) from exc

            applied = tuple(f.stem for f in pending)
            logger.info(
                "Migrations applied: %d, skipped: %d", len(applied), len(skipped)
            )
            return MigrationResult(applied=applied, skipped=skipped)
    finally:
        if locked:
            try:
                conn.rollback()
                conn.execute("SELECT pg_advisory_unlock(%s)", (MIGRATE_LOCK_KEY,))
                conn.commit()
            except psycopg.Error:
                pass
        conn.close()


def run_init(settings: Settings) -> InitResult:
    """Create database, schema ``raw`` and the log table; never star-schema DDL.

    Connects to the maintenance database ``postgres`` (deliberately not via
    :func:`connect`, which targets the store database), creates the store
    database if missing, then bootstraps schema and log table in the target
    database and reports the migration status.
    """
    conninfo: dict[str, str | int] = {
        key: value
        for key, value in conninfo_to_dict(settings.database_url).items()
        if value is not None
    }
    target_db = cast(
        "str", conninfo.get("dbname") or conninfo.get("user") or "postgres"
    )

    maintenance_dsn = make_conninfo(settings.database_url, dbname="postgres")

    with translate_db_errors():
        with (
            psycopg.connect(
                maintenance_dsn,
                autocommit=True,
                row_factory=dict_row,
            ) as maintenance,
            maintenance.cursor() as cur,
        ):
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (target_db,))
            db_created = cur.fetchone() is None
            if db_created:
                logger.info("Creating database %s", target_db)
                try:
                    cur.execute(
                        psycopg.sql.SQL("CREATE DATABASE {}").format(
                            psycopg.sql.Identifier(target_db)
                        )
                    )
                except psycopg.errors.DuplicateDatabase:
                    db_created = False  # a parallel init won the race
                except psycopg.errors.InsufficientPrivilege as exc:
                    raise StoreError(
                        "Missing CREATEDB privilege (SQLSTATE 42501). "
                        "Hint: docker compose up -d db"
                    ) from exc

        conn = connect(settings)
        try:
            with conn.cursor() as cur:
                cur.execute(BOOTSTRAP_SQL)
                conn.commit()
                cur.execute("SELECT migration_id FROM raw.schema_migration")
                applied_db = {row["migration_id"] for row in cur.fetchall()}
                conn.commit()
            # _plan's second value is the *skipped* set: files already applied
            # before this init (init never applies migrations itself).
            pending, already_applied = _plan(discover_migrations(), applied_db)
        finally:
            conn.close()

    return InitResult(
        db_created=db_created,
        applied=already_applied,
        pending=tuple(f.stem for f in pending),
    )
