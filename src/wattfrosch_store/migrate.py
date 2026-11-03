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
from wattfrosch_contracts import CONTRACT_VERSION

from wattfrosch_store import StoreError
from wattfrosch_store.config import Settings
from wattfrosch_store.db import connect

logger = logging.getLogger(__name__)

MIGRATE_LOCK_KEY = 0x77617474  # "watt", fixed documented constant

# v1 runs the CLI from the repo (uv run); the pipeline never calls this.
MIGRATIONS_DIR = Path(__file__).resolve().parents[2] / "sql" / "migrations"

_MIGRATION_NAME_RE = re.compile(r"^\d{4}_[a-z0-9_]+\.sql$")

# Bootstrapped by init/the runner, never by a migration file.
_BOOTSTRAP_SQL = """
CREATE TABLE IF NOT EXISTS raw.schema_migration (
    migration_id     text PRIMARY KEY,
    contract_version text NOT NULL,
    applied_at       timestamptz NOT NULL DEFAULT now()
)
"""


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

            cur.execute("CREATE SCHEMA IF NOT EXISTS raw")
            cur.execute(_BOOTSTRAP_SQL)
            conn.commit()

            cur.execute("SELECT migration_id FROM raw.schema_migration")
            applied_db = {row["migration_id"] for row in cur.fetchall()}
            conn.commit()  # close the read transaction so each file gets its own
            pending, skipped = _plan(files, applied_db)

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
