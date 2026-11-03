"""T7 — advisory lock timeout during migrate."""

from __future__ import annotations

from datetime import timedelta

import psycopg
import pytest

from wattfrosch_store.config import Settings
from wattfrosch_store.migrate import (
    MIGRATE_LOCK_KEY,
    MigrationLockTimeoutError,
    run_migrations,
)


def _table_names(settings: Settings, schema: str) -> set[str]:
    with psycopg.connect(settings.database_url, autocommit=True) as conn:
        rows = conn.execute(
            "SELECT tablename FROM pg_tables WHERE schemaname = %s", (schema,)
        ).fetchall()
    return {row[0] for row in rows}


@pytest.mark.integration
def test_t7_lock_timeout_aborts(clean_db):
    # short lock timeout keeps the suite fast; set here, not globally
    settings = Settings(
        database_url=clean_db.database_url, migrate_lock_timeout=timedelta(seconds=1)
    )
    with psycopg.connect(settings.database_url, autocommit=True) as holder:
        holder.execute("SELECT pg_advisory_lock(%s)", (MIGRATE_LOCK_KEY,))

        with pytest.raises(MigrationLockTimeoutError, match="already running"):
            run_migrations(settings)

        assert _table_names(settings, "raw") == set()  # nothing applied

        holder.execute("SELECT pg_advisory_unlock(%s)", (MIGRATE_LOCK_KEY,))

    # the aborted run left the lock free: a normal run succeeds now
    result = run_migrations(settings)
    assert result.applied == ("0001_initial",)
