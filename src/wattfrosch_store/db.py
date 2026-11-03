"""Connection layer and central database error translation.

One CLI run = one connection (no pool; pools come with ``forecast``).
``init`` (C1) deliberately does not use :func:`connect`: it connects to the
maintenance database ``postgres`` instead of the store database.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Iterator
from contextlib import contextmanager
from typing import cast

import psycopg
from psycopg.rows import dict_row

from wattfrosch_store import StoreError
from wattfrosch_store.config import Settings

logger = logging.getLogger(__name__)

CONNECT_ATTEMPTS = 5
CONNECT_RETRY_DELAY_S = 2.0

_UNREACHABLE_SQLSTATES = frozenset({"08001", "08006"})

# Bootstrapped by init/the migration runner, never by a migration file.
BOOTSTRAP_SQL = """
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.schema_migration (
    migration_id     text PRIMARY KEY,
    contract_version text NOT NULL,
    applied_at       timestamptz NOT NULL DEFAULT now()
)
"""


class StoreConnectionError(StoreError):
    """The database is not reachable."""


class DatabaseMissingError(StoreError):
    """The configured database does not exist."""


class SchemaMissingError(StoreError):
    """The schema ``raw`` does not exist."""


def _translate_connect_error(
    exc: psycopg.Error,
) -> StoreError | psycopg.Error:
    sqlstate = getattr(exc, "sqlstate", None)
    if sqlstate == "3D000":
        return DatabaseMissingError(
            "Database does not exist (SQLSTATE 3D000). Run: wattfrosch-store init"
        )
    if sqlstate in _UNREACHABLE_SQLSTATES or sqlstate is None:
        return StoreConnectionError(
            f"Database not reachable (SQLSTATE {sqlstate or 'unknown'}). "
            f"Hint: docker compose up -d db"
        )
    return exc


def connect(settings: Settings) -> psycopg.Connection[dict]:
    """Open a connection to the store database.

    Retries briefly while the database is not reachable (fresh compose stacks
    need a moment), fails fast on a missing database.
    """
    last_exc: psycopg.Error | None = None
    for attempt in range(1, CONNECT_ATTEMPTS + 1):
        try:
            conn = cast(
                "psycopg.Connection[dict]",
                psycopg.connect(
                    settings.database_url,
                    row_factory=dict_row,
                    autocommit=False,
                ),
            )
            logger.debug("Connected to %s", settings.masked_dsn)
            return conn
        except psycopg.Error as exc:
            translated = _translate_connect_error(exc)
            if not isinstance(translated, StoreConnectionError):
                raise translated from exc
            last_exc = exc
            if attempt < CONNECT_ATTEMPTS:
                logger.debug(
                    "Connect attempt %d/%d failed (%s), retrying in %.0fs",
                    attempt,
                    CONNECT_ATTEMPTS,
                    exc,
                    CONNECT_RETRY_DELAY_S,
                )
                time.sleep(CONNECT_RETRY_DELAY_S)
    raise StoreConnectionError(
        f"Database not reachable after {CONNECT_ATTEMPTS} attempts. "
        f"Hint: docker compose up -d db"
    ) from last_exc


@contextmanager
def translate_db_errors() -> Iterator[None]:
    """Translate query-phase SQLSTATEs into typed store errors.

    Wrap the first queries that touch the store database (schema ``raw``);
    ``3F000`` surfaces there. Connection-phase errors are handled by
    :func:`connect`, but are translated consistently here as well.
    """
    try:
        yield
    except psycopg.Error as exc:
        sqlstate = getattr(exc, "sqlstate", None)
        if sqlstate == "3F000":
            raise SchemaMissingError(
                "Schema 'raw' does not exist (SQLSTATE 3F000). "
                "Run: wattfrosch-store migrate"
            ) from exc
        if sqlstate == "3D000":
            raise DatabaseMissingError(
                "Database does not exist (SQLSTATE 3D000). Run: wattfrosch-store init"
            ) from exc
        if sqlstate in _UNREACHABLE_SQLSTATES or sqlstate is None:
            raise StoreConnectionError(
                f"Database not reachable (SQLSTATE {sqlstate or 'unknown'}). "
                f"Hint: docker compose up -d db"
            ) from exc
        raise
