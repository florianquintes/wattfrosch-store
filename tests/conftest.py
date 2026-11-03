"""Shared fixtures for tests against the compose database."""

import psycopg
import pytest

from wattfrosch_store.config import load_settings


def _db_available() -> bool:
    try:
        with psycopg.connect(load_settings().database_url, connect_timeout=2):
            return True
    except psycopg.Error:
        return False


@pytest.fixture
def clean_db():
    """Fresh schema state: drops ``raw`` before the test; skips without DB."""
    if not _db_available():
        pytest.skip("compose DB not running")
    settings = load_settings()
    with psycopg.connect(settings.database_url, autocommit=True) as conn:
        conn.execute("DROP SCHEMA IF EXISTS raw CASCADE")
    yield settings
