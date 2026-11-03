"""Shared fixtures for integration tests against a session Postgres container.

The container starts lazily: only tests that request ``clean_db`` (or
``db_settings``) pay the startup cost, unit tests never need Docker.
"""

import psycopg
import pytest
from testcontainers.community.postgres import PostgresContainer

from wattfrosch_store.config import Settings


@pytest.fixture(scope="session")
def db_container():
    with PostgresContainer(
        "postgres:16", username="wattfrosch", password="wattfrosch", dbname="wattfrosch"
    ) as container:
        yield container


@pytest.fixture(scope="session")
def db_settings(db_container) -> Settings:
    url = db_container.get_connection_url(driver=None)
    return Settings(database_url=url)


@pytest.fixture
def clean_db(db_settings) -> Settings:
    """Fresh schema state: drops ``raw`` before the test."""
    with psycopg.connect(db_settings.database_url, autocommit=True) as conn:
        conn.execute("DROP SCHEMA IF EXISTS raw CASCADE")
    yield db_settings
