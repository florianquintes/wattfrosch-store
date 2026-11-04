"""Tests for the connection layer and error translation (A3)."""

import psycopg
import pytest
from psycopg.rows import dict_row

from wattfrosch_store import StoreError
from wattfrosch_store.config import load_settings
from wattfrosch_store.db import (
    CONNECT_ATTEMPTS,
    DatabaseMissingError,
    SchemaMissingError,
    StoreConnectionError,
    connect,
    translate_db_errors,
)


def _error(sqlstate: str | None) -> psycopg.Error:
    exc = psycopg.OperationalError("connection refused")
    exc.sqlstate = sqlstate
    return exc


@pytest.fixture
def fail_connect(monkeypatch):
    """Make ``psycopg.connect`` raise ``factory()`` on every call."""
    recorded: list[str] = []

    def install(factory, sleep_calls: list[float] | None = None):
        def fake_connect(dsn, **kwargs):
            recorded.append(dsn)
            raise factory()

        monkeypatch.setattr(psycopg, "connect", fake_connect)
        if sleep_calls is not None:
            monkeypatch.setattr("wattfrosch_store.db.time.sleep", sleep_calls.append)
        return recorded

    return install


def test_connect_retries_on_unreachable_then_raises(fail_connect, monkeypatch):
    sleep_calls: list[float] = []
    fail_connect(lambda: _error("08001"), sleep_calls)

    with pytest.raises(StoreConnectionError, match="docker compose up -d db"):
        connect(load_settings())

    assert len(sleep_calls) == CONNECT_ATTEMPTS - 1
    assert all(delay == 2.0 for delay in sleep_calls)


def test_connect_retries_on_missing_sqlstate(fail_connect, monkeypatch):
    fail_connect(lambda: _error(None))
    with pytest.raises(StoreConnectionError):
        connect(load_settings())


def test_connect_fails_fast_on_missing_database(fail_connect, monkeypatch):
    sleep_calls: list[float] = []
    fail_connect(lambda: _error("3D000"), sleep_calls)

    with pytest.raises(DatabaseMissingError, match="wattfrosch-store init"):
        connect(load_settings())

    assert sleep_calls == []  # no retry: a missing database cannot heal itself


def test_connect_passes_through_unknown_sqlstate(fail_connect):
    fail_connect(lambda: _error("28P01"))  # invalid password
    with pytest.raises(psycopg.OperationalError):
        connect(load_settings())


def test_connect_success_configuration(monkeypatch):
    opened: dict = {}

    def fake_connect(dsn, **kwargs):
        opened.update(kwargs)
        return object()

    monkeypatch.setattr(psycopg, "connect", fake_connect)
    conn = connect(load_settings())

    assert isinstance(conn, object)
    assert opened["autocommit"] is False
    assert opened["row_factory"] is dict_row


def test_translate_db_errors_schema_missing():
    exc = psycopg.errors.InvalidSchemaName("schema does not exist")
    exc.sqlstate = "3F000"
    with (
        pytest.raises(SchemaMissingError, match="wattfrosch-store migrate"),
        translate_db_errors(),
    ):
        raise exc


def test_translate_db_errors_database_missing():
    exc = psycopg.errors.InvalidCatalogName("database does not exist")
    exc.sqlstate = "3D000"
    with pytest.raises(DatabaseMissingError), translate_db_errors():
        raise exc


def test_translate_db_errors_unreachable():
    with pytest.raises(StoreConnectionError), translate_db_errors():
        raise _error("08006")


def test_translate_db_errors_unknown_passes_through():
    exc = psycopg.errors.UndefinedTable("no such table")
    exc.sqlstate = "42P01"
    with pytest.raises(psycopg.errors.UndefinedTable), translate_db_errors():
        raise exc


def test_translate_db_errors_no_error_is_transparent():
    with translate_db_errors():
        pass


def test_store_error_hierarchy():
    for cls in (StoreConnectionError, DatabaseMissingError, SchemaMissingError):
        assert issubclass(cls, StoreError)


@pytest.mark.integration
def test_connect_against_real_db(db_settings):
    with connect(db_settings) as conn:
        assert conn.info.parameter_status("server_version") is not None
        with conn.cursor() as cur:
            cur.execute("SELECT 1 AS one")
            row = cur.fetchone()
        assert row == {"one": 1}  # dict_row
        assert conn.autocommit is False
