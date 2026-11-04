"""Tests for the environment configuration (A2)."""

from datetime import timedelta

import pytest

from wattfrosch_store.config import (
    DEFAULT_DATABASE_URL,
    ConfigError,
    _parse_duration,
    load_settings,
)


def test_defaults_match_spec(monkeypatch):
    for var in (
        "DATABASE_URL",
        "LOG_LEVEL",
        "BATCH_SIZE",
        "MIGRATE_LOCK_TIMEOUT",
    ):
        monkeypatch.delenv(var, raising=False)

    settings = load_settings()

    assert settings.database_url == DEFAULT_DATABASE_URL
    assert settings.log_level == "INFO"
    assert settings.batch_size == 1000
    assert settings.migrate_lock_timeout == timedelta(seconds=30)


def test_env_overrides(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@db.internal:5433/store")
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")
    monkeypatch.setenv("BATCH_SIZE", "50")
    monkeypatch.setenv("MIGRATE_LOCK_TIMEOUT", "1s")

    settings = load_settings()

    assert settings.database_url == "postgresql://u:p@db.internal:5433/store"
    assert settings.log_level == "DEBUG"
    assert settings.batch_size == 50
    assert settings.migrate_lock_timeout == timedelta(seconds=1)


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("30s", timedelta(seconds=30)),
        ("1s", timedelta(seconds=1)),
        ("2m", timedelta(minutes=2)),
        ("3h", timedelta(hours=3)),
        ("PT30S", timedelta(seconds=30)),
    ],
)
def test_duration_parsing(monkeypatch, raw, expected):
    monkeypatch.setenv("MIGRATE_LOCK_TIMEOUT", raw)
    assert load_settings().migrate_lock_timeout == expected


@pytest.mark.parametrize("raw", ["abc", "-5s", "s", ""])
def test_duration_parsing_rejects_garbage(monkeypatch, raw):
    monkeypatch.setenv("MIGRATE_LOCK_TIMEOUT", raw)
    with pytest.raises(ConfigError):
        load_settings()


def test_batch_size_must_be_positive(monkeypatch):
    monkeypatch.setenv("BATCH_SIZE", "0")
    with pytest.raises(ConfigError):
        load_settings()


def test_log_level_must_be_known(monkeypatch):
    monkeypatch.setenv("LOG_LEVEL", "VERBOSE")
    with pytest.raises(ConfigError):
        load_settings()


def test_masked_dsn_replaces_password():
    settings = load_settings()

    masked = settings.masked_dsn

    assert masked == "postgresql://wattfrosch:***@localhost:5432/wattfrosch"
    assert "wattfrosch:***" in masked
    assert masked.count("***") == 1
    assert "wattfrosch:wattfrosch@" not in masked


def test_masked_dsn_without_password(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://localhost/wattfrosch")
    assert load_settings().masked_dsn == "postgresql://localhost/wattfrosch"


def test_masked_dsn_round_trips_host_and_db(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:secret@db.internal:5433/store")
    masked = load_settings().masked_dsn
    assert masked == "postgresql://u:***@db.internal:5433/store"


def test_masked_dsn_without_port(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:secret@db.internal/store")
    assert load_settings().masked_dsn == "postgresql://u:***@db.internal/store"


def test_masked_dsn_without_host(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:secret@/store")
    assert load_settings().masked_dsn == "postgresql://u:***@/store"


def test_masked_dsn_with_ipv6_host(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:secret@[::1]:5432/store")
    assert load_settings().masked_dsn == "postgresql://u:***@[::1]:5432/store"


def test_parse_duration_rejects_garbage_directly():
    with pytest.raises(ValueError, match="Invalid duration"):
        _parse_duration("abc")


def test_parse_duration_accepts_fractional_seconds():
    assert _parse_duration("0.5s") == timedelta(milliseconds=500)
