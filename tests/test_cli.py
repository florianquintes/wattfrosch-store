"""Tests for the CLI (B3): exit codes, output, real console script."""

import subprocess
from pathlib import Path

import pytest

import wattfrosch_store.cli as cli_module
from wattfrosch_store import StoreError
from wattfrosch_store.cli import main
from wattfrosch_store.config import ConfigError
from wattfrosch_store.db import StoreConnectionError
from wattfrosch_store.migrate import (
    InitResult,
    MigrationApplyError,
    MigrationLockTimeoutError,
    MigrationResult,
    MigrationStateError,
)

REPO_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def fake_run(monkeypatch):
    calls: list[object] = []

    def install(result=None, error: Exception | None = None):
        def run(settings):
            calls.append(settings)
            if error is not None:
                raise error
            return result

        monkeypatch.setattr(cli_module, "run_migrations", run)
        return calls

    return install


@pytest.fixture
def fake_init(monkeypatch):
    calls: list[object] = []

    def install(result=None, error: Exception | None = None):
        def run(settings):
            calls.append(settings)
            if error is not None:
                raise error
            return result

        monkeypatch.setattr(cli_module, "run_init", run)
        return calls

    return install


def test_migrate_success_exit_0(fake_run, capsys):
    fake_run(MigrationResult(applied=("0001_initial",), skipped=()))
    assert main(["migrate"]) == 0
    err = capsys.readouterr().err
    assert "applied 0001_initial" in err


def test_migrate_nothing_to_do(fake_run, capsys):
    fake_run(MigrationResult(applied=(), skipped=("0001_initial",)))
    assert main(["migrate"]) == 0
    assert "nothing to do" in capsys.readouterr().err


@pytest.mark.parametrize(
    "error",
    [
        StoreError("generic store failure"),
        StoreConnectionError("down"),
        MigrationLockTimeoutError("locked"),
        MigrationStateError("state"),
        MigrationApplyError("apply"),
    ],
)
def test_migrate_store_errors_exit_2(fake_run, capsys, error):
    fake_run(error=error)
    assert main(["migrate"]) == 2
    assert f"error: {error}" in capsys.readouterr().err


def test_config_error_exit_1(monkeypatch, capsys):
    def broken_settings():
        raise ConfigError("bad DATABASE_URL")

    monkeypatch.setattr(cli_module, "load_settings", broken_settings)
    assert main(["migrate"]) == 1
    assert "error: bad DATABASE_URL" in capsys.readouterr().err


def test_missing_command_exit_1(capsys):
    assert main([]) == 1
    assert "error:" in capsys.readouterr().err


def test_unknown_command_exit_1(capsys):
    assert main(["frobnicate"]) == 1
    assert "error:" in capsys.readouterr().err


def test_help_exits_0(capsys):
    assert main(["--help"]) == 0
    out = capsys.readouterr().out
    assert "migrate" in out


def test_masked_dsn_logged(fake_run, caplog):
    fake_run(MigrationResult(applied=(), skipped=()))
    with caplog.at_level("INFO", logger="wattfrosch_store"):
        main(["migrate"])
    assert any("postgresql://wattfrosch:***@" in r.getMessage() for r in caplog.records)


def test_init_reports_status(fake_init, capsys):
    fake_init(
        InitResult(db_created=True, applied=("0001_initial",), pending=("0002_next",))
    )
    assert main(["init"]) == 0
    err = capsys.readouterr().err
    assert "database created" in err
    assert "applied migrations: 0001_initial" in err
    assert "pending migrations: 0002_next" in err
    assert "wattfrosch-store migrate" in err


def test_init_no_pending(fake_init, capsys):
    fake_init(InitResult(db_created=False, applied=(), pending=()))
    assert main(["init"]) == 0
    err = capsys.readouterr().err
    assert "database present" in err
    assert "applied migrations: none" in err
    assert "pending migrations: none" in err


def test_init_store_error_exit_2(fake_init, capsys):
    fake_init(error=StoreConnectionError("down"))
    assert main(["init"]) == 2
    assert "error: down" in capsys.readouterr().err


@pytest.mark.integration
def test_console_script_against_compose_db(clean_db):
    first = subprocess.run(
        ["uv", "run", "wattfrosch-store", "migrate"],
        check=False,
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
        timeout=120,
    )
    assert first.returncode == 0, first.stderr
    assert "applied 0001_initial" in first.stderr

    second = subprocess.run(
        ["uv", "run", "wattfrosch-store", "migrate"],
        check=False,
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
        timeout=120,
    )
    assert second.returncode == 0, second.stderr
    assert "nothing to do" in second.stderr


@pytest.mark.integration
def test_init_console_script(clean_db):
    first = subprocess.run(
        ["uv", "run", "wattfrosch-store", "init"],
        check=False,
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
        timeout=120,
    )
    assert first.returncode == 0, first.stderr
    assert "database present" in first.stderr
    assert "pending migrations: 0001_initial" in first.stderr

    subprocess.run(
        ["uv", "run", "wattfrosch-store", "migrate"],
        check=False,
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
        timeout=120,
    )

    second = subprocess.run(
        ["uv", "run", "wattfrosch-store", "init"],
        check=False,
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
        timeout=120,
    )
    assert second.returncode == 0, second.stderr
    assert "applied migrations: 0001_initial" in second.stderr
    assert "pending migrations: none" in second.stderr
