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
