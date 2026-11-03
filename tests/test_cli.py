"""Tests for the CLI (B3/D4): exit codes, output, real console script."""

import subprocess
from pathlib import Path

import pytest

import wattfrosch_store.cli as cli_module
from wattfrosch_store import StoreError
from wattfrosch_store.cli import main
from wattfrosch_store.config import ConfigError
from wattfrosch_store.db import StoreConnectionError
from wattfrosch_store.io import RecordValidationError
from wattfrosch_store.loader import LoadResult
from wattfrosch_store.migrate import (
    InitResult,
    MigrationApplyError,
    MigrationLockTimeoutError,
    MigrationResult,
    MigrationStateError,
)
from wattfrosch_store.verify import VerifyResult

REPO_ROOT = Path(__file__).resolve().parents[1]
FIXTURES = Path(__file__).resolve().parent / "fixtures"


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


# --- load (D4) ---


def _load_result(**overrides: object) -> LoadResult:
    defaults: dict[str, object] = {
        "status": "ok",
        "records_read": 2,
        "rows_written": 2,
        "batches_committed": 1,
        "batches_total": 1,
        "dims_inserted": 0,
        "divergences": [],
        "error": None,
    }
    defaults.update(overrides)
    return LoadResult(**defaults)  # type: ignore[arg-type]


@pytest.fixture
def fake_load(monkeypatch):
    calls: list[object] = []

    def install(result=None):
        def run(records, settings):
            calls.append(records)
            return result if result is not None else _load_result()

        monkeypatch.setattr(cli_module, "read_records", lambda path: iter(()))
        monkeypatch.setattr(cli_module, "load_records", run)
        return calls

    return install


def test_load_requires_from_file_exit_1(capsys):
    assert main(["load"]) == 1
    err = capsys.readouterr().err
    assert "--from-file" in err
    assert "pipeline" in err  # production writes via wattfrosch-pipeline


def test_load_missing_file_exit_1(capsys):
    assert main(["load", "--from-file", "no/such/file.csv"]) == 1
    assert "File not found" in capsys.readouterr().err


def test_load_unknown_extension_exit_1(tmp_path, capsys):
    path = tmp_path / "data.txt"
    path.write_text("whatever")

    assert main(["load", "--from-file", str(path)]) == 1
    err = capsys.readouterr().err
    for fmt in (".parquet", ".csv", ".ndjson", ".json"):
        assert fmt in err


def test_load_happy_exit_0(fake_load, capsys, tmp_path):
    fake_load()
    path = tmp_path / "valid.csv"
    path.write_text("stub")

    assert main(["load", "--from-file", str(path)]) == 0
    err = capsys.readouterr().err
    assert "read 2 record(s)" in err
    assert "wrote 2 row(s)" in err
    assert "1/1 batch(es)" in err


def test_load_empty_exit_0_with_warning(fake_load, capsys, tmp_path):
    fake_load(_load_result(status="empty", records_read=0, rows_written=0))
    path = tmp_path / "empty.csv"
    path.write_text("stub")

    assert main(["load", "--from-file", str(path)]) == 0
    assert "no records" in capsys.readouterr().err


def test_load_unreadable_file_exit_1(capsys, tmp_path):
    path = tmp_path / "corrupt.parquet"
    path.write_bytes(b"not a parquet file")

    assert main(["load", "--from-file", str(path)]) == 1
    assert "Cannot read" in capsys.readouterr().err


def test_load_failed_exit_2(fake_load, capsys, tmp_path):
    path = tmp_path / "bad.csv"
    path.write_text("stub")
    fake_load(
        _load_result(
            status="failed",
            error=RecordValidationError("Error line 2: ts_utc without timezone", {}),
        )
    )

    assert main(["load", "--from-file", str(path)]) == 2
    err = capsys.readouterr().err
    assert "store_validation_failed" in err
    assert "without timezone" in err


def test_load_content_divergence_warns_exit_0(fake_load, capsys, tmp_path):
    from wattfrosch_store.sync_dims import Divergence

    path = tmp_path / "valid.csv"
    path.write_text("stub")
    fake_load(
        _load_result(
            divergences=[
                Divergence(
                    dimension="metric",
                    key="generation.solar",
                    valid_from=None,
                    kind="content_differs",
                    fields={"display_name": ("catalog", "db")},
                )
            ]
        )
    )

    assert main(["load", "--from-file", str(path)]) == 0
    err = capsys.readouterr().err
    assert "catalog vs database divergences" in err
    assert "generation.solar" in err


# --- verify (D4) ---


@pytest.fixture
def fake_verify(monkeypatch):
    def install(result: VerifyResult):
        def run(settings, check_values=False):
            return result

        monkeypatch.setattr(cli_module, "run_verify", run)

    return install


def test_verify_ok_exit_0(fake_verify, capsys):
    fake_verify(
        VerifyResult(
            ok=True,
            empty=False,
            failures=[],
            counts={
                "fact_measurement": 5,
                "dim_time": 5,
                "dim_region": 6,
                "dim_metric": 21,
                "dim_source": 3,
            },
            outliers={},
        )
    )

    assert main(["verify"]) == 0
    err = capsys.readouterr().err
    assert "raw.fact_measurement: 5 row(s)" in err
    assert "failure" not in err


def test_verify_empty_exit_0_with_notice(fake_verify, capsys):
    fake_verify(
        VerifyResult(
            ok=True,
            empty=True,
            failures=[],
            counts={
                "fact_measurement": 0,
                "dim_time": 0,
                "dim_region": 0,
                "dim_metric": 0,
                "dim_source": 0,
            },
            outliers={},
        )
    )

    assert main(["verify"]) == 0
    assert "database is empty" in capsys.readouterr().err


def test_verify_failures_exit_2(fake_verify, capsys):
    fake_verify(
        VerifyResult(
            ok=False,
            empty=False,
            failures=["region 'DE-LU' (valid_from 2018-10-01): content_differs"],
            counts={
                "fact_measurement": 5,
                "dim_time": 5,
                "dim_region": 6,
                "dim_metric": 21,
                "dim_source": 3,
            },
            outliers={},
        )
    )

    assert main(["verify"]) == 2
    assert "content_differs" in capsys.readouterr().err


def test_verify_check_values_reports_outliers_exit_0(fake_verify, capsys):
    fake_verify(
        VerifyResult(
            ok=True,
            empty=False,
            failures=[],
            counts={
                "fact_measurement": 5,
                "dim_time": 5,
                "dim_region": 6,
                "dim_metric": 21,
                "dim_source": 3,
            },
            outliers={"generation.solar": 3},
        )
    )

    assert main(["verify", "--check-values"]) == 0
    err = capsys.readouterr().err
    assert "generation.solar: 3 outlier(s)" in err
    assert "not failed" in err


# --- integration: console script ---


@pytest.mark.integration
def test_console_script_against_real_db(clean_db, monkeypatch):
    monkeypatch.setenv("DATABASE_URL", clean_db.database_url)
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
def test_init_console_script(clean_db, monkeypatch):
    monkeypatch.setenv("DATABASE_URL", clean_db.database_url)
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


def _store(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["uv", "run", "wattfrosch-store", *args],
        check=False,
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
        timeout=120,
    )


@pytest.mark.integration
def test_load_console_script_paths(clean_db, monkeypatch):
    monkeypatch.setenv("DATABASE_URL", clean_db.database_url)
    assert _store("init").returncode == 0
    assert _store("migrate").returncode == 0

    # happy path
    first = _store("load", "--from-file", str(FIXTURES / "valid.ndjson"))
    assert first.returncode == 0, first.stderr
    assert "read 2 record(s)" in first.stderr

    # naive timestamp -> validation failure, exit 2
    naive = _store("load", "--from-file", str(FIXTURES / "naive_ts.csv"))
    assert naive.returncode == 2, naive.stderr
    assert "store_validation_failed" in naive.stderr
    assert "without timezone" in naive.stderr

    # parquet is supported -> exit 0
    parquet = _store("load", "--from-file", str(FIXTURES / "scd2_grenze.parquet"))
    assert parquet.returncode == 0, parquet.stderr

    missing = _store("load", "--from-file", "nope.parquet")
    assert missing.returncode == 1
    assert "File not found" in missing.stderr
