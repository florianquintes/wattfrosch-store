"""CLI entry point (``wattfrosch-store``).

Exit codes (README "Exit-Codes (store)"):
- 0 success (empty result is not an error, warn on stderr)
- 1 config/usage error (``ConfigError``, argparse errors, file not found,
  unknown file extension)
- 2 check or run failure (``StoreError``: connection, lock, migration,
  batch, validation, divergences)
"""

from __future__ import annotations

import argparse
import logging
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Final, NoReturn

import pyarrow

from wattfrosch_store import StoreError
from wattfrosch_store.config import ConfigError, Settings, load_settings
from wattfrosch_store.io import read_records
from wattfrosch_store.loader import load_records
from wattfrosch_store.migrate import run_init, run_migrations
from wattfrosch_store.verify import run_verify

_EXIT_OK: Final = 0
_EXIT_CONFIG: Final = 1
_EXIT_FAILED: Final = 2


class _ArgumentParser(argparse.ArgumentParser):
    """argparse usage errors must exit 1 (default 2 collides with exit code 2)."""

    def error(self, message: str) -> NoReturn:
        raise ConfigError(message)


def _build_parser() -> _ArgumentParser:
    parser = _ArgumentParser(
        prog="wattfrosch-store",
        description="Postgres persistence module for MeasurementRecords.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser(
        "init",
        help="create database, schema raw and log table if missing (no schema DDL)",
    )
    subparsers.add_parser("migrate", help="apply pending migrations")
    load = subparsers.add_parser(
        "load",
        help="load records from a file (dev/test path; production uses the pipeline)",
    )
    load.add_argument(
        "--from-file",
        metavar="PATH",
        help="input file (.parquet, .csv, .ndjson, .json); required",
    )
    verify = subparsers.add_parser(
        "verify",
        help="read-only checks: version, dimensions, row counts",
    )
    verify.add_argument(
        "--check-values",
        action="store_true",
        help="full scan of value vs plausible_min/max (outliers reported, not failed)",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run one store command; returns the process exit code."""
    parser = _build_parser()
    try:
        try:
            args = parser.parse_args(argv)
        except SystemExit as exc:
            return exc.code if isinstance(exc.code, int) else _EXIT_OK

        settings = load_settings()
        logging.basicConfig(
            level=settings.log_level, format="%(levelname)s %(name)s: %(message)s"
        )
        # a default DSN must be visible when it silently applies (A2)
        logging.getLogger("wattfrosch_store").info(
            "Using database %s", settings.masked_dsn
        )

        if args.command == "migrate":
            result = run_migrations(settings)
            if result.applied:
                print(
                    f"migrate: applied {', '.join(result.applied)}",
                    file=sys.stderr,
                )
            else:
                print(
                    f"migrate: nothing to do ({len(result.skipped)} already applied)",
                    file=sys.stderr,
                )
        elif args.command == "init":
            result = run_init(settings)
            state = "created" if result.db_created else "present"
            print(f"init: database {state}", file=sys.stderr)
            applied = ", ".join(result.applied) if result.applied else "none"
            print(f"init: applied migrations: {applied}", file=sys.stderr)
            if result.pending:
                print(
                    f"init: pending migrations: {', '.join(result.pending)} "
                    "— run: wattfrosch-store migrate",
                    file=sys.stderr,
                )
            else:
                print("init: pending migrations: none", file=sys.stderr)
        elif args.command == "load":
            return _run_load(args, settings)
        elif args.command == "verify":
            return _run_verify(args, settings)
        else:  # pragma: no cover - subparsers are required
            parser.error(f"unknown command {args.command!r}")
    except ConfigError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return _EXIT_CONFIG
    except StoreError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return _EXIT_FAILED
    return _EXIT_OK


def _run_load(args: argparse.Namespace, settings: Settings) -> int:
    if not args.from_file:
        raise ConfigError(
            "load requires --from-file <path>. Production writes exclusively "
            "via wattfrosch-pipeline calling load_records()."
        )
    path = Path(args.from_file)
    if not path.is_file():
        raise ConfigError(f"File not found: {path}")
    try:
        records = read_records(path)
    except pyarrow.ArrowException as exc:
        raise ConfigError(f"Cannot read {path}: {exc}") from exc
    result = load_records(records, settings)

    content_diffs = [d for d in result.divergences if d.kind == "content_differs"]
    if content_diffs:
        print(
            "load: warning: catalog vs database divergences (load continues; "
            "definitions are never overwritten by the sync):",
            file=sys.stderr,
        )
        for divergence in content_diffs:
            fields = ", ".join(
                f"{name} catalog={catalog!r} db={db!r}"
                for name, (catalog, db) in divergence.fields.items()
            )
            print(
                f"  {divergence.dimension} '{divergence.key}' "
                f"(valid_from {divergence.valid_from}): {fields}",
                file=sys.stderr,
            )

    if result.status == "failed":
        error = result.error
        code = getattr(error, "code", "wattfrosch.store_error")
        print(f"load: failed [{code}]: {error}", file=sys.stderr)
        return _EXIT_FAILED
    if result.status == "empty":
        print("load: warning: file contains no records", file=sys.stderr)
        return _EXIT_OK
    print(
        f"load: read {result.records_read} record(s), wrote "
        f"{result.rows_written} row(s) in {result.batches_committed}/"
        f"{result.batches_total} batch(es), dims inserted "
        f"{result.dims_inserted}",
        file=sys.stderr,
    )
    return _EXIT_OK


def _run_verify(args: argparse.Namespace, settings: Settings) -> int:
    result = run_verify(settings, check_values=args.check_values)

    for table in sorted(result.counts):
        print(f"verify: raw.{table}: {result.counts[table]} row(s)", file=sys.stderr)
    if result.empty:
        print(
            "verify: database is empty (no facts, no times) — nothing to check",
            file=sys.stderr,
        )
    for failure in result.failures:
        print(f"verify: failure: {failure}", file=sys.stderr)
    if result.outliers:
        print(
            "verify: warning: values outside plausible_min/max (reported, not failed):",
            file=sys.stderr,
        )
        for metric_key, count in sorted(result.outliers.items()):
            print(f"  {metric_key}: {count} outlier(s)", file=sys.stderr)
    return _EXIT_OK if result.ok else _EXIT_FAILED
