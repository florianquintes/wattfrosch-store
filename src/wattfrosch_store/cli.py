"""CLI entry point (``wattfrosch-store``).

Exit codes (README "Exit-Codes (store)"):
- 0 success (empty result is not an error, warn on stderr)
- 1 config/usage error (``ConfigError``, argparse errors)
- 2 check or run failure (``StoreError``: connection, lock, migration)

Only ``migrate`` is implemented so far; ``init`` follows in C1 and
``load``/``verify`` in D4.
"""

from __future__ import annotations

import argparse
import logging
import sys
from collections.abc import Sequence
from typing import Final, NoReturn

from wattfrosch_store import StoreError
from wattfrosch_store.config import ConfigError, load_settings
from wattfrosch_store.migrate import run_init, run_migrations

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
        else:  # pragma: no cover - subparsers are required
            parser.error(f"unknown command {args.command!r}")
    except ConfigError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return _EXIT_CONFIG
    except StoreError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return _EXIT_FAILED
    return _EXIT_OK
