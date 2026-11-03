"""``verify`` — read-only database state checks (README "``verify`` — Umfang").

``verify`` never mutates anything (no dimension sync — otherwise it would
change the database and be worthless as a CI check). Checks:

1. ``schema_migration`` present, contract version compatible (Major.Minor)
2. Dimensions strictly via :func:`collect_divergences` — unlike ``load``,
   both missing rows and differing content are failures
3. Row counts: ``fact_measurement > 0`` and ``dim_time > 0`` — except on a
   fresh, completely empty database, which is reported as ok-but-empty
4. Optional ``--check-values``: full scan of ``value`` against
   ``plausible_min``/``plausible_max`` per ``metric_key``; outliers are
   reported but are not failures (``can_be_negative`` metrics, wide bounds)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Final, LiteralString, cast

import psycopg

from wattfrosch_store.config import Settings
from wattfrosch_store.db import connect, translate_db_errors
from wattfrosch_store.loader import check_contract_version
from wattfrosch_store.sync_dims import Divergence, collect_divergences

logger = logging.getLogger(__name__)

_COUNTED_TABLES: Final[tuple[str, ...]] = (
    "fact_measurement",
    "dim_time",
    "dim_region",
    "dim_metric",
    "dim_source",
)


@dataclass(frozen=True)
class VerifyResult:
    """Outcome of one ``verify`` run; the CLI maps ``ok`` to the exit code."""

    ok: bool
    empty: bool  # fresh database: no facts AND no dim_time rows
    failures: list[str]
    counts: dict[str, int]
    outliers: dict[str, int]  # metric_key -> outlier count (only --check-values)


def run_verify(
    settings: Settings,
    check_values: bool = False,
) -> VerifyResult:
    """Run the read-only verify checks; raises ``StoreError`` when unreachable.

    Check failures (version, divergences, row counts) are collected into the
    returned :class:`VerifyResult`; the caller decides the exit code.
    """
    conn = connect(settings)
    try:
        with translate_db_errors():
            check_contract_version(conn)
            conn.rollback()  # read-only so far; discard the version-check read
            divergences = collect_divergences(conn)
            counts = _row_counts(conn)
            empty = counts["fact_measurement"] == 0 and counts["dim_time"] == 0
            if empty:
                # Fresh database: dimension rows are missing because the first
                # load has not synced them yet — not a divergence. Differing
                # content stays strict even on an empty database.
                divergences = [d for d in divergences if d.kind == "content_differs"]
            failures = _divergence_failures(divergences)
            if not empty:
                failures.extend(_count_failures(counts))
            outliers = _value_outliers(conn) if check_values else {}
    finally:
        conn.close()
    return VerifyResult(
        ok=not failures,
        empty=empty,
        failures=failures,
        counts=counts,
        outliers=outliers,
    )


def _divergence_failures(divergences: list[Divergence]) -> list[str]:
    failures = []
    for d in divergences:
        detail = ", ".join(
            f"{name}: catalog={catalog!r} db={db!r}"
            for name, (catalog, db) in d.fields.items()
        )
        failures.append(
            f"{d.dimension} '{d.key}' (valid_from {d.valid_from}): "
            f"{d.kind}" + (f" ({detail})" if detail else "")
        )
    return failures


def _row_counts(conn: psycopg.Connection[dict]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for table in _COUNTED_TABLES:
        with conn.cursor() as cur:
            cur.execute(cast("LiteralString", f"SELECT count(*) AS n FROM raw.{table}"))
            row = cur.fetchone()
        counts[table] = int(row["n"]) if row is not None else 0
    return counts


def _count_failures(counts: dict[str, int]) -> list[str]:
    failures = []
    for table in ("fact_measurement", "dim_time"):
        if counts[table] == 0:
            failures.append(f"raw.{table} is empty ({counts[table]} rows)")
    return failures


def _value_outliers(conn: psycopg.Connection[dict]) -> dict[str, int]:
    """Count values outside the metric's plausible range, per metric key.

    Rows with a NULL bound (or no plausible range) are skipped — outliers
    are reported, never failures (README "``verify --check-values``").
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT m.metric_key, count(*) AS outliers
            FROM raw.fact_measurement f
            JOIN raw.dim_metric m ON m.metric_id = f.metric_id
            WHERE (m.plausible_min IS NOT NULL AND f.value < m.plausible_min)
               OR (m.plausible_max IS NOT NULL AND f.value > m.plausible_max)
            GROUP BY m.metric_key
            """
        )
        return {row["metric_key"]: int(row["outliers"]) for row in cur.fetchall()}
