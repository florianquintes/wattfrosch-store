"""Insert-only dimension sync (README "Dimensionsabgleich").

First step of every ``load``, in its own, separately committed transaction:
catalog definitions (wattfrosch-contracts) are inserted into ``dim_region``,
``dim_metric`` and ``dim_source`` with ``ON CONFLICT ... DO NOTHING``.
Existing rows are never overwritten and never closed — a definition change is
a hand-written migration, the loader never versions dimensions itself.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date
from typing import Literal, LiteralString, cast

import psycopg
from wattfrosch_contracts import METRICS, REGIONS, SOURCES
from wattfrosch_contracts.metrics import MetricEntry
from wattfrosch_contracts.regions import RegionEntry
from wattfrosch_contracts.sources import SourceEntry

logger = logging.getLogger(__name__)

# Conflict targets match the schema's unique constraints (uq_region,
# uq_metric, dim_source.source_key). Insert-only: no UPDATE, no row closing.
_INSERT_REGION_SQL = """
INSERT INTO raw.dim_region (region_code, name, kind, tz, valid_from, valid_to)
VALUES (%(region_code)s, %(name)s, %(kind)s, %(tz)s, %(valid_from)s, %(valid_to)s)
ON CONFLICT (region_code, valid_from) DO NOTHING
"""

_INSERT_METRIC_SQL = """
INSERT INTO raw.dim_metric (
    metric_key, unit, category, aggregation, is_derived, is_renewable,
    can_be_negative, plausible_min, plausible_max, display_name, description,
    valid_from, valid_to
) VALUES (
    %(metric_key)s, %(unit)s, %(category)s, %(aggregation)s, %(is_derived)s,
    %(is_renewable)s, %(can_be_negative)s, %(plausible_min)s, %(plausible_max)s,
    %(display_name)s, %(description)s, %(valid_from)s, %(valid_to)s
)
ON CONFLICT (metric_key, valid_from) DO NOTHING
"""

_INSERT_SOURCE_SQL = """
INSERT INTO raw.dim_source (source_key, name, license, url)
VALUES (%(source_key)s, %(name)s, %(license)s, %(url)s)
ON CONFLICT (source_key) DO NOTHING
"""


def sync_dims(conn: psycopg.Connection[dict]) -> SyncResult:
    """Insert missing catalog rows into the dimension tables.

    The caller must not have an open transaction: the sync runs in its own
    transaction, committed before the loader writes facts. Afterwards the
    shared read-only comparison runs — divergences are what the insert-only
    sync could not fix (content differs in the DB) or must not fix (a row was
    removed by hand; the next sync re-inserts it).
    """
    inserted = 0
    with conn.transaction():
        inserted += _sync_regions(conn)
        inserted += _sync_metrics(conn)
        inserted += _sync_sources(conn)
    logger.info("Dimension sync inserted %d row(s)", inserted)
    divergences = collect_divergences(conn)
    return SyncResult(inserted=inserted, divergences=divergences)


@dataclass(frozen=True)
class Divergence:
    """One catalog-vs-DB difference (see README "Divergenz Katalog vs. DB")."""

    dimension: Literal["region", "metric", "source"]
    key: str  # region_code / metric_key / source_key
    valid_from: date | None
    kind: Literal["missing_in_db", "content_differs"]
    fields: dict[str, tuple[object, object]] = field(default_factory=dict)
    # fields: column -> (catalog value, DB value); empty for missing rows


@dataclass(frozen=True)
class SyncResult:
    inserted: int
    divergences: list[Divergence]


def _sync_dimension(
    conn: psycopg.Connection[dict],
    sql: LiteralString,
    params_rows: list[dict[str, object]],
) -> int:
    """Insert one dimension's catalog rows; returns the number inserted."""
    with conn.cursor() as cur:
        count = 0
        for params in params_rows:
            cur.execute(sql, params)
            count += cur.rowcount
    return count


def _sync_regions(conn: psycopg.Connection[dict]) -> int:
    return _sync_dimension(
        conn,
        _INSERT_REGION_SQL,
        [
            {
                "region_code": entry.code,
                "name": entry.name,
                "kind": entry.kind,
                "tz": entry.tz,
                "valid_from": entry.valid_from,
                "valid_to": entry.valid_to,
            }
            for entry in REGIONS.values()
        ],
    )


def _sync_metrics(conn: psycopg.Connection[dict]) -> int:
    return _sync_dimension(
        conn,
        _INSERT_METRIC_SQL,
        [_metric_params(entry) for entry in METRICS.values()],
    )


def _sync_sources(conn: psycopg.Connection[dict]) -> int:
    return _sync_dimension(
        conn,
        _INSERT_SOURCE_SQL,
        [
            {
                "source_key": source_key,
                "name": entry.name,
                "license": entry.license,
                "url": entry.url,
            }
            for source_key, entry in SOURCES.items()
        ],
    )


# --- shared read-only catalog-vs-DB comparison (sync: warn, verify: hard) ---


def _plausible_bounds(entry: MetricEntry) -> tuple[object, object]:
    if entry.plausible_range is None:
        return None, None
    return entry.plausible_range


def _region_row(entry: RegionEntry) -> dict[str, object]:
    return {
        "name": entry.name,
        "kind": entry.kind,
        "tz": entry.tz,
        "valid_to": entry.valid_to,
    }


def _metric_row(entry: MetricEntry) -> dict[str, object]:
    plausible_min, plausible_max = _plausible_bounds(entry)
    return {
        "unit": entry.unit,
        "category": entry.category,
        "aggregation": entry.aggregation,
        "is_derived": entry.derived,
        "is_renewable": entry.is_renewable,
        "can_be_negative": entry.can_be_negative,
        "plausible_min": plausible_min,
        "plausible_max": plausible_max,
        "display_name": entry.display_name,
        "description": entry.description,
        "valid_to": entry.valid_to,
    }


def _metric_params(entry: MetricEntry) -> dict[str, object]:
    return {
        "metric_key": entry.key,
        **_metric_row(entry),
        "valid_from": entry.valid_from,
    }


def _source_row(entry: SourceEntry) -> dict[str, object]:
    return {"name": entry.name, "license": entry.license, "url": entry.url}


def collect_divergences(conn: psycopg.Connection[dict]) -> list[Divergence]:
    """Compare the catalogs against the DB rows; read-only, deterministic.

    Identity is ``(key, valid_from)`` — the same conflict targets the sync
    uses. Rows in the DB that the catalog does not know are not divergences:
    the loader never resolves them.
    """
    divergences: list[Divergence] = []
    divergences.extend(
        _collect_dimension(
            conn,
            "region",
            "dim_region",
            "region_code",
            [(key, e.valid_from, _region_row(e)) for key, e in REGIONS.items()],
        )
    )
    divergences.extend(
        _collect_dimension(
            conn,
            "metric",
            "dim_metric",
            "metric_key",
            [(key, e.valid_from, _metric_row(e)) for key, e in METRICS.items()],
        )
    )
    divergences.extend(
        _collect_dimension(
            conn,
            "source",
            "dim_source",
            "source_key",
            [(key, None, _source_row(e)) for key, e in SOURCES.items()],
        )
    )
    return divergences


def _collect_dimension(
    conn: psycopg.Connection[dict],
    dimension: Literal["region", "metric", "source"],
    table: str,
    key_column: str,
    entries: list[tuple[str, date | None, dict[str, object]]],
) -> list[Divergence]:
    with conn.cursor() as cur:
        cur.execute(cast("LiteralString", f"SELECT * FROM raw.{table}"))
        rows = {(row[key_column], row.get("valid_from")): row for row in cur.fetchall()}
    divergences: list[Divergence] = []
    for key, valid_from, expected in entries:
        db_row = rows.get((key, valid_from))
        if db_row is None:
            divergences.append(
                Divergence(
                    dimension=dimension,
                    key=key,
                    valid_from=valid_from,
                    kind="missing_in_db",
                )
            )
            continue
        fields = {
            name: (catalog_value, db_row[name])
            for name, catalog_value in expected.items()
            if db_row[name] != catalog_value
        }
        if fields:
            divergences.append(
                Divergence(
                    dimension=dimension,
                    key=key,
                    valid_from=valid_from,
                    kind="content_differs",
                    fields=fields,
                )
            )
    return divergences
