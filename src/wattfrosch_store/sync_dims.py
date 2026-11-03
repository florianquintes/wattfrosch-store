"""Insert-only dimension sync (README "Dimensionsabgleich").

First step of every ``load``, in its own, separately committed transaction:
catalog definitions (wattfrosch-contracts) are inserted into ``dim_region``,
``dim_metric`` and ``dim_source`` with ``ON CONFLICT ... DO NOTHING``.
Existing rows are never overwritten and never closed — a definition change is
a hand-written migration, the loader never versions dimensions itself.
"""

from __future__ import annotations

import logging
from typing import cast

import psycopg
from wattfrosch_contracts import METRICS, REGIONS, SOURCES

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


def sync_dims(conn: psycopg.Connection[dict]) -> int:
    """Insert missing catalog rows into the dimension tables.

    Returns the number of rows actually inserted (0 when everything is
    already present). The caller must not have an open transaction: the sync
    runs in its own transaction, committed before the loader writes facts.
    """
    inserted = 0
    with conn.transaction():
        inserted += _sync_regions(conn)
        inserted += _sync_metrics(conn)
        inserted += _sync_sources(conn)
    logger.info("Dimension sync inserted %d row(s)", inserted)
    return inserted


def _sync_regions(conn: psycopg.Connection[dict]) -> int:
    with conn.cursor() as cur:
        count = 0
        for entry in REGIONS.values():
            cur.execute(
                _INSERT_REGION_SQL,
                {
                    "region_code": entry.code,
                    "name": entry.name,
                    "kind": entry.kind,
                    "tz": entry.tz,
                    "valid_from": entry.valid_from,
                    "valid_to": entry.valid_to,
                },
            )
            count += cast("int", cur.rowcount)
    return count


def _sync_metrics(conn: psycopg.Connection[dict]) -> int:
    with conn.cursor() as cur:
        count = 0
        for entry in METRICS.values():
            plausible_min, plausible_max = None, None
            if entry.plausible_range is not None:
                plausible_min, plausible_max = entry.plausible_range
            cur.execute(
                _INSERT_METRIC_SQL,
                {
                    "metric_key": entry.key,
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
                    "valid_from": entry.valid_from,
                    "valid_to": entry.valid_to,
                },
            )
            count += cast("int", cur.rowcount)
    return count


def _sync_sources(conn: psycopg.Connection[dict]) -> int:
    with conn.cursor() as cur:
        count = 0
        for source_key, entry in SOURCES.items():
            cur.execute(
                _INSERT_SOURCE_SQL,
                {
                    "source_key": source_key,
                    "name": entry.name,
                    "license": entry.license,
                    "url": entry.url,
                },
            )
            count += cast("int", cur.rowcount)
    return count
