"""Dimension resolution for the loader (README "Loader/Auflösung").

Resolves ``dim_time``/``dim_region``/``dim_metric``/``dim_source`` ids for
measurement records. Resolution happens **once per run** and is cached in
dicts — never per record. Region and metric are time-dependent (SCD-2
validity windows); the timestamp is always cast with
``(ts AT TIME ZONE 'UTC')::date`` so the result is independent of the session
``TimeZone``.
"""

from __future__ import annotations

import logging
from datetime import UTC, date, datetime
from typing import LiteralString, cast

import psycopg

from wattfrosch_store import StoreError

logger = logging.getLogger(__name__)

# DO UPDATE is required: DO NOTHING returns no row for an existing ts_utc.
_UPSERT_TIME_SQL = """
INSERT INTO raw.dim_time (ts_utc) VALUES (%(ts)s)
ON CONFLICT (ts_utc) DO UPDATE SET ts_utc = EXCLUDED.ts_utc
RETURNING time_id
"""

_RESOLVE_REGION_SQL = """
SELECT region_id
FROM raw.dim_region
WHERE region_code = %(region_code)s
  AND (%(ts)s AT TIME ZONE 'UTC')::date >= valid_from
  AND ((%(ts)s AT TIME ZONE 'UTC')::date < valid_to OR valid_to IS NULL)
"""

_RESOLVE_METRIC_SQL = """
SELECT metric_id
FROM raw.dim_metric
WHERE metric_key = %(metric_key)s
  AND (%(ts)s AT TIME ZONE 'UTC')::date >= valid_from
  AND ((%(ts)s AT TIME ZONE 'UTC')::date < valid_to OR valid_to IS NULL)
"""

_LOOKUP_SOURCE_SQL = """
SELECT source_id FROM raw.dim_source WHERE source_key = %(source_key)s
"""


class _ResolutionError(StoreError):
    """Base for dimension lookups the loader could not resolve."""

    code: str

    def __init__(self, message: str, context: dict[str, object]) -> None:
        super().__init__(message)
        self.context = context


class RegionNotFoundError(_ResolutionError):
    code = "wattfrosch.store_region_not_found"


class MetricNotFoundError(_ResolutionError):
    code = "wattfrosch.store_metric_not_found"


class SourceNotFoundError(_ResolutionError):
    code = "wattfrosch.store_source_not_found"


class DimensionResolver:
    """Resolves dimension ids for one ``load_records`` run.

    The resolver caches every lookup, so each distinct timestamp, region-date
    pair or source key hits the database exactly once per run. Not-found
    lookups raise typed errors; ``load_records`` converts them into a
    ``LoadResult`` with ``status="failed"`` — no exception crosses the module
    boundary.
    """

    def __init__(self, conn: psycopg.Connection[dict]) -> None:
        self._conn = conn
        self._time_ids: dict[datetime, int] = {}
        self._region_ids: dict[tuple[str, date], int] = {}
        self._metric_ids: dict[tuple[str, date], int] = {}
        self._source_ids: dict[str, int] = {}

    def time_id(self, ts_utc: datetime) -> int:
        """Upsert the timestamp into ``dim_time`` and return its id."""
        cached = self._time_ids.get(ts_utc)
        if cached is not None:
            return cached
        with self._conn.cursor() as cur:
            cur.execute(_UPSERT_TIME_SQL, {"ts": ts_utc})
            row = cur.fetchone()
        if row is None:
            raise StoreError("dim_time upsert returned no row")
        time_id = cast("int", row["time_id"])
        self._time_ids[ts_utc] = time_id
        return time_id

    def region_id(self, region_code: str, ts_utc: datetime) -> int:
        """Resolve the ``dim_region`` row valid at the record's UTC date."""
        return self._resolve_scd2(
            cache=self._region_ids,
            sql=_RESOLVE_REGION_SQL,
            key_name="region_code",
            key=region_code,
            ts_utc=ts_utc,
            id_column="region_id",
            error_cls=RegionNotFoundError,
            noun="region",
        )

    def metric_id(self, metric_key: str, ts_utc: datetime) -> int:
        """Resolve the ``dim_metric`` row valid at the record's UTC date."""
        return self._resolve_scd2(
            cache=self._metric_ids,
            sql=_RESOLVE_METRIC_SQL,
            key_name="metric_key",
            key=metric_key,
            ts_utc=ts_utc,
            id_column="metric_id",
            error_cls=MetricNotFoundError,
            noun="metric",
        )

    def source_id(self, source_key: str) -> int:
        """Look up the flat ``dim_source`` row (no validity window)."""
        cached = self._source_ids.get(source_key)
        if cached is not None:
            return cached
        with self._conn.cursor() as cur:
            cur.execute(_LOOKUP_SOURCE_SQL, {"source_key": source_key})
            row = cur.fetchone()
        if row is None:
            raise SourceNotFoundError(
                f"Source '{source_key}' not found in raw.dim_source",
                {"source": source_key},
            )
        source_id = cast("int", row["source_id"])
        self._source_ids[source_key] = source_id
        return source_id

    def _resolve_scd2(
        self,
        cache: dict[tuple[str, date], int],
        sql: LiteralString,
        key_name: str,
        key: str,
        ts_utc: datetime,
        id_column: str,
        error_cls: type[_ResolutionError],
        noun: str,
    ) -> int:
        # Cache key is the UTC date: the validity windows are date-based, so
        # every timestamp on one UTC date resolves to the same row.
        ts_date = ts_utc.astimezone(UTC).date()
        cache_key = (key, ts_date)
        cached = cache.get(cache_key)
        if cached is not None:
            return cached
        params = {key_name: key, "ts": ts_utc}
        with self._conn.cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
        if row is None:
            raise error_cls(
                f"{noun.capitalize()} '{key}' is not valid at "
                f"{ts_utc.isoformat()} (no dim row covers this UTC date)",
                {key_name: key, "ts_utc": ts_utc.isoformat()},
            )
        resolved_id = cast("int", row[id_column])
        cache[cache_key] = resolved_id
        return resolved_id
