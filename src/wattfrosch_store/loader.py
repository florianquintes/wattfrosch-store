"""Dimension resolution, fact upsert and ``load_records`` (README "Loader").

Resolution happens **once per run** and is cached in dicts — never per record.
Region and metric are time-dependent (SCD-2 validity windows); the timestamp
is always cast with ``(ts AT TIME ZONE 'UTC')::date`` so the result is
independent of the session ``TimeZone``.

``load_records`` is the module boundary towards wattfrosch-pipeline: it never
raises, it always returns a :class:`LoadResult` and the caller checks
``status == "failed"``.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from itertools import islice
from typing import Literal, LiteralString, cast

import psycopg
from wattfrosch_contracts import CONTRACT_VERSION
from wattfrosch_contracts.records import MeasurementRecord

from wattfrosch_store import StoreError
from wattfrosch_store.config import Settings, load_settings
from wattfrosch_store.db import (
    SchemaMissingError,
    StoreConnectionError,
    connect,
    translate_db_errors,
)
from wattfrosch_store.sync_dims import Divergence, sync_dims

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

_UPSERT_FACT_SQL = """
INSERT INTO raw.fact_measurement
    (time_id, region_id, metric_id, source_id, resolution, value)
VALUES (%(time_id)s, %(region_id)s, %(metric_id)s,
        %(source_id)s, %(resolution)s, %(value)s)
ON CONFLICT (time_id, region_id, metric_id, source_id, resolution)
DO UPDATE SET value = EXCLUDED.value, ingested_at = now()
"""


class _LoadError(StoreError):
    """Base for load errors carrying a store error code and context."""

    code: str

    def __init__(self, message: str, context: dict[str, object]) -> None:
        super().__init__(message)
        self.context = context


class RegionNotFoundError(_LoadError):
    code = "wattfrosch.store_region_not_found"


class MetricNotFoundError(_LoadError):
    code = "wattfrosch.store_metric_not_found"


class SourceNotFoundError(_LoadError):
    code = "wattfrosch.store_source_not_found"


class ContractVersionError(_LoadError):
    code = "wattfrosch.store_contract_divergence"


class BatchFailedError(_LoadError):
    code = "wattfrosch.store_batch_failed"


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
        error_cls: type[_LoadError],
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


@dataclass(frozen=True)
class LoadResult:
    """Outcome of one ``load_records`` run (README "Schnittstellen").

    Never raises across the module boundary: failures are reported via
    ``status="failed"`` plus ``error``; ``rows_written``/``batches_committed``
    then carry the already-committed partial progress.
    """

    status: Literal["ok", "empty", "failed"]
    records_read: int
    rows_written: int
    batches_committed: int
    batches_total: int
    dims_inserted: int
    divergences: list[Divergence]
    error: StoreError | None


@dataclass
class _Progress:
    """Mutable counters collected while the run is in flight."""

    records_read: int = 0
    rows_written: int = 0
    batches_committed: int = 0
    batches_total: int = 0
    dims_inserted: int = 0
    divergences: list[Divergence] = field(default_factory=list)


def load_records(
    records: Iterable[MeasurementRecord],
    settings: Settings | None = None,
) -> LoadResult:
    """Load ``MeasurementRecord`` objects into the store (README "Ablauf").

    Steps: contract-version check, insert-only dimension sync (own committed
    transaction), batched fact upsert with one commit per ``BATCH_SIZE``.
    Never raises — always returns a :class:`LoadResult`.
    """
    settings = settings or load_settings()
    progress = _Progress()
    try:
        conn = connect(settings)
    except StoreError as exc:
        return _failed(exc, progress)
    except psycopg.Error as exc:
        return _failed(StoreConnectionError(f"Database not reachable: {exc}"), progress)
    try:
        with translate_db_errors():
            _check_contract_version(conn)
            conn.commit()  # close the version-check read before the sync's own transaction
            sync = sync_dims(conn)
            progress.dims_inserted = sync.inserted
            progress.divergences = sync.divergences
            _abort_on_missing_divergences(sync.divergences)
            _upsert_batches(conn, iter(records), settings.batch_size, progress)
        if progress.batches_total == 0:
            logger.warning("No records to load: input was empty")
            status: Literal["ok", "empty", "failed"] = "empty"
        else:
            logger.info(
                "Load finished: %d record(s) read, %d row(s) written, "
                "%d/%d batch(es) committed",
                progress.records_read,
                progress.rows_written,
                progress.batches_committed,
                progress.batches_total,
            )
            status = "ok"
        return LoadResult(
            status=status,
            records_read=progress.records_read,
            rows_written=progress.rows_written,
            batches_committed=progress.batches_committed,
            batches_total=progress.batches_total,
            dims_inserted=progress.dims_inserted,
            divergences=progress.divergences,
            error=None,
        )
    except psycopg.errors.UndefinedTable as exc:
        conn.rollback()
        return _failed(
            SchemaMissingError(
                f"Required table does not exist (SQLSTATE {exc.sqlstate}). "
                "Run: wattfrosch-store migrate"
            ),
            progress,
        )
    except StoreError as exc:
        conn.rollback()
        return _failed(exc, progress)
    except psycopg.Error as exc:
        conn.rollback()
        return _failed(StoreError(f"Unexpected database error: {exc}"), progress)
    finally:
        conn.close()


def _failed(error: StoreError, progress: _Progress) -> LoadResult:
    logger.error("Load failed: %s", error)
    return LoadResult(
        status="failed",
        records_read=progress.records_read,
        rows_written=progress.rows_written,
        batches_committed=progress.batches_committed,
        batches_total=progress.batches_total,
        dims_inserted=progress.dims_inserted,
        divergences=progress.divergences,
        error=error,
    )


def _check_contract_version(conn: psycopg.Connection[dict]) -> None:
    """Compare the applied contract version with the module's (Major.Minor)."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT contract_version FROM raw.schema_migration "
            "ORDER BY applied_at DESC, migration_id DESC LIMIT 1"
        )
        row = cur.fetchone()
    if row is None:
        raise ContractVersionError(
            "No applied migrations found (raw.schema_migration is empty). "
            "Run: wattfrosch-store migrate",
            {},
        )
    db_version = cast("str", row["contract_version"])
    module_version = CONTRACT_VERSION
    if _major_minor(db_version) != _major_minor(module_version):
        raise ContractVersionError(
            f"Contract version mismatch: database has {db_version}, "
            f"module is built against {module_version} "
            "(Major.Minor must match)",
            {"database_version": db_version, "module_version": module_version},
        )


def _major_minor(version: str) -> str:
    return ".".join(version.split(".")[:2])


def _abort_on_missing_divergences(divergences: list[Divergence]) -> None:
    """Abort when the DB is missing catalog rows the loader must resolve."""
    missing = [d for d in divergences if d.kind == "missing_in_db"]
    if not missing:
        return
    keys = ", ".join(f"{d.dimension}:{d.key}" for d in missing)
    raise ContractVersionError(
        f"Catalog rows missing in the database (cannot resolve ids): {keys}",
        {"missing": [(d.dimension, d.key) for d in missing]},
    )


def _upsert_batches(
    conn: psycopg.Connection[dict],
    records: Iterator[MeasurementRecord],
    batch_size: int,
    progress: _Progress,
) -> None:
    """Consume records in batches of ``batch_size``; one commit per batch."""
    resolver = DimensionResolver(conn)
    batch_no = 0
    while True:
        batch = list(islice(records, batch_size))
        if not batch:
            break
        batch_no += 1
        progress.batches_total = batch_no
        progress.records_read += len(batch)
        progress.rows_written += _flush_batch(conn, resolver, batch, batch_no, progress)
        progress.batches_committed = batch_no


def _flush_batch(
    conn: psycopg.Connection[dict],
    resolver: DimensionResolver,
    batch: list[MeasurementRecord],
    batch_no: int,
    progress: _Progress,
) -> int:
    """Resolve dimensions and upsert one batch; commits on success."""
    params = [
        {
            "time_id": resolver.time_id(rec.ts_utc),
            "region_id": resolver.region_id(rec.region, rec.ts_utc),
            "metric_id": resolver.metric_id(rec.metric, rec.ts_utc),
            "source_id": resolver.source_id(rec.source),
            "resolution": rec.resolution,
            "value": rec.value,
        }
        for rec in batch
    ]
    try:
        with conn.cursor() as cur:
            cur.executemany(_UPSERT_FACT_SQL, params)
            written = cast("int", cur.rowcount)
        conn.commit()
    except psycopg.Error as exc:
        raise BatchFailedError(
            f"Batch {batch_no} failed at record ts_utc="
            f"{batch[0].ts_utc.isoformat()}, region={batch[0].region}, "
            f"metric={batch[0].metric}: {exc}. "
            f"{progress.batches_committed} batch(es) "
            f"({progress.rows_written} row(s)) are already committed and "
            f"remain valid. Re-run resumes idempotently.",
            {
                "batch": batch_no,
                "records_in_batch": len(batch),
                "ts_utc": batch[0].ts_utc.isoformat(),
                "region": batch[0].region,
                "metric": batch[0].metric,
            },
        ) from exc
    return written
