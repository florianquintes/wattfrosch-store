"""File readers for the dev/test import path (README "Datei-Import").

``load --from-file`` and the tests use these readers; production writes
exclusively via wattfrosch-pipeline calling :func:`wattfrosch_store.load_records`.
There is no second write path — the readers yield ``MeasurementRecord``
objects that go through the same validation and loader.

Input format mirrors the wattfrosch-fetch export: columns
``ts_utc, region, metric, value, source, resolution``; parquet carries
``ts_utc`` as a timezone-aware UTC timestamp, CSV/NDJSON as ISO 8601 strings.
Validation is strict: naive timestamps (no ``Z``/offset) are rejected with
the file line number, unparseable values abort with the line number.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path
from typing import Any, Final

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq
from pydantic import ValidationError
from wattfrosch_contracts.records import MeasurementRecord

from wattfrosch_store import StoreError
from wattfrosch_store.config import ConfigError

logger = logging.getLogger(__name__)

SUPPORTED_FORMATS: Final[tuple[str, ...]] = (".parquet", ".csv", ".ndjson", ".json")

_READ_BATCH_SIZE = 8192

_EXPECTED_TS_FORMAT = 'Expected: ISO 8601 with offset, e.g. "2026-10-10T13:00:00Z".'


class UnsupportedFormatError(ConfigError):
    """The file extension is not one of the supported import formats."""


class RecordValidationError(StoreError):
    """A record in the input file failed strict validation."""

    code = "wattfrosch.store_validation_failed"

    def __init__(self, message: str, context: dict[str, object]) -> None:
        super().__init__(message)
        self.context = context


def read_records(path: Path) -> Iterator[MeasurementRecord]:
    """Stream ``MeasurementRecord`` objects from ``path``.

    The format is detected by the file extension. Reading is streaming for
    all formats; the caller decides how much to consume (a partially
    consumed generator simply leaves the file to be closed by the garbage
    collector).

    Extension and (for parquet) the file footer are validated eagerly —
    before the first record is requested — so a wrong or unreadable input
    aborts before the loader opens a database connection.
    """
    suffix = path.suffix.lower()
    if suffix not in SUPPORTED_FORMATS:
        raise UnsupportedFormatError(
            f"Unsupported file extension {suffix!r} for {path}. "
            f"Supported formats: {', '.join(SUPPORTED_FORMATS)}"
        )
    if suffix == ".parquet":
        parquet_file = pq.ParquetFile(str(path))
        return _iter_parquet(parquet_file, path)
    if suffix == ".csv":
        return _iter_csv(path)
    return _iter_ndjson(path)


def _iter_parquet(
    parquet_file: pq.ParquetFile, path: Path
) -> Iterator[MeasurementRecord]:
    ordinal = 0  # parquet has no file lines; report the record ordinal
    for batch in parquet_file.iter_batches(batch_size=_READ_BATCH_SIZE):
        for row in batch.to_pylist():
            ordinal += 1
            yield _validated(row, ordinal, path)


def _iter_csv(path: Path) -> Iterator[MeasurementRecord]:
    # ts_utc is forced to a string: pyarrow's own timestamp inference is
    # neither strict nor line-aware, and naive timestamps must be rejected
    # with a line number instead of being silently parsed.
    convert_options = pacsv.ConvertOptions(column_types={"ts_utc": pa.string()})
    reader = pacsv.open_csv(str(path), convert_options=convert_options)
    try:
        line = 1  # the header occupies line 1
        for batch in reader:
            for row in batch.to_pylist():
                line += 1
                yield _validated(row, line, path)
    finally:
        reader.close()


def _iter_ndjson(path: Path) -> Iterator[MeasurementRecord]:
    with path.open("r", encoding="utf-8") as fh:
        for line_no, line in enumerate(fh, start=1):
            stripped = line.strip()
            if not stripped:
                continue
            try:
                row = json.loads(stripped)
            except json.JSONDecodeError as exc:
                raise RecordValidationError(
                    f"Error line {line_no}: invalid JSON: {exc.msg}",
                    {"line": line_no, "path": str(path)},
                ) from None
            if not isinstance(row, dict):
                raise RecordValidationError(
                    f"Error line {line_no}: expected a JSON object",
                    {"line": line_no, "path": str(path)},
                )
            yield _validated(row, line_no, path)


def _validated(row: dict[str, Any], line: int, path: Path) -> MeasurementRecord:
    """Validate one raw row; errors carry the line number."""
    ts = row.get("ts_utc")
    if isinstance(ts, str):
        try:
            parsed = datetime.fromisoformat(ts)
        except ValueError:
            raise RecordValidationError(
                f"Error line {line}: unparseable ts_utc ({ts!r}). "
                f"{_EXPECTED_TS_FORMAT}",
                {"line": line, "path": str(path), "field": "ts_utc"},
            ) from None
        if parsed.tzinfo is None:
            raise RecordValidationError(
                f"Error line {line}: ts_utc without timezone ({ts!r}). "
                f"{_EXPECTED_TS_FORMAT}",
                {"line": line, "path": str(path), "field": "ts_utc"},
            )
    elif isinstance(ts, datetime) and ts.tzinfo is None:
        raise RecordValidationError(
            f"Error line {line}: ts_utc without timezone ({ts!r}). "
            f"{_EXPECTED_TS_FORMAT}",
            {"line": line, "path": str(path), "field": "ts_utc"},
        )
    try:
        return MeasurementRecord(**row)
    except ValidationError as exc:
        raise RecordValidationError(
            f"Error line {line}: invalid record: {exc}",
            {"line": line, "path": str(path)},
        ) from exc
