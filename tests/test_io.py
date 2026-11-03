"""Tests for the file readers (D3)."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from wattfrosch_contracts.records import MeasurementRecord

from wattfrosch_store.io import (
    RecordValidationError,
    UnsupportedFormatError,
    read_records,
)
from wattfrosch_store.loader import load_records
from wattfrosch_store.migrate import run_init, run_migrations

FIXTURES = Path(__file__).parent / "fixtures"


def _record(
    ts: str,
    value: float,
    region: str = "DE-LU",
    metric: str = "generation.solar",
    source: str = "energy-charts",
) -> MeasurementRecord:
    return MeasurementRecord(
        ts_utc=ts,
        region=region,
        metric=metric,
        value=value,
        source=source,
        resolution="PT1H",
    )


def _write_csv(path: Path, rows: list[str]) -> Path:
    header = "ts_utc,region,metric,value,source,resolution"
    path.write_text("\n".join([header, *rows]) + "\n", encoding="utf-8")
    return path


def _write_ndjson(path: Path, rows: list[dict]) -> Path:
    path.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")
    return path


# --- format round-trips ---


def test_read_parquet_fixture():
    records = list(read_records(FIXTURES / "scd2_grenze.parquet"))

    assert len(records) == 4
    assert records[0] == _record("2018-09-30T22:00:00Z", 100.0, region="DE-AT-LU")
    assert records[0].region == "DE-AT-LU"  # both SCD-2 sides in one file
    assert records[2] == _record(
        "2018-10-01T00:00:00Z", 40.5, metric="price.day_ahead", source="smard"
    )
    assert records[2].region == "DE-LU"
    assert records[2].metric == "price.day_ahead"
    assert records[2].source == "smard"
    assert all(r.ts_utc.tzinfo is UTC for r in records)


def test_read_csv_parses_iso_strings(tmp_path):
    path = _write_csv(
        tmp_path / "valid.csv",
        [
            "2024-06-01T00:00:00Z,DE-LU,generation.solar,1.5,energy-charts,PT1H",
            "2024-06-01T01:00:00+02:00,DE-LU,generation.solar,2.5,energy-charts,PT1H",
        ],
    )

    records = list(read_records(path))

    assert records == [
        _record("2024-06-01T00:00:00Z", 1.5),
        _record("2024-06-01T01:00:00+02:00", 2.5),
    ]
    assert all(r.ts_utc.tzinfo is not None for r in records)


def test_read_ndjson_and_json_alias(tmp_path):
    rows = [
        {
            "ts_utc": "2024-06-01T00:00:00Z",
            "region": "DE-LU",
            "metric": "generation.solar",
            "value": 1.5,
            "source": "energy-charts",
            "resolution": "PT1H",
        },
    ]
    ndjson = _write_ndjson(tmp_path / "a.ndjson", rows)
    (json_path := tmp_path / "b.json").write_text(ndjson.read_text())

    assert list(read_records(ndjson)) == [_record("2024-06-01T00:00:00Z", 1.5)]
    assert list(read_records(json_path)) == [_record("2024-06-01T00:00:00Z", 1.5)]


def test_read_ndjson_fixture():
    records = list(read_records(FIXTURES / "valid.ndjson"))

    assert records == [
        _record("2024-06-01T00:00:00Z", 1.5),
        _record("2024-06-01T01:00:00Z", 2.5),
    ]


# --- strict validation ---


def test_read_csv_rejects_naive_timestamp_with_line_number():
    path = FIXTURES / "naive_ts.csv"

    with pytest.raises(RecordValidationError) as exc_info:
        list(read_records(path))

    assert exc_info.value.code == "wattfrosch.store_validation_failed"
    message = str(exc_info.value)
    assert "line 2" in message  # header occupies line 1
    assert "without timezone" in message
    assert '"2026-10-10T13:00:00"' in message or "2026-10-10T13:00:00" in message
    assert "Expected: ISO 8601 with offset" in message
    assert exc_info.value.context == {
        "line": 2,
        "path": str(path),
        "field": "ts_utc",
    }


def test_read_ndjson_rejects_naive_timestamp(tmp_path):
    path = _write_ndjson(
        tmp_path / "naive.ndjson",
        [
            {
                "ts_utc": "2024-06-01T00:00:00",
                "region": "DE-LU",
                "metric": "generation.solar",
                "value": 1.0,
                "source": "energy-charts",
                "resolution": "PT1H",
            },
        ],
    )

    with pytest.raises(RecordValidationError) as exc_info:
        list(read_records(path))

    assert "line 1" in str(exc_info.value)
    assert "without timezone" in str(exc_info.value)


def test_read_csv_rejects_unparseable_value_with_line_number():
    path = FIXTURES / "bad_value.csv"

    with pytest.raises(RecordValidationError) as exc_info:
        list(read_records(path))

    message = str(exc_info.value)
    assert "line 2" in message
    assert "invalid record" in message


def test_read_csv_rejects_unknown_region_with_line_number(tmp_path):
    path = _write_csv(
        tmp_path / "unknown_region.csv",
        [
            "2024-06-01T00:00:00Z,XX-NOPE,generation.solar,1.0,energy-charts,PT1H",
        ],
    )

    with pytest.raises(RecordValidationError) as exc_info:
        list(read_records(path))

    assert "line 2" in str(exc_info.value)
    assert "XX-NOPE" in str(exc_info.value)


def test_read_ndjson_rejects_invalid_json_line(tmp_path):
    path = tmp_path / "broken.ndjson"
    valid = json.dumps(
        {
            "ts_utc": "2024-06-01T00:00:00Z",
            "region": "DE-LU",
            "metric": "generation.solar",
            "value": 1.0,
            "source": "energy-charts",
            "resolution": "PT1H",
        }
    )
    path.write_text(valid + "\nnot json\n")

    with pytest.raises(RecordValidationError) as exc_info:
        list(read_records(path))

    assert "line 2" in str(exc_info.value)
    assert "invalid JSON" in str(exc_info.value)


def test_read_ndjson_rejects_non_object_line(tmp_path):
    path = tmp_path / "array.ndjson"
    path.write_text("[1, 2, 3]\n")

    with pytest.raises(RecordValidationError) as exc_info:
        list(read_records(path))

    assert "expected a JSON object" in str(exc_info.value)


def test_read_ndjson_skips_blank_lines(tmp_path):
    valid = json.dumps(
        {
            "ts_utc": "2024-06-01T00:00:00Z",
            "region": "DE-LU",
            "metric": "generation.solar",
            "value": 1.0,
            "source": "energy-charts",
            "resolution": "PT1H",
        }
    )
    path = tmp_path / "blank_lines.ndjson"
    path.write_text("\n" + valid + "\n\n" + valid + "\n\n")

    records = list(read_records(path))

    assert len(records) == 2  # blank lines skipped, line numbers unchanged


def test_read_rejects_unparseable_ts_string(tmp_path):
    path = _write_ndjson(
        tmp_path / "bad_ts.ndjson",
        [
            {
                "ts_utc": "not-a-date",
                "region": "DE-LU",
                "metric": "generation.solar",
                "value": 1.0,
                "source": "energy-charts",
                "resolution": "PT1H",
            },
        ],
    )

    with pytest.raises(RecordValidationError) as exc_info:
        list(read_records(path))

    assert "unparseable ts_utc" in str(exc_info.value)
    assert exc_info.value.context["field"] == "ts_utc"


def test_read_parquet_rejects_naive_ts_column(tmp_path):
    import pyarrow as pa
    import pyarrow.parquet as pq

    # fetch never writes this, but a hand-built parquet with a tz-less
    # timestamp column must be rejected, not silently accepted
    schema = pa.schema(
        [
            ("ts_utc", pa.timestamp("us")),  # no tz
            ("region", pa.string()),
            ("metric", pa.string()),
            ("value", pa.float64()),
            ("source", pa.string()),
            ("resolution", pa.string()),
        ]
    )
    table = pa.Table.from_pylist(
        [
            {
                "ts_utc": datetime(2024, 6, 1, 0, 0),
                "region": "DE-LU",
                "metric": "generation.solar",
                "value": 1.0,
                "source": "energy-charts",
                "resolution": "PT1H",
            }
        ],
        schema=schema,
    )
    path = tmp_path / "naive.parquet"
    pq.write_table(table, path)

    with pytest.raises(RecordValidationError) as exc_info:
        list(read_records(path))

    assert "line 1" in str(exc_info.value)
    assert "without timezone" in str(exc_info.value)


# --- format detection ---


def test_read_unknown_extension_lists_supported_formats(tmp_path):
    path = tmp_path / "data.txt"
    path.write_text("whatever")

    with pytest.raises(UnsupportedFormatError) as exc_info:
        list(read_records(path))

    message = str(exc_info.value)
    for fmt in (".parquet", ".csv", ".ndjson", ".json"):
        assert fmt in message


def test_read_missing_file_raises_file_not_found(tmp_path):
    with pytest.raises(FileNotFoundError):
        list(read_records(tmp_path / "nope.parquet"))


# --- empty files ---


def test_read_empty_csv_yields_nothing():
    assert list(read_records(FIXTURES / "empty.csv")) == []


def test_read_empty_ndjson_yields_nothing():
    assert list(read_records(FIXTURES / "empty.ndjson")) == []


# --- integration: fixture through the real loader ---


@pytest.mark.integration
def test_integration_fixture_loads_through_load_records(clean_db):
    run_init(clean_db)
    run_migrations(clean_db)

    result = load_records(read_records(FIXTURES / "scd2_grenze.parquet"))

    assert result.status == "ok"
    assert result.records_read == 4
    assert result.rows_written == 4
