"""T5 — SCD-2 boundary behavior of dim_region resolution."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import psycopg
import pytest
from psycopg.rows import dict_row

from wattfrosch_store.config import Settings
from wattfrosch_store.io import read_records
from wattfrosch_store.loader import (
    DimensionResolver,
    RegionNotFoundError,
    load_records,
)
from wattfrosch_store.migrate import run_init, run_migrations
from wattfrosch_store.sync_dims import sync_dims

FIXTURES = Path(__file__).resolve().parent / "fixtures"


@pytest.mark.integration
def test_t5_region_ids_differ_across_boundary(clean_db):
    run_init(clean_db)
    run_migrations(clean_db)

    with psycopg.connect(clean_db.database_url, row_factory=dict_row) as conn:
        sync_dims(conn)
        resolver = DimensionResolver(conn)

        # DE-AT-LU: [2015-01-01, 2018-10-01), DE-LU: [2018-10-01, open)
        # 2018-06-01 and 2018-12-01 land in different validity windows
        pre = resolver.region_id("DE-AT-LU", datetime(2018, 6, 1, tzinfo=UTC))
        post = resolver.region_id("DE-LU", datetime(2018, 12, 1, tzinfo=UTC))
        assert pre != post

        # [) half-openness: 2018-09-30 -> DE-AT-LU, 2018-10-01 -> DE-LU
        at_lu_id = resolver.region_id("DE-AT-LU", datetime(2018, 9, 30, 12, tzinfo=UTC))
        with pytest.raises(RegionNotFoundError):
            resolver.region_id("DE-AT-LU", datetime(2018, 10, 1, 0, 0, tzinfo=UTC))

        lu_id = resolver.region_id("DE-LU", datetime(2018, 10, 1, 12, tzinfo=UTC))
        with pytest.raises(RegionNotFoundError):
            resolver.region_id("DE-LU", datetime(2018, 9, 30, 23, 59, tzinfo=UTC))

    assert at_lu_id != lu_id


@pytest.mark.integration
def test_t5_boundary_utc_cast_independent_of_session_timezone(clean_db):
    run_init(clean_db)
    run_migrations(clean_db)

    # 2018-09-30T23:30Z is already 2018-10-01 in Auckland: a bare ::date
    # would flip the UTC date and resolve the wrong (or no) region.
    ts = datetime(2018, 9, 30, 23, 30, tzinfo=UTC)
    with psycopg.connect(clean_db.database_url, row_factory=dict_row) as conn:
        conn.execute("SET TimeZone = 'Pacific/Auckland'")
        sync_dims(conn)
        resolver = DimensionResolver(conn)
        at_lu_id = resolver.region_id("DE-AT-LU", ts)
    with psycopg.connect(clean_db.database_url, row_factory=dict_row) as conn:
        sync_dims(conn)
        resolver = DimensionResolver(conn)
        at_lu_id_default_tz = resolver.region_id("DE-AT-LU", ts)
    assert at_lu_id == at_lu_id_default_tz


@pytest.mark.integration
def test_t5_record_before_valid_from_fails_without_fact_row(clean_db):
    """DE-LU @ 2018-06-01 predates valid_from — rejected at record level.

    The contracts model validates region validity, so the record never
    reaches the loader (``store_validation_failed``). The DB-side resolution
    failure (``store_region_not_found``) is covered by
    ``test_integration_unresolvable_region_fails_without_fact_row``.
    """
    run_init(clean_db)
    run_migrations(clean_db)

    result = load_records(
        read_records(FIXTURES / "unresolvable_region.csv"),
        Settings(database_url=clean_db.database_url),
    )

    assert result.status == "failed"
    assert result.error is not None
    assert result.error.code == "wattfrosch.store_validation_failed"
    assert "DE-LU" in str(result.error)
    assert "2018-06-01" in str(result.error)
    with psycopg.connect(clean_db.database_url, row_factory=dict_row) as conn:
        count = conn.execute(
            "SELECT count(*) AS n FROM raw.fact_measurement"
        ).fetchone()
    assert count is not None and count["n"] == 0
