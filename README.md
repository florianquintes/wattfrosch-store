# wattfrosch-store

Database schema, migrations and an idempotent loader for wattfrosch
`MeasurementRecord`s (Postgres).

Part of the [wattfrosch](https://github.com/florianquintes/wattfrosch) module
family.

## What is this?

This is the persistence stage of the wattfrosch pipeline. It owns the database
schema as a contract — transformation and forecasting build on it — and writes
`MeasurementRecord` objects from
[wattfrosch-contracts](https://github.com/florianquintes/wattfrosch-contracts)
into a relational star schema. Production writes go exclusively through
wattfrosch-pipeline; this module is never the fetcher.

Features:

- SQL migrations as numbered `.sql` files (pure DDL) with a small own runner —
  no ORM, no Alembic; an advisory lock makes concurrent runs safe
- Automatic dimension sync from the contract catalogs (`METRICS`, `REGIONS`,
  `SOURCES`): insert-only, never overwrites or closes existing rows;
  SCD Type 2 for `dim_region` and `dim_metric`
- Idempotent fact upsert (`INSERT ... ON CONFLICT`), one commit per batch with
  fail-fast semantics — a re-run after an abort continues harmlessly
- **No exceptions across the module boundary:** `load_records` always returns a
  `LoadResult`; the pipeline inspects `status` and decides what to do
- CLI with explicit, separated commands: `init`, `migrate`, `load`, `verify`
- File import (parquet | csv | ndjson) as a development/test entry point
- Integration tests against a real Postgres (testcontainers)

Package structure:

```
sql/migrations/          # DDL, numbered (0001_initial.sql, ...)
src/wattfrosch_store/
  __init__.py            # load_records, LoadResult, StoreError
  config.py              # DATABASE_URL, LOG_LEVEL, BATCH_SIZE, MIGRATE_LOCK_TIMEOUT
  db.py                  # connection (psycopg 3), DSN handling, SQLSTATE translation
  migrate.py             # migration runner (advisory lock, one transaction per file)
  sync_dims.py           # insert-only dimension sync + collect_divergences
  loader.py              # time dimension, resolution caching, upsert, batching
  io.py                  # file readers (parquet | csv | ndjson), strict validation
  verify.py              # read-only state checks
  cli.py                 # init | migrate | load | verify
tests/                   # integration tests (testcontainers) + fixtures
```

## Installation

Requires Python 3.13+ (`requires-python >= 3.13`) and a Postgres 16 server
(e.g. via the included `docker-compose.yml`). This repo depends on the sibling
repo `wattfrosch-contracts` as an editable path source (`[tool.uv.sources]`),
so it must exist next to this repo or `uv sync` fails:

```sh
git clone https://github.com/florianquintes/wattfrosch-contracts
git clone https://github.com/florianquintes/wattfrosch-store
cd wattfrosch-store
docker compose up -d db --wait
uv sync --locked
```

The contract version is checked on every load (major.minor comparison; patches
are accepted) and aborts with `wattfrosch.store_contract_divergence` on
mismatch.

## Usage

CLI — four explicit commands; `init` is *not* a hidden migration (it only
creates the database, the `raw` schema and the `raw.schema_migration` log
table), while `migrate` is the explicit, potentially dangerous one:

```sh
uv run wattfrosch-store init                       # create DB/schema if missing
uv run wattfrosch-store migrate                    # apply pending migrations
uv run wattfrosch-store verify [--check-values]    # read-only state check
uv run wattfrosch-store load --from-file <path>    # dev/test import only
```

CLI flags:

| Flag | Meaning |
|---|---|
| `--from-file` | required for `load`; production writes go through the pipeline, so `load` without it is a usage error (exit 1) |
| `--check-values` | optional for `verify`; full-scan plausibility check of `value` against `dim_metric.plausible_min/max` |

Batch size is configured via the `BATCH_SIZE` environment variable (see below),
not via a CLI flag.

The file format is detected by extension: `.parquet`, `.csv`,
`.ndjson`/`.json`. An unknown extension aborts with the list of supported
formats. All readers stream and validate strictly: naive timestamps (no
`Z`/offset) are rejected with a line number, and an empty file is success with
a warning.

Exit codes: `0` success (an **empty result is also success**, with a stderr
warning), `1` configuration/usage error (file not found, unknown extension,
`load` without `--from-file`), `2` a check or run failed (divergence, missing
schema, migration error, batch failure).

### Configuration (environment variables)

| Variable | Default | Meaning |
|---|---|---|
| `DATABASE_URL` | `postgresql://wattfrosch:wattfrosch@localhost:5432/wattfrosch` | DSN; the default is **local only** |
| `LOG_LEVEL` | `INFO` | logging |
| `BATCH_SIZE` | `1000` | records per transaction |
| `MIGRATE_LOCK_TIMEOUT` | `30s` | wait for the advisory lock (accepts `s`/`m`/`h` suffixes) |

The effective DSN is logged at startup **without the password**
(`postgresql://wattfrosch:***@localhost:5432/wattfrosch`): a default silently
takes over when the env var is missing, and that must be visible in container
setups.

Connection errors are translated centrally with actionable hints:

| SQLSTATE | Meaning | Hint |
|---|---|---|
| `3D000` | database does not exist | run `wattfrosch-store init` |
| `3F000` | schema `raw` does not exist | run `wattfrosch-store migrate` |
| `08001`/`08006` | DB not reachable | `docker compose up -d db` |

### Library

```python
from wattfrosch_contracts import MeasurementRecord
from wattfrosch_store import load_records

records: list[MeasurementRecord] = ...   # from wattfrosch-pipeline/fetch

result = load_records(records)
if result.status == "failed":
    assert result.error is not None
    print(result.error.code, result.error.message)
    raise RuntimeError(result.error.message)   # the caller raises, not the store

print(result.records_read, result.rows_written, result.batches_committed)
for div in result.divergences:          # catalog != db, warnings only
    print(div)
```

`records` are pydantic `MeasurementRecord` objects — never raw dicts. The
`LoadResult` carries partial progress on failure (already committed batches
stay valid; a re-run is idempotent):

```python
@dataclass(frozen=True)
class LoadResult:
    status: Literal["ok", "empty", "failed"]
    records_read: int
    rows_written: int              # on status="failed": committed partial progress
    batches_committed: int
    batches_total: int
    dims_inserted: int
    divergences: list[Divergence]  # catalog != db (warnings only)
    error: StoreError | None       # code, message, context, exception
```

Error codes follow the contracts convention (`wattfrosch.<module>_<reason>`):

| Code | Meaning |
|---|---|
| `wattfrosch.store_region_not_found` | no `dim_region` row valid at the record's timestamp |
| `wattfrosch.store_metric_not_found` | metric key missing in the DB |
| `wattfrosch.store_source_not_found` | source key missing in the DB |
| `wattfrosch.store_batch_failed` | a batch insert failed; run aborted fail-fast |
| `wattfrosch.store_contract_divergence` | catalog row missing in the DB (load aborts, verify errors) |
| `wattfrosch.store_validation_failed` | a record failed contract validation |
| `wattfrosch.store_database_missing` | database does not exist |
| `wattfrosch.store_schema_missing` | schema `raw` does not exist |

## The schema

A small star schema in the `raw` schema (the verbatim DDL lives in
[`sql/migrations/0001_initial.sql`](sql/migrations/0001_initial.sql); design
rationale and deliberate deviations in [`docs/design.md`](docs/design.md)):

| Table | Content | Notes |
|---|---|---|
| `dim_time` | `ts_utc` | deliberately slim — calendar attributes are derived, not stored |
| `dim_region` | bidding zones + weather points | SCD Type 2: `UNIQUE (region_code, valid_from)` + GiST `EXCLUDE` against overlapping validity windows |
| `dim_metric` | all `MetricEntry` fields | SCD Type 2 like `dim_region` |
| `dim_source` | name, license, url | flat — sources have no definition history |
| `fact_measurement` | value, ingested_at | the grain **is** the primary key: `(time_id, region_id, metric_id, source_id, resolution)` |

Key rules:

- Dimension sync is **insert-only**: existing rows are never overwritten or
  closed. A metric/region definition change is a hand-written migration; the
  loader never versions dimensions itself (it cannot distinguish a typo from a
  definition change).
- Region/metric resolution matches the record's timestamp against the SCD-2
  validity windows using `(ts AT TIME ZONE 'UTC')::date` — a bare
  `timestamptz::date` depends on the session `TimeZone` and is
  non-deterministic.
- The time dimension is resolved with
  `INSERT ... ON CONFLICT (ts_utc) DO UPDATE ... RETURNING time_id` (`DO
  UPDATE` is required because `DO NOTHING` returns no row on conflict).
- Migrations: numbered files, applied once and logged in `raw.schema_migration`
  with the `CONTRACT_VERSION` from wattfrosch-contracts. **Applied migrations
  are never edited** — fixes come as new numbered files. `migrate` shows
  pending migrations before applying, aborts fail-fast if the database is
  newer than the module, and takes a session-level advisory lock
  (`pg_advisory_lock`, documented key `0x77617474`) **before** reading the
  applied list — that ordering is the real protection against two concurrent
  runs.

## Contributing

Tooling is uv (Python 3.13). CI order matters: lint → format → typecheck → test.

```bash
uv sync --locked                # install deps into .venv
uv run ruff check .             # lint
uv run ruff format --check .    # formatting gate
uv run pyrefly check            # type check (pyrefly, not mypy)
uv run pytest --cov -q          # tests with coverage gate (fail_under = 90)
```

Conventions:

- Type checking is **pyrefly** (always use pyrefly, not mypy).
- Tests are integration tests against a real Postgres started via
  testcontainers — no mocking of SQL behaviour; each test runs against a
  clean schema.
- Applied migrations are never edited; fixes come as new numbered files in
  `sql/migrations/`.
- All timestamp→date logic must use `(ts AT TIME ZONE 'UTC')::date`.
- `load_records` never raises across the module boundary — it always returns a
  `LoadResult`.
- Store error codes follow the contracts `error-type` convention.

Open points:

- Bulk load via COPY instead of `executemany` — measure first, then decide
- Index design for dashboard queries (`idx_fact_measurement_lookup` does not
  cover "all metrics, one region, time range")
- `checksum` column in `schema_migration` (integrity, tamper protection)
- `dim_date` (`is_weekend`, `is_holiday`) — region- and timezone-dependent
- DST classification as a real column in `dim_time` (02:00–03:00 exists twice
  in October and not at all in March — not derivable from `ts_utc`)
- `tz` in `dim_region` is not used by the loader yet; local time comes from
  `ts_utc AT TIME ZONE r.tz`

## License

See [LICENSE](LICENSE).
