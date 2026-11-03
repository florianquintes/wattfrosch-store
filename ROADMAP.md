# Roadmap - wattfrosch-store
Duration: 1 week (5 working days).

## Day 1
- Repo skeleton; docker-compose (db only — store runs locally in v1)
- Connection layer (psycopg 3) + config (DATABASE_URL with local default,
  BATCH_SIZE, MIGRATE_LOCK_TIMEOUT); central error translation
  (3D000 -> init, 3F000 -> migrate, 08006 -> docker compose up)

## Day 2
- Migration 0001_initial.sql: schema raw.* (dim_time, dim_region, dim_metric,
  dim_source, fact_measurement) + btree_gist + idx_fact_measurement_lookup
- Note: schema_migration is NOT part of 0001 — bootstrapped by init/runner
- Migration runner + CLI command: migrate (lists pending migrations,
  advisory lock with 30s lock_timeout, lock before reading the applied list)

## Day 3
- CLI command: init (creates the database itself via the maintenance DB,
  autocommit, duplicate_database tolerated; creates schema raw + schema_migration;
  no star-schema DDL)
- Dimension sync from METRICS/REGIONS/SOURCES, insert-only
  (dim_metric with all MetricEntry fields, SCD-2; dim_region with SCD-2
  validity windows — DE-AT-LU/DE-LU exist in parallel; dim_source with
  name/license/url)
- Divergence check via shared collect_divergences: load warns, verify fails

## Day 4
- Loader: upsert (INSERT ... ON CONFLICT) + fill time dimension (RETURNING);
  batch commit (1000), fail fast, LoadResult instead of exceptions
- CLI commands: load --from-file, verify
- File reader for fixtures: parquet | csv | ndjson (strict, naive timestamps
  rejected)

## Day 5
- Integration test (testcontainers): init -> migrate -> load -> check
- Tests T1-T7: idempotency, SCD-2 boundary (incl. AT TIME ZONE 'UTC'),
  unresolvable region, advisory-lock timeout
- Documented EXPLAIN ANALYZE example

## Definition of Done
- docker compose up -d db starts Postgres
- init, migrate, load are idempotent
- loader uses real SQL; integration tests T1-T7 green
- EXPLAIN ANALYZE shows the grain PK serves time-range filters
  (hypothesis: no extra index on time_id needed)

## Schema decisions (deviations from §6.6)
- dim_time slim: time_id integer identity + ts_utc; calendar attributes
  derivable, not stored (placeholder for DST/holidays)
- fact_measurement: no surrogate measurement_id; the grain is the primary key
- dim_metric time-dependent (UNIQUE (metric_key, valid_from) + EXCLUDE)
- resolution without CHECK constraint (PT15M later without migration)
- no checksum in schema_migration (v1: discipline rule)

## Open for detail work
- normalization level
- bulk load (COPY) now or later — decided: later, v1 uses executemany
- index design for dashboard queries (depends on module 6 queries)
- checksum column in schema_migration
- dim_date (is_weekend, is_holiday) and DST classification
