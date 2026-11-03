-- 0001_initial.sql — Star schema v1 (see README "Datenbankschema v1").
-- Pure DDL, deterministic, offline-capable. `raw.schema_migration` is NOT
-- created here — it is bootstrapped by `init` / the migration runner.

CREATE SCHEMA IF NOT EXISTS raw;
CREATE EXTENSION IF NOT EXISTS btree_gist;

-- Time dimension: deliberately slim. Calendar attributes (date, hour, month,
-- year, day_of_week) are NOT stored — they are derivable from ts_utc:
--   (ts_utc AT TIME ZONE 'UTC')::date      -- or AT TIME ZONE <dim_region.tz>
--   EXTRACT(hour FROM ts_utc AT TIME ZONE 'UTC')
-- Placeholder for later non-derivable attributes (DST classification,
-- is_holiday) and a future dim_date.
CREATE TABLE raw.dim_time (
    time_id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ts_utc  timestamptz NOT NULL UNIQUE
);

-- Time-dependent (SCD Type 2): region_code is NOT unique.
-- Zone change DE-AT-LU -> DE-LU on 2018-10-01 without a code change.
CREATE TABLE raw.dim_region (
    region_id   smallint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    region_code text NOT NULL,
    name        text NOT NULL,
    kind        text NOT NULL,          -- 'bidding_zone' | 'weather_point'
    tz          text NOT NULL,          -- e.g. 'Europe/Berlin'
    valid_from  date NOT NULL,
    valid_to    date,                  -- NULL = currently valid
    CONSTRAINT uq_region UNIQUE (region_code, valid_from),
    CONSTRAINT ck_region_period CHECK (valid_to IS NULL OR valid_to > valid_from)
);
ALTER TABLE raw.dim_region ADD CONSTRAINT no_overlap_region
    EXCLUDE USING gist (
        region_code WITH =,
        daterange(valid_from, valid_to, '[)') WITH &&
    );

-- Time-dependent (SCD Type 2), all MetricEntry fields as columns.
CREATE TABLE raw.dim_metric (
    metric_id       smallint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    metric_key      text NOT NULL,
    unit            text NOT NULL,
    category        text NOT NULL,
    aggregation     text NOT NULL,
    is_derived      boolean NOT NULL DEFAULT false,
    is_renewable    boolean NOT NULL DEFAULT false,
    can_be_negative boolean NOT NULL DEFAULT false,
    plausible_min   double precision,
    plausible_max   double precision,
    display_name    text,
    description     text,
    valid_from      date NOT NULL,
    valid_to        date,
    CONSTRAINT uq_metric UNIQUE (metric_key, valid_from),
    CONSTRAINT ck_metric_period CHECK (valid_to IS NULL OR valid_to > valid_from)
);
ALTER TABLE raw.dim_metric ADD CONSTRAINT no_overlap_metric
    EXCLUDE USING gist (
        metric_key WITH =,
        daterange(valid_from, valid_to, '[)') WITH &&
    );

-- Flat: sources have no definition history.
CREATE TABLE raw.dim_source (
    source_id  smallint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_key text NOT NULL UNIQUE,
    name       text NOT NULL,
    license    text,
    url        text
);

-- Fact table: the grain IS the primary key (one index instead of two).
CREATE TABLE raw.fact_measurement (
    time_id     integer  NOT NULL REFERENCES raw.dim_time(time_id),
    region_id   smallint NOT NULL REFERENCES raw.dim_region(region_id),
    metric_id   smallint NOT NULL REFERENCES raw.dim_metric(metric_id),
    source_id   smallint NOT NULL REFERENCES raw.dim_source(source_id),
    resolution  text     NOT NULL DEFAULT 'PT1H',   -- deliberately no CHECK: PT15M later
    value       double precision NOT NULL,
    ingested_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (time_id, region_id, metric_id, source_id, resolution)
);

CREATE INDEX idx_fact_measurement_lookup
    ON raw.fact_measurement (metric_id, region_id, time_id);
