> **Note:** This is the v1 development spec (German) — the authoritative record of
> design decisions, schema rationale and deviations. The published module
> documentation lives in [README.md](../README.md).

# wattfrosch-store
Persistenz-Modul: Datenbankschema, Migrationen und idempotenter Loader.
Lädt MeasurementRecords aus wattfrosch-fetch in eine relationale Datenbank (Postgres).

## Rolle im System
Persistenzschicht. Besitzt das Datenbank-Schema als Contract, auf das
Transformation und Prognose aufsetzen.

## In Scope
- SQL-Migrationen (reines DDL) als nummerierte .sql-Dateien
- Eigener Migrationsrunner (kein ORM, kein Alembic), inkl. Advisory Lock
- Loader mit echtem SQL: Upsert (INSERT ... ON CONFLICT), Batch-Commit
- Automatischer Dimensionsabgleich aus den Contract-Registern
  (insert-only; SCD-2 für dim_region und dim_metric)
- CLI mit getrennten Befehlen: init, migrate, load, verify
- Datei-Import (parquet | csv | ndjson) als Entwicklungs-/Testeinstieg
- docker-compose mit der Datenbank (store selbst läuft in v1 lokal)
- Integrationstests gegen eine echte DB (testcontainers)

## Out of Scope
- Transformation/Analytik, externe API-Abrufe, Dashboard
- Backfill (Nachladen externer Daten) -> wattfrosch-pipeline
- Bulk-Load via COPY -> spätere Optimierung, in v1 bewusst nicht

## Schnittstellen

### Bibliothek (Produktionspfad)
```python
from wattfrosch_store import load_records

result = load_records(records)   # records: Iterable[MeasurementRecord]
```
- `records` sind `MeasurementRecord`-Objekte aus wattfrosch-contracts —
  keine rohen Dicts. Aufrufer ist ausschließlich wattfrosch-pipeline.
- **Keine Exception über die Modulgrenze.** `load_records` gibt immer ein
  `LoadResult` zurück; die Pipeline wertet `status` aus und bricht bei
  `"failed"` selbst ab (`if result.status == "failed": raise`).

```python
@dataclass(frozen=True)
class LoadResult:
    status: Literal["ok", "empty", "failed"]
    records_read: int
    rows_written: int          # bei status="failed": bereits committeter Teilfortschritt
    batches_committed: int
    batches_total: int
    dims_inserted: int
    divergences: list[Divergence]   # Katalog != DB (nur Warnungen)
    error: StoreError | None       # code, message, context, exception

@dataclass(frozen=True)
class StoreError:
    code: str            # z. B. "wattfrosch.store_region_not_found"
    message: str
    context: dict
    exception: BaseException | None   # Original-Traceback für Logging/Prefect
```

Fehler-Codes (Konvention wie die `error-type`-Konstanten der Contracts):
`wattfrosch.store_region_not_found`, `wattfrosch.store_metric_not_found`,
`wattfrosch.store_source_not_found`, `wattfrosch.store_batch_failed`,
`wattfrosch.store_contract_divergence`, `wattfrosch.store_validation_failed`,
`wattfrosch.store_database_missing`, `wattfrosch.store_schema_missing`.

### CLI
```
wattfrosch-store init                          # DB/Schema anlegen, falls nötig
wattfrosch-store migrate                       # ausstehende Migrationen anwenden
wattfrosch-store load --from-file <pfad>         # Testfixtures laden
wattfrosch-store verify [--check-values]         # Zustand prüfen
```
- `load` **erfordert** `--from-file`. Ohne das Flag: Usage-Fehler (Exit 1) mit
  Verweis auf die Pipeline. Produktiv schreibt nur die Pipeline.
- Format wird über die Dateiendung erkannt: `.parquet`, `.csv`, `.ndjson`/`.json`.
  Unbekannte Endung -> Abbruch mit Liste der unterstützten Formate.
- `init` ist **kein** versteckter Migrationsbefehl: es legt nur Datenbank,
  Schema `raw` und die Protokolltabelle `raw.schema_migration` an.
- `migrate` ist ein eigener, sichtbarer Befehl (potenziell gefährlich).

### Exit-Codes (store)
| Code | Bedeutung |
|------|-----------|
| 0 | Erfolg (leeres Ergebnis = Erfolg mit Warnung) |
| 1 | Konfiguration/Usage, Datei nicht gefunden, unbekannte Endung |
| 2 | Prüfung oder Lauf fehlgeschlagen (Divergenz, Schema, Migrationsfehler, Batch-Fehler) |

HTTP-Codes entfallen (store ruft keine APIs).

## Konfiguration (Umgebungsvariablen)
| Variable | Default | Bedeutung |
|---|---|---|
| `DATABASE_URL` | `postgresql://wattfrosch:wattfrosch@localhost:5432/wattfrosch` | DSN; Default **nur für lokal** |
| `LOG_LEVEL` | `INFO` | Logging |
| `BATCH_SIZE` | `1000` | Records pro Transaktion |
| `MIGRATE_LOCK_TIMEOUT` | `30s` | Wartezeit auf Advisory Lock (im Test überschreibbar) |

Die effektive DSN wird beim Start **ohne Passwort** geloggt
(`postgresql://wattfrosch:***@localhost:5432/wattfrosch`). Grund: ein Default
greift still, wenn die Env-Variable fehlt — im Containerbetrieb muss das sichtbar sein.

Verbindungsfehler werden zentral in `db.py` abgefangen und übersetzt:
| SQLSTATE | Bedeutung | Hinweis |
|---|---|---|
| `3D000` | Datenbank existiert nicht | `wattfrosch-store init` ausführen |
| `3F000` | Schema `raw` existiert nicht | `wattfrosch-store migrate` ausführen |
| `08001`/`08006` | DB nicht erreichbar | `docker compose up -d db` |

`init` selbst nutzt diesen Pfad nicht (es verbindet über die Wartungs-DB `postgres`).

## Datenbankschema v1 (`0001_initial.sql`)

```sql
CREATE SCHEMA IF NOT EXISTS raw;
CREATE EXTENSION IF NOT EXISTS btree_gist;

-- Zeitdimension: bewusst schlank. Kalenderattribute (date, hour, month, year,
-- day_of_week) werden NICHT gespeichert — sie sind aus ts_utc ableitbar:
--   (ts_utc AT TIME ZONE 'UTC')::date      -- bzw. AT TIME ZONE <dim_region.tz>
--   EXTRACT(hour FROM ts_utc AT TIME ZONE 'UTC')
-- Platzhalter für spätere, nicht ableitbare Attribute (DST-Klassifikation,
-- is_holiday) sowie für eine spätere dim_date.
CREATE TABLE raw.dim_time (
    time_id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ts_utc  timestamptz NOT NULL UNIQUE
);

-- Zeitabhängig (SCD Type 2): region_code ist NICHT eindeutig.
-- Zonenwechsel DE-AT-LU -> DE-LU am 2018-10-01 ohne Codeänderung.
CREATE TABLE raw.dim_region (
    region_id   smallint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    region_code text NOT NULL,
    name        text NOT NULL,
    kind        text NOT NULL,          -- 'bidding_zone' | 'weather_point'
    tz          text NOT NULL,          -- z. B. 'Europe/Berlin'
    valid_from  date NOT NULL,
    valid_to    date,                  -- NULL = aktuell gültig
    CONSTRAINT uq_region UNIQUE (region_code, valid_from),
    CONSTRAINT ck_region_period CHECK (valid_to IS NULL OR valid_to > valid_from)
);
ALTER TABLE raw.dim_region ADD CONSTRAINT no_overlap_region
    EXCLUDE USING gist (
        region_code WITH =,
        daterange(valid_from, valid_to, '[)') WITH &&
    );

-- Zeitabhängig (SCD Type 2), alle MetricEntry-Felder als Spalten.
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

-- Flach: Quellen haben keine Definitionshistorie.
CREATE TABLE raw.dim_source (
    source_id  smallint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_key text NOT NULL UNIQUE,
    name       text NOT NULL,
    license    text,
    url        text
);

-- Faktentabelle: der Grain IST der Primary Key (ein Index statt zwei).
CREATE TABLE raw.fact_measurement (
    time_id     integer  NOT NULL REFERENCES raw.dim_time(time_id),
    region_id   smallint NOT NULL REFERENCES raw.dim_region(region_id),
    metric_id   smallint NOT NULL REFERENCES raw.dim_metric(metric_id),
    source_id   smallint NOT NULL REFERENCES raw.dim_source(source_id),
    resolution  text     NOT NULL DEFAULT 'PT1H',   -- bewusst kein CHECK: PT15M später
    value       double precision NOT NULL,
    ingested_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (time_id, region_id, metric_id, source_id, resolution)
);

CREATE INDEX idx_fact_measurement_lookup
    ON raw.fact_measurement (metric_id, region_id, time_id);
```

`schema_migration` wird **nicht** von `0001_initial.sql` erzeugt, sondern vom Runner
bzw. von `init` gebootstrappt:

```sql
CREATE TABLE IF NOT EXISTS raw.schema_migration (
    migration_id     text PRIMARY KEY,
    contract_version text NOT NULL,
    applied_at       timestamptz NOT NULL DEFAULT now()
);
```

### Bewusste Abweichungen von §6.6 im zentralen Knowledge-File
1. `time_id` ist ein Surrogat (`integer` Identity) statt Smart-Key `YYYYMMDDHHMM`.
   Damit ist `bigint` nicht mehr nötig; der Grain-Index führt trotzdem mit `time_id`.
2. Kalenderattribute (`date`, `hour`, `month`, `year`, `day_of_week`,
   `is_weekend`) entfallen in `dim_time` — ableitbar aus `ts_utc`.
3. `fact_measurement` hat **keinen** Surrogat-PK `measurement_id`; der Grain ist der
   Primary Key (ein Index weniger, kein `nextval` pro Zeile).
4. `dim_metric` ist zeitabhängig (`UNIQUE (metric_key, valid_from)` + EXCLUDE)
   statt `metric_key UNIQUE`.
5. Keine `checksum`-Spalte in `schema_migration` (v1: Disziplin, Checksum später).
6. Region-/Metrik-Auflösung nutzt `(%(ts)s AT TIME ZONE 'UTC')::date` statt
   `%(ts)s::date` — `timestamptz::date` hängt sonst an der Session-`TimeZone`
   und ist nicht deterministisch.

## Migrationskonzept
- `sql/migrations/` enthält fortlaufend nummerierte Dateien:
  `0001_initial.sql`, `0002_*.sql`, ...
- Eine Tabelle `raw.schema_migration` protokolliert angewandte Migrationen
  (`migration_id`, `contract_version`, `applied_at`). `contract_version` trägt die
  `CONTRACT_VERSION` aus wattfrosch-contracts (aktuell `"0.1.0"`).
- Der Runner (`migrate`) wendet alle noch nicht angewandten Migrationen der Reihe
  nach an und trägt jede am Ende ein -> idempotent.
- **Angewandte Migrationen werden NIE editiert**; Korrekturen kommen als neue Datei.
  (Keine Checksumme in v1 — bewusste Disziplin-Regel; Checksumme später, auch als
  Schutz gegen Manipulation.)
- Migrationsregel: nur deterministische, offline-fähige Schritte.
  Neue externe Daten gehören NICHT hierher (-> Backfill in der pipeline).
- Vor dem Anwenden zeigt `migrate` an, welche Migrationen ausstehen.
- Abbruch, wenn die DB neuer ist als das Modul (fail fast).

### Nebenläufigkeit (Advisory Lock)
```python
MIGRATE_LOCK_KEY = 0x77617474   # "watt", feste dokumentierte Konstante

cur.execute("SET lock_timeout = '30s'")
try:
    cur.execute("SELECT pg_advisory_lock(%s)", (MIGRATE_LOCK_KEY,))
except psycopg.errors.LockNotAvailable:
    raise MigrationLockTimeoutError(
        "Migration läuft bereits in einem anderen Prozess (30s gewartet). "
        "Lauf abgebrochen, keine Änderung angewendet."
    )
finally:
    cur.execute("SET lock_timeout = 0")   # Migrationen brauchen kein Lock-Limit
```
- Session-Level-Lock (`pg_advisory_lock`), **nicht** `pg_advisory_xact_lock`:
  der Lock muss über alle Migrationsdateien hinweg halten. Dafür eine einzige
  Verbindung für den gesamten Lauf offen halten.
- **Erst Lock, dann Applied-Liste lesen** — das ist der eigentliche Schutz gegen
  das Rennen zweier gleichzeitiger Läufe.
- `lock_timeout` gilt in Postgres auch für Advisory Locks (SQLSTATE `55P03`).
- Freigabe im `finally` bzw. beim Verbindungsabbau, sonst blockiert ein
  abgebrochener Lauf alle folgenden.

## Dimensionsabgleich
Läuft **automatisch als erster Schritt von `load`** (nicht als eigener Befehl).

Ablauf eines `load`-Laufs:
1. Contract-Version prüfen (`ContractVersionError` bei Major/Minor-Abweichung)
2. Dimensionsabgleich (insert-only) in **eigener, separat committeter Transaktion**
3. Divergenz-Warnungen ausgeben
4. Records batchweise upserten (`BATCH_SIZE`, Default 1000)
5. Zusammenfassung: gelesen / geschrieben / Batches

### Insert-only
```sql
INSERT INTO raw.dim_metric (metric_key, unit, category, ..., valid_from, valid_to)
VALUES (...)
ON CONFLICT (metric_key, valid_from) DO NOTHING;
```
Bestehende Zeilen werden **nie** überschrieben und **nie** geschlossen.
Eine Definitionsänderung ist eine handgeschriebene Migration
(`0002_metric_other_renewable_v2.sql`: alte Zeile `valid_to` setzen, neue Zeile
einfügen). Der Loader versioniert nie selbst — er kann Tippfehler nicht von
Definitionsänderungen unterscheiden.

### Divergenz Katalog (Python) vs. DB
| Fall | Verhalten |
|---|---|
| Zeile existiert, Inhalt weicht ab (z. B. `display_name`, `plausible_max`) | **eine konsolidierte Warnung** auf stderr; `load` läuft weiter (Exit 0) |
| Zeile fehlt ganz (Katalog hat `valid_from`, DB nicht) | **Abbruch** — Auflösung `(key, ts) -> id` liefe ins Leere |
| `verify` | beide Fälle = **Fehler**, Exit 2 |

Die Vergleichslogik ist **geteilt**: eine read-only Funktion
`collect_divergences(conn) -> list[Divergence]`, die sowohl der Sync (nur warnend)
als auch `verify` (hart) aufruft. Zwei getrennte Vergleiche würden auseinanderlaufen.

Warum das gefahrlos ist: semantische Änderungen (Einheit, Bedeutung, Entfernen)
sind laut §6.9 ein **MAJOR**-Bump und werden vom `ContractVersionError`-Guard
abgefangen. Nur **PATCH**-Änderungen (Beschreibungstexte) kommen durch den Guard
— genau dieser schmale Rest ist der Warn-Pfad. Die beiden Checks decken disjunkte
Fälle ab.

### Namens-Mapping Katalog -> DB
| Katalog (Python) | DB-Spalte |
|---|---|
| `code` | `region_code` |
| `source_name` | `source_key` |
| `derived` | `is_derived` |
| `plausible_range` | `plausible_min` / `plausible_max` |

Katalog-Felder sind Python-Namen, DB-Spalten snake_case.

## Loader

### Auflösung
- **Zeit:** `INSERT INTO raw.dim_time (ts_utc) VALUES (%s)
  ON CONFLICT (ts_utc) DO UPDATE SET ts_utc = EXCLUDED.ts_utc RETURNING time_id`
  (`DO UPDATE` ist nötig, weil `DO NOTHING` bei existierender Zeile kein
  `RETURNING` liefert.)
- **Region / Metrik:** Auflösungsabfrage über `region_code` / `metric_key` +
  Zeitstempel; pro Lauf **einmal** auflösen und im Dict cachen (nicht pro Record).
```sql
SELECT region_id
FROM raw.dim_region
WHERE region_code = %(region_code)s
  AND (%(ts)s AT TIME ZONE 'UTC')::date >= valid_from
  AND ((%(ts)s AT TIME ZONE 'UTC')::date < valid_to OR valid_to IS NULL);
```
- **Quelle:** einfacher Lookup auf `source_key`.

### Upsert
```sql
INSERT INTO raw.fact_measurement
    (time_id, region_id, metric_id, source_id, resolution, value)
VALUES (%(time_id)s, %(region_id)s, %(metric_id)s,
        %(source_id)s, %(resolution)s, %(value)s)
ON CONFLICT (time_id, region_id, metric_id, source_id, resolution)
DO UPDATE SET value = EXCLUDED.value, ingested_at = now();
```

### Transaktionen und Fehler
- **Ein Commit pro Batch** (Default 1000). Begründung: kürzere Locks, weniger
  WAL-Spitze; der Upsert ist idempotent, ein erneuter Lauf ist unschädlich.
- **Fail fast** pro Batch: Fehler -> Rollback dieses Batches, Abbruch des Laufs,
  `status="failed"` im `LoadResult`.
```python
raise StoreLoadError(
    f"Batch {i}/{n} fehlgeschlagen bei Record ts_utc={rec.ts_utc}, "
    f"region={rec.region}, metric={rec.metric}: {exc}. "
    f"{committed_batches} Batches ({committed_rows} Zeilen) sind bereits "
    f"committet und bleiben gültig. Erneuter Lauf setzt idempotent fort."
)
```
- Der Zustand nach Abbruch ist **unvollständig, aber konsistent**: jede
  committete Batch ist für sich vollständig und gültig.
- **Leeres Ergebnis ist kein Fehler** (Exit 0 + Warnung) — sonst würden Backfills
  von Zeiträumen abgebrochen, für die eine Quelle nichts hat.

### Datei-Import (Dev/Test)
`load --from-file <pfad>` — dieselbe Pipeline: Datei -> Reader ->
`MeasurementRecord`-Validierung -> `load_records()`. Kein zweiter Schreibweg.
- `.parquet` via `pyarrow.parquet.ParquetFile.iter_batches()`
- `.csv` via `pyarrow.csv.open_csv()`
- `.ndjson` / `.json` zeilenweise
Alle streaming-fähig.
- **Strenge Validierung:** naive Zeitstempel (ohne `Z`/Offset) werden
  **abgelehnt**; unparsebare Werte -> Abbruch mit Zeilennummer.
  ```
  Fehler Zeile 42: ts_utc ohne Zeitzone ("2026-10-10 13:00:00").
  Erwartet: ISO 8601 mit Offset, z. B. "2026-10-10T13:00:00Z".
  ```
- Eine leere Datei (nur Schema) ergibt 0 Records = Erfolg + Warnung.

## `init` — Ablauf
1. DSN parsen (`psycopg.conninfo.conninfo_to_dict(DATABASE_URL)`)
2. Verbindung zur Wartungs-DB (`dbname=postgres`, gleicher Host/Port/User) mit
   `autocommit=True` (`CREATE DATABASE` läuft nicht in einer Transaktion)
3. `SELECT 1 FROM pg_database WHERE datname = %s`; wenn vorhanden: überspringen
4. `CREATE DATABASE <name>` über `psycopg.sql.Identifier` (keine String-Formatierung)
   — `duplicate_database` (SQLSTATE `42P04`) bei parallelen Läufen abfangen = Erfolg
5. Verbindung zur Zieldatenbank, dann `CREATE SCHEMA IF NOT EXISTS raw` +
   `CREATE TABLE IF NOT EXISTS raw.schema_migration`
6. Statusausgabe: erreichbar, Schema vorhanden, angewandte Migrationen,
   ausstehende Migrationen (Hinweis „führe `migrate` aus“), Exit 0

`init` macht **kein** DDL am Sternschema. Fehlende `CREATEDB`-Rechte
(SQLSTATE `42501`) -> Hinweis auf `docker compose up -d db`.
Im Compose-Betrieb ist `init` ein No-op (die DB existiert durch `POSTGRES_DB`).

## `verify` — Umfang
Standard (read-only, Exit != 0 bei Fehlschlag):
1. `schema_migration` vorhanden, `contract_version` kompatibel (Major.Minor)
2. Dimensionen: fehlender Katalog-Eintrag = Fehler; abweichender Inhalt = Fehler
   (streng, anders als `load`)
3. Zeilenzahlen: `fact_measurement` > 0, `dim_time` > 0, plus Anzahl je Dimension

`verify` ruft **keinen** Sync auf — sonst würde es die DB verändern und wäre als
CI-Check wertlos.

Optional `verify --check-values`: prüft `value` gegen `plausible_min`/`plausible_max`
aus `dim_metric` (Full Scan, daher opt-in; meldet Ausreißer je `metric_key`).
Ausreißer sind nicht automatisch Fehler (`can_be_negative`-Metriken, weite Grenzen).

## Docker (v1)
Nur die Datenbank im Container; store läuft lokal.

```yaml
services:
  db:
    image: postgres:16
    environment:
      POSTGRES_USER: wattfrosch
      POSTGRES_PASSWORD: wattfrosch
      POSTGRES_DB: wattfrosch
    ports: ["5432:5432"]
    volumes: [pgdata:/var/lib/postgresql/data]
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U wattfrosch -d wattfrosch"]
      interval: 2s
      timeout: 3s
      retries: 15
volumes:
  pgdata:
```
- **Kein `docker-entrypoint-initdb.d`**: das Schema entsteht ausschließlich über
  Migrationen (sonst zwei Wahrheiten für dasselbe Schema).
- **`healthcheck` statt `sleep`**: `docker compose up -d --wait` wartet, bis die DB
  wirklich annahmefähig ist.
- Das Passwort ist nur lokal und kein Geheimnis; sobald es einen echten Server gibt,
  kommt die DSN aus einer nicht eingecheckten `.env`.
- Später (Modul 7, platform): store als eigener Service. Dann wird nur
  `DATABASE_URL` von `localhost:5432` auf `db:5432` geändert — der Service-Name ist
  im Compose-Netz der Hostname.

## Struktur
```
sql/
  migrations/            # DDL, nummeriert (0001_initial.sql, ...)
  load/                  # Loader-Queries (Upsert, ggf. Bulk)
src/wattfrosch_store/
  __init__.py            # load_records, LoadResult, StoreError
  db.py                  # Verbindung (psycopg 3), DSN-Handling, Fehlerübersetzung
  config.py              # DATABASE_URL, LOG_LEVEL, BATCH_SIZE, MIGRATE_LOCK_TIMEOUT
  migrate.py             # Migrationsrunner (Advisory Lock, Transaktion pro Datei)
  sync_dims.py           # Dimensionsabgleich + collect_divergences
  loader.py              # Zeitdimension, Auflösung, Upsert, Batching
  io.py                  # Datei-Reader (parquet | csv | ndjson)
  cli.py                 # init | migrate | load | verify
tests/
  fixtures/              # kleine Testdateien (alle Testfälle abdeckend)
  test_migrate.py        # T1, T2
  test_loader.py         # T3, T6
  test_verify.py         # T4
  test_scd2.py           # T5
  test_lock.py           # T7
docker-compose.yml
Dockerfile               # erst ab Modul 7 genutzt
```

Verbindungsschicht: psycopg 3, `row_factory=dict_row`, `autocommit=False`,
**kein Connection-Pool** (ein CLI-Lauf = eine Verbindung; Pools erst für
`forecast`), kurzer Retry beim Verbindungsaufbau (~5x 2s).

## Quickstart
```sh
docker compose up -d db
uv sync
uv run wattfrosch-store init
uv run wattfrosch-store migrate
uv run wattfrosch-store verify

# Testfixtures laden (Entwicklungs-/Testpfad)
uv run wattfrosch-store load --from-file tests/fixtures/scd2_grenze.parquet
```

## Tests
Kern:
- **T1** `migrate` auf leerer DB -> `0001` angewendet, `schema_migration` hat
  1 Zeile mit `contract_version = 0.1.0`.
- **T2** `migrate` zweimal -> keine zweite Zeile, kein Fehler.
- **T3** `load` zweimal mit denselben Records -> gleiche Zeilenzahl in
  `fact_measurement`, `value`/`ingested_at` aktualisiert.
- **T4** `load` mit Katalog-Divergenz -> Warnung, Exit 0; `verify` -> Exit 2.

Randfälle:
- **T5** SCD-2-Grenze: `DE-LU` mit `ts_utc` 2018-06-01 und 2018-12-01 -> zwei
  verschiedene `region_id`; zusätzlich die Grenzpunkte selbst: 2018-09-30 ->
  DE-AT-LU, 2018-10-01 -> DE-LU (prüft die `[)`-HalbOffenheit des `daterange`).
  Zusätzlich: `ts_utc = 2018-09-30T23:30Z` -> DE-AT-LU, **unabhängig von der
  Session-`TimeZone`** (sichert die `AT TIME ZONE 'UTC'`-Korrektur ab).
- **T6** Region nicht auflösbar (`ts_utc` vor `valid_from`) -> `status="failed"`,
  CLI-Exit 2, keine Faktzeile, Meldung nennt Region + Zeitstempel.
- **T7** Advisory-Lock-Timeout: zweite Verbindung hält den Lock, `migrate` läuft
  -> Abbruch, `MigrationLockTimeoutError`, keine Änderung.
  `MIGRATE_LOCK_TIMEOUT` im Test auf `1s` setzen (Suite-Laufzeit).

Weitere sinnvolle Tests: Lauf mit absichtlich fehlerhaftem Batch in der Mitte ->
erneuter Lauf -> Endzustand identisch zum fehlerfreien Lauf; leere Datei -> Exit 0 +
Warnung; naiver Zeitstempel in CSV -> Abbruch mit Zeilennummer; `init` gegen
fehlende DB (DSN mit nicht existierendem `dbname`).

## EXPLAIN ANALYZE: Braucht es einen Index auf `time_id`?

Hypothese: nein — der Grain-PK `(time_id, region_id, metric_id, source_id,
resolution)` führt mit `time_id` und bedient zeitreihengetriebene Abfragen
direkt. Messung gegen die Compose-DB (`postgres:16`) mit 368 256 Faktzeilen
(5 Regionen, 10 Metriken, stündlich, 3 Jahre; `ANALYZE` danach ausgeführt).
Repräsentative Abfrage — ein Tag, eine Metrik, eine Region:

```sql
EXPLAIN (ANALYZE, COSTS OFF)
SELECT m.metric_key, r.region_code, t.ts_utc, f.value
FROM raw.fact_measurement f
JOIN raw.dim_time t USING (time_id)
JOIN raw.dim_region r USING (region_id)
JOIN raw.dim_metric m USING (metric_id)
WHERE t.ts_utc >= '2024-06-01T00:00:00Z' AND t.ts_utc < '2024-06-02T00:00:00Z'
  AND m.metric_key = 'generation.solar' AND r.region_code = 'DE-LU';
```

Plan (gekürzt):

```
Nested Loop (actual time=0.074..0.120 rows=24 loops=1)
  -> Nested Loop
     -> Bitmap Heap Scan on dim_metric m
        Index Cond: (metric_key = 'generation.solar'::text)
     -> Nested Loop
        -> Bitmap Heap Scan on dim_time t
           Index Cond: (ts_utc >= '...' AND ts_utc < '...')
        -> Index Scan using fact_measurement_pkey on fact_measurement f
           Index Cond: ((time_id = t.time_id) AND (metric_id = m.metric_id))
```

**Ergebnis:** Der Planner startet bei `dim_time` (Unique-Index auf `ts_utc`)
und `dim_metric` und greift die Faktzeilen über den PK-Vorspann `(time_id, …)`
ab — 24 Zeilen in ~0,5 ms. Ein zusätzlicher Index auf `time_id` ist **nicht
nötig**; die Hypothese bestätigt sich.

Randnotiz: Bei metrikfirst getriebenen Abfragen ohne Zeitfilter („eine Metrik,
eine Region, komplette Historie") wählt der Planner bei ~370 000 Zeilen noch
einen (parallelen) Seq Scan — `idx_fact_measurement_lookup`
(`metric_id, region_id, time_id`) lohnt sich erst bei größeren Datenmengen.
Das Index-Design für Dashboard-Abfragen bleibt deshalb ein offener Punkt.

## Fertig, wenn
- `docker compose up -d db` startet Postgres
- `init`, `migrate`, `load` sind idempotent
- Loader nutzt echtes SQL (`INSERT ... ON CONFLICT`)
- Dimensionsabgleich funktioniert (inkl. SCD-2 für `dim_region` und `dim_metric`)
- Integrationstests T1–T7 grün
- `EXPLAIN ANALYZE`-Beispiel dokumentiert (Frage: braucht es einen Index auf
  `time_id`? Hypothese: nein, weil der PK mit `time_id` führt)

## Offene Punkte
- Bulk-Load via COPY statt `executemany` — erst messen (Schritt 8), dann entscheiden.
- Index-Design für Dashboard-Abfragen: `idx_fact_measurement_lookup` deckt
  „alle Metriken, eine Region, Zeitraum" nicht ab; Auswahl erst mit bekannten
  Abfragen (Modul 6).
- `checksum`-Spalte in `schema_migration` (Integrität, Schutz gegen Manipulation).
- `dim_date` (`is_weekend`, `is_holiday`) — regions- und zeitzonenabhängig, später.
- DST-Klassifikation als echte Spalte in `dim_time` (Stunde 02:00–03:00 existiert
  im Oktober doppelt, im März nicht — nicht aus `ts_utc` ableitbar).
- `tz` in `dim_region` wird vom Loader bisher nicht genutzt; Lokalzeit entsteht
  über `ts_utc AT TIME ZONE r.tz`.
