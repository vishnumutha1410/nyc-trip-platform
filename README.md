# NYC Trip Analytics Platform

An end-to-end data platform for a ride-hailing operator, built on real NYC TLC
trip records. Batch processing at scale, a dimensional warehouse model, and —
in later phases — a live event stream and change data capture from an
operational database.

```
┌─ PHASE 1 · batch ────────────────────────────────────────────────┐
│  NYC TLC Parquet                                                 │
│        │                                                         │
│        ▼                                                         │
│   S3 raw/            immutable, exactly as published             │
│        │                                                         │
│        ▼  PySpark: conform schema · quality rules · partition    │
│   S3 curated/  ────────────────►  S3 quarantine/ (+ reason)      │
│        │                                                         │
│        ▼  COPY INTO                                              │
│   Snowflake RAW ──► dbt STAGING ──► dbt MARTS (star schema)      │
└──────────────────────────────────────────────────────────────────┘

PHASE 2  trip event generator → Kafka → consumer → incremental marts
PHASE 3  Postgres ops DB → Debezium → Kafka → SCD Type 2 driver dimension
PHASE 4  Airflow orchestration · CI/CD · reconciliation · monitoring
```

**Status:** phase 1 complete.

---

## What phase 1 does

Downloads monthly NYC yellow-taxi trip records and lands them in S3 untouched.
A PySpark job then conforms the schema across years — the TLC has added and
renamed columns over time — applies eight data quality rules, quarantines
failing rows *with the reason attached* rather than dropping them, and writes
partitioned Parquet to a curated prefix. Snowflake loads that via a storage
integration, and dbt builds a star schema with tests on top.

## Quickstart

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env          # fill in AWS + Snowflake

make download                 # TLC -> S3 raw
make curate                   # PySpark -> S3 curated + quarantine
make load                     # COPY INTO Snowflake
make dbt                      # build + test the star schema
make test                     # local test suite, no warehouse needed
```

Full setup, including the Snowflake↔S3 IAM trust relationship, is in
[docs/02-setup.md](docs/02-setup.md).

## Every tool, and why it and not something else

[docs/01-tools-explained.md](docs/01-tools-explained.md) covers each technology:
what it does, why it was chosen over the obvious alternative, how it's used
here, and the trade-off behind the choice. Written to be read, not skimmed.

## The data model

Star schema at the grain of **one completed trip**.

```
                dim_date
                    │
dim_zone (pickup) ──┼── fct_trips ──┬── dim_vendor
dim_zone (dropoff) ─┘               ├── dim_rate_code
                                    └── dim_payment_type
```

`fct_trips` carries foreign keys and additive measures — fare, tips, tolls,
distance, duration. Descriptive attributes live on the dimensions. Three
derived ratio columns (`fare_per_mile`, `avg_speed_mph`, `tip_rate`) are
explicitly non-additive and named so nobody sums them.

## Data quality

Ten rules run during curation, defined once in `spark/jobs/rules.py` as SQL
predicates. Spark applies them; the test suite runs the identical strings
against DuckDB. One definition, two engines, no drift between the rule that is
documented and the rule that ships.

| Rule | Catches |
|---|---|
| `fare_not_negative` | Negative fares, which are genuinely present in this dataset |
| `total_not_negative` | Negative totals |
| `distance_positive` | Zero-distance trips |
| `distance_plausible` | Over 300 miles — meter faults |
| `pickup_before_dropoff` | Trips that end before they start |
| `duration_plausible` | Over 12 hours |
| `passenger_count_sane` | Impossible occupancy |
| `zones_present` | Missing pickup or dropoff zone |
| `pickup_after_2009` | Impossible dates - caught 2002-dated rows in a 2024 file |
| `pickup_not_in_future` | Pickup timestamps in the future |

Failing rows go to `s3://<bucket>/quarantine/trips/` with a `_quality_failure`
column naming the first rule they broke. A drop in curated row count is always
explainable.

On top of that, dbt tests run on every build: uniqueness and not-null on keys,
`relationships` tests on every foreign key, and an `equal_rowcount` check
between the fact table and staging. Those last two are what catch **silent
join loss** — a fact table quietly dropping rows because a key didn't match,
which column-level tests cannot see because every surviving row is perfect.

## Testing without a warehouse

```bash
make test        # 15 tests, ~2 seconds, no cloud credentials
```

`tests/dbt_render.py` resolves the Jinja this project uses (`ref`, `source`,
`generate_surrogate_key`) so the actual dbt model SQL executes in DuckDB
against synthetic fixtures. That means the star schema logic — grain, fan-out,
foreign key integrity, divide-by-zero handling, surrogate key determinism — is
verified locally in seconds rather than by a round trip to Snowflake.

## Repo map

```
ingest/download_tlc.py        fetch TLC months, land in S3 raw
spark/jobs/rules.py           quality rules, shared by Spark and tests
spark/jobs/curate_trips.py    schema conformance, quarantine, partitioning
snowflake/01_setup.sql        role, warehouse, database, schemas
snowflake/02_storage_*.sql    S3 storage integration (IAM trust, no keys)
snowflake/03_load_curated.sql COPY INTO
warehouse/load_to_snowflake.py  runs the COPY from Python
dbt/models/staging/           renaming and typing only
dbt/models/marts/             the star schema
tests/                        local verification, DuckDB
docs/                         tool explanations and setup
```

## Engineering decisions

**Quarantine rather than drop.** Losing rows silently makes a row-count drop
unexplainable. Every rejected row keeps its reason.

**Quality rules as shared SQL strings.** Two implementations of the same rule
drift apart. One definition, applied by Spark and asserted by tests.

**Thin staging.** Renaming and typing only, no business logic, so the source
can be re-pointed without rewriting marts.

**XSMALL warehouse, 60-second auto-suspend.** This volume does not need more,
and idle compute is the most common source of surprise warehouse bills.

**Storage integration instead of AWS keys in Snowflake.** Snowflake assumes an
IAM role in the AWS account; no credentials are stored in the warehouse.

**Spark despite the volume.** At 40M rows a single-node engine like DuckDB
would be faster. Spark is here because the job is written to scale past one
machine unchanged. See [docs/01-tools-explained.md](docs/01-tools-explained.md)
for the full argument, including when Spark is the wrong choice.

## Known limitations

- Full-refresh load. Incremental models arrive with the streaming path in phase 2.
- No orchestration yet — `make` targets run in sequence. Airflow is phase 4.
- Zone dimension is a static seed; it has no history tracking. SCD Type 2 arrives
  in phase 3 on the driver dimension, where attributes actually change.

## License

MIT

## Measured results

Full year of 2024 yellow taxi data, run on a laptop (WSL2, Spark local mode,
3GB driver, XSMALL Snowflake warehouse):

| | |
|---|---|
| Source files | 12 monthly TLC Parquet files |
| Rows read | 41,169,720 |
| Rows curated | 39,692,972 |
| Rows quarantined | 1,476,748 (3.59%) |
| Rows in `fct_trips` | 39,692,969 |
| Curated output | 13 Parquet files, 929 MB |
| Curation runtime | ~28 min |
| `fct_trips` build | 56 s |
| dbt models | 7 |
| dbt tests | 28, all passing |

### Why rows were quarantined

| Rule | Rows |
|---|---|
| `fare_not_negative` | 731,023 |
| `distance_positive` | 720,062 |
| `duration_plausible` | 19,365 |
| `total_not_negative` | 2,861 |
| `pickup_before_dropoff` | 2,364 |
| `distance_plausible` | 1,016 |
| `pickup_outside_source_month` | 49 |
| `total_plausible` | 8 |

Zero-distance trips are the single largest cause: roughly 2% of a year of New
York taxi rides went nowhere.

Rows are attributed to the **first** rule they violate, not every rule, so these
counts shift if the rule order changes.

Spark and DuckDB produce identical counts across every row-local rule, which is
the point of defining them once as shared SQL strings.

## Defects found and fixed

Seven, and **not one of them threw an error**. Every single one was caught by
comparing something against something else.

**Trips dated 2002 in a 2024 file.** Inspecting the raw Parquet showed rows
outside the source month, two dated 2002-12-31.

**A cross-engine SQL dialect break.** `datediff()` takes a quoted unit in DuckDB
and an unquoted one in Spark, so a shared rule string passed the tests and
failed in the Spark job. Replaced with an `INTERVAL` comparison, valid in both.
The real constraint on sharing rules across engines is that they must stay
inside the SQL subset both understand, and the test suite is what enforces it.

**Timestamps misread by exactly 1,000,000x.** `COPY INTO` with
`MATCH_BY_COLUMN_NAME` loaded every row successfully and reported no errors,
but read Spark's microsecond timestamps as seconds, producing years like
39006190. The row count was exactly right the whole time. Caught by recomputing
trip duration from the stored timestamps and comparing it against the value
Spark wrote before the file was even written. That comparison is now part of
the load.

**One file written per input task.** A 3-row partition became 3 files of ~6KB,
almost entirely Parquet metadata. Repartitioning on the partition columns before
write fixed it.

**Spark spilling into a RAM disk.** The 12-month shuffle died with "No space
left on device" while 951GB of disk sat unused - `spark.local.dir` defaults to
`/tmp`, which on WSL is a 1.9GB tmpfs.

**A $335,550.94 taxi fare.** `total_not_negative` only bounded the lower end.
The dbt `accepted_range` test on staging caught it and correctly blocked
`fct_trips` from building on top. The ceiling of $5,000 derives from the
existing 300-mile distance cap, not from taste.

**Duplicate records with no natural key.** TLC publishes byte-identical trip
records - three pairs in 39.7M rows. Nothing in the file uniquely identifies a
trip, so a surrogate key built from attributes cannot distinguish a duplicated
record from a duplicated key. Deduplicated in staging, which is the earliest
point the duplicate is visible.

## Source-month alignment: a check that cannot be row-local

Ten of the eleven rules examine one row and ask whether its values are sane. A
trip dated 2026-06 passes all of them - it is a perfectly plausible trip. It is
only wrong because it arrived in a file of 2024-01 data.

Spark knows the source month from the raw path's Hive partition
(`raw/yellow/year=2024/month=01/`), so the job carries it through and compares
each pickup date against that month with a 3-day tolerance for genuine boundary
spill - a trip starting at 23:58 on the last day of a month belongs in the next
month's partition and appears in TLC files routinely.

Caught 49 rows across 41.2M, and eliminated five S3 partitions that existed
solely to hold a handful of misfiled rows. Output partitions: 18 -> 13.
