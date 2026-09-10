# NYC Trip Platform

An end-to-end data platform built on real NYC TLC taxi data and a simulated
operational database. Batch and streaming, both orchestrated, both tested
against the source.

The point of this repo is not that the pipelines run. It is that when they are
wrong, something says so.

```
BATCH                                   STREAMING
─────                                   ─────────
TLC parquet (41M rows)                  Postgres (orders app)
      │                                       │  write-ahead log
      ▼                                       ▼
   S3 raw/                               Debezium
      │  PySpark: conform, quality        (Kafka Connect)
      ▼  rules, quarantine                     │
   S3 curated/  ──┐                            ▼
                  │                       Redpanda topics
                  ▼                            │  consumer: retry, DLQ,
              Snowflake  ◀────────────────────┘   offsets after write
                  │
                  ▼
                 dbt  ──▶  star schema + SCD Type 2
                  │
                  ▼
            reconciliation vs source

           Airflow schedules both
```

---

## What it actually did

**Batch, 12 months of 2024 yellow taxi data**

| | |
|---|---|
| rows read | 41,169,720 |
| rows curated | 39,692,972 |
| rows quarantined | 1,476,748 (3.59%) |
| S3 partitions written | 13, 929 MB |
| `fct_trips` build on XSMALL | 56s |

Quarantined rows are never dropped. Each one is written to a separate dataset
with the name of the rule it failed:

```
fare_not_negative            731,023
distance_positive            720,062
duration_plausible            19,365
total_not_negative             2,861
pickup_before_dropoff          2,364
distance_plausible             1,016
pickup_outside_source_month       49
total_plausible                    8
```

**Streaming, change data capture**

| | |
|---|---|
| chaos test | 3 hard SIGKILLs mid-stream |
| events checked | 3,319 |
| missing Kafka offsets | 0 |
| duplicate events landed | 0 |
| warehouse vs Postgres | converged |

---

## The defects this caught

Every one of these reported success while doing something wrong. None of them
threw an error. Each was found by computing the same number two different ways
and comparing.

| Defect | How it presented | How it was found |
|---|---|---|
| Timestamps loaded 1,000,000× off | dates rendered as "Invalid date" | recomputed trip duration in Snowflake vs the duration Spark had stored |
| Silent join loss in the star schema | fact table simply had fewer rows | `relationships` and `equal_rowcount` tests |
| 3 byte-identical duplicate trips | `unique` test on the fact key | distinct-count-per-column diff |
| A $335,550 fare | passed every rule | an accepted-range test on the dbt side that the Spark rules did not have |
| `_SUCCESS` file broke `COPY INTO` | "Parquet file size is 0 bytes" | read the error instead of setting `ON_ERROR = SKIP_FILE`, which would have hidden real corruption too |
| Trips dated years outside their source file | plausible in isolation | a check that compares each row against the month of the file it arrived in — structurally impossible as a row-local rule |
| Consumer drain exited having done nothing | printed `written: 0` and exited 0 | the chaos test's per-offset assertion |

That last one is the pattern in one line. A drain that quits after three quiet
polls looks identical to a successful drain with nothing to do.

---

## Design decisions worth defending

**Quarantine with a reason, never filter.** Dropping bad rows silently is how
you end up unable to explain why a row count fell. The failing rule's name is
stored on the row.

**One rule definition, two engines.** `spark/jobs/rules.py` holds quality rules
as SQL predicate strings. Spark applies them; the DuckDB test suite asserts the
identical strings against synthetic rows. The documented rule is provably the
shipped rule.

**`REPLICA IDENTITY FULL` on the CDC source.** At the Postgres default, an
UPDATE writes only the primary key of the old row to the WAL, so the `before`
image is nulls and SCD Type 2 is unbuildable. It costs WAL volume. That is the
trade.

**Offsets committed after the warehouse write, not before.** Auto-commit is on
by default in every Kafka client, which makes the default at-most-once: a crash
between commit and write loses the batch silently. Committing after the write
is at-least-once, so duplicates are guaranteed — which is why the sink merges
on `topic:partition:offset`, Kafka's own unique address for a message.

**Bad message and bad sink get different responses.** Unparseable JSON goes to
a dead-letter topic, because retrying it will fail identically forever and
block the partition. An unreachable warehouse gets bounded retry and then stops
the consumer, because the data is fine. Conflating the two fills your DLQ with
valid records every time Snowflake blips.

**LSN, never timestamp, for CDC ordering.** Two commits in the same millisecond
share a `ts_ms`, and wall clocks are not monotonic. The log sequence number is
the database's own total order.

**Reconciliation refuses to run while consumer lag is non-zero.** A check that
goes red for reasons unrelated to correctness gets ignored by the third time
someone sees it.

**Retry policy differs per Airflow task.** Downloads retry with exponential
backoff. The Spark job retries zero times, because it fails on memory or disk
and an automatic retry burns thirty minutes to fail the same way.

---

## Tests

| Suite | Count | Needs |
|---|---|---|
| dbt models and tests, batch | 36 | Snowflake |
| dbt models and tests, CDC | 27 | Snowflake |
| dbt SQL executed in DuckDB | 15 | nothing |
| CDC parser unit tests | 11 | nothing |
| Airflow DAG integrity | 7 | Airflow venv |
| chaos test | 3 assertions | full local stack |

The DuckDB suite runs real dbt model SQL — `ref()`, `source()` and
`generate_surrogate_key` resolved by a small Jinja renderer — in about two
seconds with no warehouse and no credentials.

---

## Layout

```
ingest/            TLC download
spark/jobs/        curation job + shared quality rules
warehouse/         Snowflake load
snowflake/         setup, storage integration, COPY
dbt/               staging, marts, CDC models, singular tests
cdc/               compose stack, Debezium config, consumer,
                   load generator, reconciliation, chaos test
airflow/           DAGs and DAG integrity tests
tests/             parser tests and the local dbt runner
docs/              phase write-ups
```

## Running it

```bash
make all                              # download -> curate -> load -> dbt
make -f Makefile.cdc cdc-up           # start the CDC stack
make -f Makefile.cdc cdc-register     # register the Debezium connector
make -f Makefile.cdc cdc-chaos        # kill the consumer, assert correctness
make -f Makefile.airflow af-up        # Airflow UI on :8080
```

Detail and reasoning for each phase is in `docs/`.

---

## Known limitations

- Airflow runs on SQLite and therefore the SequentialExecutor. The move to
  LocalExecutor against Postgres is documented in `docs/phase3-airflow.md` but
  not the default here.
- The CDC source is a load generator, not a real application. It changes
  customer tiers far more often than a real business would, so the "72% of
  orders would be misattributed without SCD Type 2" figure from this dataset is
  a property of the simulation, not a real-world number. The real number is
  much smaller and never zero.
- The dead-letter path is unit tested but has not handled a malformed message
  from a live topic.
- Snowflake holds a point-in-time load of change events rather than a
  continuously running feed; `cdc_micro_batch` is what would keep it current.
