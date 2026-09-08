# Every tool in this platform, and why it and not something else

Phase 1 tools are covered in full. Kafka, Debezium and Airflow arrive in
phases 2–4 and get the same treatment then.

Each section answers four questions: what the tool does, why we chose it over
the obvious alternative, how we use it *here*, and what to say when an
interviewer asks. That last part is not padding — "why did you use X?" is the
most common follow-up to any project you put on a resume, and a candidate who
can name the trade-off sounds different from one who names the tool.

---

## 1. Parquet — the file format

**What it does.** Stores tabular data column by column instead of row by row,
with the schema and per-column statistics written into the file itself.

**Why not CSV or JSON.** Three reasons that compound at this scale:

- *Read less.* If your query touches 3 of 20 columns, a columnar reader loads
  3 columns off disk. A CSV reader parses all 20, every time.
- *Compress harder.* A column holds one kind of value, so similar bytes sit
  next to each other and compress far better than a row of mixed types.
- *Skip work entirely.* Each Parquet chunk stores min/max per column, so an
  engine can skip whole blocks that cannot match your filter without reading
  them. That is predicate pushdown, and CSV has no equivalent.

There is also a correctness argument: CSV has no types. `01` is a string until
something guesses otherwise, and that guess is where silent data corruption
comes from. Parquet stores the type.

**How we use it.** TLC publishes Parquet directly, and our Spark job writes
Parquet out to the curated layer. Same format end to end.

**If an interviewer asks:** "Columnar, so we only read the columns a query
needs, we get much better compression, and the row-group statistics let the
engine skip data instead of scanning it. On the taxi dataset that's the
difference between a scan and a seek."

---

## 2. Amazon S3 — the data lake

**What it does.** Object storage. You put files in, you get files out, it is
effectively infinite, and it costs about $0.023 per GB per month.

**Why not just load everything straight into Snowflake.** Because storage and
compute should be separable, and because raw data should be immutable.

Keeping an untouched raw layer means that when you discover a bug in your
transformation six months from now — and you will — you re-run the
transformation against raw rather than re-downloading from a source that may
have changed or disappeared. The raw layer is your ability to rebuild
everything from scratch. Loading straight to the warehouse throws that away
and costs more, because warehouse storage is pricier than object storage.

**Why not HDFS.** HDFS ties storage to a cluster you have to run. S3 doesn't.
That decoupling is the whole reason the modern lakehouse pattern exists.

**How we use it.** Three prefixes, each with a job:

```
s3://your-bucket/
  raw/yellow/year=2024/month=01/    exactly what TLC published, untouched
  curated/trips/pickup_year=/...     cleaned, conformed, partitioned
  quarantine/trips/                  rows that failed quality rules, with reasons
```

The `year=/month=` naming is Hive-style partitioning. Query engines read those
directory names as column values, so filtering on `pickup_year = 2024` skips
every other directory without opening a file.

**If an interviewer asks:** "Raw is immutable and replayable. Curated is what
downstream consumes. Quarantine keeps bad rows with their failure reason
instead of dropping them, so a drop in row count is always explainable."

---

## 3. PySpark — the processing engine

**What it does.** Splits a dataset across many workers, runs your
transformation on each piece in parallel, and combines the results. You write
what looks like a DataFrame API; Spark builds a query plan and executes it
across cores or machines.

**Why not Pandas.** Pandas loads everything into one machine's memory. Twelve
months of taxi data is roughly 40 million rows, which will not fit comfortably
in 8GB alongside everything else your laptop is doing. Spark streams through
it in partitions and never needs the whole dataset resident.

**Why not DuckDB or Polars — and this is the honest answer.** For 40 million
rows on one machine, DuckDB would genuinely be faster than Spark, and simpler.
Spark carries real overhead: JVM startup, shuffle machinery, a planner built
for a cluster you don't have.

We are using Spark for two defensible reasons. The pattern is
scale-out — the same job runs unchanged on EMR or Glue against 5 billion rows,
where DuckDB stops working. And PySpark is on the majority of data engineering
job descriptions, so demonstrating it has direct career value.

Say that out loud in an interview. "I used Spark because the job needs to scale
past one machine, though I'd note DuckDB would beat it at this specific volume"
is a much stronger answer than pretending Spark is always right. Knowing when a
tool is overkill is a senior signal.

**How we use it.** `spark/jobs/curate_trips.py` does three things:

1. *Conforms the schema.* TLC column names have changed over the years —
   `airport_fee` appeared in 2022, `cbd_congestion_fee` in 2025, casing is
   inconsistent. We map every known spelling to one snake_case name and add
   missing optional columns as typed nulls, so five years of files become one
   coherent table.
2. *Splits valid from invalid.* Rows failing a quality rule go to quarantine
   with the reason attached, rather than being silently dropped.
3. *Partitions the output* by pickup year and month, targeting large files
   rather than thousands of small ones.

Two config choices worth understanding in `build_spark()`:

- `spark.sql.adaptive.enabled` lets Spark decide the number of shuffle
  partitions at runtime from actual data size, instead of always using 200.
  On a laptop-sized dataset, 200 partitions means 200 tiny files, which is
  the small-file problem in miniature.
- `spark.driver.memory` is capped at 3g so Spark cannot eat your 8GB machine.

**If an interviewer asks:** "It scales out and it's what production runs on.
At the volume I tested, a single-node engine would have been faster — I'd pick
Spark once the data outgrows one machine, which is the situation the job is
written for."

---

## 4. Snowflake — the warehouse

**What it does.** A cloud data warehouse where storage and compute are separate
services. Your data sits in cloud storage; you spin up a "virtual warehouse"
(a compute cluster) to query it, and turn it off when you're done.

**Why not Postgres.** Postgres is row-oriented and single-node — built for
transactions, for reading and writing individual rows fast. Analytics does the
opposite: scan hundreds of millions of rows, touch a few columns, aggregate.
Snowflake stores columnar and parallelises across nodes. Different shape of
problem, different engine.

**Why not BigQuery, Redshift, or Databricks.** All defensible. Snowflake here
for two reasons: separation of storage and compute is unusually clean (you can
resize compute without touching data, and two teams can query the same data
with separate warehouses and never contend), and it is already on your resume
so this project deepens an existing claim rather than adding a shallow new one.

**How we use it.** Three schemas that mirror the pipeline stages:

| Schema | Contents | Built by |
|---|---|---|
| `RAW` | Curated parquet loaded verbatim from S3 | `COPY INTO` |
| `STAGING` | Renamed, typed, one row per source row | dbt views |
| `MARTS` | The star schema | dbt tables |

Two settings in `snowflake/01_setup.sql` are deliberate. `WAREHOUSE_SIZE =
XSMALL` because this volume genuinely does not need more, and `AUTO_SUSPEND =
60` so the warehouse stops billing a minute after you stop querying. Being able
to talk about credit consumption is unusual at entry level and lands well.

The storage integration in `02_storage_integration.sql` is the part worth doing
carefully. Snowflake reads your S3 bucket by assuming an IAM role in your AWS
account — no AWS keys are ever stored in Snowflake. It's a genuine
cross-cloud-account trust relationship, and it's real cloud engineering rather
than "I used a bucket."

**If an interviewer asks:** "Columnar and MPP, so it suits scan-heavy analytics
that Postgres would struggle with, and separating storage from compute means I
can size compute to the workload and suspend it when idle. I connect it to S3
with a storage integration so no credentials live in the warehouse."

---

## 5. dbt — the transformation layer

**What it does.** You write `SELECT` statements. dbt handles the rest: working
out the dependency order, materialising each model as a view or table, running
tests, and generating documentation and lineage.

**Why not just SQL scripts in a folder.** Four things you get and would
otherwise hand-build:

- *A dependency graph.* Writing `{{ ref('stg_trips') }}` instead of a hardcoded
  table name tells dbt this model depends on that one. It computes the build
  order itself. No numbered filenames, no ordering bugs.
- *Tests as part of the build.* `not_null`, `unique`, `relationships` and your
  own SQL assertions run every time. A failing test stops the pipeline before
  bad data reaches anyone.
- *Environments for free.* The same code builds into your dev schema and into
  production, because the target is config rather than something baked into
  the SQL.
- *Documentation and lineage that can't rot*, because it's generated from the
  code that actually runs.

**Why not do the transformations in Spark.** You could. The reason not to is
that the transformations are set logic over data already in the warehouse, and
the warehouse is very good at set logic. Pulling data out to Spark and pushing
it back would add movement and latency for no gain. Rough rule: Spark for
getting data *into* the warehouse at scale, SQL for reshaping it once it's
there. Being able to state that boundary is worth a lot in interviews.

**How we use it.** Two layers with clearly different jobs.

*Staging* (`stg_trips`) does renaming, casting, and a surrogate key. No
business logic, one row in per row out. Thin staging is what lets you swap the
source without rewriting your marts.

*Marts* is the star schema — `fct_trips` plus its dimensions. This is where
business meaning lives.

Note the tests in `_marts.yml`. Alongside the usual `not_null` and `unique`
there's `dbt_utils.equal_rowcount` against staging, and `relationships` tests
on the foreign keys. Those two catch **silent join loss** — a fact table
quietly dropping rows because a key didn't match — which column-level tests
cannot see, because every surviving row is individually perfect.

**If an interviewer asks:** "dbt gives me a dependency graph from `ref`,
testing built into the build, and environments from config. I keep staging
thin — renaming and typing only — so business logic lives in one place."

---

## 6. Dimensional modelling — the star schema

**What it does.** Splits your data into *facts* (measurable events, one row per
thing that happened) and *dimensions* (descriptive context you filter and group
by). Facts hold foreign keys and numbers. Dimensions hold labels.

Here: `fct_trips` is one row per completed trip, holding `total_amount`,
`trip_distance`, `tip_amount` and foreign keys. `dim_zone`, `dim_date`,
`dim_vendor`, `dim_rate_code` and `dim_payment_type` hold the words.

**Why not one big flat table.** OBT is genuinely popular and genuinely fine for
some workloads, so know the trade-off rather than a rule:

- Flat means no joins, which is fast and simple.
- But zone names repeat across a hundred million rows instead of living in 265
  dimension rows, so it costs more storage.
- And when a zone gets renamed, you rewrite the fact table instead of one
  dimension row.
- And the table becomes unreadable — 60 columns with no structure telling you
  which are measures and which are attributes.

**Why not fully normalised (3NF).** That's what the *source* system should be,
because it optimises for writing rows safely. Analytics optimises for reading
and aggregating, and 3NF makes every analytical question a six-table join.

**The grain is the most important decision.** "One row per completed trip" is
stated at the top of `fct_trips.sql` on purpose. Every question about a fact
table — can I sum this, will this join fan out, why is my revenue doubled — is
answered by knowing the grain. Ambiguous grain is the single most common cause
of wrong numbers in a warehouse.

Note also the ratio columns at the bottom of `fct_trips`: `fare_per_mile`,
`avg_speed_mph`, `tip_rate`. Those are **non-additive**. Averaging them across
rows is valid; summing them is meaningless. They're named as ratios so nobody
is tempted.

**If an interviewer asks:** "Star schema at trip grain. Facts carry keys and
additive measures, dimensions carry descriptive attributes. I'd consider one
big table if the workload were narrow and read-only, but the star keeps
dimension changes cheap and makes the model self-describing."

---

## 7. Docker — packaging

**What it does.** Bundles an application with its dependencies and its
operating system libraries into an image that runs identically anywhere.

**Why not a virtualenv.** A venv isolates Python packages. It does not give you
Java 17 for Spark, or a Postgres server, or Kafka. Phase 3 of this project runs
five services that have to find each other on a network — that's a compose
file, not a requirements.txt.

**How we use it.** Phase 1 runs Spark locally without containers because your
machine has 8GB and containerising Spark for a single-node job spends memory
for no benefit. Docker arrives properly in phase 2 with Kafka. That is itself a
decision worth defending: containerise when it earns its keep, not reflexively.

---

## 8. Python

**Why it's the default for data engineering.** Not because it's fast — it
isn't. Because the ecosystem is where the tools live (Spark, Airflow, dbt and
every cloud SDK all have first-class Python), and because most pipeline code is
orchestration and glue where the heavy lifting happens inside Spark or the
warehouse anyway. Your Python tells fast things what to do.

**Where Scala or Java still win:** custom Spark internals, very low-latency
streaming, or a JVM shop. For everything in this project, Python is right.

---

## Coming in later phases

**Kafka / Redpanda** (phase 2) — a durable, replayable event log rather than a
queue. The distinction from SQS is the important one and we'll cover it there.

**Debezium** (phase 3) — reads the Postgres write-ahead log to capture every
change, including deletes, without querying the source table.

**Airflow** (phase 4) — orchestration with dependencies, retries and backfills.
Deliberately last: your machine has 8GB, and orchestration is worth adding once
there is something worth orchestrating.
