# Phase 3 — Orchestration with Airflow

Phases 1 and 2 both run because a person types a command. That is the gap this
closes. Not "add Airflow because the job description mentions it" — the two
pipelines have real properties an orchestrator exists to handle: a twelve-way
fan-out where one month can fail on its own, a Spark step that must not run
concurrently with anything else on an 8GB machine, a data-quality check that
should never be retried, and a replication slot that quietly threatens the
source database's disk.

---

## Why Airflow lives in a separate virtualenv

`apache-airflow` pins a long list of libraries, and it has a history of
conflicting with `dbt-core` and `pyspark` over the same ones. Install them
together and you end up unable to run either.

So Airflow gets `~/airflow-venv`, and every task in these DAGs is a
`BashOperator` that invokes **the project's** interpreter:

```python
PROJECT_PY = "~/nyc-trip-platform/.venv/bin/python"
BashOperator(bash_command=f"{PROJECT_PY} -m spark.jobs.curate_trips", ...)
```

Not a `PythonOperator` importing the job. That is not a workaround, it is the
normal production arrangement — in a real deployment the separation is a
container image or a Spark cluster rather than a second virtualenv, but the
principle is the same: **the scheduler schedules, it does not execute your job
inside its own process.** A `PythonOperator` that imports pyspark puts your
job's memory usage inside the scheduler's worker, and one OOM takes down the
thing that was supposed to notice the failure.

---

## Setup

```bash
# 1. separate environment for the orchestrator
uv venv ~/airflow-venv --python 3.11
source ~/airflow-venv/bin/activate

# 2. Airflow must be installed with its constraints file. It is not optional:
#    without it pip resolves a combination that does not work.
export AIRFLOW_VERSION=2.10.5
export PY=3.11
uv pip install "apache-airflow==${AIRFLOW_VERSION}" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PY}.txt"

# 3. point Airflow at this repo's dags folder
export AIRFLOW_HOME=~/airflow
mkdir -p $AIRFLOW_HOME
echo "dags_folder = $HOME/nyc-trip-platform/airflow/dags" >/dev/null  # see below

# 4. first run creates the config and the metadata DB
airflow standalone
```

`airflow standalone` prints a generated admin password. The UI is on
<http://localhost:8080> — note that clashes with Redpanda Console, so stop the
CDC console or move one of them.

Then edit `~/airflow/airflow.cfg`:

```ini
dags_folder = /home/<you>/nyc-trip-platform/airflow/dags
load_examples = False
```

Restart `airflow standalone` and both DAGs appear.

---

## The executor ladder

`airflow standalone` gives you SQLite and the **SequentialExecutor**: exactly
one task at a time, no matter what the DAG says. Fine for seeing this work,
wrong for anything else — the twelve mapped download tasks will run one after
another.

The ladder, and the actual reason for each rung:

| Executor | Metadata DB | Runs tasks | When |
|---|---|---|---|
| Sequential | SQLite | one at a time, in-process | local demo only. SQLite has no row-level locking, so nothing else is safe |
| **Local** | Postgres/MySQL | subprocesses on one machine | a single box, real parallelism. Where this project belongs |
| Celery | Postgres + Redis | a pool of worker machines | horizontal scale, workers survive scheduler restarts |
| Kubernetes | Postgres | one pod per task | isolation and per-task resources, at the cost of pod startup latency |

To move to LocalExecutor, reuse the Postgres container phase 2 already runs:

```bash
docker exec -it cdc-postgres psql -U orders -d ordersdb -c "CREATE DATABASE airflow;"
```

```ini
# ~/airflow/airflow.cfg
executor = LocalExecutor
sql_alchemy_conn = postgresql+psycopg2://orders:orders@localhost:5432/airflow
```

```bash
airflow db migrate
```

Say that out loud in an interview — "SQLite forces SequentialExecutor because
it has no row-level locking, so the first real step is LocalExecutor against
Postgres" — and you have answered the question behind the question.

---

## `nyc_trips_batch`

```
start → download_month [×12] → curate_trips → load_snowflake → dbt_build → run_summary
```

Four decisions in it worth defending:

**Downloads are fanned out with dynamic task mapping.** One task instance per
month, not one task looping twelve times. If August's file 404s, August fails
and retries by itself; the other eleven are untouched, and the UI shows you
*which* month broke instead of one opaque red square.

**Curate is deliberately not mapped.** The whole point of that job is a single
shuffle that writes one file per output partition. Twelve separate Spark jobs
would write twelve small files per partition and reintroduce the small-file
problem the `repartition` was there to solve.

**Retry policy differs per task, because "retry it" is not always right.**
Downloads get three attempts with exponential backoff — they fail for transient
reasons and immediate retries against a rate-limited server just get rate
limited again. Spark gets **zero**: it fails on disk or memory pressure, and an
automatic retry burns thirty minutes arriving at the same place. That is a task
you want a human to look at.

**`catchup=False`.** With catchup on, unpausing this DAG against a 2024 start
date immediately queues twenty monthly runs, each launching Spark on an 8GB
laptop. Backfills should be a decision, not something that happens when you
flip a switch.

---

## `cdc_micro_batch`

```
drain_kafka_to_snowflake → dbt_build_cdc_models → reconcile_against_source → check_replication_slot
```

A scheduled DAG for a streaming pipeline sounds contradictory. It is not.
Debezium and Kafka run continuously and must not be scheduled by anything —
you cannot afford to miss a change. What *is* scheduled is everything
downstream of the log: draining the backlog into Snowflake and rebuilding the
models on top of it. Capture is continuous because missing data is
unrecoverable; transformation is periodic because nobody needs a dimension
rebuilt forty times a second, and Snowflake bills by the second the warehouse
is awake.

**This is only safe to schedule because of how phase 2 was built.** The
consumer commits offsets after the write and the sink merges on `event_uid`, so
an overlapping run or a replayed batch changes nothing. A pipeline that is not
idempotent cannot go on a schedule without a lock.

`reconcile_against_source` has `retries=0` on purpose. A failing data-quality
check is a finding, not a flake. Retrying it until it goes green is how a team
trains itself to ignore its own alarms.

`check_replication_slot` runs with `trigger_rule="all_done"` — on every run,
success or failure. An inactive replication slot pins the WAL and Postgres
stops recycling log segments, so a consumer that has been down since Friday can
fill **the source database's** disk by Monday. Not the warehouse. The
production database. It is the operational risk in any CDC deployment and it is
invisible until it is catastrophic.

---

## DAG integrity tests

```bash
~/airflow-venv/bin/python -m pytest airflow/tests/test_dag_integrity.py -q
```

A broken DAG file does not raise anywhere you will see it. The scheduler
catches the exception, writes it to an Import Errors panel, and keeps
scheduling every other DAG — so a typo means this pipeline silently stops
running while everything looks normal, until somebody asks where the data went.

Same signature as the timestamp bug, the silent join loss, and the drain that
printed `written: 0`. **The failures that matter report success.** These tests
turn that particular one into a red build.
