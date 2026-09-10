"""Phase 1's batch pipeline, as a real dependency graph.

    download (one task per month, fanned out)
        -> curate (Spark, all months at once)
            -> load (Snowflake COPY)
                -> dbt build
                    -> summary (always runs, even on failure)

WHY AIRFLOW RUNS THE JOBS INSTEAD OF IMPORTING THEM
    Every task here is a BashOperator invoking the PROJECT's interpreter, not
    a PythonOperator importing the project's code. That is deliberate.

    apache-airflow pins a long list of libraries, and it has a history of
    fighting dbt-core and pyspark over the same ones. Installing them into one
    environment leaves you unable to run either. Keeping the orchestrator's
    dependencies separate from the jobs' dependencies is the normal production
    arrangement - in a real deployment the separation is a container boundary
    or a Spark cluster rather than a second virtualenv, but the principle is
    identical: the scheduler schedules, it does not execute your job in its
    own process.

WHAT MAKES THIS MORE THAN FIVE BashOperators IN A ROW
    - the download step fans out per month via dynamic task mapping, so one
      bad month fails and retries on its own instead of taking the other
      eleven with it
    - concurrency is capped, because Spark needs the whole machine
    - retry policy differs by task, because "retry it" is not always right
    - the summary task runs whatever happened, so a failed run still reports
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

# Where the project lives and which interpreter runs its jobs. Both overridable
# so the DAG is not hard-coded to one laptop.
PROJECT_DIR = os.getenv("NYC_PROJECT_DIR", os.path.expanduser("~/nyc-trip-platform"))
PROJECT_PY = os.getenv("NYC_PROJECT_PYTHON", f"{PROJECT_DIR}/.venv/bin/python")
DBT_BIN = os.getenv("NYC_DBT_BIN", f"{PROJECT_DIR}/.venv/bin/dbt")

MONTHS = [f"2024-{m:02d}" for m in range(1, 13)]

default_args = {
    "owner": "vishnu",
    # Two retries with exponential backoff. Most failures in this pipeline are
    # transient - a TLC download timing out, S3 throttling, a Snowflake
    # session dropping - and all of those clear on their own. Backoff matters
    # because retrying a rate-limited service immediately just gets you rate
    # limited again.
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=20),
    "depends_on_past": False,
}

with DAG(
    dag_id="nyc_trips_batch",
    description="TLC monthly files -> S3 -> Spark curate -> Snowflake -> dbt",
    start_date=datetime(2024, 1, 1),
    schedule="0 6 5 * *",   # 06:00 on the 5th - TLC publishes with a lag
    # catchup=False is not a detail. With catchup on, unpausing this DAG in
    # 2026 against a 2024 start date would immediately queue ~20 monthly runs,
    # each launching a Spark job on an 8GB laptop. Backfills should be a
    # decision you make, not something that happens the moment you flip a
    # switch.
    catchup=False,
    # Spark wants the whole machine. Letting Airflow run several heavy tasks
    # side by side on a laptop is how you get an OOM kill that looks like a
    # data bug.
    max_active_tasks=3,
    max_active_runs=1,
    default_args=default_args,
    tags=["nyc", "batch", "phase1"],
    doc_md=__doc__,
) as dag:

    start = EmptyOperator(task_id="start")

    # DYNAMIC TASK MAPPING. One task instance per month, generated at parse
    # time. The alternative - one task looping over twelve months - means a
    # failure on month seven forces you to rerun all twelve, and the Airflow UI
    # shows you a single opaque green or red square instead of which month
    # actually broke.
    #
    # TLC_MONTHS is read from the environment by download_tlc.py. It works
    # because load_dotenv() does not override variables that are already set,
    # so what Airflow exports wins over .env.
    download = BashOperator.partial(
        task_id="download_month",
        cwd=PROJECT_DIR,
        execution_timeout=timedelta(minutes=20),
        # A download either works or it does not; three attempts is plenty,
        # and the file is written under a deterministic key so a retry
        # overwrites rather than duplicating.
        retries=3,
    ).expand(
        bash_command=[
            f"TLC_MONTHS={month} {PROJECT_PY} -m ingest.download_tlc"
            for month in MONTHS
        ]
    )

    # One Spark job across all months. Not mapped: the whole point of the
    # curate step is a single shuffle that writes one file per output
    # partition. Twelve separate Spark jobs would produce twelve small files
    # per partition and reintroduce the small-file problem the repartition was
    # there to solve.
    curate = BashOperator(
        task_id="curate_trips",
        bash_command=f"{PROJECT_PY} -m spark.jobs.curate_trips",
        cwd=PROJECT_DIR,
        execution_timeout=timedelta(hours=2),
        # Deliberately NOT retried by default. A Spark failure here is usually
        # disk pressure or memory, and an automatic retry burns 30 minutes to
        # fail the same way. This is the task you want a human to look at.
        retries=0,
    )

    load = BashOperator(
        task_id="load_snowflake",
        bash_command=f"{PROJECT_PY} -m warehouse.load_to_snowflake",
        cwd=PROJECT_DIR,
        execution_timeout=timedelta(minutes=45),
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        # `dbt build` runs models and their tests interleaved, so a failing
        # test stops its downstream models instead of letting bad data
        # propagate and then telling you about it afterwards.
        bash_command=f"{DBT_BIN} deps && {DBT_BIN} build",
        cwd=f"{PROJECT_DIR}/dbt",
        execution_timeout=timedelta(minutes=30),
        retries=1,
    )

    # trigger_rule="all_done" means this runs whether the pipeline succeeded or
    # failed. A run that dies at the Spark step should still leave a record of
    # how far it got - a pipeline that only reports on success is a pipeline
    # you learn nothing from on the days it matters.
    summary = BashOperator(
        task_id="run_summary",
        bash_command=(
            'echo "run: {{ run_id }}"; '
            'echo "logical date: {{ ds }}"; '
            'echo "months requested: ' + str(len(MONTHS)) + '"'
        ),
        trigger_rule="all_done",
        retries=0,
    )

    start >> download >> curate >> load >> dbt_build >> summary
