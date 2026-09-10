"""Phase 2's stream, on a schedule.

    drain kafka -> dbt build (cdc models) -> reconcile -> report

WHY A SCHEDULED DAG FOR A STREAMING PIPELINE
    This looks contradictory and is not. Debezium and Kafka run continuously -
    they are not scheduled by anything and must not be. What is scheduled is
    the part downstream of the log: draining the current backlog into the
    warehouse and rebuilding the models on top of it.

    That split is the normal shape of a real CDC deployment. Capture is
    continuous because you cannot afford to miss a change; transformation is
    periodic because nobody needs a dimension table rebuilt forty times a
    second, and Snowflake charges by the second the warehouse is awake.

    The consumer is safe to schedule precisely because of how phase 2 was
    built: it commits offsets after the write, and the sink merges on
    event_uid. Two runs overlapping, or a run replaying a batch, changes
    nothing. A pipeline that is not idempotent cannot be put on a schedule
    without a lock.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.operators.bash import BashOperator

from airflow import DAG

PROJECT_DIR = os.getenv("NYC_PROJECT_DIR", os.path.expanduser("~/nyc-trip-platform"))
PROJECT_PY = os.getenv("NYC_PROJECT_PYTHON", f"{PROJECT_DIR}/.venv/bin/python")
DBT_BIN = os.getenv("NYC_DBT_BIN", f"{PROJECT_DIR}/.venv/bin/dbt")

with DAG(
    dag_id="cdc_micro_batch",
    description="Drain the CDC topics into Snowflake, rebuild models, reconcile",
    start_date=datetime(2026, 9, 1),
    schedule="*/30 * * * *",
    catchup=False,
    # Never two at once. Not for correctness - the consumer is idempotent and
    # Kafka consumer groups would coordinate anyway - but because two Snowflake
    # warehouses spinning up to do the same work is money for nothing.
    max_active_runs=1,
    default_args={
        "owner": "vishnu",
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
        "retry_exponential_backoff": True,
    },
    tags=["nyc", "cdc", "phase2"],
    doc_md=__doc__,
) as dag:

    drain = BashOperator(
        task_id="drain_kafka_to_snowflake",
        bash_command=(
            f"CDC_GROUP_ID=cdc-snowflake-loader "
            f"{PROJECT_PY} cdc/consume_changes.py --target snowflake --once"
        ),
        cwd=PROJECT_DIR,
        # Bounded so a run cannot sit forever holding max_active_runs against
        # the next one. --once exits when consumer lag reaches zero; this is
        # the backstop for when the broker is unreachable and it never does.
        execution_timeout=timedelta(minutes=15),
    )

    dbt_cdc = BashOperator(
        task_id="dbt_build_cdc_models",
        bash_command=f"{DBT_BIN} build --select tag:cdc",
        cwd=f"{PROJECT_DIR}/dbt",
        execution_timeout=timedelta(minutes=20),
        retries=1,
    )

    # The check that makes the whole thing trustworthy. It compares warehouse
    # current state against a live SELECT from Postgres, and refuses to answer
    # at all while consumer lag is non-zero rather than reporting a mismatch
    # that only means "the pipeline is still catching up".
    #
    # retries=0 on purpose: a reconciliation failure is a real finding. Retrying
    # it until it passes is how a team trains itself to ignore its own alarms.
    reconcile = BashOperator(
        task_id="reconcile_against_source",
        bash_command=f"{PROJECT_PY} cdc/reconcile.py",
        cwd=PROJECT_DIR,
        execution_timeout=timedelta(minutes=10),
        retries=0,
    )

    # Replication slot health. This is the operational risk in any CDC setup
    # and it is invisible until it is catastrophic: an inactive slot pins the
    # WAL, Postgres refuses to recycle log segments, and the SOURCE database
    # fills its disk. Not the warehouse - the production database. Worth
    # looking at on every single run.
    slot_health = BashOperator(
        task_id="check_replication_slot",
        bash_command=(
            "docker exec cdc-postgres psql -U orders -d ordersdb -c "
            '"SELECT slot_name, active, '
            "pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) "
            'AS retained_wal FROM pg_replication_slots;"'
        ),
        trigger_rule="all_done",
        retries=0,
    )

    drain >> dbt_cdc >> reconcile >> slot_health
