"""DAG integrity tests. Run these in the AIRFLOW venv, not the project venv.

    ~/airflow-venv/bin/python -m pytest airflow/tests/test_dag_integrity.py -q

A broken DAG file does not raise anywhere useful. Airflow's scheduler catches
the exception, writes it to the Import Errors panel in the UI, and carries on
scheduling every other DAG - so a typo means your pipeline silently stops
running and everything looks normal until someone asks where the data went.
Same failure signature as every other bug in this project: it reports success
while doing nothing.

These tests turn that into a red build instead.
"""
from __future__ import annotations

import os
from pathlib import Path

import pytest

DAGS_DIR = Path(__file__).resolve().parents[1] / "dags"
os.environ.setdefault("AIRFLOW__CORE__DAGS_FOLDER", str(DAGS_DIR))
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")

from airflow.models import DagBag  # noqa: E402


@pytest.fixture(scope="session")
def dagbag() -> DagBag:
    return DagBag(dag_folder=str(DAGS_DIR), include_examples=False)


def test_no_import_errors(dagbag):
    """The one that actually matters."""
    assert not dagbag.import_errors, (
        "DAG files failed to import:\n"
        + "\n".join(f"  {f}: {e}" for f, e in dagbag.import_errors.items())
    )


def test_expected_dags_present(dagbag):
    assert set(dagbag.dag_ids) >= {"nyc_trips_batch", "cdc_micro_batch"}


def test_every_dag_has_an_owner_and_tags(dagbag):
    """Untagged, unowned DAGs are unfindable the moment there are twenty."""
    for dag_id, dag in dagbag.dags.items():
        assert dag.tags, f"{dag_id} has no tags"
        assert dag.default_args.get("owner"), f"{dag_id} has no owner"


def test_no_dag_has_catchup_enabled(dagbag):
    """Catchup on a DAG with an old start_date queues a run per missed
    interval the instant it is unpaused. Backfills should be deliberate."""
    for dag_id, dag in dagbag.dags.items():
        assert dag.catchup is False, f"{dag_id} has catchup enabled"


def test_every_task_has_an_execution_timeout_or_is_trivial(dagbag):
    """A task with no timeout can hang forever, holding a slot and blocking
    every run behind it. Echo/Empty tasks are exempt."""
    trivial = {"EmptyOperator"}
    for dag_id, dag in dagbag.dags.items():
        for task in dag.tasks:
            if type(task).__name__ in trivial or task.task_id in {"run_summary",
                                                                  "check_replication_slot"}:
                continue
            assert task.execution_timeout is not None, (
                f"{dag_id}.{task.task_id} has no execution_timeout"
            )


def test_reconciliation_never_retries(dagbag):
    """A failing data-quality check is a finding, not a flake. Retrying it
    until it passes is how a team learns to ignore its own alarms."""
    dag = dagbag.dags["cdc_micro_batch"]
    assert dag.get_task("reconcile_against_source").retries == 0


def test_no_cycles(dagbag):
    from airflow.utils.dag_cycle_tester import check_cycle

    for dag in dagbag.dags.values():
        check_cycle(dag)
