"""Additional DAG quality checks, separate from the default test_dag_integrity.py.

These go beyond "does it import / does it have tags & retries" and check:
- DAG-level hygiene (owner, max_active_runs, has at least one task)
- A specific DAG's expected task structure
- The actual behavior of a task's Python callable (not just its metadata)
"""

import os
import logging
from contextlib import contextmanager
from unittest.mock import patch, MagicMock

import pytest
from airflow.models import DagBag


@contextmanager
def suppress_logging(namespace):
    logger = logging.getLogger(namespace)
    old_value = logger.disabled
    logger.disabled = True
    try:
        yield
    finally:
        logger.disabled = old_value


def get_dags():
    """Generate a tuple of dag_id, <DAG object>, fileloc for every DAG in the DagBag."""
    with suppress_logging("airflow"):
        dag_bag = DagBag(include_examples=False)

    def strip_path_prefix(path):
        return os.path.relpath(path, os.environ.get("AIRFLOW_HOME"))

    return [(k, v, strip_path_prefix(v.fileloc)) for k, v in dag_bag.dags.items()]


# --------------------------------------------------------------------------- #
# DAG-level hygiene checks
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "dag_id,dag,fileloc", get_dags(), ids=[x[2] for x in get_dags()]
)
def test_dag_has_owner(dag_id, dag, fileloc):
    """Every DAG should have a real owner, not the Airflow default 'airflow'."""
    owner = dag.default_args.get("owner", "airflow")
    assert owner != "airflow", f"{dag_id} in {fileloc} has no explicit owner set."


@pytest.mark.parametrize(
    "dag_id,dag,fileloc", get_dags(), ids=[x[2] for x in get_dags()]
)
def test_dag_has_tasks(dag_id, dag, fileloc):
    """A DAG with zero tasks is almost certainly a mistake."""
    assert len(dag.tasks) > 0, f"{dag_id} in {fileloc} has no tasks."


@pytest.mark.parametrize(
    "dag_id,dag,fileloc", get_dags(), ids=[x[2] for x in get_dags()]
)
def test_dag_max_active_runs_is_sane(dag_id, dag, fileloc):
    """Guard against runaway concurrent DAG runs piling up."""
    assert dag.max_active_runs is not None and dag.max_active_runs <= 16, (
        f"{dag_id} in {fileloc} has no sane max_active_runs limit "
        f"(got {dag.max_active_runs})."
    )


# --------------------------------------------------------------------------- #
# Structure check for a specific, known-important DAG
# --------------------------------------------------------------------------- #


def test_astronauts_dag_has_expected_tasks():
    """Locks in the task contract for example_astronauts so a rename/removal is caught."""
    dag_bag = DagBag(include_examples=False)
    dag = dag_bag.dags["example_astronauts"]
    task_ids = {t.task_id for t in dag.tasks}
    assert {"get_astronauts", "print_astronaut_craft"} <= task_ids

# --------------------------------------------------------------------------- #
# Unit test of actual task behavior (not just metadata)
# --------------------------------------------------------------------------- #


def test_get_astronauts_falls_back_on_api_failure():
    """When the API call fails, get_astronauts should return the hardcoded fallback list."""
    dag_bag = DagBag(include_examples=False)
    dag = dag_bag.dags["example_astronauts"]
    task = dag.get_task("get_astronauts")

    with patch("requests.get", side_effect=ConnectionError("no dns")):
        result = task.python_callable(ti=MagicMock())

    assert len(result) == 2
    assert result[0]["name"] == "Marco Alain Sieber"
    assert result[0]["craft"] == "ISS"


def test_get_astronauts_returns_list_of_dicts_with_expected_keys():
    """Whatever path is taken (real API or fallback), the shape of the data should be consistent."""
    dag_bag = DagBag(include_examples=False)
    dag = dag_bag.dags["example_astronauts"]
    task = dag.get_task("get_astronauts")

    with patch("requests.get", side_effect=ConnectionError("no dns")):
        result = task.python_callable(ti=MagicMock())

    assert isinstance(result, list)
    for person in result:
        assert set(person.keys()) == {"craft", "name"}