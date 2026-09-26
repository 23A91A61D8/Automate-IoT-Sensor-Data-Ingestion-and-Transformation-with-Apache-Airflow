"""
test_dag.py
-----------
Basic structural tests for the iot_data_pipeline DAG.

These tests verify:
    - The DAG loads without import errors.
    - The expected tasks are present.
    - Task dependencies are correct.
    - Default args and schedule are configured properly.

Run with:
    pytest tests/test_dag.py -v
"""

import sys
from pathlib import Path

import pytest

# Make dags/ and scripts/ importable
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "dags"))
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

# Mock Airflow internals if not installed (for CI environments)
try:
    from airflow.models import DagBag
    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False

pytestmark = pytest.mark.skipif(
    not AIRFLOW_AVAILABLE,
    reason="Apache Airflow not installed – skipping DAG tests.",
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def dag_bag():
    """Load the DAG from the dags/ directory."""
    bag = DagBag(dag_folder=str(PROJECT_ROOT / "dags"), include_examples=False)
    return bag


@pytest.fixture(scope="module")
def pipeline_dag(dag_bag):
    """Return the iot_data_pipeline DAG object."""
    dag = dag_bag.get_dag("iot_data_pipeline")
    assert dag is not None, "DAG 'iot_data_pipeline' not found in DagBag."
    return dag


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestDagLoading:
    def test_no_import_errors(self, dag_bag):
        """DagBag should have zero import errors."""
        assert dag_bag.import_errors == {}, (
            f"DAG import errors: {dag_bag.import_errors}"
        )

    def test_dag_exists(self, dag_bag):
        """The iot_data_pipeline DAG must be present in the DagBag."""
        assert "iot_data_pipeline" in dag_bag.dag_ids


class TestDagConfig:
    def test_dag_id(self, pipeline_dag):
        assert pipeline_dag.dag_id == "iot_data_pipeline"

    def test_schedule_interval(self, pipeline_dag):
        assert pipeline_dag.schedule_interval == "@hourly"

    def test_catchup_disabled(self, pipeline_dag):
        assert pipeline_dag.catchup is False

    def test_max_active_runs(self, pipeline_dag):
        assert pipeline_dag.max_active_runs == 1

    def test_tags_present(self, pipeline_dag):
        expected_tags = {"iot", "data-engineering", "etl"}
        assert expected_tags.issubset(set(pipeline_dag.tags))

    def test_owner(self, pipeline_dag):
        assert pipeline_dag.default_args.get("owner") == "data-engineering"

    def test_retries_configured(self, pipeline_dag):
        assert pipeline_dag.default_args.get("retries") == 3


class TestTaskPresence:
    EXPECTED_TASKS = [
        "generate_sensor_data",
        "ingest_raw_data",
        "transform_data",
        "run_data_quality_checks",
        "load_processed_data",
    ]

    def test_all_tasks_present(self, pipeline_dag):
        task_ids = [t.task_id for t in pipeline_dag.tasks]
        for expected in self.EXPECTED_TASKS:
            assert expected in task_ids, f"Task '{expected}' not found in DAG."

    def test_task_count(self, pipeline_dag):
        assert len(pipeline_dag.tasks) == len(self.EXPECTED_TASKS)

    def test_all_tasks_use_python_operator(self, pipeline_dag):
        from airflow.operators.python import PythonOperator
        for task in pipeline_dag.tasks:
            assert isinstance(task, PythonOperator), (
                f"Task '{task.task_id}' is not a PythonOperator."
            )

    def test_on_failure_callback_set(self, pipeline_dag):
        for task in pipeline_dag.tasks:
            assert task.on_failure_callback is not None, (
                f"Task '{task.task_id}' has no on_failure_callback."
            )


class TestTaskDependencies:
    def test_generate_runs_before_ingest(self, pipeline_dag):
        ingest = pipeline_dag.get_task("ingest_raw_data")
        upstream_ids = {t.task_id for t in ingest.upstream_list}
        assert "generate_sensor_data" in upstream_ids

    def test_ingest_runs_before_transform(self, pipeline_dag):
        transform = pipeline_dag.get_task("transform_data")
        upstream_ids = {t.task_id for t in transform.upstream_list}
        assert "ingest_raw_data" in upstream_ids

    def test_transform_runs_before_quality(self, pipeline_dag):
        quality = pipeline_dag.get_task("run_data_quality_checks")
        upstream_ids = {t.task_id for t in quality.upstream_list}
        assert "transform_data" in upstream_ids

    def test_quality_runs_before_load(self, pipeline_dag):
        load = pipeline_dag.get_task("load_processed_data")
        upstream_ids = {t.task_id for t in load.upstream_list}
        assert "run_data_quality_checks" in upstream_ids

    def test_linear_dependency_chain(self, pipeline_dag):
        """Verify the entire chain: generate → ingest → transform → quality → load."""
        order = [
            "generate_sensor_data",
            "ingest_raw_data",
            "transform_data",
            "run_data_quality_checks",
            "load_processed_data",
        ]
        for i in range(len(order) - 1):
            upstream_task = pipeline_dag.get_task(order[i])
            downstream_task = pipeline_dag.get_task(order[i + 1])
            assert upstream_task in downstream_task.upstream_list, (
                f"'{order[i]}' should be upstream of '{order[i + 1]}'."
            )

    def test_generate_has_no_upstream(self, pipeline_dag):
        generate = pipeline_dag.get_task("generate_sensor_data")
        assert generate.upstream_list == [], (
            "generate_sensor_data should have no upstream tasks."
        )

    def test_load_has_no_downstream(self, pipeline_dag):
        load = pipeline_dag.get_task("load_processed_data")
        assert load.downstream_list == [], (
            "load_processed_data should have no downstream tasks."
        )
