"""Validate real provider imports and DAG serialization without cloud calls."""

from __future__ import annotations

import json
import os
import sys
from importlib.metadata import distribution, version
from pathlib import Path

from airflow.models import DagBag
from airflow.serialization.serialized_objects import DagSerialization


def main() -> None:
    for package in (
        "apache-airflow",
        "apache-airflow-providers-databricks",
        "apache-airflow-providers-common-ai",
    ):
        print(f"{package}=={version(package)}")
    source = distribution("apache-airflow-providers-databricks").read_text(
        "direct_url.json"
    )
    if source:
        print(f"Databricks source: {json.loads(source)['url']}")

    dags_folder = Path(
        os.environ.get("AIRFLOW__CORE__DAGS_FOLDER", "/opt/airflow/dags")
    )
    # Standalone DagBag parsing needs the bundle path for shared DAG helpers.
    sys.path.insert(0, str(dags_folder))
    dag_bag = DagBag(dag_folder=str(dags_folder))
    if dag_bag.import_errors:
        raise SystemExit(f"DAG import errors: {dag_bag.import_errors}")
    expected = {
        "databricks_agent_invoke",
        "databricks_agent_hook",
        "databricks_agent_as_tool",
    }
    if set(dag_bag.dags) != expected:
        raise SystemExit(f"Expected {sorted(expected)}, found {sorted(dag_bag.dags)}")
    for dag_id, dag in sorted(dag_bag.dags.items()):
        dag.validate()
        DagSerialization.to_dict(dag)
        print(f"OK: {dag_id} ({len(dag.tasks)} tasks; serialized)")


if __name__ == "__main__":
    main()
