"""Run the Databricks agent as an Airflow task, with both waiting modes."""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

from airflow.providers.databricks.operators.agent import DatabricksAgentInvokeOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Param
from databricks_demo.inspection import inspect_response

APP_URL = os.environ.get(
    "DATABRICKS_AGENT_APP_URL", "https://YOUR-APP.databricksapps.com"
)


with DAG(
    dag_id="databricks_agent_invoke",
    description="Invoke a Durable Agent with synchronous or deferrable waiting.",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    render_template_as_native_obj=True,
    default_args={"retries": 0, "execution_timeout": timedelta(minutes=15)},
    params={
        "agent_input": Param(
            {"messages": [{"role": "user", "content": "Hello from Apache Airflow!"}]},
            type="object",
            description="Input accepted by your deployed agent; edit it when triggering the DAG.",
        ),
    },
    tags=["databricks", "durable-agents", "operator"],
    doc_md="""
    Invokes the app twice with separate sessions: once with worker polling and
    once with deferrable waiting. The latter needs a running triggerer.
    Each operator returns the full response in its `return_value` XCom.
    Open inspect_sync or inspect_deferrable logs to read the agent's output
    in the same format as the hook and toolset examples.
    """,
) as dag:
    for mode, deferrable in (("sync", False), ("deferrable", True)):
        invoke = DatabricksAgentInvokeOperator(
            task_id=f"invoke_{mode}",
            databricks_conn_id="databricks_oauth",
            app_url=APP_URL,
            input="{{ params.agent_input }}",
            session_id="{{ dag.dag_id }}-{{ task.task_id }}-{{ run_id }}",
            deferrable=deferrable,
            polling_period_seconds=5,
            timeout=600,
        )
        inspect_task = PythonOperator(
            task_id=f"inspect_{mode}",
            python_callable=inspect_response,
            op_kwargs={"response": invoke.output},
            multiple_outputs=True,
        )
        invoke >> inspect_task
