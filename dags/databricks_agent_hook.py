"""Call the hook through Common AI without a caller LLM or AgentOperator."""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from uuid import NAMESPACE_URL, uuid5

from airflow.providers.common.ai.managed_agents import ManagedAgentRequest
from airflow.providers.databricks.hooks.agent import DatabricksAgentHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Param, get_current_context
from databricks_demo.inspection import inspect_response

APP_URL = os.environ.get(
    "DATABRICKS_AGENT_APP_URL", "https://YOUR-APP.databricksapps.com"
)


def invoke_with_hook(app_url: str, prompt: str) -> dict:
    ti = get_current_context()["ti"]
    # Reuse the invocation for unchanged input when this task is retried/cleared.
    identity = json.dumps(
        [app_url.rstrip("/"), ti.dag_id, ti.task_id, ti.run_id, ti.map_index, prompt]
    )
    invocation_id = str(uuid5(NAMESPACE_URL, identity))
    agent = DatabricksAgentHook(databricks_conn_id="databricks_oauth").agent(app_url)
    response = agent.invoke(
        ManagedAgentRequest(
            prompt=prompt,
            session_id=invocation_id,
            timeout=300,
            vendor_options={"invocation_id": invocation_id},
        )
    )
    return {"text": response.text, "raw": response.raw, "trace_ref": response.trace_ref}


with DAG(
    dag_id="databricks_agent_hook",
    description="Invoke DatabricksAgentHook directly through the Common AI contract.",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 0, "execution_timeout": timedelta(minutes=10)},
    params={"prompt": Param("Hello from the Airflow Databricks hook!", type="string")},
    tags=["databricks", "durable-agents", "hook"],
) as dag:
    invoke = PythonOperator(
        task_id="invoke_with_hook",
        python_callable=invoke_with_hook,
        op_kwargs={"app_url": APP_URL, "prompt": "{{ params.prompt }}"},
        multiple_outputs=True,
    )
    inspect = PythonOperator(
        task_id="inspect_hook",
        python_callable=inspect_response,
        op_kwargs={"response": invoke.output["raw"]},
        multiple_outputs=True,
    )
    invoke >> inspect
