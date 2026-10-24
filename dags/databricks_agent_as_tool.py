"""Let an AgentOperator consult the deployed Databricks agent as a tool."""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

from airflow.providers.common.ai.operators.agent import AgentOperator
from airflow.providers.common.ai.toolsets import ManagedAgentToolset
from airflow.providers.databricks.hooks.agent import DatabricksAgentHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Param
from databricks_demo.inspection import inspect_agent_response

APP_URL = os.environ.get(
    "DATABRICKS_AGENT_APP_URL", "https://YOUR-APP.databricksapps.com"
)


with DAG(
    dag_id="databricks_agent_as_tool",
    description="AgentOperator → ManagedAgentToolset → Databricks Durable Agent.",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 0, "execution_timeout": timedelta(minutes=15)},
    params={
        "prompt": Param(
            "Ask the Databricks agent to introduce itself, then summarize its answer.",
            type="string",
        ),
    },
    tags=["databricks", "durable-agents", "common-ai", "tool"],
    doc_md="""
    Requires both `databricks_oauth` and `pydanticai_default` connections.
    The caller LLM chooses tool calls. Check the task logs to confirm it called
    `ask_databricks_agent`. Calls run synchronously in the worker; each call has
    its own invocation and session. The final answer is returned through XCom.
    Open inspect_agent_response logs to see both the final answer and the
    Databricks agent's responses, extracted from the tool transcript.
    """,
) as dag:
    invoke = AgentOperator(
        task_id="consult_databricks_agent",
        llm_conn_id="pydanticai_default",
        prompt="{{ params.prompt }}",
        system_prompt=(
            "Use ask_databricks_agent to answer the user's request. "
            "Consult it at least once before answering. "
            "Summarize its response faithfully; do not invent a response on its behalf."
        ),
        toolsets=[
            ManagedAgentToolset(
                DatabricksAgentHook(databricks_conn_id="databricks_oauth").agent(
                    APP_URL
                ),
                tool_name="ask_databricks_agent",
                description="Consult the Durable Agent deployed on Databricks Apps.",
                timeout=300,
            ),
        ],
        usage_limits={"request_limit": 5, "tool_calls_limit": 3},
        # Common AI 1.0 emits the transcript when message_history is supplied.
        message_history=[],
    )
    inspect = PythonOperator(
        task_id="inspect_agent_response",
        python_callable=inspect_agent_response,
        op_kwargs={
            "response": invoke.output,
            "message_history": invoke.output["message_history"],
        },
        multiple_outputs=True,
    )
    invoke >> inspect
