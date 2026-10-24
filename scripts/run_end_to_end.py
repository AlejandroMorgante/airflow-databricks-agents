"""Trigger the real DAGs through Airflow's API and verify their XCom results."""

from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path
from uuid import uuid4

import requests

DAGS = {
    "databricks_agent_invoke": (
        "invoke_sync",
        "inspect_sync",
        "invoke_deferrable",
        "inspect_deferrable",
    ),
    "databricks_agent_hook": ("invoke_with_hook", "inspect_hook"),
    "databricks_agent_as_tool": ("consult_databricks_agent", "inspect_agent_response"),
}


class AirflowAPI:
    def __init__(self) -> None:
        self.url = "http://localhost:8080"
        self.session = requests.Session()
        self.session.trust_env = False
        password_file = Path(
            "/opt/airflow/simple_auth_manager_passwords.json.generated"
        )
        password = json.loads(password_file.read_text())["admin"]
        token = self.request(
            "POST", "/auth/token", json={"username": "admin", "password": password}
        )["access_token"]
        self.session.headers["Authorization"] = f"Bearer {token}"

    def request(self, method: str, path: str, **kwargs):
        response = self.session.request(method, self.url + path, timeout=15, **kwargs)
        if not response.ok:
            raise RuntimeError(f"Airflow {method} {path}: HTTP {response.status_code}")
        return response.json()

    def xcom(self, run_path: str, task_id: str, key: str = "return_value"):
        value = self.request(
            "GET",
            f"{run_path}/taskInstances/{task_id}/xcomEntries/{key}",
            params={"deserialize": "true"},
        )["value"]
        if isinstance(value, str):
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                pass
        return value


def require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def verify_invocation(response) -> None:
    require(isinstance(response, dict), "Expected a full invocation response in XCom")
    require(response.get("status") == "completed", "Invocation did not complete")
    require(bool(response.get("id")), "Invocation response has no ID")
    require("output" in response, "Invocation response has no output")
    output = response["output"]
    require(
        not isinstance(output, dict) or output.get("status") != "interrupted",
        "Agent paused for input instead of completing this smoke test",
    )


def verify_inspection(inspected: dict, answer: str, outputs: list) -> None:
    require(
        set(inspected) == {"final_answer", "databricks_responses"},
        "Unexpected inspection response format",
    )
    require(inspected["final_answer"] == answer, "Inspection changed the final answer")
    require(
        inspected["databricks_responses"] == outputs,
        "Inspection did not preserve the remote agent's responses",
    )


def verify_results(api: AirflowAPI, dag_id: str, run_path: str) -> None:
    if dag_id == "databricks_agent_invoke":
        for mode in ("sync", "deferrable"):
            response = api.xcom(run_path, f"invoke_{mode}")
            verify_invocation(response)
            verify_inspection(
                api.xcom(run_path, f"inspect_{mode}"),
                response["output"]["message"],
                [response["output"]],
            )
    elif dag_id == "databricks_agent_hook":
        response = api.xcom(run_path, "invoke_with_hook")
        verify_invocation(response["raw"])
        require(bool(response["text"]), "Hook returned no normalized text")
        require(
            response["trace_ref"] == response["raw"]["id"],
            "Hook trace reference does not match the invocation ID",
        )
        verify_inspection(
            api.xcom(run_path, "inspect_hook"),
            response["raw"]["output"]["message"],
            [response["raw"]["output"]],
        )
    else:
        task_id = "consult_databricks_agent"
        answer = api.xcom(run_path, task_id)
        require(bool(answer), "Caller LLM returned no answer")
        history = api.xcom(run_path, task_id, "message_history")
        parts = [part for message in history for part in message.get("parts", [])]
        for kind in ("tool-call", "tool-return"):
            require(
                any(
                    part.get("part_kind") == kind
                    and part.get("tool_name") == "ask_databricks_agent"
                    for part in parts
                ),
                f"No {kind} for ask_databricks_agent in the caller's transcript",
            )
        inspected = api.xcom(run_path, "inspect_agent_response")
        tool_returns = [
            part
            for part in parts
            if part.get("part_kind") == "tool-return"
            and part.get("tool_name") == "ask_databricks_agent"
        ]
        outputs = []
        for part in tool_returns:
            content = part["content"]
            if isinstance(content, str):
                try:
                    content = json.loads(content)
                except json.JSONDecodeError:
                    pass
            outputs.append(content)
        verify_inspection(inspected, answer, outputs)


def run_dag(api: AirflowAPI, dag_id: str, timeout: float) -> None:
    dag_path = f"/api/v2/dags/{dag_id}"
    was_paused = api.request("GET", dag_path)["is_paused"]
    run_id = f"e2e__{uuid4().hex}"
    run_path = f"{dag_path}/dagRuns/{run_id}"
    if was_paused:
        api.request("PATCH", dag_path, json={"is_paused": False})
    try:
        api.request(
            "POST",
            f"{dag_path}/dagRuns",
            json={"dag_run_id": run_id, "logical_date": None, "conf": {}},
        )
        print(f"Started {dag_id}: {run_id}", flush=True)
        deadline = time.monotonic() + timeout
        previous_states = None
        while time.monotonic() < deadline:
            run = api.request("GET", run_path)
            tasks = api.request("GET", f"{run_path}/taskInstances")["task_instances"]
            states = {task["task_id"]: task["state"] for task in tasks}
            if states != previous_states:
                print(f"{dag_id}: {states}", flush=True)
                previous_states = states
            require(
                run["state"] != "failed",
                f"{dag_id} failed; inspect run {run_id} in the UI",
            )
            if run["state"] == "success":
                require(
                    states == dict.fromkeys(DAGS[dag_id], "success"),
                    f"Unexpected task states in {dag_id}: {states}",
                )
                verify_results(api, dag_id, run_path)
                print(
                    f"PASS: {dag_id}; task states and XCom results verified", flush=True
                )
                return
            time.sleep(min(2, max(0, deadline - time.monotonic())))
        raise RuntimeError(
            f"Timed out waiting for {dag_id}/{run_id}; remote work is not cancelled"
        )
    finally:
        if was_paused:
            api.request("PATCH", dag_path, json={"is_paused": True})


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dag", action="append", choices=list(DAGS))
    parser.add_argument(
        "--timeout", type=float, default=900, help="Wait budget per DAG in seconds"
    )
    parser.add_argument(
        "--check-only",
        action="store_true",
        help="Check local readiness without running DAGs",
    )
    args = parser.parse_args()
    require(args.timeout > 0, "--timeout must be positive")
    dag_ids = args.dag or list(DAGS)
    api = AirflowAPI()
    health = api.request("GET", "/api/v2/monitor/health")
    for name in ("metadatabase", "scheduler", "triggerer", "dag_processor"):
        require(
            health.get(name, {}).get("status") == "healthy",
            f"Unhealthy Airflow component: {name}",
        )
    errors = api.request("GET", "/api/v2/importErrors")
    require(
        errors["total_entries"] == 0,
        "Airflow has DAG import errors; inspect them in the UI",
    )
    for dag_id in dag_ids:
        api.request("GET", f"/api/v2/dags/{dag_id}")
    if args.check_only:
        print("Local readiness passed. No DAGs or Databricks invocations were started.")
        return
    required = ["DATABRICKS_AGENT_APP_URL", "AIRFLOW_CONN_DATABRICKS_OAUTH"]
    if "databricks_agent_as_tool" in dag_ids:
        required.append("AIRFLOW_CONN_PYDANTICAI_DEFAULT")
    for name in required:
        value = os.environ.get(name, "")
        require(
            bool(value) and "YOUR-" not in value,
            f"Configure {name} in .env, then run make up",
        )
    for dag_id in dag_ids:
        run_dag(api, dag_id, args.timeout)


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, requests.RequestException) as exc:
        raise SystemExit(str(exc)) from None
