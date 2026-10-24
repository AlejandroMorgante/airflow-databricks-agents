"""Reconcile the Databricks demo and write its local Airflow connections."""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import re
import subprocess
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

from poc_config import (
    OAuthRejected,
    connection_json,
    llm_connection,
    oauth_token,
    read_env,
    write_env,
)

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MODEL = "databricks-gpt-oss-20b"
AGENT_FILES = ("hello_agent.py", "requirements.txt", "app.yaml", "LICENSE", "NOTICE")


def log(message: str) -> None:
    print(message, flush=True)


class CLI:
    def __init__(self, profile: str = ""):
        self.profile = profile

    def __call__(self, *args: str, body: dict | None = None, raw: bool = False):
        command = ["databricks", *args]
        if self.profile:
            command += ["--profile", self.profile]
        if not raw:
            command += ["--output", "json"]
        if body is not None:
            command += ["--json", json.dumps(body)]
        try:
            result = subprocess.run(
                command, capture_output=True, text=True, timeout=1800
            )
        except FileNotFoundError:
            raise RuntimeError(
                "Install the Databricks CLI before running make up"
            ) from None
        except subprocess.TimeoutExpired:
            raise RuntimeError(
                f"Databricks {' '.join(args[:2])} timed out; inspect the resource before retrying"
            ) from None
        if result.returncode:
            # Do not dump command arguments or JSON responses: some contain credentials.
            detail = result.stderr.strip()[:800]
            raise RuntimeError(f"Databricks {' '.join(args[:2])} failed: {detail}")
        return result.stdout if raw else json.loads(result.stdout or "{}")


def choose_profile(cli: CLI, requested: str) -> tuple[str, str]:
    profiles = cli("auth", "profiles", "--skip-validate")["profiles"]
    matches = (
        [p for p in profiles if p["name"] == requested]
        if requested
        else [p for p in profiles if p.get("default")]
    )
    if not matches and not requested:
        matches = [p for p in profiles if p.get("workspace_id")]
    if len(matches) != 1:
        raise RuntimeError(
            "Choose a workspace profile: make up DATABRICKS_PROFILE=YOUR_PROFILE"
        )
    profile = matches[0]
    host = profile["host"].rstrip("/")
    if not host.startswith("https://") or "accounts." in host:
        raise RuntimeError("Select a workspace profile, not an account-console profile")
    return profile["name"], host


def parse_connection(env: dict, key: str) -> dict:
    value = env.get(key, "")
    if not value or "YOUR-" in value:
        return {}
    try:
        result = json.loads(value)
    except ValueError:
        raise RuntimeError(
            f"Invalid JSON in {key}; fix it before running setup"
        ) from None
    if not isinstance(result, dict):
        raise RuntimeError(f"{key} must be a JSON object")
    return result


def choose_app(apps: list[dict], requested: str, env: dict, host: str) -> str:
    if requested:
        return requested
    previous = parse_connection(env, "AIRFLOW_CONN_DATABRICKS_OAUTH")
    url = env.get("DATABRICKS_AGENT_APP_URL")
    if previous.get("host", "").rstrip("/") == host and url and "YOUR-" not in url:
        matches = [
            app["name"]
            for app in apps
            if app.get("url", "").rstrip("/") == url.rstrip("/")
        ]
        if len(matches) != 1:
            raise RuntimeError(
                "The configured app URL was not found. Set DATABRICKS_APP_NAME explicitly to choose or create an app"
            )
        return matches[0]
    return "airflow-durable-agent-demo"


def has_permission(acl: dict, client_id: str, allowed: set[str]) -> bool:
    return any(
        entry.get("service_principal_name") == client_id
        and any(
            p.get("permission_level") in allowed
            for p in entry.get("all_permissions", [])
        )
        for entry in acl.get("access_control_list", [])
    )


def ensure_caller(
    cli: CLI, env_path: Path, env: dict, host: str, app_name: str
) -> tuple[dict, dict]:
    previous = parse_connection(env, "AIRFLOW_CONN_DATABRICKS_OAUTH")
    same_workspace = previous.get("host", "").rstrip("/") == host
    if same_workspace and previous.get("login"):
        selector = "applicationId eq " + json.dumps(previous["login"])
    else:
        selector = "displayName eq " + json.dumps(app_name + "-airflow-caller")
    matches = cli("service-principals", "list", "--filter", selector)
    if len(matches) > 1:
        raise RuntimeError(
            "Multiple caller service principals match; select one in AIRFLOW_CONN_DATABRICKS_OAUTH"
        )
    if not matches and same_workspace:
        raise RuntimeError(
            "The configured caller service principal no longer exists; select a valid caller or reset that connection"
        )
    if matches:
        principal = matches[0]
        log("Reusing caller service principal: " + principal["displayName"])
    else:
        log("Creating a dedicated Airflow caller service principal")
        principal = cli(
            "service-principals",
            "create",
            body={
                "displayName": app_name + "-airflow-caller",
                "active": True,
                "entitlements": [{"value": "workspace-access"}],
            },
        )
    if not principal.get("active", True):
        raise RuntimeError(
            "The caller service principal is disabled; enable it before continuing"
        )
    if not any(
        e.get("value") == "workspace-access" for e in principal.get("entitlements", [])
    ):
        cli(
            "service-principals",
            "patch",
            str(principal["id"]),
            body={
                "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
                "Operations": [
                    {
                        "op": "add",
                        "path": "entitlements",
                        "value": [{"value": "workspace-access"}],
                    }
                ],
            },
        )
    conn = {
        "conn_type": "databricks",
        "host": host,
        "login": principal["applicationId"],
        "password": previous.get("password", "") if same_workspace else "",
        "extra": {"service_principal_oauth": True},
    }
    token = None
    if conn["password"]:
        try:
            token = oauth_token(conn)
        except OAuthRejected:
            log(
                "The existing OAuth secret is invalid or expired; creating a replacement"
            )
    if token is None:
        log("Creating a seven-day OAuth secret for the caller (stored only in .env)")
        secret = cli(
            "service-principal-secrets-proxy",
            "create",
            str(principal["id"]),
            "--lifetime",
            "604800s",
        )
        conn["password"] = secret["secret"]
        # Save immediately so a later failed deployment does not create another secret.
        write_env(
            env_path,
            {
                "AIRFLOW_CONN_DATABRICKS_OAUTH": connection_json(conn),
                "DATABRICKS_CALLER_SECRET_EXPIRES_AT": secret.get("expire_time", ""),
            },
        )
        token = oauth_token(conn)
    return conn, token


def agent_sources(root: Path, model: str) -> dict[str, bytes]:
    sources = {name: (root / "agent" / name).read_bytes() for name in AGENT_FILES}
    config, count = re.subn(
        r"(?m)(  - name: DATABRICKS_MODEL_ENDPOINT\n    value: )[^\n]+",
        lambda m: m[1] + json.dumps(model),
        sources["app.yaml"].decode(),
    )
    if count != 1:
        raise RuntimeError(
            "agent/app.yaml must define DATABRICKS_MODEL_ENDPOINT exactly once"
        )
    sources["app.yaml"] = config.encode()
    return sources


def ensure_deployment(cli: CLI, app: dict, root: Path, model: str) -> dict:
    sources = agent_sources(root, model)
    digest = hashlib.sha256()
    for name, content in sorted(sources.items()):
        digest.update(name.encode() + b"\0" + content + b"\0")
    remote = f"/Workspace/Shared/airflow-databricks-agents/{app['name']}/agent/{digest.hexdigest()[:20]}"
    deployment = app.get("active_deployment", {})
    if app.get("compute_status", {}).get("state") != "ACTIVE":
        log("Starting app compute: " + app["name"])
        cli("apps", "start", app["name"])
    if (
        deployment.get("source_code_path") != remote
        or deployment.get("status", {}).get("state") != "SUCCEEDED"
    ):
        log("Deploying agent source and model configuration: " + app["name"])
        with tempfile.TemporaryDirectory(
            dir=root / ".databricks", prefix="agent-"
        ) as directory:
            for name, content in sources.items():
                (Path(directory) / name).write_bytes(content)
            cli("workspace", "import-dir", directory, remote, "--overwrite", raw=True)
        cli("apps", "deploy", app["name"], "--source-code-path", remote)
    else:
        log("Agent source unchanged; skipping deployment")
    result = cli("apps", "get", app["name"])
    if result.get("app_status", {}).get("state") != "RUNNING":
        raise RuntimeError(
            "App is not RUNNING; inspect its deployment logs before starting Airflow"
        )
    return result


def bootstrap(
    root: Path, profile: str = "", app_name: str = "", model: str = ""
) -> None:
    env_path = root / ".env"
    env = read_env(env_path)
    profile, host = choose_profile(CLI(), profile or env.get("DATABRICKS_PROFILE", ""))
    cli = CLI(profile)
    log(f"Workspace: {host}; profile: {profile}")
    try:
        cli("current-user", "me")
    except RuntimeError:
        raise RuntimeError(
            f"Authenticate first: databricks auth login --profile {profile} --host {host}"
        ) from None
    model = model or env.get("DATABRICKS_MODEL_ENDPOINT") or DEFAULT_MODEL
    if not re.fullmatch(r"[A-Za-z0-9_.-]+", model):
        raise RuntimeError("Invalid model endpoint name")
    endpoint = cli("serving-endpoints", "get", model)
    if endpoint.get("task") not in (None, "llm/v1/chat"):
        raise RuntimeError("Choose a text chat model endpoint")
    if endpoint.get("state", {}).get("ready") != "READY":
        raise RuntimeError(f"Model endpoint {model} is not READY")
    apps = cli("apps", "list")
    app_name = choose_app(
        apps, app_name or env.get("DATABRICKS_APP_NAME", ""), env, host
    )
    if not re.fullmatch(r"[a-z0-9][a-z0-9-]*", app_name):
        raise RuntimeError("Invalid app name")
    app = next((a for a in apps if a["name"] == app_name), None)
    if app is None:
        log("Creating Databricks App: " + app_name)
        cli(
            "apps",
            "create",
            app_name,
            "--no-compute",
            "--no-wait",
            body={
                "description": "Airflow POC: DurableAgentServer with a Databricks-hosted LLM",
                "compute_min_instances": 1,
                "compute_max_instances": 1,
            },
        )
    app = cli("apps", "get", app_name)
    if app.get("compute_max_instances", 1) > 1:
        raise RuntimeError(
            "This in-memory demo requires one app instance; select a single-instance app"
        )
    conn, token = ensure_caller(cli, env_path, env, host, app_name)
    acl = cli("apps", "get-permissions", app_name)
    if not has_permission(acl, conn["login"], {"CAN_USE", "CAN_MANAGE"}):
        log("Granting the caller CAN_USE on the demo app")
        cli(
            "apps",
            "update-permissions",
            app_name,
            body={
                "access_control_list": [
                    {
                        "service_principal_name": conn["login"],
                        "permission_level": "CAN_USE",
                    },
                ]
            },
        )
    if endpoint.get("id"):
        acl = cli("serving-endpoints", "get-permissions", endpoint["id"])
        grants = [
            {"service_principal_name": identity, "permission_level": "CAN_QUERY"}
            for identity in {conn["login"], app["service_principal_client_id"]}
            if not has_permission(acl, identity, {"CAN_QUERY", "CAN_MANAGE"})
        ]
        if grants:
            log("Granting model CAN_QUERY to the app and caller where needed")
            cli(
                "serving-endpoints",
                "update-permissions",
                endpoint["id"],
                body={"access_control_list": grants},
            )
    else:
        log("Using workspace foundation-model access (no dedicated endpoint ACL)")
    app = ensure_deployment(cli, app, root, model)
    # App startup can take several minutes: obtain the caller token after deployment.
    token = oauth_token(conn)
    expiry = datetime.now(timezone.utc) + timedelta(seconds=token["expires_in"])
    updates = {
        "DATABRICKS_PROFILE": profile,
        "DATABRICKS_APP_NAME": app_name,
        "DATABRICKS_MODEL_ENDPOINT": model,
        "DATABRICKS_AGENT_APP_URL": app["url"],
        "AIRFLOW_CONN_DATABRICKS_OAUTH": connection_json(conn),
    }
    previous_llm = parse_connection(env, "AIRFLOW_CONN_PYDANTICAI_DEFAULT")
    previous_oauth = parse_connection(env, "AIRFLOW_CONN_DATABRICKS_OAUTH")
    databricks_hosts = {
        h for h in (host, previous_oauth.get("host", "").rstrip("/")) if h
    }
    previous_llm_host = previous_llm.get("host", "").rstrip("/")
    if not previous_llm or (
        previous_llm_host.endswith("/serving-endpoints")
        and previous_llm_host.removesuffix("/serving-endpoints") in databricks_hosts
    ):
        updates["AIRFLOW_CONN_PYDANTICAI_DEFAULT"] = llm_connection(host, token, model)
        updates["DATABRICKS_LLM_TOKEN_EXPIRES_AT"] = expiry.isoformat()
        log(
            "Refreshed Databricks caller LLM token; expires around "
            + expiry.isoformat()
        )
    else:
        log("Preserving the custom caller LLM connection")
    write_env(env_path, updates)
    log("Databricks ready; .env updated with mode 0600. App: " + app["url"])


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--profile",
        default=os.getenv("DATABRICKS_PROFILE")
        or os.getenv("DATABRICKS_CONFIG_PROFILE", ""),
    )
    parser.add_argument("--app-name", default=os.getenv("DATABRICKS_APP_NAME", ""))
    parser.add_argument("--model", default=os.getenv("DATABRICKS_MODEL_ENDPOINT", ""))
    args = parser.parse_args()
    state = ROOT / ".databricks"
    state.mkdir(mode=0o700, exist_ok=True)
    with (state / "bootstrap.lock").open("w") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise SystemExit(
                "Another Databricks setup is running; wait for it to finish"
            ) from None
        try:
            bootstrap(ROOT, args.profile, args.app_name, args.model)
        except (RuntimeError, KeyError, ValueError) as exc:
            raise SystemExit(str(exc)) from None


if __name__ == "__main__":
    main()
