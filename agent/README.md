# Example agent for Databricks Agent Runtime

This directory contains the remote side of the Airflow examples: a small Python
handler served by the real `DurableAgentServer` from `databricks_agentkit`.
Deploy it as a Databricks App, then configure Airflow with the app's URL and
workspace OAuth connection. Airflow calls `/api/invocations`; it does not load
the agent's Python code into the worker.

The handler sends `input.messages` to **GPT OSS 20B** through Databricks Model
Serving and returns the model's answer, endpoint name, token usage, and session
ID. The SDK uses the app's automatically supplied OAuth credentials and manages
token renewal; there is no OpenAI API key or copied access token in the agent.
The `databricks_agent_as_tool` DAG also has a separate caller LLM in Airflow that
decides when to consult this remote agent.

`DATABRICKS_MODEL_ENDPOINT` in `app.yaml` selects the endpoint. Its initial value
is `databricks-gpt-oss-20b`. This model is not guaranteed to exist in every
workspace: check [model availability](https://docs.databricks.com/aws/en/machine-learning/model-serving/foundation-model-overview)
and choose an available text chat endpoint if necessary. The sample calls the
Databricks SDK's chat interface and expects a text answer. It has no tools or
conversation memory; send earlier messages explicitly if you need context.

The starting point is Apache Airflow's
[agent system-test fixture](https://github.com/apache/airflow/tree/main/providers/databricks/tests/system/databricks/resources/agent_server).
The copied source files retain their Apache license notices; the license and
upstream attribution are included in `LICENSE` and `NOTICE`. The original
fixture was modified to query an LLM, validate messages, and return model usage.

## Files

| File | Purpose |
| --- | --- |
| `hello_agent.py` | Registers the handler and queries the model with app OAuth. |
| `app.yaml` | Starts the server and selects the model endpoint. |
| `requirements.txt` | Pins the direct server dependencies. |
| `LICENSE`, `NOTICE` | Apache license and attribution for the adapted fixture. |

## Try the agent locally

From the repository root, with Python 3.12 and `uv` installed:

```bash
uv venv --python 3.12 .venv
uv pip install --python .venv/bin/python -r agent/requirements.txt
make check-agent
```

The default check uses the real server's HTTP application in process with a
mocked model SDK call. It verifies a background invocation, polling, response
mapping, session propagation, and reuse of the invocation ID without a second
model call. It makes no Databricks calls and does not test cloud OAuth.

To test the same agent against a real model using your authenticated CLI profile:

```bash
DATABRICKS_CONFIG_PROFILE=YOUR_PROFILE .venv/bin/python scripts/check_agent.py --live
```

This makes an inference request. It prints the answer and checks token usage.
For a different local model, set `DATABRICKS_MODEL_ENDPOINT` in the shell;
`app.yaml` is loaded by Databricks Apps, not by the local test script.

To run a local HTTP server for exploration:

```bash
DATABRICKS_CONFIG_PROFILE=YOUR_PROFILE .venv/bin/python -m uvicorn hello_agent:app --app-dir agent --host 127.0.0.1 --port 8000
```

## Automatic setup

From the repository root, authenticate with a workspace CLI profile and run
`make up DATABRICKS_PROFILE=YOUR_PROFILE`. This deploys the agent, configures
its caller and Airflow connections, and starts Airflow. Later `make up` runs
renew the caller token and skip unchanged deployments. See the
[root quickstart](../README.md#1-authenticate-once).

Use `DATABRICKS_MODEL_ENDPOINT` as a make variable or `.env` setting for this
workflow. Setup renders the selected model into the deployed `app.yaml`.

## Deploy to Databricks Apps

The following is the manual alternative to `make up`.

Run these commands from the repository root. Choose an app name and workspace
directory for your deployment; the values below are examples.

```bash
export DATABRICKS_CONFIG_PROFILE=YOUR_PROFILE
export AGENT_APP_NAME=airflow-durable-agent-demo
export AGENT_WORKSPACE_PATH=/Workspace/Shared/airflow-databricks-agents/agent

databricks auth login --host https://YOUR-WORKSPACE.cloud.databricks.com \
  --profile "$DATABRICKS_CONFIG_PROFILE"
databricks workspace import-dir agent "$AGENT_WORKSPACE_PATH"
databricks apps create "$AGENT_APP_NAME"
```

Ensure the app's service principal can query the model selected in `app.yaml`.
For an endpoint with its own permissions, add it as a **Serving endpoint** app
resource with **Can query**, as described in the
[Databricks resource guide](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/model-serving).
Workspace foundation model access may already include the app's identity.
The app's model permission is separate from Airflow's permission to invoke the app.
Then deploy:

```bash
databricks apps deploy "$AGENT_APP_NAME" --source-code-path "$AGENT_WORKSPACE_PATH"
databricks apps get "$AGENT_APP_NAME" --output json
```

Wait for `compute_status.state=ACTIVE` and `app_status.state=RUNNING`. Copy the
`url` returned by `apps get` into `DATABRICKS_AGENT_APP_URL` in the root `.env`.
Use the base URL without `/api/invocations`.

For later code updates, upload into your same directory with
`databricks workspace import-dir agent "$AGENT_WORKSPACE_PATH" --overwrite`,
then run `apps deploy` again. Skip `apps create` for an existing app.
The entrypoint uses the `DATABRICKS_APP_PORT` assigned by Databricks Apps.

## Connect Airflow

The automatic `make up` workflow handles the app URL, caller service principal,
OAuth secret, model token, and `.env` connections. Run `make e2e` afterward to
execute all three DAGs.

If you deployed manually and want to retain full control of that deployment,
follow the [manual connection configuration](../README.md#manual-connection-configuration).
Create or select a caller with workspace access and **CAN_USE** on the app, then
put its credentials and the app URL in the root `.env`. Keep secrets outside
this deployable directory. Use `make up-local` to load them without cloud changes.
The caller is separate from the app's own model-query identity.

For manual tests, use:

```bash
docker compose exec -T airflow python /opt/airflow-tools/run_end_to_end.py
```

## Customize the agent

Edit `SYSTEM_PROMPT` and `query_model` in `hello_agent.py` to change the behavior.
The handler accepts `input.messages` with `user` and `assistant` roles and text
content. It adds its own system prompt and limits each completion to 2,048 tokens.
The synchronous SDK request runs in a thread so background status polling can
continue while the model responds. An empty model answer or inference error
fails the invocation rather than returning a canned answer.

To add tools or use an agent framework, replace that model call while keeping the
invocation handler's JSON-serializable output and `input.messages` interface.

The DAGs continue to use the app URL and OAuth connection. Model access needed
inside the remote handler must be configured for the Databricks App separately
from the caller LLM used by Airflow's `AgentOperator`.

## Runtime state and cleanup

This minimal Apps deployment uses the server's default in-process Runtime Store.
Keep it on one app instance. Invocation records and results are lost on restart;
the example does not configure automatic recovery or durable conversation memory.
Using `DurableAgentServer` alone does not make this deployment's storage persistent.

For persistent run state and crash recovery, follow the supported
[Agent Bricks deployment workflow](https://docs.databricks.com/aws/en/agents/deploy/agent-server#run-state),
which provisions the managed Runtime Store, and register a recovery handler
appropriate to your agent. That is a separate deployment configuration from this
minimal integration example.

Stop your app when you finish testing:

```bash
databricks apps stop "$AGENT_APP_NAME"
```

`make down` stops local Airflow; it does not stop Databricks Apps.
