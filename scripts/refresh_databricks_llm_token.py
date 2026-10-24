"""Refresh only the Databricks caller LLM token; make up does this automatically."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from poc_config import llm_connection, oauth_token, read_env, write_env


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--model")
    args = parser.parse_args()
    env = read_env(args.env_file)
    conn = json.loads(env["AIRFLOW_CONN_DATABRICKS_OAUTH"])
    model = (
        args.model or env.get("DATABRICKS_MODEL_ENDPOINT") or "databricks-gpt-oss-20b"
    )
    token = oauth_token(conn)
    expiry = datetime.now(timezone.utc) + timedelta(seconds=token["expires_in"])
    write_env(
        args.env_file,
        {
            "AIRFLOW_CONN_PYDANTICAI_DEFAULT": llm_connection(
                conn["host"], token, model
            ),
            "DATABRICKS_LLM_TOKEN_EXPIRES_AT": expiry.isoformat(),
        },
    )
    print(f"Configured {model}; OAuth token expires around {expiry.isoformat()}.")
    print("Run make up-local to load the updated connection into Airflow.")


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, KeyError, ValueError) as exc:
        raise SystemExit(str(exc)) from None
