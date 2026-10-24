"""Exercise setup reconciliation without creating cloud resources."""

from __future__ import annotations

import contextlib
import copy
import io
import json
import shutil
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import bootstrap_databricks as setup  # noqa: E402
from poc_config import OAuthRejected, connection_json, read_env, write_env  # noqa: E402

HOST = "https://example.cloud.databricks.com"


class FakeCloud:
    def __init__(self):
        self.app = None
        self.principal = None
        self.app_acl = []
        self.calls = []
        self.secret_count = 0
        self.token_count = 0

    def token(self, conn):
        if conn["host"] != HOST:
            raise AssertionError("Credential sent to the wrong workspace")
        self.token_count += 1
        return {"access_token": f"test-token-{self.token_count}", "expires_in": 3600}

    def __call__(self, *args, body=None, raw=False):
        self.calls.append((args, body))
        command = args[:2]
        if command == ("auth", "profiles"):
            return {"profiles": [{"name": "demo", "host": HOST, "default": True}]}
        if command == ("current-user", "me"):
            return {"userName": "test@example.com"}
        if command == ("serving-endpoints", "get"):
            return {"name": args[2], "task": "llm/v1/chat", "state": {"ready": "READY"}}
        if command == ("apps", "list"):
            return [copy.deepcopy(self.app)] if self.app else []
        if command == ("apps", "create"):
            self.app = {
                "name": args[2],
                "url": "https://test.databricksapps.com",
                "service_principal_client_id": "app-client-id",
                "compute_status": {"state": "STOPPED"},
            }
            return copy.deepcopy(self.app)
        if command == ("apps", "get"):
            return copy.deepcopy(self.app)
        if command == ("service-principals", "list"):
            return [copy.deepcopy(self.principal)] if self.principal else []
        if command == ("service-principals", "create"):
            self.principal = {**body, "id": "123", "applicationId": "caller-client-id"}
            return copy.deepcopy(self.principal)
        if command == ("service-principal-secrets-proxy", "create"):
            self.secret_count += 1
            return {
                "secret": f"test-secret-{self.secret_count}",
                "expire_time": "2030-01-01T00:00:00Z",
            }
        if command == ("apps", "get-permissions"):
            return {"access_control_list": copy.deepcopy(self.app_acl)}
        if command == ("apps", "update-permissions"):
            self.app_acl = [
                {
                    "service_principal_name": x["service_principal_name"],
                    "all_permissions": [{"permission_level": x["permission_level"]}],
                }
                for x in body["access_control_list"]
            ]
            return {}
        if command == ("apps", "start"):
            self.app["compute_status"] = {"state": "ACTIVE"}
            return copy.deepcopy(self.app)
        if command == ("workspace", "import-dir"):
            files = sorted(p.name for p in Path(args[2]).iterdir())
            if files != sorted(setup.AGENT_FILES):
                raise AssertionError(f"Unexpected upload contents: {files}")
            return ""
        if command == ("apps", "deploy"):
            self.app["active_deployment"] = {
                "source_code_path": args[4],
                "status": {"state": "SUCCEEDED"},
            }
            self.app["app_status"] = {"state": "RUNNING"}
            return {}
        raise AssertionError(f"Unexpected CLI command: {command}")

    def count(self, *command):
        return sum(args[: len(command)] == command for args, _ in self.calls)


class SetupTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        (self.root / ".databricks").mkdir()
        (self.root / "agent").mkdir()
        for name in setup.AGENT_FILES:
            shutil.copy(setup.ROOT / "agent" / name, self.root / "agent" / name)
        shutil.copy(setup.ROOT / ".env.example", self.root / ".env")
        self.cloud = FakeCloud()
        self.output = io.StringIO()
        self.enterContext(
            patch.object(setup, "CLI", side_effect=lambda *args: self.cloud)
        )
        self.enterContext(
            patch.object(setup, "oauth_token", side_effect=self.cloud.token)
        )
        self.enterContext(contextlib.redirect_stdout(self.output))

    def run_setup(self, **kwargs):
        setup.bootstrap(self.root, **kwargs)
        return read_env(self.root / ".env")

    def test_fresh_setup_and_repeat_reuse_resources_but_refresh_token(self):
        first = self.run_setup()
        second = self.run_setup()
        self.assertEqual(self.cloud.count("apps", "create"), 1)
        self.assertEqual(self.cloud.count("apps", "deploy"), 1)
        self.assertEqual(self.cloud.count("service-principals", "create"), 1)
        self.assertEqual(self.cloud.secret_count, 1)
        self.assertEqual(
            first["AIRFLOW_CONN_DATABRICKS_OAUTH"],
            second["AIRFLOW_CONN_DATABRICKS_OAUTH"],
        )
        self.assertNotEqual(
            first["AIRFLOW_CONN_PYDANTICAI_DEFAULT"],
            second["AIRFLOW_CONN_PYDANTICAI_DEFAULT"],
        )
        self.assertEqual((self.root / ".env").stat().st_mode & 0o777, 0o600)
        self.assertNotIn("test-secret", self.output.getvalue())
        self.assertNotIn("test-token", self.output.getvalue())

    def test_source_or_model_change_redeploys_without_new_secret(self):
        self.run_setup()
        with (self.root / "agent/hello_agent.py").open("a") as output:
            output.write("\n# deployment change\n")
        self.run_setup()
        self.run_setup(model="another-chat-model")
        self.assertEqual(self.cloud.count("apps", "deploy"), 3)
        self.assertEqual(self.cloud.secret_count, 1)

    def test_invalid_secret_is_replaced_once(self):
        self.run_setup()

        def token(conn):
            if conn["password"] == "test-secret-1":
                raise OAuthRejected("invalid_client")
            return self.cloud.token(conn)

        with patch.object(setup, "oauth_token", side_effect=token):
            self.run_setup()
            self.run_setup()
        self.assertEqual(self.cloud.secret_count, 2)

    def test_network_error_does_not_rotate_or_overwrite_credentials(self):
        self.run_setup()
        original = (self.root / ".env").read_bytes()
        with patch.object(
            setup, "oauth_token", side_effect=RuntimeError("Network unavailable")
        ):
            with self.assertRaisesRegex(RuntimeError, "Network"):
                self.run_setup()
        self.assertEqual(self.cloud.secret_count, 1)
        self.assertEqual((self.root / ".env").read_bytes(), original)

    def test_custom_llm_and_other_env_values_are_preserved(self):
        custom = connection_json(
            {
                "conn_type": "pydanticai",
                "password": "external-key",
                "extra": {"model": "openai:example"},
            }
        )
        write_env(
            self.root / ".env",
            {"AIRFLOW_CONN_PYDANTICAI_DEFAULT": custom, "AIRFLOW_PORT": "8089"},
        )
        result = self.run_setup()
        self.assertEqual(result["AIRFLOW_CONN_PYDANTICAI_DEFAULT"], custom)
        self.assertEqual(result["AIRFLOW_PORT"], "8089")

    def test_partial_deployment_failure_does_not_generate_more_secrets(self):
        original_cli = self.cloud.__call__

        def fail_deploy(*args, **kwargs):
            if args[:2] == ("apps", "deploy"):
                raise RuntimeError("deployment failed")
            return original_cli(*args, **kwargs)

        with patch.object(setup, "CLI", side_effect=lambda *args: fail_deploy):
            with self.assertRaisesRegex(RuntimeError, "deployment failed"):
                self.run_setup()
        self.run_setup()
        self.assertEqual(self.cloud.secret_count, 1)

    def test_existing_app_is_discovered_by_url(self):
        env = self.run_setup(app_name="existing-poc-app")
        write_env(self.root / ".env", {"DATABRICKS_APP_NAME": ""})
        result = self.run_setup()
        self.assertEqual(result["DATABRICKS_APP_NAME"], "existing-poc-app")
        self.assertEqual(
            env["DATABRICKS_AGENT_APP_URL"], result["DATABRICKS_AGENT_APP_URL"]
        )
        self.assertEqual(self.cloud.count("apps", "create"), 1)

    def test_connection_round_trip_preserves_secret_characters(self):
        secret = "literal$not_expanded'and\\slashes"
        write_env(
            self.root / ".env", {"CONNECTION": connection_json({"password": secret})}
        )
        self.assertEqual(
            json.loads(read_env(self.root / ".env")["CONNECTION"])["password"], secret
        )


if __name__ == "__main__":
    unittest.main()
