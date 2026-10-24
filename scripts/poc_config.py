"""Local configuration and OAuth helpers; never print credentials."""

from __future__ import annotations

import base64
import json
import os
import shlex
import tempfile
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


class OAuthRejected(RuntimeError):
    """The client credential is invalid, so a replacement may be needed."""


def read_env(path: Path) -> dict[str, str]:
    values = {}
    if not path.exists():
        return values
    for line in path.read_text().splitlines():
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        key, separator, value = line.partition("=")
        key = key.strip()
        if not separator or key in values:
            raise RuntimeError(f"Invalid or duplicate setting in {path}: {key}")
        parts = shlex.split(value, comments=True)
        if len(parts) > 1:
            raise RuntimeError(f"Quote the value of {key} in {path}")
        values[key] = parts[0] if parts else ""
    return values


def connection_json(value: dict) -> str:
    # JSON's Unicode escape preserves apostrophes inside dotenv single quotes.
    return json.dumps(value, separators=(",", ":")).replace("'", "\\u0027")


def write_env(path: Path, updates: dict[str, str]) -> None:
    read_env(path)  # Reject ambiguity before changing an existing file.
    for value in updates.values():
        if any(char in value for char in ("'", "\n", "\r")):
            raise RuntimeError("Configuration values must fit in dotenv single quotes")
    remaining = dict(updates)
    lines = []
    for line in path.read_text().splitlines() if path.exists() else []:
        key = line.partition("=")[0].strip()
        if not line.lstrip().startswith("#") and key in remaining:
            line = f"{key}='{remaining.pop(key)}'"
        lines.append(line)
    lines.extend(f"{key}='{value}'" for key, value in remaining.items())
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", dir=path.parent, prefix=".env.", delete=False
        ) as output:
            temporary = Path(output.name)
            os.chmod(temporary, 0o600)
            output.write("\n".join(lines) + "\n")
        temporary.replace(path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def oauth_token(conn: dict) -> dict:
    host = conn["host"].rstrip("/")
    if not host.startswith("https://"):
        raise RuntimeError("The workspace host must use HTTPS")
    credentials = base64.b64encode(
        f"{conn['login']}:{conn['password']}".encode()
    ).decode()
    request = Request(
        host + "/oidc/v1/token",
        data=b"grant_type=client_credentials&scope=all-apis",
        headers={
            "Authorization": "Basic " + credentials,
            "Content-Type": "application/x-www-form-urlencoded",
        },
    )
    try:
        with urlopen(request, timeout=30) as response:
            return json.load(response)
    except HTTPError as exc:
        try:
            error = json.loads(exc.read()).get("error")
        except (ValueError, AttributeError):
            error = None
        if error == "invalid_client":
            raise OAuthRejected("Databricks rejected the client credential") from None
        raise RuntimeError(
            f"Databricks OAuth failed: HTTP {exc.code}; credentials were not rotated"
        ) from None
    except URLError:
        raise RuntimeError(
            "Could not reach Databricks OAuth; credentials were not rotated"
        ) from None


def llm_connection(host: str, token: dict, model: str) -> str:
    return connection_json(
        {
            "conn_type": "pydanticai",
            "host": host.rstrip("/") + "/serving-endpoints",
            "password": token["access_token"],
            "extra": {"model": "openai-chat:" + model},
        }
    )
