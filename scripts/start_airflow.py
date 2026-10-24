"""Set the local demo login before starting Airflow standalone."""

import json
import os
from pathlib import Path

password_file = (
    Path(os.environ.get("AIRFLOW_HOME", "/opt/airflow"))
    / "simple_auth_manager_passwords.json.generated"
)
passwords = json.loads(password_file.read_text()) if password_file.exists() else {}
passwords["admin"] = os.environ.get("AIRFLOW_LOCAL_ADMIN_PASSWORD", "admin")
password_file.write_text(json.dumps(passwords))
password_file.chmod(0o600)
os.execvp("airflow", ["airflow", "standalone"])
