#!/usr/bin/env python3
"""Rotate local service credentials without printing secret values.

This deployment helper is intentionally conservative: it refuses to rotate the
Hummingbot configuration encryption password while connector credential files
exist.  Database, API, MQTT and Condor credentials are updated as one operation;
on failure, the PostgreSQL password and edited files are restored.
"""

from __future__ import annotations

import argparse
import os
import re
import secrets
import shutil
import subprocess
import tempfile
from pathlib import Path

import yaml


ROTATED_KEYS = (
    "PASSWORD",
    "CONFIG_PASSWORD",
    "BROKER_PASSWORD",
    "BROKER_DASHBOARD_PASSWORD",
    "HB_DB_PASSWORD",
    "DATABASE_URL",
    "GATEWAY_PASSPHRASE",
)


def new_secret() -> str:
    # Hex is strong and safe in dotenv, URLs, CSV bootstrap files and SQL literals.
    return secrets.token_hex(32)


def read_dotenv(path: Path) -> tuple[list[str], dict[str, str]]:
    lines = path.read_text(encoding="utf-8").splitlines()
    values: dict[str, str] = {}
    for line in lines:
        if not line or line.lstrip().startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip("'\"")
    return lines, values


def render_dotenv(lines: list[str], replacements: dict[str, str]) -> str:
    rendered: list[str] = []
    seen: set[str] = set()
    for line in lines:
        match = re.match(r"^([A-Za-z_][A-Za-z0-9_]*)=", line)
        if match and match.group(1) in replacements:
            key = match.group(1)
            rendered.append(f"{key}={replacements[key]}")
            seen.add(key)
        else:
            rendered.append(line)
    for key, value in replacements.items():
        if key not in seen:
            rendered.append(f"{key}={value}")
    return "\n".join(rendered) + "\n"


def atomic_write(path: Path, content: str, mode: int = 0o600) -> None:
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(content)
        os.chmod(temp_name, mode)
        os.replace(temp_name, path)
    finally:
        if os.path.exists(temp_name):
            os.unlink(temp_name)


def alter_database_password(password: str) -> None:
    if not re.fullmatch(r"[0-9a-f]{64}", password):
        raise ValueError("database password must be a 64-character hex value")
    sql = f"ALTER ROLE hbot WITH PASSWORD '{password}';"
    subprocess.run(
        [
            "docker",
            "exec",
            "hummingbot-postgres",
            "psql",
            "-v",
            "ON_ERROR_STOP=1",
            "-U",
            "hbot",
            "-d",
            "hummingbot_api",
            "-c",
            sql,
        ],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--api-dir", type=Path, required=True)
    parser.add_argument("--condor-config", type=Path, required=True)
    args = parser.parse_args()

    api_dir = args.api_dir.resolve()
    env_path = api_dir / ".env"
    condor_path = args.condor_config.resolve()
    connector_dir = api_dir / "bots" / "credentials" / "master_account" / "connectors"

    if not env_path.is_file() or not condor_path.is_file():
        raise SystemExit("required deployment configuration is missing")

    connector_files = [
        path for path in connector_dir.glob("*.yml") if path.is_file()
    ]
    if connector_files:
        raise SystemExit(
            "refusing to rotate CONFIG_PASSWORD while connector credential files exist"
        )

    env_lines, old_values = read_dotenv(env_path)
    missing = [key for key in ROTATED_KEYS if key not in old_values]
    if missing:
        raise SystemExit(f"dotenv is missing required keys: {', '.join(missing)}")

    condor_data = yaml.safe_load(condor_path.read_text(encoding="utf-8"))
    local_server = condor_data.get("servers", {}).get("local")
    if not isinstance(local_server, dict):
        raise SystemExit("Condor local server configuration is missing")

    api_password = new_secret()
    config_password = new_secret()
    broker_password = new_secret()
    broker_dashboard_password = new_secret()
    db_password = new_secret()
    replacements = {
        "PASSWORD": api_password,
        "CONFIG_PASSWORD": config_password,
        "BROKER_PASSWORD": broker_password,
        "BROKER_DASHBOARD_PASSWORD": broker_dashboard_password,
        "HB_DB_PASSWORD": db_password,
        "DATABASE_URL": (
            "postgresql+asyncpg://hbot:"
            f"{db_password}@postgres:5432/hummingbot_api"
        ),
        "GATEWAY_PASSPHRASE": config_password,
    }

    old_env_text = env_path.read_text(encoding="utf-8")
    old_condor_text = condor_path.read_text(encoding="utf-8")
    db_changed = False
    try:
        alter_database_password(db_password)
        db_changed = True
        atomic_write(env_path, render_dotenv(env_lines, replacements))

        local_server["password"] = api_password
        atomic_write(
            condor_path,
            yaml.safe_dump(condor_data, allow_unicode=True, sort_keys=False),
        )

        # No exchange credentials exist, so these can be safely regenerated with
        # the new CONFIG_PASSWORD at the next API start.
        verification = (
            api_dir
            / "bots"
            / "credentials"
            / "master_account"
            / ".password_verification"
        )
        verification.unlink(missing_ok=True)

        certs_dir = api_dir / "bots" / "gateway-files" / "certs"
        if certs_dir.exists():
            expected = (api_dir / "bots" / "gateway-files").resolve()
            if expected not in certs_dir.resolve().parents:
                raise RuntimeError("refusing to remove an unexpected certificate path")
            shutil.rmtree(certs_dir)
    except Exception:
        atomic_write(env_path, old_env_text)
        atomic_write(condor_path, old_condor_text)
        if db_changed:
            old_db_password = old_values["HB_DB_PASSWORD"]
            alter_database_password(old_db_password)
        raise

    print("Local API, MQTT, database and encryption credentials rotated successfully.")
    print("No secret values were printed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
