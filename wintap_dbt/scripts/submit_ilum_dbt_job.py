#!/usr/bin/env python3
"""Submit a Wintap dbt proof-of-concept job to Ilum.

The Ilum REST API has evolved across releases, so this script keeps the payload
simple and configurable. Override ILUM_JOB_PAYLOAD_JSON to send an exact payload
for your deployment; otherwise a reasonable Spark app payload is generated from
environment variables.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from typing import Any


TERMINAL_STATES = {"COMPLETED", "SUCCEEDED", "SUCCESS", "FAILED", "ERROR", "KILLED", "CANCELLED"}
SUCCESS_STATES = {"COMPLETED", "SUCCEEDED", "SUCCESS"}


def env(name: str, default: str | None = None, required: bool = False) -> str:
    value = os.environ.get(name, default)
    if required and not value:
        raise SystemExit(f"{name} is required")
    return value or ""


def request_json(method: str, url: str, token: str, payload: dict[str, Any] | None = None) -> Any:
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    headers = {"Accept": "application/json"}
    if data is not None:
        headers["Content-Type"] = "application/json"
    if token:
        headers["Authorization"] = f"Bearer {token}"

    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=int(env("ILUM_API_TIMEOUT", "60"))) as resp:
            body = resp.read().decode("utf-8")
            if not body:
                return {}
            return json.loads(body)
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise SystemExit(f"{method} {url} failed: HTTP {exc.code}\n{body}") from exc


def default_payload() -> dict[str, Any]:
    repo = env("ILUM_GIT_REPO", "https://github.com/LLNL/Wintap-PyUtil.git")
    branch = env("ILUM_GIT_BRANCH", "grants-add-dbt")
    command = env("ILUM_DBT_COMMAND", "dbt build --project-dir wintap_dbt --profiles-dir wintap_dbt --target ilum --select poc_s3_raw_process")

    # Command run by the submitted app. Assumes the Ilum image has git, Python,
    # dbt-spark, and the needed Hadoop/S3 Spark libraries available.
    shell_command = " && ".join(
        [
            "set -euxo pipefail",
            "rm -rf /tmp/Wintap-PyUtil",
            f"git clone --branch {branch} --depth 1 {repo} /tmp/Wintap-PyUtil",
            "cd /tmp/Wintap-PyUtil",
            command,
        ]
    )

    return {
        "name": env("ILUM_JOB_NAME", "wintap-dbt-poc"),
        "type": env("ILUM_JOB_TYPE", "sparkSubmit"),
        "sparkConf": {
            "spark.sql.catalogImplementation": env("SPARK_SQL_CATALOG_IMPLEMENTATION", "hive"),
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        },
        "env": {
            "WINTAP_DBT_TARGET": "ilum",
            "WINTAP_DBT_DATASET": env("WINTAP_DBT_DATASET", required=True),
            "WINTAP_DBT_START_DAY": env("WINTAP_DBT_START_DAY", required=True),
            "WINTAP_DBT_END_DAY": env("WINTAP_DBT_END_DAY", required=True),
            "ILUM_KYUUBI_HOST": env("ILUM_KYUUBI_HOST", required=True),
            "ILUM_KYUUBI_PORT": env("ILUM_KYUUBI_PORT", "10009"),
            "WINTAP_DBT_SCHEMA": env("WINTAP_DBT_SCHEMA", "wintap"),
        },
        "mainApplicationFile": "local:///opt/spark/bin/spark-submit",
        "arguments": ["/bin/bash", "-lc", shell_command],
    }


def load_payload() -> dict[str, Any]:
    payload_json = os.environ.get("ILUM_JOB_PAYLOAD_JSON")
    if payload_json:
        return json.loads(payload_json)

    payload_file = os.environ.get("ILUM_JOB_PAYLOAD_FILE")
    if payload_file:
        with open(payload_file, "r", encoding="utf-8") as f:
            return json.load(f)

    return default_payload()


def extract_group_id(response: Any) -> str:
    if isinstance(response, dict):
        for key in ("groupId", "group_id", "id", "jobId", "job_id", "name"):
            if response.get(key):
                return str(response[key])
        for value in response.values():
            if isinstance(value, dict):
                found = extract_group_id(value)
                if found:
                    return found
    return ""


def extract_state(response: Any) -> str:
    if isinstance(response, dict):
        for key in ("state", "status", "phase"):
            if response.get(key):
                return str(response[key]).upper()
        for value in response.values():
            if isinstance(value, dict):
                found = extract_state(value)
                if found:
                    return found
    return ""


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--no-wait", action="store_true", help="submit and print the response without polling")
    parser.add_argument("--poll-interval", type=int, default=int(env("ILUM_POLL_INTERVAL", "15")))
    parser.add_argument("--max-wait-seconds", type=int, default=int(env("ILUM_MAX_WAIT_SECONDS", "3600")))
    args = parser.parse_args()

    api_url = env("ILUM_API_URL", required=True).rstrip("/")
    token = env("ILUM_API_TOKEN", "")
    payload = load_payload()

    print("Submitting Ilum dbt job payload:")
    print(json.dumps(payload, indent=2, sort_keys=True))
    response = request_json("POST", f"{api_url}/api/v1/jobs", token, payload)
    print("Submit response:")
    print(json.dumps(response, indent=2, sort_keys=True))

    group_id = extract_group_id(response)
    if args.no_wait or not group_id:
        return 0

    deadline = time.time() + args.max_wait_seconds
    status_url = f"{api_url}/api/v1/jobs/{group_id}"
    while time.time() < deadline:
        status = request_json("GET", status_url, token)
        state = extract_state(status)
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {group_id}: {state or 'UNKNOWN'}")
        if state in TERMINAL_STATES:
            print(json.dumps(status, indent=2, sort_keys=True))
            return 0 if state in SUCCESS_STATES else 1
        time.sleep(args.poll_interval)

    print(f"Timed out waiting for {group_id}", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
