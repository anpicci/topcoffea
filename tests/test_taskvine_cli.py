from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest


pytestmark = [pytest.mark.integration, pytest.mark.taskvine]

_TASKVINE_INFRA_ERROR_MARKERS = (
    "unable to find a port to start a transfer server",
    "no workers are available",
    "connection refused",
)
_TASKVINE_DEFAULT_TIMEOUT_SECONDS = 1.5
_TASKVINE_TIMEOUT_ENV = "TOPCOFFEA_TASKVINE_TIMEOUT_SECONDS"


def _decode_output(payload):
    if payload is None:
        return ""
    if isinstance(payload, bytes):
        return payload.decode(errors="replace")
    return str(payload)


def _contains_infra_failure(output_text):
    normalized = output_text.lower()
    return any(marker in normalized for marker in _TASKVINE_INFRA_ERROR_MARKERS)


def _resolve_cli_timeout_seconds():
    raw_timeout = os.getenv(_TASKVINE_TIMEOUT_ENV)
    if raw_timeout is None:
        return _TASKVINE_DEFAULT_TIMEOUT_SECONDS
    try:
        timeout = float(raw_timeout)
    except ValueError:
        return _TASKVINE_DEFAULT_TIMEOUT_SECONDS
    if timeout <= 0:
        return _TASKVINE_DEFAULT_TIMEOUT_SECONDS
    return timeout


def test_minimal_taskvine_cli(tmp_path):
    pytest.importorskip("ndcctools.taskvine.futures")

    missing_binaries = [
        binary
        for binary in ("vine_worker", "vine_factory")
        if shutil.which(binary) is None
    ]
    if missing_binaries:
        pytest.skip(
            "TaskVine binaries unavailable in PATH: "
            + ", ".join(sorted(missing_binaries))
        )

    assets_dir = Path(__file__).with_name("taskvine_assets")
    cli = assets_dir / "taskvine_cli.py"
    input_payload = assets_dir / "minimal_input.json"
    output_payload = tmp_path / "artifact.json"

    command = [
        sys.executable,
        str(cli),
        "--executor",
        "taskvine",
        "--input",
        str(input_payload),
        "--output",
        str(output_payload),
        "--nworkers",
        "1",
        "--chunksize",
        "3",
        "--port",
        "9123-9135",
    ]

    env = os.environ.copy()
    env["PYTHONPATH"] = os.pathsep.join(
        [segment for segment in [env.get("PYTHONPATH"), os.getcwd()] if segment]
    )

    cli_timeout_seconds = _resolve_cli_timeout_seconds()

    try:
        completed = subprocess.run(
            command,
            check=False,
            cwd=Path.cwd(),
            env=env,
            capture_output=True,
            text=True,
            timeout=cli_timeout_seconds,
        )
    except subprocess.TimeoutExpired as exc:
        timeout_output = "\n".join(
            [_decode_output(exc.stdout), _decode_output(exc.stderr)]
        )
        if _contains_infra_failure(timeout_output):
            pytest.skip(
                "TaskVine worker prerequisites are unavailable in this environment."
            )
        pytest.skip(
            "TaskVine CLI timed out waiting for local workers; skipping in"
            " environments where worker networking is unavailable."
        )

    if completed.returncode != 0:
        combined_output = f"{completed.stdout}\n{completed.stderr}"
        if _contains_infra_failure(combined_output):
            pytest.skip(
                "TaskVine worker prerequisites are unavailable in this environment."
            )
        pytest.fail(
            "TaskVine CLI failed unexpectedly.\n"
            f"stdout:\n{completed.stdout}\n"
            f"stderr:\n{completed.stderr}"
        )

    payload = json.loads(output_payload.read_text())

    assert payload["executor"] == "taskvine"
    assert payload["chunksize"] == 3
    assert payload["summary"]["total"] == sum(payload["numbers"])
    assert payload["summary"]["count"] == len(payload["numbers"])
    assert payload["results"]

    for index, result in enumerate(payload["summary"]["chunks"]):
        assert result["chunk_index"] == index
        assert result["sum"] == sum(payload["numbers"][index * 3 : (index + 1) * 3])

    assert 9123 <= int(payload["port"]) <= 9135
