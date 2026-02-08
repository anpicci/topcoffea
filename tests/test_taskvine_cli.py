from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest


_TASKVINE_INFRA_ERROR_MARKERS = (
    "unable to find a port to start a transfer server",
    "no workers are available",
    "connection refused",
)
_TASKVINE_CLI_TIMEOUT_SECONDS = 30


def test_minimal_taskvine_cli(tmp_path):
    pytest.importorskip("ndcctools.taskvine.futures")

    if shutil.which("vine_worker") is None:
        pytest.skip("TaskVine worker binary not available in PATH")

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

    try:
        completed = subprocess.run(
            command,
            check=False,
            cwd=Path.cwd(),
            env=env,
            capture_output=True,
            text=True,
            timeout=_TASKVINE_CLI_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired:
        pytest.skip(
            "TaskVine CLI timed out waiting for local workers; skipping in"
            " environments where worker networking is unavailable."
        )

    if completed.returncode != 0:
        combined_output = f"{completed.stdout}\n{completed.stderr}".lower()
        if any(marker in combined_output for marker in _TASKVINE_INFRA_ERROR_MARKERS):
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
