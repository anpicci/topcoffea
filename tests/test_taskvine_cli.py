from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest


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

    subprocess.run(command, check=True, cwd=Path.cwd(), env=env)

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
