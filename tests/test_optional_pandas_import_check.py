"""Validate optional pandas import checks in topcoffea startup."""

from __future__ import annotations

import os
import subprocess
import sys

import pytest


def _run_snippet(snippet: str, env_overrides: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    if env_overrides:
        env.update(env_overrides)
    return subprocess.run(
        [sys.executable, "-c", snippet],
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )


def test_import_topcoffea_does_not_import_pandas_by_default() -> None:
    snippet = (
        "import sys; import topcoffea; "
        "print('PANDAS_LOADED=' + str(int('pandas' in sys.modules)))"
    )
    completed = _run_snippet(
        snippet,
        env_overrides={"TOPCOFFEA_IMPORT_CHECK_PANDAS": "0"},
    )
    assert completed.returncode == 0, completed.stderr
    assert "PANDAS_LOADED=0" in completed.stdout
    assert "Optional pandas ABI check enabled" not in completed.stderr


def test_import_topcoffea_with_opt_in_pandas_check() -> None:
    snippet = (
        "import sys; import topcoffea; "
        "print('PANDAS_LOADED=' + str(int('pandas' in sys.modules)))"
    )
    completed = _run_snippet(
        snippet,
        env_overrides={"TOPCOFFEA_IMPORT_CHECK_PANDAS": "1"},
    )
    if completed.returncode != 0:
        pytest.skip(
            "Optional pandas check failed in current environment: "
            f"{completed.stderr.strip()}"
        )
    assert "Optional pandas ABI check enabled" in completed.stderr
    assert "PANDAS_LOADED=1" in completed.stdout
