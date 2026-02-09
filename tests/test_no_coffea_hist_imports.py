from __future__ import annotations

import subprocess
from pathlib import Path


_ROOT = Path(__file__).resolve().parents[1]
_TEXT_SUFFIXES = {".py", ".md", ".rst", ".txt", ".ipynb"}
_LEGACY_MODULE_TOKEN = "coffea" + ".hist"
_LEGACY_IMPORT_TOKEN = "from coffea import " + "hist"


def _tracked_text_files() -> list[Path]:
    completed = subprocess.run(
        ["git", "ls-files"],
        cwd=_ROOT,
        check=True,
        text=True,
        capture_output=True,
    )
    files: list[Path] = []
    for relpath in completed.stdout.splitlines():
        candidate = _ROOT / relpath
        if candidate.exists() and candidate.suffix in _TEXT_SUFFIXES:
            files.append(candidate)
    return files


def test_no_legacy_coffea_hist_references() -> None:
    matches: list[str] = []
    for path in _tracked_text_files():
        relpath = path.relative_to(_ROOT)
        for lineno, line in enumerate(path.read_text(encoding="utf-8", errors="ignore").splitlines(), start=1):
            if _LEGACY_MODULE_TOKEN in line or _LEGACY_IMPORT_TOKEN in line:
                matches.append(f"{relpath}:{lineno}:{line.strip()}")

    assert not matches, "Legacy coffea histogram imports are forbidden:\n" + "\n".join(matches)
