import importlib
import os
import sys
from pathlib import Path

from topcoffea.modules import paths as current_paths


def _clear_topcoffea_modules():
    removed = {}
    for name in list(sys.modules):
        if name == "topcoffea" or name.startswith("topcoffea."):
            removed[name] = sys.modules.pop(name)
    return removed


def test_topcoffea_path_matches_current_installation():
    pileup = current_paths.topcoffea_path("data/pileup/pileup_2016GH.root")
    assert Path(pileup).is_file()


def test_topcoffea_path_handles_nested_checkout(tmp_path, monkeypatch):
    nested_repo = tmp_path / "outer" / "repo" / "src"
    nested_repo.mkdir(parents=True)

    package_root = Path(current_paths._package_root())
    nested_pkg = nested_repo / "topcoffea"

    # Mimic a checkout with an extra top-level folder by symlinking the real
    # package into the fake nested repository and importing from there.
    os.symlink(package_root, nested_pkg, target_is_directory=True)
    monkeypatch.syspath_prepend(str(nested_repo))

    removed = _clear_topcoffea_modules()
    try:
        nested_paths = importlib.import_module("topcoffea.modules.paths")
        pileup = nested_paths.topcoffea_path("data/pileup/pileup_2016GH.root")
        assert Path(pileup).is_file()
    finally:
        _clear_topcoffea_modules()
        sys.modules.update(removed)
