import importlib
import importlib.machinery
import sys
from types import SimpleNamespace

import pytest

from topcoffea.modules import hist_utils


@pytest.fixture(autouse=True)
def _reset_histEFT_module():
    target = "topcoffea.modules.histEFT"
    preexisting = sys.modules.pop(target, None)
    try:
        yield
    finally:
        if preexisting is not None:
            sys.modules[target] = preexisting


def test_py39_histEFT_patch(monkeypatch):
    def fake_get_source(fullname):
        assert fullname == "topcoffea.modules.histEFT"
        return """
from __future__ import annotations
from typing import Mapping

ArrayLike = object
value: ArrayLike | Mapping | None = None
"""

    loader = SimpleNamespace(get_source=fake_get_source)
    fake_spec = importlib.machinery.ModuleSpec("topcoffea.modules.histEFT", loader)

    real_find_spec = importlib.util.find_spec

    def fake_find_spec(fullname, *args, **kwargs):
        if fullname == "topcoffea.modules.histEFT":
            return fake_spec
        return real_find_spec(fullname, *args, **kwargs)

    monkeypatch.setattr(hist_utils.sys, "version_info", (3, 9, 0))
    monkeypatch.setattr(importlib.util, "find_spec", fake_find_spec)

    hist_utils.ensure_histEFT_py39_compat()

    patched_module = sys.modules.get("topcoffea.modules.histEFT")
    assert patched_module is not None
    assert patched_module.__annotations__["value"] == "Union[ArrayLike, Mapping, None]"
def test_hist_utils_fallback(monkeypatch):
    real_import_module = hist_utils.importlib.import_module

    def fake_import_module(name, *args, **kwargs):
        if name == "topcoffea.modules.hist_utils":
            raise ModuleNotFoundError
        return real_import_module(name, *args, **kwargs)

    monkeypatch.setattr(hist_utils.importlib, "import_module", fake_import_module)

    hist_utils.ensure_hist_utils()

    module_name = "topcoffea.modules.hist_utils"
    assert module_name in sys.modules
    assert sys.modules[module_name] is hist_utils
    assert getattr(importlib.import_module("topcoffea.modules"), "hist_utils") is hist_utils


def test_compat_module_exposes_shims():
    compat = importlib.import_module("topcoffea.modules.compat")

    assert compat.ensure_histEFT_py39_compat is hist_utils.ensure_histEFT_py39_compat
    assert compat.ensure_hist_utils is hist_utils.ensure_hist_utils
    assert compat.iterate_hist_from_pkl is hist_utils.iterate_hist_from_pkl
    assert compat.iterate_histograms_from_pkl is hist_utils.iterate_histograms_from_pkl
