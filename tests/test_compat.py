import importlib
import importlib.machinery
import sys
from types import SimpleNamespace

import pytest

from topcoffea.modules import compat as compat_mod


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

    monkeypatch.setattr(compat_mod.sys, "version_info", (3, 9, 0))
    monkeypatch.setattr(importlib.util, "find_spec", fake_find_spec)

    compat_mod.ensure_histEFT_py39_compat()

    patched_module = sys.modules.get("topcoffea.modules.histEFT")
    assert patched_module is not None
    assert patched_module.__annotations__["value"] == "Union[ArrayLike, Mapping, None]"


def test_hist_utils_fallback(monkeypatch):
    real_import_module = importlib.import_module

    def fake_import_module(name, *args, **kwargs):
        if name == "topcoffea.modules.hist_utils":
            raise ModuleNotFoundError
        return real_import_module(name, *args, **kwargs)

    monkeypatch.setattr(compat_mod.importlib, "import_module", fake_import_module)

    compat_mod.ensure_hist_utils()

    module_name = "topcoffea.modules.hist_utils"
    assert module_name in sys.modules
    assert sys.modules[module_name] is compat_mod._fallback_hist_utils
    assert getattr(importlib.import_module("topcoffea.modules"), "hist_utils") is compat_mod._fallback_hist_utils
