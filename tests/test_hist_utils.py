import gzip
import importlib
import importlib.machinery
import pickle
import sys
from types import SimpleNamespace

import pytest

from topcoffea.modules import hist_utils


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


@pytest.fixture(autouse=True)
def _reset_histEFT_module():
    target = "topcoffea.modules.histEFT"
    preexisting = sys.modules.pop(target, None)
    try:
        yield
    finally:
        if preexisting is not None:
            sys.modules[target] = preexisting


class _DummyHist:
    def __init__(self, name: str, empty_flag: bool):
        self.name = name
        self._empty_flag = empty_flag

    def empty(self):
        return self._empty_flag

    def __eq__(self, other):
        return isinstance(other, _DummyHist) and (self.name, self._empty_flag) == (
            other.name,
            other._empty_flag,
        )


@pytest.mark.skipif(
    not hist_utils.HAS_STREAMING_SUPPORT,
    reason="Streaming helpers are unavailable on this Python version",
)
def test_streaming_unpickler_filters_empty(tmp_path):
    hist_map = {
        "keep": _DummyHist("keep", False),
        "drop": _DummyHist("drop", True),
    }

    path = tmp_path / "hist.pkl.gz"
    with gzip.open(path, "wb") as fout:
        pickle.dump(hist_map, fout, protocol=pickle.HIGHEST_PROTOCOL)

    loaded = hist_utils.iterate_hist_from_pkl(
        str(path), allow_empty=False, materialize=True
    )

    assert loaded == {"keep": hist_map["keep"]}
