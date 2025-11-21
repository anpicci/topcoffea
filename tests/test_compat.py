import importlib.util
import sys

from topcoffea.modules import compat, hist_utils


def test_compat_exports_shims():
    assert compat.ensure_hist_utils is hist_utils.ensure_hist_utils
    assert compat.ensure_histEFT_py39_compat is hist_utils.ensure_histEFT_py39_compat


def test_ensure_hist_utils_imports(monkeypatch):
    monkeypatch.delitem(sys.modules, "topcoffea.modules.hist_utils", raising=False)

    module = compat.ensure_hist_utils()

    assert module is sys.modules["topcoffea.modules.hist_utils"]
    assert module.__name__ == "topcoffea.modules.hist_utils"
    assert module is __import__("topcoffea.modules.hist_utils", fromlist=["*"])


def test_ensure_histEFT_py39_compat_imports(monkeypatch):
    if importlib.util.find_spec("hist") is None:
        import pytest

        pytest.skip("hist dependency is unavailable in this environment")

    monkeypatch.delitem(sys.modules, "topcoffea.modules.histEFT", raising=False)

    module = compat.ensure_histEFT_py39_compat()

    assert module.__name__ == "topcoffea.modules.histEFT"
    assert module is sys.modules["topcoffea.modules.histEFT"]
