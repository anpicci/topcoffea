import pytest

from topcoffea.modules import compat


def test_ensure_histEFT_py39_compat_adds_self(monkeypatch):
    try:
        import numpy.typing as npt
    except ModuleNotFoundError:
        pytest.skip("numpy.typing is unavailable")

    monkeypatch.delattr(npt, "Self", raising=False)

    module = compat.ensure_histEFT_py39_compat()

    assert hasattr(npt, "Self")
    assert module is not None
    assert getattr(module, "Self", None) is getattr(npt, "Self")


def test_ensure_hist_utils_imports_module():
    module = compat.ensure_hist_utils()

    assert hasattr(module, "iterate_hist_from_pkl")
    assert hasattr(module, "HAS_STREAMING_SUPPORT")
