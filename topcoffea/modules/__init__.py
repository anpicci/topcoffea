"""Topcoffea analysis modules."""

from __future__ import annotations

import importlib
import sys
from types import ModuleType


def _install_compat_shim() -> None:
    """Expose histogram compatibility helpers under ``topcoffea.modules.compat``.

    The compat module is synthesized at import time so callers can rely on the
    documented import path without requiring a dedicated source file.
    """

    fullname = "topcoffea.modules.compat"
    if fullname in sys.modules:
        return

    hist_utils = importlib.import_module("topcoffea.modules.hist_utils")

    compat = ModuleType(fullname)
    compat.ensure_histEFT_py39_compat = hist_utils.ensure_histEFT_py39_compat
    compat.ensure_hist_utils = hist_utils.ensure_hist_utils
    compat.iterate_hist_from_pkl = hist_utils.iterate_hist_from_pkl
    compat.iterate_histograms_from_pkl = hist_utils.iterate_histograms_from_pkl
    compat.__all__ = [
        "ensure_histEFT_py39_compat",
        "ensure_hist_utils",
        "iterate_hist_from_pkl",
        "iterate_histograms_from_pkl",
    ]

    sys.modules[fullname] = compat
    setattr(sys.modules[__name__], "compat", compat)


def _ensure_compat_available() -> None:
    # Install the shim once the package is imported so users can load
    # ``topcoffea.modules.compat`` directly.
    _install_compat_shim()


_ensure_compat_available()
