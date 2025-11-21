"""Compatibility shims for histogram helpers.

This module re-exports the public compat helpers from :mod:`topcoffea.modules.hist_utils`
so callers can rely on the documented ``topcoffea.modules.compat`` import path.
"""

from __future__ import annotations

from .hist_utils import (
    ensure_histEFT_py39_compat,
    ensure_hist_utils,
    iterate_hist_from_pkl,
    iterate_histograms_from_pkl,
)

__all__ = [
    "ensure_histEFT_py39_compat",
    "ensure_hist_utils",
    "iterate_hist_from_pkl",
    "iterate_histograms_from_pkl",
]
