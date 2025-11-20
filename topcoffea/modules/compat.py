"""Compatibility helpers for environments with older dependencies.

This module now re-exports shims that live in :mod:`topcoffea.modules.hist_utils`.
"""

from __future__ import annotations

from .hist_utils import ensure_histEFT_py39_compat, ensure_hist_utils

__all__ = [
    "ensure_histEFT_py39_compat",
    "ensure_hist_utils",
]
