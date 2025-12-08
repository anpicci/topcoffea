"""Compatibility helpers for :mod:`topcoffea` modules.

These helpers are intentionally lightweight and safe to call multiple times.
They provide shims for environments that lack newer typing helpers or optional
pickle streaming support.
"""

from __future__ import annotations

from typing import Any


def ensure_histEFT_py39_compat():
    """Provide a ``Self`` attribute on ``numpy.typing`` when missing.

    Older ``numpy`` releases on Python 3.9 lack ``numpy.typing.Self``, which can
    break imports or unpickling when annotations reference that attribute. This
    helper ensures the attribute is present and mirrors the behaviour of newer
    versions by aliasing it to :class:`typing.Any`.
    """

    try:
        import numpy.typing as npt
    except ModuleNotFoundError:
        return None

    if not hasattr(npt, "Self"):
        npt.Self = Any  # type: ignore[assignment]

    try:
        import topcoffea.modules.histEFT as histEFT
    except Exception:
        return npt

    # Ensure the HistEFT module also exposes the attribute for annotations.
    if not hasattr(histEFT, "Self"):
        histEFT.Self = npt.Self  # type: ignore[assignment]

    return histEFT


def ensure_hist_utils():
    """Import and return the histogram utilities module.

    Centralises the import path for histogram helpers and ensures the fallback
    codepaths in :mod:`topcoffea.modules.hist_utils` are initialised.
    """

    from . import hist_utils

    return hist_utils


__all__ = [
    "ensure_histEFT_py39_compat",
    "ensure_hist_utils",
]
