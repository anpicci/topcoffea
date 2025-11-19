"""Helpers for resolving paths inside the topcoffea python package."""

from __future__ import annotations

import os
from importlib import resources
from pathlib import Path
from typing import Union

import topcoffea

PathLike = Union[str, os.PathLike[str]]


def _package_root() -> Path:
    """Return the on-disk location of the installed topcoffea package."""

    # ``importlib.resources.files`` knows how to resolve package resources even
    # when topcoffea is installed as part of another project or vendored inside
    # an additional folder.  Fall back to ``__path__`` for very old Python
    # versions where ``files`` is not available.
    try:
        files = resources.files  # type: ignore[attr-defined]
    except AttributeError:  # pragma: no cover - legacy Python fallback
        return Path(topcoffea.__path__[0])

    return Path(files(topcoffea))


def topcoffea_path(path_in_repo: PathLike) -> str:
    """Return an absolute path for a resource shipped with topcoffea."""

    package_root = _package_root()
    rel_path = Path(path_in_repo)
    return str((package_root / rel_path).resolve())
