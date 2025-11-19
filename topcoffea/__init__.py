"""Expose the public ``topcoffea`` namespace."""

from __future__ import annotations

from importlib import import_module
from pathlib import Path
from typing import Any

try:  # Python 3.8 compat (fallback used while running from source)
    from importlib.metadata import PackageNotFoundError, version
except ImportError:  # pragma: no cover
    from importlib_metadata import PackageNotFoundError, version  # type: ignore

__all__ = [
    "modules",
    "scripts",
    "params_path",
    "data_path",
    "__version__",
]

_PACKAGE_ROOT = Path(__file__).resolve().parent

try:
    __version__ = version("topcoffea")
except PackageNotFoundError:
    __version__ = "0.0.0"


def __getattr__(name: str) -> Any:
    """Lazily expose frequently used subpackages.

    Downstream projects such as ``topeft`` frequently rely on
    ``import topcoffea.modules`` resolving without additional namespace hacks.
    Importing the subpackages lazily keeps import time fast while ensuring the
    attribute exists on the top-level package when requested.
    """

    if name in {"modules", "scripts"}:
        module = import_module(f"topcoffea.{name}")
        globals()[name] = module
        return module
    raise AttributeError(f"module 'topcoffea' has no attribute {name!r}")


def _path_from_package_root(folder: str, *parts: str) -> str:
    return str(_PACKAGE_ROOT.joinpath(folder, *parts))


def params_path(*parts: str) -> str:
    """Return an absolute path under ``topcoffea/params``."""

    return _path_from_package_root("params", *parts)


def data_path(*parts: str) -> str:
    """Return an absolute path under ``topcoffea/data``."""

    return _path_from_package_root("data", *parts)

