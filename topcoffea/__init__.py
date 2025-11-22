"""Expose the public ``topcoffea`` namespace."""

from __future__ import annotations

import importlib
from importlib import import_module as _import_module
from pathlib import Path
from types import ModuleType
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
    "import_module",
    "__version__",
]

_PACKAGE_ROOT = Path(__file__).resolve().parent


def _ensure_not_vendored_in_topeft(package_root: Path) -> None:
    """Prevent imports from a vendored copy inside ``topeft``.

    The ``topeft`` repository sometimes vendors a copy of ``topcoffea`` under
    ``topeft/topcoffea`` for CI purposes. Loading the package from that path can
    silently mask the real ``topcoffea`` checkout, leading to mismatched
    versions. Fail fast with a helpful error so users reinstall the intended
    sibling checkout on the ``ch_update_calcoffea`` branch.
    """

    resolved = package_root.resolve()
    vendored_parent = resolved.parent
    if resolved.name.lower() == "topcoffea" and vendored_parent.name.lower() == "topeft":
        raise RuntimeError(
            "Detected topcoffea imported from a vendored copy inside a topeft "
            "checkout. Please remove the embedded topeft/topcoffea directory "
            "and install the real topcoffea from the ch_update_calcoffea "
            "branch (e.g., `python -m pip install -e /path/to/topcoffea`)."
        )


_ensure_not_vendored_in_topeft(_PACKAGE_ROOT)


def _verify_numpy_pandas_abi() -> None:
    """Fail fast when pandas/NumPy wheels are built against incompatible ABIs."""

    try:  # pragma: no cover - environment guard
        np = importlib.import_module("numpy")
        pd = importlib.import_module("pandas")
    except Exception as exc:
        raise RuntimeError(
            "Failed to import numpy/pandas during topcoffea startup. Recreate the "
            "coffea2025 environment and rebuild the TaskVine tarball before "
            "rerunning: `conda env update -f environment.yml --prune` followed "
            "by `python -m topcoffea.modules.remote_environment`."
        ) from exc

    try:  # pragma: no cover - environment guard
        from pandas import _libs as _pd_libs

        _ = _pd_libs.hashtable.Int64HashTable
    except Exception as exc:
        raise RuntimeError(
            "Detected a pandas/NumPy ABI mismatch (numpy "
            f"{np.__version__}, pandas {pd.__version__}). Recreate the "
            "coffea2025 environment and rebuild the TaskVine tarball: `conda env "
            "update -f environment.yml --prune` followed by `python -m "
            "topcoffea.modules.remote_environment`. Use the refreshed "
            "environment for both futures and TaskVine runs."
        ) from exc


_verify_numpy_pandas_abi()

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
        module = _import_module(f"topcoffea.{name}")
        globals()[name] = module
        return module
    raise AttributeError(f"module 'topcoffea' has no attribute {name!r}")


def import_module(name: str) -> ModuleType:
    """Expose ``importlib.import_module`` to downstream helpers.

    The ``topeft`` repository optionally reuses this helper when ensuring
    ``topcoffea.modules`` imports are resolved before attribute access.
    Keeping the shim here avoids re-implementing the same logic downstream
    while preserving backwards compatibility for callers that imported the
    helper previously via ``import importlib``.
    """

    return _import_module(name)


def _path_from_package_root(folder: str, *parts: str) -> str:
    return str(_PACKAGE_ROOT.joinpath(folder, *parts))


def params_path(*parts: str) -> str:
    """Return an absolute path under ``topcoffea/params``."""

    return _path_from_package_root("params", *parts)


def data_path(*parts: str) -> str:
    """Return an absolute path under ``topcoffea/data``."""

    return _path_from_package_root("data", *parts)

