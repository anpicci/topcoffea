"""Expose the public ``topcoffea`` namespace."""

from __future__ import annotations

import importlib
from importlib import import_module as _import_module
from pathlib import Path
import os
import sys
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
    sibling checkout on a coordinated ref documented in
    ``docs/topeft_integration.md``.
    """

    resolved = package_root.resolve()
    vendored_parent = resolved.parent
    if resolved.name.lower() == "topcoffea" and vendored_parent.name.lower() == "topeft":
        raise RuntimeError(
            "Detected topcoffea imported from a vendored copy inside a topeft "
            "checkout. Please remove the embedded topeft/topcoffea directory "
            "and install the real topcoffea from a coordinated ref (release "
            "tag or matched feature ref); see docs/topeft_integration.md "
            "(e.g., `python -m pip install -e /path/to/topcoffea`)."
        )


_ensure_not_vendored_in_topeft(_PACKAGE_ROOT)


def _env_truthy(name: str) -> bool:
    value = os.environ.get(name)
    if value is None:
        return False
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _verify_numpy_pandas_abi() -> None:
    """Verify runtime imports, with pandas ABI checks enabled only on demand."""

    try:  # pragma: no cover - environment guard
        importlib.import_module("numpy")
    except Exception as exc:
        raise RuntimeError(
            "Failed to import numpy during topcoffea startup. Recreate the "
            "coffea2025 environment and rebuild the TaskVine tarball before "
            "rerunning: `conda env update -f environment.yml --prune` followed "
            "by `python -m topcoffea.modules.remote_environment`."
        ) from exc

    if not _env_truthy("TOPCOFFEA_IMPORT_CHECK_PANDAS"):
        return

    print(
        "[topcoffea] Optional pandas ABI check enabled via "
        "TOPCOFFEA_IMPORT_CHECK_PANDAS=1",
        file=sys.stderr,
        flush=True,
    )
    try:  # pragma: no cover - environment guard
        pd = importlib.import_module("pandas")
        from pandas import _libs as _pd_libs

        _ = _pd_libs.hashtable.Int64HashTable
        print(
            f"[topcoffea] Optional pandas ABI check passed (pandas {pd.__version__})",
            file=sys.stderr,
            flush=True,
        )
    except Exception as exc:
        print(
            "[topcoffea] Optional pandas ABI check failed while "
            "TOPCOFFEA_IMPORT_CHECK_PANDAS=1 is enabled.",
            file=sys.stderr,
            flush=True,
        )
        raise RuntimeError(
            "Optional pandas ABI check failed. Recreate the coffea2025 "
            "environment and rebuild the TaskVine tarball: `conda env update -f "
            "environment.yml --prune` followed by `python -m "
            "topcoffea.modules.remote_environment`."
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
