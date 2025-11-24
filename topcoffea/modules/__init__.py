"""Expose modules via attribute access.

The downstream ``topeft`` repository historically accessed helpers via
``topcoffea.modules.<name>`` without importing each submodule explicitly.
Providing a ``__getattr__`` shim keeps that style working while preserving
lazy imports for faster interpreter start-up.
"""

from __future__ import annotations

from importlib import import_module
from types import ModuleType
from typing import Any

from . import env_cache, executor, remote_environment

_SUBMODULE_ALIASES = {
    # Historical capitalization preferred by downstream projects.
    "HistEFT": "histEFT",
}

__all__ = [
    "env_cache",
    "executor",
    "remote_environment",
]


def _import(name: str) -> ModuleType:
    target = _SUBMODULE_ALIASES.get(name, name)
    module = import_module(f"{__name__}.{target}")
    globals()[target] = module
    globals()[name] = module
    return module


def __getattr__(name: str) -> Any:
    if name.startswith("_"):
        raise AttributeError(name)
    return _import(name)
