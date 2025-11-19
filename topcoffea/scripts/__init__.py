"""Expose scripts via attribute access."""

from __future__ import annotations

from importlib import import_module
from types import ModuleType
from typing import Any

__all__: list[str] = []


def _import(name: str) -> ModuleType:
    module = import_module(f"{__name__}.{name}")
    globals()[name] = module
    if name not in __all__:
        __all__.append(name)
    return module


def __getattr__(name: str) -> Any:
    if name.startswith("_"):
        raise AttributeError(name)
    return _import(name)
