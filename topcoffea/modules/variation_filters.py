"""Normalized representation of allowed jet/MET variations."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Optional, Set


@dataclass(frozen=True)
class AllowedVariations:
    """Container describing which variation bases/components should be built."""

    allow_jes: bool
    jes_components: Optional[Set[str]]
    allow_jer: bool
    allow_ues: bool

    def allows_jes_component(self, component: str) -> bool:
        """Return True if ``component`` should be materialised."""

        if not self.allow_jes:
            return False
        if self.jes_components is None:
            return True
        return component in self.jes_components


def _normalize_component_list(raw: object) -> Optional[Set[str]]:
    """Convert a component selector into a canonical set or ``None`` for all."""

    if raw is None:
        return None
    if isinstance(raw, str):
        if raw.lower() in {"all", "*"}:
            return None
        return {raw}
    if isinstance(raw, Mapping):
        raise TypeError("JES components must be provided as a list/iterable of names")
    try:
        items = set(str(item) for item in raw)  # type: ignore[arg-type]
    except TypeError as exc:  # pragma: no cover - defensive
        raise TypeError("Invalid JES component selector") from exc
    return items


def normalize_allowed_variations(config: Optional[Mapping[str, object]]) -> AllowedVariations:
    """Return an :class:`AllowedVariations` object from the user-provided config."""

    if config is None:
        return AllowedVariations(
            allow_jes=True,
            jes_components=None,
            allow_jer=True,
            allow_ues=True,
        )

    if not isinstance(config, Mapping):
        raise TypeError("allowed_variations must be a mapping when provided")

    raw_jes = config.get("jes")
    allow_jes = True
    jes_components: Optional[Set[str]] = None
    if isinstance(raw_jes, Mapping):
        allow_jes = bool(raw_jes.get("enabled", True))
        jes_components = _normalize_component_list(raw_jes.get("components"))
    elif isinstance(raw_jes, bool):
        allow_jes = raw_jes
    elif raw_jes is not None:
        jes_components = _normalize_component_list(raw_jes)

    raw_jer = config.get("jer", True)
    raw_ues = config.get("ues", True)

    return AllowedVariations(
        allow_jes=allow_jes,
        jes_components=jes_components,
        allow_jer=bool(raw_jer),
        allow_ues=bool(raw_ues),
    )


__all__ = ["AllowedVariations", "normalize_allowed_variations"]
