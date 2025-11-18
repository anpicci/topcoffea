"""Helpers for working with tuple-keyed histogram outputs.

Histogram payloads keyed by ``(variable, channel, application, sample,
systematic)`` tuples provide an axis-free way to persist application-region
metadata alongside other identifiers.  Utilities in this module keep those
entries deterministic during serialisation while preserving the original
histogram objects so downstream consumers can materialise values on demand.
"""

from __future__ import annotations

from collections import OrderedDict
from typing import Any, Dict, Mapping, Optional, Tuple

import numpy as np

try:  # pragma: no cover - optional dependency during some tests
    from hist import Hist
except Exception:  # pragma: no cover - fallback when histogram extras missing
    Hist = None  # type: ignore[assignment]

try:  # pragma: no cover - topcoffea is optional for a subset of the tests
    from topcoffea.modules.histEFT import HistEFT
except Exception:  # pragma: no cover - fallback when HistEFT is unavailable
    HistEFT = None  # type: ignore[assignment]

TupleKey = Tuple[
    str,
    Optional[str],
    Optional[str],
    Optional[str],
    Optional[str],
]
"""Tuple identifier for histogram entries."""

_TUPLE_FORMAT = "(variable, channel, application, sample, systematic)"
"""Human-readable description of the required tuple layout."""

SUMMARY_KEY = "__tuple_summary__"
"""Optional key holding histogram summaries for tuple entries."""


def _ensure_numpy(array: Any) -> np.ndarray:
    """Return *array* as a :class:`numpy.ndarray` without copying when possible."""

    if isinstance(array, np.ndarray):
        return array
    return np.asarray(array)


def _hist_like(instance: Any) -> bool:
    """Return ``True`` when *instance* behaves like a histogram object."""

    hist_classes: Tuple[type, ...] = tuple(
        cls for cls in (Hist, HistEFT) if isinstance(cls, type)
    )
    if not hist_classes:
        return False
    return isinstance(instance, hist_classes)


def _tuple_sort_key(key: TupleKey) -> Tuple[Any, ...]:
    """Return a sort key that safely orders tuple identifiers with optional fields."""

    variable, channel, application, sample, systematic = key
    ordered_parts = [variable]

    for optional_value in (channel, application, sample, systematic):
        ordered_parts.append(
            (
                optional_value is not None,
                "" if optional_value is None else str(optional_value),
            )
        )

    return tuple(ordered_parts)


def _validate_tuple_key(key: TupleKey) -> TupleKey:
    """Ensure *key* follows the five-element tuple schema."""

    if len(key) != 5:
        raise ValueError(
            f"Histogram accumulator keys must be 5-tuples of {_TUPLE_FORMAT}."
        )
    return key


def _summarise_histogram(histogram: Any) -> Dict[str, Any]:
    """Create a deterministic summary payload for *histogram*."""

    values: Optional[np.ndarray] = None
    variances: Optional[np.ndarray] = None

    if HistEFT is not None and isinstance(histogram, HistEFT):
        for dense_hist in getattr(histogram, "_dense_hists", {}).values():
            current_values = _ensure_numpy(dense_hist.values(flow=True))
            raw_variances = dense_hist.variances(flow=True)
            current_variances = None if raw_variances is None else _ensure_numpy(raw_variances)

            values = current_values if values is None else values + current_values
            if current_variances is None:
                variances = None if variances is None else None
            else:
                variances = (
                    current_variances
                    if variances is None
                    else variances + current_variances
                )
    elif Hist is not None and isinstance(histogram, Hist):
        values = _ensure_numpy(histogram.values(flow=True))
        raw_variances = histogram.variances(flow=True)
        variances = None if raw_variances is None else _ensure_numpy(raw_variances)
    else:
        raise TypeError(f"Unsupported histogram type: {type(histogram)!r}")

    if values is None:
        values = np.array([])

    summary: Dict[str, Any] = {
        "sumw": float(np.sum(values)) if values.size else 0.0,
        "sumw2": float(np.sum(variances)) if variances is not None else None,
        "values": values,
        "variances": variances,
    }
    return summary


def materialise_tuple_dict(
    hist_store: Mapping[TupleKey, Any]
) -> "OrderedDict[TupleKey, Dict[str, Any]]":
    """Return an :class:`OrderedDict` keyed by sorted histogram tuple identifiers."""

    tuple_entries = []
    for key, histogram in hist_store.items():
        if not isinstance(key, tuple):
            continue
        tuple_entries.append((_validate_tuple_key(key), histogram))

    ordered_items = []
    for key, histogram in sorted(tuple_entries, key=lambda item: _tuple_sort_key(item[0])):
        summary = _summarise_histogram(histogram)
        ordered_items.append((key, summary))

    return OrderedDict(ordered_items)


def _tuple_entries(payload: Mapping[Any, Any]) -> Dict[TupleKey, Any]:
    """Extract histogram-like entries keyed by tuple identifiers from *payload*."""

    result: Dict[TupleKey, Any] = {}
    for key, value in payload.items():
        if isinstance(key, tuple) and _hist_like(value):
            result[_validate_tuple_key(key)] = value
    return result


def normalise_runner_output(payload: Mapping[Any, Any]) -> Mapping[Any, Any]:
    """Return a tuple-keyed ordered mapping preserving histogram payloads.

    Tuple-keyed histogram entries are emitted in lexicographic order to provide
    deterministic serialisation while their original histogram objects remain
    untouched.  Non-histogram entries are preserved in their original insertion
    order.  Consumers that need a deterministic, serialisable summary of the
    histogram contents can call :func:`materialise_tuple_dict` on the returned
    mapping as required.
    """

    if not isinstance(payload, Mapping):
        return payload

    tuple_histograms = _tuple_entries(payload)
    if not tuple_histograms:
        return payload

    ordered: "OrderedDict[Any, Any]" = OrderedDict()
    for key, histogram in sorted(
        tuple_histograms.items(), key=lambda item: _tuple_sort_key(item[0])
    ):
        ordered[key] = histogram
    for key, value in payload.items():
        if key not in tuple_histograms:
            ordered[key] = value
    return ordered


def tuple_dict_stats(tuple_dict: Mapping[Any, Any]) -> Tuple[int, int]:
    """Return the total and non-zero bin counts for *tuple_dict* entries."""

    total_bins = 0
    filled_bins = 0
    summaries: Optional[Mapping[TupleKey, Mapping[str, Any]]] = None

    if isinstance(tuple_dict, Mapping):
        candidate = tuple_dict.get(SUMMARY_KEY)
        if isinstance(candidate, Mapping):
            summaries = candidate  # type: ignore[assignment]

    if summaries is None:
        summaries = OrderedDict()
        for key, value in tuple_dict.items():
            if isinstance(key, tuple) and _hist_like(value):
                validated_key = _validate_tuple_key(key)
                summaries[validated_key] = _summarise_histogram(value)

    for summary in summaries.values():
        values = summary.get("values")
        if values is None:
            continue
        array = _ensure_numpy(values)
        total_bins += int(array.size)
        filled_bins += int(np.count_nonzero(array))
    return total_bins, filled_bins


__all__ = [
    "SUMMARY_KEY",
    "TupleKey",
    "materialise_tuple_dict",
    "normalise_runner_output",
    "tuple_dict_stats",
]
