"""Helpers for integrating dynamic data reduction with topcoffea workflows."""

from __future__ import annotations

import logging
from typing import Any, Dict, Mapping, MutableMapping, Optional, Sequence, Tuple

try:  # pragma: no cover - optional dependency
    from dynamic_data_reduction import preprocess, CoffeaDynamicDataReduction
except ImportError as exc:  # pragma: no cover - handled at runtime
    preprocess = None  # type: ignore[assignment]
    CoffeaDynamicDataReduction = None  # type: ignore[assignment]
    _DDR_IMPORT_ERROR = exc
else:  # pragma: no cover - ensures attribute defined for type checkers
    _DDR_IMPORT_ERROR = None  # type: ignore[assignment]

logger = logging.getLogger(__name__)

__all__ = [
    "build_ddr_data_from_flist",
    "run_ddr",
]


def _normalize_file_entries(entry: Any) -> Tuple[Tuple[str, Optional[MutableMapping[str, Any]]], ...]:
    """Return a tuple of (path, metadata) pairs extracted from *entry*."""

    if isinstance(entry, Mapping):
        files_candidate = entry.get("files", entry)
    else:
        files_candidate = entry

    if isinstance(files_candidate, Mapping):
        normalized = []
        for path, metadata in files_candidate.items():
            normalized.append((str(path), metadata if isinstance(metadata, MutableMapping) else None))
        return tuple(normalized)

    if isinstance(files_candidate, (list, tuple, set)):
        return tuple((str(path), None) for path in files_candidate)

    if isinstance(files_candidate, str):
        return ((files_candidate, None),)

    raise TypeError(f"Unsupported flist entry type: {type(entry)!r}")


def build_ddr_data_from_flist(
    flist: Mapping[str, Any],
    *,
    object_path: str = "Events",
) -> Dict[str, Dict[str, Any]]:
    """Convert a ``sample -> files`` mapping into the structure expected by DDR."""

    data: Dict[str, Dict[str, Any]] = {}
    for sample, entry in flist.items():
        normalized_files = _normalize_file_entries(entry)
        files_dict: Dict[str, Dict[str, Any]] = {}
        for path, metadata in normalized_files:
            file_meta = dict(metadata or {})
            file_meta.setdefault("object_path", object_path)
            files_dict[path] = file_meta
        data[sample] = {"files": files_dict}
        logger.debug("Prepared DDR data for sample %s (%d files)", sample, len(files_dict))

    logger.info("Prepared DDR payload for %d samples", len(data))
    return data


def run_ddr(
    *,
    manager: Any,
    data: Mapping[str, Any],
    processors: Mapping[str, Any],
    accumulator: Any,
    schema: Any,
    extra_files: Optional[Sequence[str]] = None,
    tree_name: str = "Events",
    preprocess_kwargs: Optional[Dict[str, Any]] = None,
    ddr_kwargs: Optional[Dict[str, Any]] = None,
) -> Any:
    """Preprocess inputs and run CoffeaDynamicDataReduction."""

    if preprocess is None or CoffeaDynamicDataReduction is None:
        raise ImportError(
            "dynamic_data_reduction is required to run DDR helpers. "
            "Install the package in the analysis environment."
        ) from _DDR_IMPORT_ERROR

    preprocess_options = dict(preprocess_kwargs or {})
    tree_arg = preprocess_options.pop("tree_name", tree_name)

    logger.info("Preprocessing DDR inputs (samples: %d)", len(data))
    preprocessed_data = preprocess(
        manager=manager,
        data=data,
        tree_name=tree_arg,
        **preprocess_options,
    )
    logger.info("Preprocessing complete")

    ddr_options = dict(ddr_kwargs or {})
    if extra_files is not None and "extra_files" not in ddr_options:
        ddr_options["extra_files"] = extra_files

    logger.info("Constructing CoffeaDynamicDataReduction (processors: %d)", len(processors))
    ddr = CoffeaDynamicDataReduction(
        manager,
        data=preprocessed_data,
        processors=processors,
        accumulator=accumulator,
        schema=schema,
        **ddr_options,
    )

    logger.info("Launching DDR compute()")
    result = ddr.compute()
    logger.info("DDR compute() finished")
    return result
