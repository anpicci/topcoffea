"""Helpers for integrating dynamic data reduction with topcoffea workflows."""

from __future__ import annotations

import logging
import numbers
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
    "filter_preprocessed_data",
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


def _is_valid_num_entries(value: Any) -> bool:
    """Return True when *value* encodes a valid entry count."""

    if isinstance(value, bool):
        return False
    if isinstance(value, numbers.Real):
        return value >= 1
    return False


def filter_preprocessed_data(
    preprocessed_data: Mapping[str, Mapping[str, Any]]
) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Any]]:
    """Filter the preprocess() output, separating usable vs failed files."""

    filtered: Dict[str, Dict[str, Any]] = {}
    bad_files = []
    dropped_datasets = []
    total_files = 0
    kept_files = 0

    for dataset, specs in preprocessed_data.items():
        files = dict(specs.get("files", {}))
        filtered_files: Dict[str, Dict[str, Any]] = {}
        for path, file_info in files.items():
            total_files += 1
            num_entries = file_info.get("num_entries")
            if _is_valid_num_entries(num_entries):
                filtered_files[path] = file_info
                kept_files += 1
            else:
                bad_files.append((dataset, path, num_entries))
        if filtered_files:
            copied_specs = dict(specs)
            copied_specs["files"] = filtered_files
            filtered[dataset] = copied_specs
        else:
            dropped_datasets.append(dataset)

    summary = {
        "total_files": total_files,
        "good_files_count": kept_files,
        "bad_files_count": len(bad_files),
        "bad_files": bad_files,
        "dropped_datasets": dropped_datasets,
    }
    return filtered, summary


def _diagnose_failed_file(path: str, tree_name: str) -> str:
    """Attempt to open *path* locally to provide diagnostics for logging."""

    try:
        import uproot
    except ImportError:  # pragma: no cover - environment issue
        return "uproot not available in driver environment"

    try:
        with uproot.open(path, timeout=10) as root_file:
            if tree_name not in root_file:
                return f"opened but tree '{tree_name}' missing"
            tree = root_file[tree_name]
            try:
                entries = tree.num_entries
            except Exception:
                entries = None
            return f"opened locally (num_entries={entries})"
    except Exception as exc:  # pragma: no cover - exercised via tests
        return f"{exc.__class__.__name__}: {exc}"


def run_ddr(
    *,
    manager: Any,
    data: Mapping[str, Any],
    processors: Mapping[str, Any],
    accumulator: Any,
    schema: Any,
    extra_files: Optional[Sequence[str]] = None,
    tree_name: str = "Events",
    step_size: Optional[int] = None,
    max_task_retries: Optional[int] = None,
    resources_processing: Optional[Mapping[str, Any]] = None,
    resources_accumulating: Optional[Mapping[str, Any]] = None,
    results_directory: Optional[str] = None,
    verbose: Optional[bool] = None,
    x509_proxy: Optional[str] = None,
    environment_variables: Optional[Mapping[str, str]] = None,
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
    if x509_proxy is not None and "x509_proxy" not in preprocess_options:
        preprocess_options["x509_proxy"] = x509_proxy

    logger.info("Preprocessing DDR inputs (samples: %d)", len(data))
    preprocessed_data = preprocess(
        manager=manager,
        data=data,
        tree_name=tree_arg,
        **preprocess_options,
    )
    logger.info("Preprocessing complete")
    filtered_data, summary = filter_preprocessed_data(preprocessed_data)

    if summary["bad_files_count"] > 0:
        logger.warning(
            "DDR preprocess marked %d/%d files unusable",
            summary["bad_files_count"],
            summary["total_files"],
        )
        for dataset, path, num_entries in summary["bad_files"]:
            diagnosis = _diagnose_failed_file(path, tree_arg)
            logger.warning(
                "Failed preprocess: dataset=%s file=%s num_entries=%r (%s)",
                dataset,
                path,
                num_entries,
                diagnosis,
            )

    if summary["dropped_datasets"]:
        logger.warning(
            "Dropped %d datasets with no usable files: %s",
            len(summary["dropped_datasets"]),
            ", ".join(summary["dropped_datasets"]),
        )

    if summary["good_files_count"] == 0 and summary["total_files"] > 0:
        raise RuntimeError(
            "Dynamic data reduction preprocessing completed but produced no usable files. "
            "Inspect TaskVine/DDR logs and XRootD connectivity for the failed files above."
        )

    ddr_options = dict(ddr_kwargs or {})
    if extra_files is not None and "extra_files" not in ddr_options:
        ddr_options["extra_files"] = extra_files
    if step_size is not None:
        ddr_options["step_size"] = int(step_size)
    if max_task_retries is not None:
        ddr_options["max_task_retries"] = int(max_task_retries)
    if resources_processing is not None:
        ddr_options["resources_processing"] = dict(resources_processing)
    if resources_accumulating is not None:
        ddr_options["resources_accumulating"] = dict(resources_accumulating)
    if results_directory is not None:
        ddr_options["results_directory"] = str(results_directory)
    if verbose is not None:
        ddr_options["verbose"] = bool(verbose)
    if x509_proxy is not None:
        ddr_options["x509_proxy"] = x509_proxy

    logger.info("Constructing CoffeaDynamicDataReduction (processors: %d)", len(processors))
    ddr = CoffeaDynamicDataReduction(
        manager,
        data=filtered_data,
        processors=processors,
        accumulator=accumulator,
        schema=schema,
        **ddr_options,
    )
    if environment_variables:
        env_updates = {str(key): str(value) for key, value in environment_variables.items()}
        ddr_env = getattr(ddr, "environment_variables", None)
        if isinstance(ddr_env, MutableMapping):
            ddr_env.update(env_updates)
        elif ddr_env is None:
            setattr(ddr, "environment_variables", dict(env_updates))
        else:  # pragma: no cover - defensive fallback for custom DDR objects
            try:
                ddr_env.update(env_updates)
            except Exception:
                setattr(ddr, "environment_variables", dict(env_updates))

    logger.info("Launching DDR compute()")
    result = ddr.compute()
    logger.info("DDR compute() finished")
    return result
