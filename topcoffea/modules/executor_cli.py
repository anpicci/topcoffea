"""Command-line helpers for selecting coffea executors.

This module centralises the executor-related arguments that analyses in
``topcoffea`` and downstream projects such as ``topeft`` expose.  The
implementation mirrors the options historically provided by
``analysis/topeft_run2/run_analysis.py`` while adopting ``taskvine`` as the
preferred distributed backend.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, Mapping, Optional, Sequence, Tuple

from . import remote_environment

__all__ = [
    "DEFAULT_EXECUTOR",
    "SUPPORTED_EXECUTORS",
    "ExecutorCLIConfig",
    "register_executor_arguments",
    "executor_config_from_values",
    "parse_port_range",
    "TASKVINE_ENVIRONMENT_AUTO",
    "TASKVINE_EPILOG",
    "TASKVINE_EXTRA_PIP_LOCAL",
    "run_coffea_runner",
]

DEFAULT_EXECUTOR = "taskvine"
SUPPORTED_EXECUTORS: Tuple[str, ...] = ("futures", "iterative", "taskvine")
_TASKVINE_FAMILY = {"taskvine"}
DEFAULT_PORT_RANGE = "9123-9130"
DEFAULT_NWORKERS = 8
DEFAULT_CHUNKSIZE = 100_000
TASKVINE_ENVIRONMENT_AUTO = "auto"

TASKVINE_EXTRA_PIP_LOCAL: Dict[str, Tuple[str, ...]] = {
    "topeft": tuple(
        remote_environment.PIP_LOCAL_TO_WATCH.get(
            "topeft", ("topeft", "setup.py")
        )
    )
}

TASKVINE_EPILOG = """\
TaskVine quick start:

  Submit workers with:
    vine_submit_workers --cores 4 --memory 6000 --disk 8000 \
      --wall-time 12h --environment <path/to/env.tar.gz>

  Recommended per-worker resources:
    • 4 CPU cores
    • 6 GiB RAM
    • 8 GiB local scratch space

  Use --environment-file auto to build or reuse the cached Conda environment
  containing editable topcoffea/topeft checkouts before launching workers.
  Select --executor taskvine to run those workers via CoffeaDynamicDataReduction
  for dynamic orchestration.
"""


@dataclass(frozen=True)
class ExecutorCLIConfig:
    """Normalised configuration derived from command-line options."""

    executor: str
    nworkers: Optional[int]
    chunksize: Optional[int]
    nchunks: Optional[int]
    port: Optional[Tuple[int, int]]
    environment_file: Optional[str]

    @property
    def requires_port(self) -> bool:
        return self.executor in _TASKVINE_FAMILY


def _normalize_executor(executor: str) -> str:
    normalized = executor.strip().lower()
    if normalized not in SUPPORTED_EXECUTORS:
        raise ValueError(
            f"Unsupported executor '{executor}'. Valid options are: {', '.join(SUPPORTED_EXECUTORS)}."
        )
    return normalized


def _parse_executor_argument(value: str) -> str:
    try:
        return _normalize_executor(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc))


def register_executor_arguments(parser) -> None:
    """Attach common executor options to an ``argparse`` parser."""

    if parser.epilog:
        parser.epilog = f"{parser.epilog}\n\n{TASKVINE_EPILOG}"
    else:
        parser.epilog = TASKVINE_EPILOG

    if parser.formatter_class is argparse.HelpFormatter:
        parser.formatter_class = argparse.RawDescriptionHelpFormatter

    parser.add_argument(
        "--executor",
        "-x",
        choices=SUPPORTED_EXECUTORS,
        default=DEFAULT_EXECUTOR,
        type=_parse_executor_argument,
        metavar="{futures,iterative,taskvine}",
        help=(
            "Which executor to use (default: %(default)s). Choose from futures, iterative, or "
            "taskvine (TaskVine via DDR)."
        ),
    )
    parser.add_argument(
        "--nworkers",
        "-n",
        type=int,
        default=DEFAULT_NWORKERS,
        help="Number of parallel workers to request.",
    )
    parser.add_argument(
        "--chunksize",
        "-s",
        type=int,
        default=DEFAULT_CHUNKSIZE,
        help="Events processed per chunk.",
    )
    parser.add_argument(
        "--nchunks",
        "-c",
        type=int,
        default=None,
        help="Limit the number of chunks processed (default: unlimited).",
    )
    parser.add_argument(
        "--port",
        default=DEFAULT_PORT_RANGE,
        help=(
            "TaskVine manager port or inclusive range (PORT or PORT_MIN-PORT_MAX). "
            "Ignored when running with the futures executor."
        ),
    )
    parser.add_argument(
        "--environment-file",
        default=None,
        help=(
            "TaskVine environment tarball to ship to workers. Specify 'auto' "
            "to build or reuse the cached Conda environment."
        ),
    )


def executor_config_from_values(
    *,
    executor: str,
    nworkers: Optional[int] = None,
    chunksize: Optional[int] = None,
    nchunks: Optional[int] = None,
    port: Optional[Sequence[int] | str] = None,
    environment_file: Optional[str] = None,
    extra_pip_local: Optional[Mapping[str, Sequence[str]]] = None,
) -> ExecutorCLIConfig:
    """Normalise user provided values into an :class:`ExecutorCLIConfig`."""

    normalized_executor = _normalize_executor(executor)

    def _maybe_int(value: Optional[int | str], default: Optional[int] = None) -> Optional[int]:
        if value in (None, ""):
            return default
        return int(value)

    port_range: Optional[Tuple[int, int]]
    if normalized_executor in _TASKVINE_FAMILY:
        port_range = parse_port_range(port)
    else:
        port_range = None

    environment_path: Optional[str] = None
    if environment_file:
        environment_file = environment_file.strip()

    if normalized_executor in _TASKVINE_FAMILY:
        if environment_file == TASKVINE_ENVIRONMENT_AUTO:
            merged_extra: Dict[str, Sequence[str]] = {
                pkg: tuple(paths) for pkg, paths in TASKVINE_EXTRA_PIP_LOCAL.items()
            }
            if extra_pip_local:
                for pkg, paths in extra_pip_local.items():
                    merged_extra[pkg] = tuple(paths)
            environment_path = remote_environment.get_environment(
                extra_pip_local={
                    pkg: list(paths) for pkg, paths in merged_extra.items()
                }
            )
        elif environment_file:
            environment_path = environment_file
    else:
        if environment_file and environment_file != TASKVINE_ENVIRONMENT_AUTO:
            environment_path = environment_file

    return ExecutorCLIConfig(
        executor=normalized_executor,
        nworkers=_maybe_int(nworkers, DEFAULT_NWORKERS),
        chunksize=_maybe_int(chunksize, DEFAULT_CHUNKSIZE),
        nchunks=_maybe_int(nchunks),
        port=port_range,
        environment_file=environment_path,
    )


def parse_port_range(port: Optional[Sequence[int] | str]) -> Tuple[int, int]:
    """Convert a port specification into a two element tuple."""

    if port is None:
        port = DEFAULT_PORT_RANGE

    values: Iterable[int]
    if isinstance(port, str):
        items = [p for p in port.split("-") if p]
        values = (int(item) for item in items)
    else:
        values = (int(item) for item in port)

    parsed = list(values)
    if not parsed:
        raise ValueError("At least one port value must be provided.")
    if len(parsed) == 1:
        parsed.append(parsed[0])
    if len(parsed) != 2:
        raise ValueError("Port specification must contain at most two values.")
    low, high = parsed
    if low > high:
        raise ValueError("Port range must be specified as MIN-MAX with MIN <= MAX.")
    return int(low), int(high)


def run_coffea_runner(
    runner: Callable[[Mapping[str, Sequence[str]], Any, str], Any],
    fileset: Mapping[str, Sequence[str]],
    processor_instance: Any,
    treename: str,
):
    """Invoke :class:`coffea.processor.Runner` using the canonical argument order.

    Coffea's :class:`~coffea.processor.Runner` is typically called as ``runner(fileset, processor_instance, treename)``.
    Downstream projects sometimes proxy this call and risk swapping positional parameters, which can surface as
    ``ProcessorABC`` errors when the processor instance is no longer passed in the expected position.  This helper keeps
    the invocation order centralised and documented to reduce regressions when upgrading coffea.
    """

    return runner(fileset, processor_instance, treename)
