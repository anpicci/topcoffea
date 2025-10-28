"""Command-line helpers for selecting coffea executors.

This module centralises the executor-related arguments that analyses in
``topcoffea`` and downstream projects such as ``topeft`` expose.  The
implementation mirrors the options historically provided by
``analysis/topeft_run2/run_analysis.py`` while adopting ``taskvine`` as the
preferred distributed backend.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Optional, Sequence, Tuple

__all__ = [
    "DEFAULT_EXECUTOR",
    "KNOWN_EXECUTORS",
    "ExecutorCLIConfig",
    "register_executor_arguments",
    "executor_config_from_values",
    "parse_port_range",
]

DEFAULT_EXECUTOR = "taskvine"
KNOWN_EXECUTORS: Tuple[str, ...] = ("futures", "taskvine")
DEFAULT_PORT_RANGE = "9123-9130"
DEFAULT_NWORKERS = 8
DEFAULT_CHUNKSIZE = 100_000


@dataclass(frozen=True)
class ExecutorCLIConfig:
    """Normalised configuration derived from command-line options."""

    executor: str
    nworkers: Optional[int]
    chunksize: Optional[int]
    nchunks: Optional[int]
    port: Optional[Tuple[int, int]]

    @property
    def requires_port(self) -> bool:
        return self.executor == "taskvine"


def register_executor_arguments(parser) -> None:
    """Attach common executor options to an ``argparse`` parser."""

    parser.add_argument(
        "--executor",
        "-x",
        choices=KNOWN_EXECUTORS,
        default=DEFAULT_EXECUTOR,
        help="Executor backend to use (default: %(default)s).",
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


def executor_config_from_values(
    *,
    executor: str,
    nworkers: Optional[int] = None,
    chunksize: Optional[int] = None,
    nchunks: Optional[int] = None,
    port: Optional[Sequence[int] | str] = None,
) -> ExecutorCLIConfig:
    """Normalise user provided values into an :class:`ExecutorCLIConfig`."""

    if executor not in KNOWN_EXECUTORS:
        raise ValueError(
            f'Unknown executor "{executor}". Expected one of {", ".join(KNOWN_EXECUTORS)}.'
        )

    def _maybe_int(value: Optional[int | str], default: Optional[int] = None) -> Optional[int]:
        if value in (None, ""):
            return default
        return int(value)

    port_range: Optional[Tuple[int, int]]
    if executor == "taskvine":
        port_range = parse_port_range(port)
    else:
        port_range = None

    return ExecutorCLIConfig(
        executor=executor,
        nworkers=_maybe_int(nworkers, DEFAULT_NWORKERS),
        chunksize=_maybe_int(chunksize, DEFAULT_CHUNKSIZE),
        nchunks=_maybe_int(nchunks),
        port=port_range,
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
