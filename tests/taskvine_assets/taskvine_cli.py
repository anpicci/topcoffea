#!/usr/bin/env python
"""Minimal TaskVine powered CLI used for integration tests."""

from __future__ import annotations

import argparse
import json
import shutil
import socket
import sys
from pathlib import Path
from typing import Sequence

from topcoffea.modules import executor_cli

CURRENT_DIR = Path(__file__).resolve().parent
if str(CURRENT_DIR) not in sys.path:
    sys.path.insert(0, str(CURRENT_DIR))

from minimal_processor import build_tasks, load_numbers, process_task, summarise  # noqa: E402

try:
    from ndcctools.taskvine.futures import FuturesExecutor
except ImportError as exc:  # pragma: no cover - handled by caller via skip
    raise SystemExit("ndcctools.taskvine is required to run this CLI") from exc


def _pick_port(port_range: tuple[int, int]) -> int:
    """Return the first available port inside *port_range*."""

    low, high = port_range
    for port in range(low, high + 1):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                sock.bind(("127.0.0.1", port))
            except OSError:
                continue
            return port
    raise RuntimeError("No available port in the requested range")


def _normalise_workers(value: int | None) -> int:
    if value is None or value < 1:
        return 1
    return int(value)


def _cli_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    executor_cli.register_executor_arguments(parser)
    parser.add_argument("--input", required=True, help="Path to the JSON payload to process.")
    parser.add_argument("--output", required=True, help="Destination JSON artifact.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _cli_parser().parse_args(argv)

    config = executor_cli.executor_config_from_values(
        executor=args.executor,
        nworkers=args.nworkers,
        chunksize=args.chunksize,
        nchunks=args.nchunks,
        port=args.port,
        environment_file=args.environment_file,
    )

    if config.executor != "taskvine":
        raise SystemExit("Only the TaskVine executor is supported by this test CLI.")

    if shutil.which("vine_worker") is None:
        raise SystemExit("TaskVine worker binary 'vine_worker' was not found in PATH.")

    input_path = Path(args.input)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    numbers = load_numbers(input_path)
    chunk_size = config.chunksize if config.chunksize else len(numbers)
    tasks = build_tasks(numbers, chunk_size, config.nchunks)

    port_range = config.port or executor_cli.parse_port_range(None)
    manager_port = _pick_port(port_range)

    executor = FuturesExecutor(port=manager_port)
    try:
        worker_count = _normalise_workers(config.nworkers)
        executor.set("min-workers", worker_count)
        executor.set("max-workers", worker_count)

        futures = [executor.submit(process_task, task.to_mapping()) for task in tasks]
        results = [future.result() for future in futures]
    finally:
        if getattr(executor, "manager", None) is not None:
            try:
                executor.manager.cancel_all()
            except Exception:
                pass
        if getattr(executor, "factory", None) is not None:
            try:
                executor.factory.stop()
            except Exception:
                pass

    summary = summarise(results)
    payload = {
        "executor": config.executor,
        "requested_workers": config.nworkers,
        "chunksize": chunk_size,
        "numbers": numbers,
        "results": results,
        "summary": summary,
        "port": manager_port,
    }

    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":  # pragma: no cover - exercised via subprocess
    raise SystemExit(main())
