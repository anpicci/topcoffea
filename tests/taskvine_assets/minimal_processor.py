from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import List, Sequence


@dataclass(frozen=True)
class TaskPayload:
    """In-memory representation of a chunk to be processed."""

    chunk_index: int
    values: List[int]

    def to_mapping(self) -> dict:
        return {"chunk_index": self.chunk_index, "values": self.values}


def load_numbers(path: Path) -> List[int]:
    """Read the list of numbers to process from ``path``."""

    payload = json.loads(Path(path).read_text())
    numbers = payload.get("numbers", [])
    if not isinstance(numbers, list):
        raise ValueError("Input payload must contain a list of numbers under 'numbers'.")
    return [int(value) for value in numbers]


def build_tasks(numbers: Sequence[int], chunk_size: int, max_chunks: int | None = None) -> List[TaskPayload]:
    """Break the input ``numbers`` into chunked :class:`TaskPayload` instances."""

    chunk_size = max(1, int(chunk_size))
    tasks: List[TaskPayload] = []
    for index, start in enumerate(range(0, len(numbers), chunk_size)):
        if max_chunks is not None and index >= max_chunks:
            break
        chunk_values = [int(value) for value in numbers[start : start + chunk_size]]
        tasks.append(TaskPayload(chunk_index=index, values=chunk_values))
    return tasks


def process_task(task: TaskPayload | dict) -> dict:
    """Return per chunk summary information."""

    if isinstance(task, dict):
        values = [int(value) for value in task["values"]]
        index = int(task["chunk_index"])
    else:
        values = [int(value) for value in task.values]
        index = int(task.chunk_index)

    return {
        "chunk_index": index,
        "values": values,
        "count": len(values),
        "sum": sum(values),
    }


def summarise(results: Sequence[dict]) -> dict:
    """Aggregate the per chunk results into a single summary payload."""

    ordered = sorted(results, key=lambda item: item["chunk_index"])
    total = sum(item["sum"] for item in ordered)
    counts = sum(item["count"] for item in ordered)
    return {"total": total, "count": counts, "chunks": ordered}
