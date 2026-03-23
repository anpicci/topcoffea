"""TaskVine manager probe utilities for attachment diagnostics."""

from __future__ import annotations

import argparse
import csv
import json
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Optional, Pattern, Sequence

CSV_HEADER = (
    "timestamp",
    "pattern",
    "matched_project",
    "workers_connected",
    "tasks_waiting",
    "tasks_running",
    "tasks_done",
    "note",
)


@dataclass(frozen=True)
class ProbeSample:
    timestamp: str
    pattern: str
    matched_project: str
    workers_connected: int
    tasks_waiting: int
    tasks_running: int
    tasks_done: int
    note: str


def compile_project_pattern(project_pattern: str) -> tuple[Pattern[str], str]:
    candidate = str(project_pattern or "").strip()
    if not candidate:
        raise ValueError("--project-pattern must be a non-empty string")
    try:
        return re.compile(candidate), "regex"
    except re.error:
        return re.compile(re.escape(candidate)), "literal-fallback"


def _safe_int(value: Any) -> int:
    if value is None:
        return 0
    try:
        return int(value)
    except (TypeError, ValueError):
        try:
            return int(float(str(value).strip()))
        except (TypeError, ValueError):
            return 0


def _normalize_manager_record(record: Mapping[str, Any]) -> Optional[dict[str, Any]]:
    project = str(
        record.get("project")
        or record.get("name")
        or record.get("manager")
        or record.get("manager_name")
        or ""
    ).strip()
    if not project:
        return None

    categories = record.get("categories")
    workers_connected = _safe_int(record.get("workers_connected", record.get("workers", 0)))
    tasks_waiting = _safe_int(record.get("tasks_waiting"))
    tasks_running = _safe_int(record.get("tasks_running"))
    tasks_done = _safe_int(record.get("tasks_done", record.get("tasks_complete")))

    if isinstance(categories, list) and categories:
        if tasks_waiting == 0:
            tasks_waiting = sum(
                _safe_int(category.get("tasks_waiting"))
                for category in categories
                if isinstance(category, Mapping)
            )
        if tasks_running == 0:
            tasks_running = sum(
                _safe_int(category.get("tasks_running"))
                for category in categories
                if isinstance(category, Mapping)
            )
        if tasks_done == 0:
            tasks_done = sum(
                _safe_int(category.get("tasks_done"))
                for category in categories
                if isinstance(category, Mapping)
            )

    return {
        "project": project,
        "workers_connected": workers_connected,
        "tasks_waiting": tasks_waiting,
        "tasks_running": tasks_running,
        "tasks_done": tasks_done,
    }


def _records_from_payload(payload: Any) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    if isinstance(payload, Mapping):
        entries = payload.get("managers") if isinstance(payload.get("managers"), list) else [payload]
    elif isinstance(payload, list):
        entries = payload
    else:
        return normalized

    for entry in entries:
        if not isinstance(entry, Mapping):
            continue
        normalized_entry = _normalize_manager_record(entry)
        if normalized_entry is not None:
            normalized.append(normalized_entry)
    return normalized


def _parse_vine_status_text(stdout: str) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for raw_line in stdout.splitlines():
        line = raw_line.strip()
        if not line:
            continue

        try:
            json_line = json.loads(line.rstrip(","))
        except json.JSONDecodeError:
            json_line = None
        if json_line is not None:
            records.extend(_records_from_payload(json_line))
            continue

        project_match = re.search(r"(?:^|\s)(?:project|name)\s*[=:]\s*([\w./:@+-]+)", line, flags=re.IGNORECASE)
        if not project_match:
            continue

        project = project_match.group(1).strip()
        workers_connected = _safe_int(
            re.search(r"workers[_ ]connected\s*[=:]\s*(\d+)", line, flags=re.IGNORECASE).group(1)
            if re.search(r"workers[_ ]connected\s*[=:]\s*(\d+)", line, flags=re.IGNORECASE)
            else 0
        )
        tasks_waiting = _safe_int(
            re.search(r"tasks[_ ]waiting\s*[=:]\s*(\d+)", line, flags=re.IGNORECASE).group(1)
            if re.search(r"tasks[_ ]waiting\s*[=:]\s*(\d+)", line, flags=re.IGNORECASE)
            else 0
        )
        tasks_running = _safe_int(
            re.search(r"tasks[_ ]running\s*[=:]\s*(\d+)", line, flags=re.IGNORECASE).group(1)
            if re.search(r"tasks[_ ]running\s*[=:]\s*(\d+)", line, flags=re.IGNORECASE)
            else 0
        )
        tasks_done = _safe_int(
            re.search(r"tasks[_ ]done\s*[=:]\s*(\d+)", line, flags=re.IGNORECASE).group(1)
            if re.search(r"tasks[_ ]done\s*[=:]\s*(\d+)", line, flags=re.IGNORECASE)
            else 0
        )

        records.append(
            {
                "project": project,
                "workers_connected": workers_connected,
                "tasks_waiting": tasks_waiting,
                "tasks_running": tasks_running,
                "tasks_done": tasks_done,
            }
        )

    return records


def parse_vine_status_output(stdout: str) -> tuple[list[dict[str, Any]], str]:
    payload = (stdout or "").strip()
    if not payload:
        return ([], "empty-output")

    try:
        decoded = json.loads(payload)
    except json.JSONDecodeError:
        text_records = _parse_vine_status_text(payload)
        return (text_records, "text")

    records = _records_from_payload(decoded)
    if records:
        return (records, "json")

    text_records = _parse_vine_status_text(payload)
    if text_records:
        return (text_records, "json-empty;text")
    return ([], "json-empty")


def _query_managers_via_taskvine_api(*, timeout: float) -> tuple[Optional[list[dict[str, Any]]], str]:
    try:
        import ndcctools.taskvine as taskvine  # type: ignore[import-untyped]
    except Exception as exc:
        return (None, f"api-import-failed:{exc.__class__.__name__}")

    _ = timeout  # currently no exposed Python API in this environment accepts catalog query timeout.
    for attr in ("catalog_query", "query_catalog", "list_managers", "managers"):
        candidate = getattr(taskvine, attr, None)
        if not callable(candidate):
            continue
        try:
            payload = candidate()
        except Exception as exc:  # pragma: no cover - defensive fallback
            return (None, f"api-{attr}-failed:{exc.__class__.__name__}")

        records = _records_from_payload(payload)
        return (records, f"api-{attr}")

    return (None, "api-unavailable")


def _query_managers_via_vine_status(*, timeout: float) -> tuple[list[dict[str, Any]], str]:
    command = [
        "vine_status",
        "--statistics",
        "--verbose",
        "--timeout",
        str(max(1, int(round(timeout)))),
    ]
    try:
        completed = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            timeout=max(1.0, float(timeout) + 2.0),
        )
    except Exception as exc:
        return ([], f"vine_status-error:{exc.__class__.__name__}")

    records, parse_mode = parse_vine_status_output(completed.stdout or "")
    stderr_line = (completed.stderr or "").strip().splitlines()
    stderr_note = stderr_line[0] if stderr_line else ""
    note_parts = [f"vine_status_rc={completed.returncode}", f"parse={parse_mode}"]
    if stderr_note:
        note_parts.append(f"stderr={stderr_note}")
    return (records, ";".join(note_parts))


def query_manager_records(*, timeout: float) -> tuple[list[dict[str, Any]], str]:
    api_records, api_note = _query_managers_via_taskvine_api(timeout=timeout)
    if api_records is not None:
        return (api_records, f"source=taskvine_api;{api_note}")

    records, vine_note = _query_managers_via_vine_status(timeout=timeout)
    return (records, f"source=vine_status;api_note={api_note};{vine_note}")


def _now_iso_utc() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def sample_project_status(*, project_pattern: str, timeout: float) -> ProbeSample:
    matcher, match_mode = compile_project_pattern(project_pattern)
    records, source_note = query_manager_records(timeout=timeout)
    matches = [record for record in records if matcher.search(str(record.get("project", "")))]

    if not matches:
        return ProbeSample(
            timestamp=_now_iso_utc(),
            pattern=project_pattern,
            matched_project="NOT_FOUND",
            workers_connected=0,
            tasks_waiting=0,
            tasks_running=0,
            tasks_done=0,
            note=f"{source_note};match_mode={match_mode};matches=0",
        )

    projects = sorted({str(record.get("project", "")).strip() for record in matches if str(record.get("project", "")).strip()})
    return ProbeSample(
        timestamp=_now_iso_utc(),
        pattern=project_pattern,
        matched_project="|".join(projects),
        workers_connected=sum(_safe_int(record.get("workers_connected")) for record in matches),
        tasks_waiting=sum(_safe_int(record.get("tasks_waiting")) for record in matches),
        tasks_running=sum(_safe_int(record.get("tasks_running")) for record in matches),
        tasks_done=sum(_safe_int(record.get("tasks_done")) for record in matches),
        note=f"{source_note};match_mode={match_mode};matches={len(matches)}",
    )


def run_probe(
    *,
    project_pattern: str,
    timeout: float,
    repeat: int,
    sleep_seconds: float,
) -> list[ProbeSample]:
    if repeat < 1:
        raise ValueError("--repeat must be >= 1")
    if sleep_seconds < 0:
        raise ValueError("--sleep must be >= 0")

    samples: list[ProbeSample] = []
    for index in range(repeat):
        samples.append(sample_project_status(project_pattern=project_pattern, timeout=timeout))
        if index + 1 < repeat:
            time.sleep(sleep_seconds)
    return samples


def render_csv_lines(samples: Sequence[ProbeSample]) -> list[str]:
    from io import StringIO

    buffer = StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(CSV_HEADER)
    for sample in samples:
        writer.writerow(
            [
                sample.timestamp,
                sample.pattern,
                sample.matched_project,
                sample.workers_connected,
                sample.tasks_waiting,
                sample.tasks_running,
                sample.tasks_done,
                sample.note,
            ]
        )
    return buffer.getvalue().splitlines()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Probe TaskVine manager state for std/light project patterns.")
    parser.add_argument("--project-pattern", required=True, help="Regular expression used to match manager project names.")
    parser.add_argument("--timeout", type=float, default=10.0, help="Timeout (seconds) for each probe sample.")
    parser.add_argument("--repeat", type=int, default=1, help="Number of probe samples to collect.")
    parser.add_argument("--sleep", type=float, default=5.0, help="Sleep interval (seconds) between samples.")
    parser.add_argument("--out", default=None, help="Optional output file path for CSV output.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)

    try:
        samples = run_probe(
            project_pattern=args.project_pattern,
            timeout=float(args.timeout),
            repeat=int(args.repeat),
            sleep_seconds=float(args.sleep),
        )
    except Exception as exc:
        print(f"[taskvine_probe] ERROR: {exc}", file=sys.stderr, flush=True)
        return 2

    lines = render_csv_lines(samples)
    if args.out:
        output_path = Path(args.out).expanduser()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    else:
        print("\n".join(lines), file=sys.stdout, flush=True)

    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
