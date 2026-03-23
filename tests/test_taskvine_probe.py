from __future__ import annotations

from topcoffea.modules import taskvine_probe as probe


def test_compile_project_pattern_regex() -> None:
    matcher, mode = probe.compile_project_pattern(r"apiccine-taskvine-coffea-std-.*")
    assert mode == "regex"
    assert matcher.search("apiccine-taskvine-coffea-std-1234") is not None


def test_compile_project_pattern_literal_fallback() -> None:
    matcher, mode = probe.compile_project_pattern("apiccine-taskvine-coffea-std-[")
    assert mode == "literal-fallback"
    assert matcher.search("apiccine-taskvine-coffea-std-[") is not None


def test_parse_vine_status_output_json_records() -> None:
    payload = (
        '[{"project":"apiccine-taskvine-coffea-std-1234","workers_connected":2,'
        '"tasks_waiting":5,"tasks_running":1,"tasks_done":7}]'
    )
    records, mode = probe.parse_vine_status_output(payload)

    assert mode == "json"
    assert records == [
        {
            "project": "apiccine-taskvine-coffea-std-1234",
            "workers_connected": 2,
            "tasks_waiting": 5,
            "tasks_running": 1,
            "tasks_done": 7,
        }
    ]


def test_parse_vine_status_output_text_records() -> None:
    payload = (
        "project=apiccine-taskvine-coffea-light-42 "
        "workers_connected=0 tasks_waiting=3 tasks_running=0 tasks_done=9"
    )
    records, mode = probe.parse_vine_status_output(payload)

    assert mode == "text"
    assert records == [
        {
            "project": "apiccine-taskvine-coffea-light-42",
            "workers_connected": 0,
            "tasks_waiting": 3,
            "tasks_running": 0,
            "tasks_done": 9,
        }
    ]


def test_sample_project_status_not_found(monkeypatch) -> None:
    monkeypatch.setattr(
        probe,
        "query_manager_records",
        lambda *, timeout: (
            [
                {
                    "project": "other-project",
                    "workers_connected": 4,
                    "tasks_waiting": 1,
                    "tasks_running": 2,
                    "tasks_done": 3,
                }
            ],
            "source=test",
        ),
    )

    sample = probe.sample_project_status(
        project_pattern=r"apiccine-taskvine-coffea-std-.*",
        timeout=5.0,
    )

    assert sample.matched_project == "NOT_FOUND"
    assert sample.workers_connected == 0
    assert sample.tasks_waiting == 0
    assert sample.tasks_running == 0
    assert sample.tasks_done == 0
    assert "matches=0" in sample.note


def test_sample_project_status_aggregates_multiple_matches(monkeypatch) -> None:
    monkeypatch.setattr(
        probe,
        "query_manager_records",
        lambda *, timeout: (
            [
                {
                    "project": "apiccine-taskvine-coffea-std-111",
                    "workers_connected": 1,
                    "tasks_waiting": 2,
                    "tasks_running": 3,
                    "tasks_done": 4,
                },
                {
                    "project": "apiccine-taskvine-coffea-std-222",
                    "workers_connected": 5,
                    "tasks_waiting": 6,
                    "tasks_running": 7,
                    "tasks_done": 8,
                },
            ],
            "source=test",
        ),
    )

    sample = probe.sample_project_status(
        project_pattern=r"apiccine-taskvine-coffea-std-.*",
        timeout=5.0,
    )

    assert sample.matched_project == "apiccine-taskvine-coffea-std-111|apiccine-taskvine-coffea-std-222"
    assert sample.workers_connected == 6
    assert sample.tasks_waiting == 8
    assert sample.tasks_running == 10
    assert sample.tasks_done == 12
    assert "matches=2" in sample.note
