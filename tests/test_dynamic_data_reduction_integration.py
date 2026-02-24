from __future__ import annotations

import pytest

from topcoffea.modules import dynamic_data_reduction as ddr_module


def test_filter_preprocessed_data_filters_bad_files():
    preprocessed = {
        "A": {
            "files": {
                "good1.root": {"num_entries": 100},
                "bad1.root": {"num_entries": None},
            }
        },
        "B": {"files": {"bad2.root": {"num_entries": 0}}},
        "C": {
            "files": {
                "good2.root": {"num_entries": 42},
                "good3.root": {"num_entries": 1.5},
            }
        },
    }

    filtered, summary = ddr_module.filter_preprocessed_data(preprocessed)

    assert summary["total_files"] == 5
    assert summary["good_files_count"] == 3
    assert summary["bad_files_count"] == 2
    assert summary["dropped_datasets"] == ["B"]
    assert "A" in filtered and "C" in filtered and "B" not in filtered
    assert list(filtered["A"]["files"].keys()) == ["good1.root"]
    assert set(filtered["C"]["files"].keys()) == {"good2.root", "good3.root"}


def test_run_ddr_raises_when_no_usable_files(monkeypatch):
    def fake_preprocess(**kwargs):
        return {"Sample": {"files": {"bad.root": {"num_entries": None}}}}

    monkeypatch.setattr(ddr_module, "preprocess", fake_preprocess)
    monkeypatch.setattr(ddr_module, "_diagnose_failed_file", lambda *args, **kwargs: "diagnosed")

    with pytest.raises(RuntimeError):
        ddr_module.run_ddr(
            manager=object(),
            data={"Sample": {}},
            processors={"proc": object()},
            schema=object(),
        )


def test_run_ddr_passes_filtered_data_to_ddr(monkeypatch):
    preprocessed = {
        "Sample": {
            "files": {
                "good.root": {"num_entries": 50, "metadata": {"foo": "bar"}},
                "bad.root": {"num_entries": None},
            },
            "metadata": {"sample_meta": True},
        }
    }

    def fake_preprocess(**kwargs):
        return preprocessed

    monkeypatch.setattr(ddr_module, "preprocess", fake_preprocess)
    monkeypatch.setattr(ddr_module, "_diagnose_failed_file", lambda *args, **kwargs: "diagnosed")

    captured = {}

    class DummyDDR:
        def __init__(self, manager, data, **kwargs):
            captured["data"] = data

        def compute(self):
            return {"status": "ok"}

    monkeypatch.setattr(ddr_module, "CoffeaDynamicDataReduction", DummyDDR)

    result = ddr_module.run_ddr(
        manager=object(),
        data={"Sample": {}},
        processors={"proc": object()},
        schema=object(),
    )

    assert result == {"status": "ok"}
    assert set(captured["data"].keys()) == {"Sample"}
    files = captured["data"]["Sample"]["files"]
    assert list(files.keys()) == ["good.root"]
    assert files["good.root"]["num_entries"] == 50
