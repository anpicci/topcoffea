"""Integration smoke test to guard ``topeft`` branch alignment."""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest
from coffea import processor
from coffea.processor.executor import ExecutorBase


class _NoOpExecutor(ExecutorBase):
    """Minimal executor that short-circuits chunk processing."""

    def __init__(self, *, result, **kwargs):
        self.result = result
        self.status = True
        self.merging = False
        self.unit = "items"
        self.desc = "Processing"
        self.compression = None
        self.function_name = None
        self.__dict__.update(kwargs)

    def __call__(self, items, function, accumulator):
        return {"out": self.result}, None


def test_topeft_analysis_processor_passes_processorabc_runtime_check(monkeypatch):
    repo_root = Path(__file__).resolve().parents[2]
    topeft_root = repo_root / "topeft"

    if not topeft_root.exists():
        pytest.skip("Sibling topeft checkout not found; skipping integration smoke test")

    branch = subprocess.check_output(
        ["git", "-C", str(topeft_root), "rev-parse", "--abbrev-ref", "HEAD"],
        text=True,
    ).strip()
    assert (
        branch == "format_update_anpicci_calcoffea"
    ), "topeft must be on format_update_anpicci_calcoffea when testing topcoffea ch_update_calcoffea"

    monkeypatch.syspath_prepend(str(topeft_root))
    monkeypatch.syspath_prepend(str(topeft_root / "analysis"))

    from types import ModuleType

    import topcoffea.modules
    import topeft.modules.topcoffea_imports as topcoffea_imports

    dummy_module = ModuleType("_dummy_topcoffea_module")
    dummy_module.__all__ = []

    hist_eft_stub = ModuleType("topcoffea.modules.HistEFT")
    hist_eft_stub.HistEFT = type("DummyHistEFT", (), {})

    setattr(topcoffea.modules, "HistEFT", hist_eft_stub)
    monkeypatch.setitem(sys.modules, "topcoffea.modules.HistEFT", hist_eft_stub)
    monkeypatch.setattr(topcoffea_imports, "require_module", lambda name: dummy_module)

    from analysis.training.simple_processor import AnalysisProcessor

    processor_instance = AnalysisProcessor(samples={})
    expected = processor_instance.accumulator.identity()

    runner = processor.Runner(
        executor=_NoOpExecutor(result=expected),
        chunksize=1,
        maxchunks=1,
        metadata_cache={},
        use_skyhook=False,
    )

    fileset = (item for item in ())
    result = runner(fileset, processor_instance)

    assert result == expected
