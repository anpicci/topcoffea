from __future__ import annotations

import sys
import types
from unittest import mock

import pytest

if "numpy" not in sys.modules:
    dummy_np = types.ModuleType("numpy")
    dummy_np.__version__ = "0.0"
    sys.modules["numpy"] = dummy_np
if "pandas" not in sys.modules:
    dummy_pd = types.ModuleType("pandas")
    dummy_pd.__version__ = "0.0"
    dummy_pd._libs = types.SimpleNamespace(  # type: ignore[attr-defined]
        hashtable=types.SimpleNamespace(Int64HashTable=object)
    )
    sys.modules["pandas"] = dummy_pd

from topcoffea.modules import dynamic_data_reduction as ddr_module
from topcoffea.modules.executor_cli import executor_config_from_values


def test_build_ddr_data_from_flist_basic():
    flist = {
        "sampleA": ["/store/user/foo.root", "/store/user/bar.root"],
        "sampleB": {"files": ["/store/user/baz.root"]},
    }

    result = ddr_module.build_ddr_data_from_flist(flist)

    assert set(result.keys()) == {"sampleA", "sampleB"}
    assert set(result["sampleA"]["files"].keys()) == {
        "/store/user/foo.root",
        "/store/user/bar.root",
    }
    assert result["sampleA"]["files"]["/store/user/foo.root"]["object_path"] == "Events"
    assert set(result["sampleB"]["files"].keys()) == {"/store/user/baz.root"}


@mock.patch.object(ddr_module, "CoffeaDynamicDataReduction")
@mock.patch.object(ddr_module, "preprocess")
def test_run_ddr_invokes_preprocess_and_ddr(mock_preprocess, mock_ddr):
    mock_preprocess.return_value = {
        "sample": {
            "files": {
                "/path.root": {"object_path": "Events", "num_entries": 5},
            }
        }
    }
    mock_ddr.return_value.compute.return_value = {"accumulator": 1}

    manager = object()
    data = {"sample": {"files": {"/path.root": {"object_path": "Events"}}}}
    processors = {"proc": object()}

    result = ddr_module.run_ddr(
        manager=manager,
        data=data,
        processors=processors,
        accumulator="accumulator",
        schema="schema",
        extra_files=("analysis.py",),
        preprocess_kwargs={"timeout": 1},
        ddr_kwargs={"results_directory": "/tmp"},
    )

    mock_preprocess.assert_called_once()
    kwargs = mock_preprocess.call_args.kwargs
    assert kwargs["manager"] is manager
    assert kwargs["data"] is data
    assert kwargs["tree_name"] == "Events"

    mock_ddr.assert_called_once()
    ddr_kwargs = mock_ddr.call_args.kwargs
    assert ddr_kwargs["data"] == {
        "sample": {
            "files": {
                "/path.root": {"object_path": "Events", "num_entries": 5},
            }
        }
    }
    assert ddr_kwargs["processors"] is processors
    assert ddr_kwargs["extra_files"] == ("analysis.py",)
    assert result == {"accumulator": 1}


@mock.patch.object(ddr_module, "CoffeaDynamicDataReduction")
@mock.patch.object(ddr_module, "preprocess")
def test_run_ddr_forwards_explicit_knobs(mock_preprocess, mock_ddr):
    mock_preprocess.return_value = {
        "sample": {
            "files": {
                "/path.root": {"object_path": "Events", "num_entries": 5},
            }
        }
    }
    ddr_instance = mock_ddr.return_value
    ddr_instance.compute.return_value = {"accumulator": 2}
    ddr_instance.environment_variables = {"EXISTING": "1"}

    result = ddr_module.run_ddr(
        manager=object(),
        data={"sample": {"files": {"/path.root": {"object_path": "Events"}}}},
        processors={"proc": object()},
        accumulator="accumulator",
        schema="schema",
        step_size=600000,
        max_task_retries=20,
        resources_processing={"cores": 2},
        resources_accumulating={"cores": 1},
        results_directory="/tmp/results",
        verbose=True,
        x509_proxy="/tmp/x509up_u123",
        environment_variables={"X509_USER_PROXY": "proxy.pem"},
        preprocess_kwargs={"timeout": 10},
    )

    preprocess_kwargs = mock_preprocess.call_args.kwargs
    assert preprocess_kwargs["x509_proxy"] == "/tmp/x509up_u123"

    ddr_kwargs = mock_ddr.call_args.kwargs
    assert ddr_kwargs["step_size"] == 600000
    assert ddr_kwargs["max_task_retries"] == 20
    assert ddr_kwargs["resources_processing"] == {"cores": 2}
    assert ddr_kwargs["resources_accumulating"] == {"cores": 1}
    assert ddr_kwargs["results_directory"] == "/tmp/results"
    assert ddr_kwargs["verbose"] is True
    assert ddr_kwargs["x509_proxy"] == "/tmp/x509up_u123"
    assert ddr_instance.environment_variables["EXISTING"] == "1"
    assert ddr_instance.environment_variables["X509_USER_PROXY"] == "proxy.pem"
    assert result == {"accumulator": 2}


@pytest.mark.integration
@pytest.mark.taskvine
def test_executor_cli_accepts_taskvine_executor(tmp_path):
    fake_env = tmp_path / "env.tar.gz"
    fake_env.write_text("placeholder")

    config = executor_config_from_values(
        executor="taskvine",
        port="9200-9201",
        environment_file=str(fake_env),
    )

    assert config.executor == "taskvine"
    assert config.port == (9200, 9201)
    assert config.environment_file == str(fake_env)
