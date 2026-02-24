from __future__ import annotations

import json
import sys
import types
from unittest import mock

import cloudpickle
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


def _preprocessed_payload() -> dict[str, dict[str, dict[str, dict[str, object]]]]:
    return {
        "sample": {
            "files": {
                "/path.root": {"object_path": "Events", "num_entries": 5},
            }
        }
    }


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
    mock_preprocess.return_value = _preprocessed_payload()
    mock_ddr.return_value.compute.return_value = {"accumulator": 1}

    manager = object()
    data = {"sample": {"files": {"/path.root": {"object_path": "Events"}}}}
    processors = {"proc": object()}

    result = ddr_module.run_ddr(
        manager=manager,
        data=data,
        processors=processors,
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
    assert ddr_kwargs["data"] == _preprocessed_payload()
    assert ddr_kwargs["processors"] is processors
    assert ddr_kwargs["extra_files"] == ("analysis.py",)
    assert "accumulator" not in ddr_kwargs
    assert result == {"accumulator": 1}


@mock.patch.object(ddr_module, "CoffeaDynamicDataReduction")
@mock.patch.object(ddr_module, "preprocess")
def test_run_ddr_skips_preprocess_when_preprocessed_data_path(
    mock_preprocess,
    mock_ddr,
    tmp_path,
):
    preprocessed_path = tmp_path / "preprocessed.json"
    preprocessed_path.write_text(json.dumps(_preprocessed_payload()), encoding="utf-8")
    mock_ddr.return_value.compute.return_value = {"status": "ok"}

    result = ddr_module.run_ddr(
        manager=object(),
        data={"sample": {"files": {"/path.root": {"object_path": "Events"}}}},
        processors={"proc": object()},
        schema=object(),
        preprocessed_data_path=str(preprocessed_path),
    )

    mock_preprocess.assert_not_called()
    mock_ddr.assert_called_once()
    assert mock_ddr.call_args.kwargs["data"] == _preprocessed_payload()
    assert result == {"status": "ok"}


@mock.patch.object(ddr_module, "CoffeaDynamicDataReduction")
@mock.patch.object(ddr_module, "preprocess")
def test_run_ddr_reuse_mode_honors_explicit_save_path(
    mock_preprocess,
    mock_ddr,
    tmp_path,
):
    preprocessed_path = tmp_path / "preprocessed.json"
    preprocessed_path.write_text(json.dumps(_preprocessed_payload()), encoding="utf-8")
    save_path = tmp_path / "rewritten-preprocessed.json"
    mock_ddr.return_value.compute.return_value = {"status": "ok"}

    ddr_module.run_ddr(
        manager=object(),
        data={"sample": {"files": {"/path.root": {"object_path": "Events"}}}},
        processors={"proc": object()},
        schema=object(),
        preprocessed_data_path=str(preprocessed_path),
        save_preprocess_path=str(save_path),
    )

    mock_preprocess.assert_not_called()
    assert json.loads(save_path.read_text(encoding="utf-8")) == _preprocessed_payload()


@mock.patch.object(ddr_module, "CoffeaDynamicDataReduction")
@mock.patch.object(ddr_module, "preprocess")
def test_run_ddr_saves_preprocess_payload(mock_preprocess, mock_ddr, tmp_path):
    mock_preprocess.return_value = _preprocessed_payload()
    mock_ddr.return_value.compute.return_value = {"status": "ok"}
    save_path = tmp_path / "saved_preprocessed.json"

    ddr_module.run_ddr(
        manager=object(),
        data={"sample": {"files": {"/path.root": {"object_path": "Events"}}}},
        processors={"proc": object()},
        schema=object(),
        save_preprocess_path=str(save_path),
    )

    loaded = json.loads(save_path.read_text(encoding="utf-8"))
    assert loaded == _preprocessed_payload()


@mock.patch.object(ddr_module, "CoffeaDynamicDataReduction")
@mock.patch.object(ddr_module, "preprocess")
def test_run_ddr_loads_cloudpickle_preprocessed_mapping(
    mock_preprocess,
    mock_ddr,
    tmp_path,
):
    preprocessed_path = tmp_path / "preprocessed.pkl"
    preprocessed_path.write_bytes(cloudpickle.dumps(_preprocessed_payload()))
    mock_ddr.return_value.compute.return_value = {"status": "ok"}

    ddr_module.run_ddr(
        manager=object(),
        data={"sample": {"files": {"/path.root": {"object_path": "Events"}}}},
        processors={"proc": object()},
        schema=object(),
        preprocessed_data_path=str(preprocessed_path),
    )

    mock_preprocess.assert_not_called()
    assert mock_ddr.call_args.kwargs["data"] == _preprocessed_payload()


@mock.patch.object(ddr_module, "CoffeaDynamicDataReduction")
@mock.patch.object(ddr_module, "preprocess")
def test_run_ddr_rejects_invalid_preprocessed_mapping(
    mock_preprocess,
    mock_ddr,
    tmp_path,
):
    invalid_path = tmp_path / "invalid.json"
    invalid_path.write_text(json.dumps(["not", "a", "mapping"]), encoding="utf-8")

    with pytest.raises(TypeError):
        ddr_module.run_ddr(
            manager=object(),
            data={"sample": {"files": {"/path.root": {"object_path": "Events"}}}},
            processors={"proc": object()},
            schema=object(),
            preprocessed_data_path=str(invalid_path),
        )

    mock_preprocess.assert_not_called()
    mock_ddr.assert_not_called()


@mock.patch.object(ddr_module, "CoffeaDynamicDataReduction")
@mock.patch.object(ddr_module, "preprocess")
def test_run_ddr_forwards_explicit_knobs(mock_preprocess, mock_ddr):
    mock_preprocess.return_value = _preprocessed_payload()
    ddr_instance = mock_ddr.return_value
    ddr_instance.compute.return_value = {"accumulator": 2}
    ddr_instance.environment_variables = {"EXISTING": "1"}

    result = ddr_module.run_ddr(
        manager=object(),
        data={"sample": {"files": {"/path.root": {"object_path": "Events"}}}},
        processors={"proc": object()},
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

    preprocess_options = mock_preprocess.call_args.kwargs
    assert preprocess_options["x509_proxy"] == "/tmp/x509up_u123"
    assert preprocess_options["environment_variables"]["X509_USER_PROXY"] == "proxy.pem"

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
