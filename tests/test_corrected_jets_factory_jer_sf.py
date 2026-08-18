import json
from pathlib import Path

import awkward as ak
import correctionlib
import numpy as np
import pytest

from topcoffea.modules.CorrectedJetsFactory import (
    CorrectedJetsFactory,
    get_corr_inputs,
    get_jer_sf_variations,
)
from topcoffea.modules.JECStack import JECStack


_PAYLOAD_ROOT = Path(__file__).parents[1] / "topcoffea" / "data" / "POG" / "JME"
_NAME_MAP = {"JetEta": "eta", "JetPt": "pt_jec"}


def _jets():
    return ak.Array([[{"eta": 0.5, "pt_jec": 50.0}]])


def _irregular_jets():
    return ak.Array(
        [
            [{"eta": 0.5, "pt_jec": 50.0}],
            [],
            [
                {"eta": -1.0, "pt_jec": 40.0},
                {"eta": 2.0, "pt_jec": 30.0},
            ],
        ]
    )


class _RecordingCorrection:
    def __init__(self, correction):
        self.correction = correction
        self.name = correction.name
        self.inputs = correction.inputs
        self.output = correction.output
        self.calls = []

    def evaluate(self, *inputs):
        self.calls.append(inputs)
        return self.correction.evaluate(*inputs)


def _correction(name, inputs, data):
    return {
        "name": name,
        "description": "test correction",
        "version": 1,
        "inputs": inputs,
        "output": {"name": "correction", "type": "real"},
        "data": data,
    }


def _synthetic_cset(corrections):
    return correctionlib.CorrectionSet.from_string(
        json.dumps({"schema_version": 2, "corrections": corrections})
    )


def _real_inputs():
    return [
        {"name": "JetEta", "type": "real"},
        {"name": "JetPt", "type": "real"},
    ]


def _factory_jets():
    return ak.Array(
        [
            [
                {"pt": 100.0, "mass": 10.0, "pt_raw": 10.0, "mass_raw": 1.0},
                {"pt": 200.0, "mass": 20.0, "pt_raw": 20.0, "mass_raw": 2.0},
            ],
            [],
            [
                {"pt": 300.0, "mass": 30.0, "pt_raw": 30.0, "mass_raw": 3.0}
            ],
        ]
    )


def _factory_stack(tmp_path):
    jec_tag = "Test"
    jet_algo = "AK4Test"
    correction_names = [
        f"{jec_tag}_L1FastJet_{jet_algo}",
        f"{jec_tag}_L2Relative_{jet_algo}",
    ]
    payload = {
        "schema_version": 2,
        "corrections": [
            _correction(
                correction_names[0],
                [{"name": "JetPt", "type": "real"}],
                2.0,
            ),
            _correction(
                correction_names[1],
                [{"name": "JetPt", "type": "real"}],
                {
                    "nodetype": "formula",
                    "expression": "x / 10.0",
                    "parser": "TFormula",
                    "variables": ["JetPt"],
                },
            ),
        ],
    }
    payload_path = tmp_path / "synthetic_jec.json"
    payload_path.write_text(json.dumps(payload))
    return JECStack(
        use_clib=True,
        jec_tag=jec_tag,
        jec_levels=["L1FastJet", "L2Relative"],
        jet_algo=jet_algo,
        json_path=str(payload_path),
    )


def _factory(stack):
    return CorrectedJetsFactory(
        {
            "JetPt": "pt",
            "JetMass": "mass",
            "ptRaw": "pt_raw",
            "massRaw": "mass_raw",
        },
        stack,
        run=None,
    )


@pytest.mark.parametrize(
    "payload,scale_factor_name",
    [
        (
            "2018_UL/jet_jerc.json.gz",
            "Summer19UL18_JRV3_MC_ScaleFactor_AK4PFchs",
        ),
        (
            "2022_Summer22/jet_jerc.json.gz",
            "Summer22_22Sep2023_JRV2_MC_ScaleFactor_AK4PFPuppi",
        ),
        (
            "2023_Summer23/jet_jerc.json.gz",
            "Summer23Prompt23_RunCv1234_JRV3_MC_ScaleFactor_AK4PFPuppi",
        ),
    ],
)
def test_current_paired_jer_sf_payloads_preserve_all_variations(
    payload, scale_factor_name
):
    correction_set = correctionlib.CorrectionSet.from_file(str(_PAYLOAD_ROOT / payload))
    scale_factor = correction_set[scale_factor_name]
    companion_name = scale_factor_name.replace("_ScaleFactor_", "_SFUncertainty_", 1)
    uncertainty = correction_set[companion_name]

    nominal, up, down = get_jer_sf_variations(
        _jets(), scale_factor, correction_set, _NAME_MAP, run=1
    )
    expected_nominal = scale_factor.evaluate(np.array([0.5]), np.array([50.0]))
    expected_uncertainty = uncertainty.evaluate(np.array([0.5]), np.array([50.0]))

    np.testing.assert_allclose(nominal, expected_nominal)
    np.testing.assert_allclose(up, expected_nominal * (1.0 + expected_uncertainty))
    np.testing.assert_allclose(down, expected_nominal * (1.0 - expected_uncertainty))


def test_current_run3_paired_jer_sf_is_nondegenerate():
    correction_set = correctionlib.CorrectionSet.from_file(
        str(_PAYLOAD_ROOT / "2023_Summer23/jet_jerc.json.gz")
    )
    scale_factor = correction_set[
        "Summer23Prompt23_RunCv1234_JRV3_MC_ScaleFactor_AK4PFPuppi"
    ]

    nominal, up, down = get_jer_sf_variations(
        _jets(), scale_factor, correction_set, _NAME_MAP, run=1
    )

    assert not np.array_equal(up, nominal)
    assert not np.array_equal(down, nominal)


def test_explicit_variation_is_inserted_at_declared_position():
    scale_factor_name = "Legacy_ScaleFactor_AK4PFPuppi"
    correction_set = _synthetic_cset(
        [
            _correction(
                scale_factor_name,
                [
                    {"name": "JetEta", "type": "real"},
                    {"name": "systematic", "type": "string"},
                    {"name": "JetPt", "type": "real"},
                ],
                {
                    "nodetype": "category",
                    "input": "systematic",
                    "content": [
                        {"key": "nom", "value": 1.0},
                        {"key": "up", "value": 1.1},
                        {"key": "down", "value": 0.9},
                    ],
                },
            )
        ]
    )
    scale_factor = _RecordingCorrection(correction_set[scale_factor_name])

    inputs = get_corr_inputs(
        _jets(), scale_factor, _NAME_MAP, run=1, variation="nom"
    )
    assert inputs[1] == "nom"
    assert isinstance(inputs[0], np.ndarray)
    assert isinstance(inputs[2], np.ndarray)
    assert not isinstance(inputs[0], ak.highlevel.Array)
    assert not isinstance(inputs[2], ak.highlevel.Array)
    np.testing.assert_allclose(inputs[0], np.array([0.5]))
    np.testing.assert_allclose(inputs[2], np.array([50.0]))

    nominal, up, down = get_jer_sf_variations(
        _jets(), scale_factor, correction_set, _NAME_MAP, run=1
    )
    np.testing.assert_allclose(nominal, np.array([1.0]))
    np.testing.assert_allclose(up, np.array([1.1]))
    np.testing.assert_allclose(down, np.array([0.9]))
    assert [call[1] for call in scale_factor.calls] == ["nom", "up", "down"]
    for call in scale_factor.calls:
        assert isinstance(call[0], np.ndarray)
        assert isinstance(call[2], np.ndarray)
        assert not any(isinstance(value, ak.highlevel.Array) for value in call)


def test_missing_paired_jer_sf_uncertainty_fails_clearly():
    scale_factor_name = "Missing_ScaleFactor_AK4PFPuppi"
    correction_set = _synthetic_cset(
        [_correction(scale_factor_name, _real_inputs(), 1.0)]
    )

    with pytest.raises(ValueError, match="requires paired SFUncertainty.*absent"):
        get_jer_sf_variations(
            _jets(), correction_set[scale_factor_name], correction_set, _NAME_MAP, run=1
        )


def test_incompatible_paired_jer_sf_uncertainty_fails_clearly():
    scale_factor_name = "Incompatible_ScaleFactor_AK4PFPuppi"
    companion_name = scale_factor_name.replace("_ScaleFactor_", "_SFUncertainty_", 1)
    correction_set = _synthetic_cset(
        [
            _correction(scale_factor_name, _real_inputs(), 1.0),
            _correction(
                companion_name,
                [
                    {"name": "JetPt", "type": "real"},
                    {"name": "JetEta", "type": "real"},
                ],
                0.1,
            ),
        ]
    )

    with pytest.raises(ValueError, match="inputs .* incompatible"):
        get_jer_sf_variations(
            _jets(), correction_set[scale_factor_name], correction_set, _NAME_MAP, run=1
        )


def test_corr_inputs_materialize_run_and_current_corrected_pt_as_numpy():
    correction_set = _synthetic_cset(
        [
            _correction(
                "RunPtCorrection",
                [
                    {"name": "run", "type": "int"},
                    {"name": "JetPt", "type": "real"},
                ],
                1.0,
            )
        ]
    )
    inputs = get_corr_inputs(
        _irregular_jets(),
        correction_set["RunPtCorrection"],
        _NAME_MAP,
        run=321,
        cache={},
        corrections=np.array([2.0, 3.0, 4.0], dtype=np.float32),
    )

    assert all(isinstance(value, np.ndarray) for value in inputs)
    assert not any(isinstance(value, ak.highlevel.Array) for value in inputs)
    np.testing.assert_array_equal(inputs[0], np.array([321, 321, 321], dtype=np.int32))
    np.testing.assert_allclose(inputs[1], np.array([100.0, 120.0, 120.0]))


def test_corrected_jets_clib_jec_preserves_cumulative_values_and_structure(tmp_path):
    stack = _factory_stack(tmp_path)
    factory = _factory(stack)
    recording_corrections = {
        name: _RecordingCorrection(correction)
        for name, correction in stack.corrections.items()
    }
    factory.corrections = recording_corrections

    corrected = factory.build(_factory_jets(), lazy_cache={})

    expected_l2_input = np.array([20.0, 40.0, 60.0])
    expected_total = np.array([4.0, 8.0, 12.0], dtype=np.float32)
    assert ak.to_list(ak.num(corrected)) == [2, 0, 1]
    np.testing.assert_allclose(
        ak.to_numpy(ak.flatten(corrected.pt)),
        np.array([40.0, 160.0, 360.0]),
    )
    np.testing.assert_allclose(
        ak.to_numpy(ak.flatten(corrected.jet_energy_correction)),
        expected_total,
    )
    np.testing.assert_allclose(
        recording_corrections[stack.jec_names_clib[1]].calls[0][0],
        expected_l2_input,
    )
    for correction in recording_corrections.values():
        for call in correction.calls:
            assert all(isinstance(value, np.ndarray) for value in call)
            assert not any(isinstance(value, ak.highlevel.Array) for value in call)


def test_corrected_jets_clib_jec_supports_all_zero_jet_events(tmp_path):
    stack = _factory_stack(tmp_path)
    factory = _factory(stack)
    factory.corrections = {
        name: _RecordingCorrection(correction)
        for name, correction in stack.corrections.items()
    }
    jets = _factory_jets()[:, :0]

    corrected = factory.build(jets, lazy_cache={})

    assert ak.to_list(corrected.pt) == [[], [], []]
    for correction in factory.corrections.values():
        assert len(correction.calls) == 1
        assert correction.calls[0][0].shape == (0,)
        assert isinstance(correction.calls[0][0], np.ndarray)
