import json
from pathlib import Path

import awkward as ak
import correctionlib
import numpy as np
import pytest

from topcoffea.modules.CorrectedJetsFactory import (
    get_corr_inputs,
    get_jer_sf_variations,
)


_PAYLOAD_ROOT = Path(__file__).parents[1] / "topcoffea" / "data" / "POG" / "JME"
_NAME_MAP = {"JetEta": "eta", "JetPt": "pt_jec"}


def _jets():
    return ak.Array([[{"eta": 0.5, "pt_jec": 50.0}]])


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
    scale_factor = correction_set[scale_factor_name]

    inputs = get_corr_inputs(
        _jets(), scale_factor, _NAME_MAP, run=1, variation="nom"
    )
    assert inputs[1] == "nom"
    np.testing.assert_allclose(inputs[0], np.array([0.5]))
    np.testing.assert_allclose(inputs[2], np.array([50.0]))

    nominal, up, down = get_jer_sf_variations(
        _jets(), scale_factor, correction_set, _NAME_MAP, run=1
    )
    np.testing.assert_allclose(nominal, np.array([1.0]))
    np.testing.assert_allclose(up, np.array([1.1]))
    np.testing.assert_allclose(down, np.array([0.9]))


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
