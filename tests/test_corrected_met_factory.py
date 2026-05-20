import math

import awkward as ak
import numpy as np
import pytest

from topcoffea.modules.CorrectedMETFactory import CorrectedMETFactory
from topcoffea.modules.Type1CorrectedMETFactory import Type1CorrectedMETFactory


NAME_MAP = {
    "METpt": "pt",
    "METphi": "phi",
    "JetPt": "pt",
    "JetPhi": "phi",
    "ptRaw": "pt_raw",
    "UnClusteredEnergyDeltaX": "MetUnclustEnUpDeltaX",
    "UnClusteredEnergyDeltaY": "MetUnclustEnUpDeltaY",
}


def _jets():
    return ak.Array([[{"pt": 50.0, "phi": 0.0, "pt_raw": 40.0}]])


def _value(array):
    return float(ak.to_numpy(ak.materialized(array))[0])


class _CorrectionInput:
    def __init__(self, name):
        self.name = name


class _ConstantCorrection:
    def __init__(self, scale):
        self.scale = np.float32(scale)
        self.inputs = [
            _CorrectionInput("JetPt"),
            _CorrectionInput("JetEta"),
            _CorrectionInput("JetA"),
            _CorrectionInput("Rho"),
        ]

    def evaluate(self, *inputs):
        return np.ones(len(np.asarray(inputs[0])), dtype=np.float32) * self.scale


class _FakeJECStack:
    def __init__(self, l1_scale=1.0, l2_scale=2.0):
        self.use_clib = True
        self.jec_names_clib = [
            "Test_L1FastJet_AK4PFPuppi",
            "Test_L2Relative_AK4PFPuppi",
        ]
        self.corrections = {
            self.jec_names_clib[0]: _ConstantCorrection(l1_scale),
            self.jec_names_clib[1]: _ConstantCorrection(l2_scale),
        }

    def get_l1_jec_names(self):
        return [self.jec_names_clib[0]]

    def get_full_jec_names(self):
        return list(self.jec_names_clib)


TYPE1_NAME_MAP = {
    "METpt": "pt",
    "METphi": "phi",
    "RawMETpt": "pt",
    "RawMETphi": "phi",
    "JetPt": "pt",
    "JetPhi": "phi",
    "JetEta": "eta",
    "JetA": "area",
    "JetRawFactor": "rawFactor",
    "JetMuonSubtrFactor": "muonSubtrFactor",
    "JetMuonSubtrDeltaPhi": "muonSubtrDeltaPhi",
    "JetChEmEF": "chEmEF",
    "JetNeEmEF": "neEmEF",
    "CorrT1JetPt": "rawPt",
    "CorrT1JetPhi": "phi",
    "CorrT1JetEta": "eta",
    "CorrT1JetArea": "area",
    "CorrT1JetMuonSubtrFactor": "muonSubtrFactor",
    "CorrT1JetMuonSubtrDeltaPhi": "muonSubtrDeltaPhi",
    "CorrT1JetEmEF": "EmEF",
    "Rho": "rho",
    "UnClusteredEnergyDeltaX": "MetUnclustEnUpDeltaX",
    "UnClusteredEnergyDeltaY": "MetUnclustEnUpDeltaY",
}


def _stored_puppimet():
    return ak.Array(
        [
            {
                "pt": 120.0,
                "phi": 0.0,
                "ptUnclusteredUp": 130.0,
                "ptUnclusteredDown": 110.0,
                "phiUnclusteredUp": 0.0,
                "phiUnclusteredDown": 0.0,
            }
        ]
    )


def _raw_puppimet():
    return ak.Array([{"pt": 100.0, "phi": 0.0}])


def _type1_jets(include_delta_phi=False, em_fail=False, low_pt=False):
    jet = {
        "pt": 50.0 if not low_pt else 10.0,
        "phi": 0.0,
        "eta": 0.2,
        "area": 0.5,
        "rawFactor": 0.2,
        "muonSubtrFactor": 0.1,
        "chEmEF": 0.2 if not em_fail else 0.6,
        "neEmEF": 0.1 if not em_fail else 0.35,
        "rho": 20.0,
        "JER": {"up": {"pt": 75.0}, "down": {"pt": 25.0}},
        "JES_Total": {"up": {"pt": 60.0}, "down": {"pt": 45.0}},
    }
    if include_delta_phi:
        jet["muonSubtrDeltaPhi"] = math.pi / 2.0
    return ak.Array([[jet]])


def _corr_t1_jets(include_delta_phi=False, include_emef=False, em_fail=False):
    corr = {
        "rawPt": 20.0,
        "phi": 0.0,
        "eta": 0.3,
        "area": 0.4,
        "muonSubtrFactor": 0.5,
        "rho": 20.0,
    }
    if include_delta_phi:
        corr["muonSubtrDeltaPhi"] = math.pi / 2.0
    if include_emef:
        corr["EmEF"] = 0.95 if em_fail else 0.1
    return ak.Array([[corr]])


def _type1_factory():
    return Type1CorrectedMETFactory(TYPE1_NAME_MAP, _FakeJECStack())


def _build_type1(jets=None, corr_t1=None):
    return _type1_factory().build(
        _stored_puppimet(),
        _raw_puppimet(),
        _type1_jets() if jets is None else jets,
        _corr_t1_jets() if corr_t1 is None else corr_t1,
        lazy_cache={},
    )


def test_met_unclustered_energy_legacy_delta_xy_mode():
    met = ak.Array(
        [
            {
                "pt": 100.0,
                "phi": 0.0,
                "MetUnclustEnUpDeltaX": 10.0,
                "MetUnclustEnUpDeltaY": 0.0,
            }
        ]
    )

    corrected = CorrectedMETFactory(NAME_MAP).build(met, _jets(), lazy_cache={})

    assert _value(corrected.pt) == pytest.approx(110.0)
    assert _value(corrected.MET_UnclusteredEnergy.up.pt) == pytest.approx(120.0)
    assert _value(corrected.MET_UnclusteredEnergy.down.pt) == pytest.approx(100.0)
    assert _value(corrected.MET_UnclusteredEnergy.up.phi) == pytest.approx(0.0)
    assert _value(corrected.MET_UnclusteredEnergy.down.phi) == pytest.approx(0.0)


def test_met_unclustered_energy_direct_pt_phi_mode():
    met = ak.Array(
        [
            {
                "pt": 100.0,
                "phi": 0.0,
                "ptUnclusteredUp": 120.0,
                "ptUnclusteredDown": 80.0,
                "phiUnclusteredUp": 0.0,
                "phiUnclusteredDown": 0.0,
            }
        ]
    )

    corrected = CorrectedMETFactory(NAME_MAP).build(met, _jets(), lazy_cache={})

    assert _value(corrected.pt) == pytest.approx(110.0)
    assert _value(corrected.MET_UnclusteredEnergy.up.pt) == pytest.approx(130.0)
    assert _value(corrected.MET_UnclusteredEnergy.up.phi) == pytest.approx(0.0)
    assert _value(corrected.MET_UnclusteredEnergy.down.pt) == pytest.approx(90.0)
    assert _value(corrected.MET_UnclusteredEnergy.down.phi) == pytest.approx(0.0)


def test_met_unclustered_energy_missing_fields_fails_clearly():
    met = ak.Array([{"pt": 100.0, "phi": 0.0}])

    with pytest.raises(ValueError, match="could not build MET_UnclusteredEnergy"):
        CorrectedMETFactory(NAME_MAP).build(met, _jets(), lazy_cache={})


def test_type1_met_nominal_formula_and_v12_fallbacks():
    corrected = _build_type1()

    # Jet: 50 * (1 - 0.2) * (1 - 0.1) = 36, full-L1 delta = 36.
    # CorrT1METJet: 20 * (1 - 0.5) = 10, full-L1 delta = 10.
    # RawPuppiMET x is 100, so Type-1 x is 100 - 46.
    assert _value(corrected.pt) == pytest.approx(54.0)
    assert _value(corrected.phi) == pytest.approx(0.0)


def test_type1_met_missing_delta_phi_fallback_uses_collection_phi():
    nominal = _build_type1()
    shifted = _build_type1(
        jets=_type1_jets(include_delta_phi=True),
        corr_t1=_corr_t1_jets(include_delta_phi=True),
    )

    assert _value(nominal.pt) == pytest.approx(54.0)
    assert _value(shifted.pt) == pytest.approx(math.hypot(100.0, 46.0))
    assert _value(shifted.phi) == pytest.approx(math.atan2(-46.0, 100.0))


def test_type1_met_missing_corr_t1_emef_skips_only_corr_t1_cut():
    missing_emef = _build_type1(corr_t1=_corr_t1_jets(include_emef=False))
    failing_emef = _build_type1(corr_t1=_corr_t1_jets(include_emef=True, em_fail=True))

    assert _value(missing_emef.pt) == pytest.approx(54.0)
    assert _value(failing_emef.pt) == pytest.approx(64.0)


def test_type1_met_jet_em_and_pt_cuts():
    jet_em_fail = _build_type1(jets=_type1_jets(em_fail=True))
    jet_low_pt = _build_type1(jets=_type1_jets(low_pt=True))

    assert _value(jet_em_fail.pt) == pytest.approx(90.0)
    assert _value(jet_low_pt.pt) == pytest.approx(90.0)


def test_type1_met_direct_unclustered_recentering():
    corrected = _build_type1()

    assert _value(corrected.MET_UnclusteredEnergy.up.pt) == pytest.approx(64.0)
    assert _value(corrected.MET_UnclusteredEnergy.down.pt) == pytest.approx(44.0)
    assert _value(corrected.MET_UnclusteredEnergy.up.phi) == pytest.approx(0.0)
    assert _value(corrected.MET_UnclusteredEnergy.down.phi) == pytest.approx(0.0)


def test_type1_met_jes_jer_variations_vary_jet_full_leg_only():
    corrected = _build_type1()

    assert "MET_UnclusteredEnergy" in ak.fields(corrected)
    assert "JER" in ak.fields(corrected)
    assert "JES_Total" in ak.fields(corrected)
    assert {"up", "down"} == set(ak.fields(corrected.JER))
    assert {"up", "down"} == set(ak.fields(corrected.JES_Total))

    # JER up scales the Jet full leg by 75/50, while the Jet L1 leg and
    # CorrT1METJet contribution stay nominal: raw 100 - (72 + 10) = 18.
    assert _value(corrected.JER.up.pt) == pytest.approx(18.0)
    # JER down scales the Jet full leg by 25/50, below the L1 leg:
    # raw 100 - ((36 - 36) + 10) = 90.
    assert _value(corrected.JER.down.pt) == pytest.approx(90.0)
    # JES_Total up/down use the same public field shape.
    assert _value(corrected.JES_Total.up.pt) == pytest.approx(39.6)
    assert _value(corrected.JES_Total.down.pt) == pytest.approx(61.2)
