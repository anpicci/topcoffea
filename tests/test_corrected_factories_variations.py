import awkward as ak
import numpy as np

from topcoffea.modules.CorrectedJetsFactory import CorrectedJetsFactory
from topcoffea.modules.CorrectedMETFactory import CorrectedMETFactory
from topcoffea.modules.JECStack import JECStack


def _example_name_map():
    return {
        "JetPt": "pt",
        "JetMass": "mass",
        "JetEta": "eta",
        "JetPhi": "phi",
        "ptRaw": "pt_raw",
        "massRaw": "mass_raw",
        "ptGenJet": "pt_gen",
        "Rho": "rho",
        "JetA": "area",
    }


def _example_met_name_map():
    return {
        "METpt": "pt",
        "METphi": "phi",
        "JetPt": "pt",
        "JetPhi": "phi",
        "ptRaw": "pt_raw",
        "UnClusteredEnergyDeltaX": "MetUnclustEnUpDeltaX",
        "UnClusteredEnergyDeltaY": "MetUnclustEnUpDeltaY",
    }


def _example_jets():
    return ak.Array(
        [
            [
                {
                    "pt": 30.0,
                    "mass": 3.0,
                    "pt_raw": 29.0,
                    "mass_raw": 3.0,
                    "eta": 0.1,
                    "phi": 0.0,
                    "pt_gen": 28.0,
                    "rho": 15.0,
                    "area": 0.5,
                },
                {
                    "pt": 40.0,
                    "mass": 4.0,
                    "pt_raw": 39.0,
                    "mass_raw": 4.0,
                    "eta": -0.2,
                    "phi": 0.2,
                    "pt_gen": 39.0,
                    "rho": 15.0,
                    "area": 0.5,
                },
            ],
            [
                {
                    "pt": 25.0,
                    "mass": 2.5,
                    "pt_raw": 24.5,
                    "mass_raw": 2.5,
                    "eta": 0.4,
                    "phi": -0.1,
                    "pt_gen": 24.0,
                    "rho": 20.0,
                    "area": 0.4,
                },
            ],
        ]
    )


def _example_met():
    return ak.Array(
        [
            {
                "pt": 50.0,
                "phi": 0.0,
                "MetUnclustEnUpDeltaX": 5.0,
                "MetUnclustEnUpDeltaY": -2.0,
            },
            {
                "pt": 35.0,
                "phi": 0.5,
                "MetUnclustEnUpDeltaX": -3.0,
                "MetUnclustEnUpDeltaY": 1.0,
            },
        ]
    )


class _FakeJEC:
    signature = ("JetPt",)

    def getCorrection(self, JetPt):
        return ak.ones_like(JetPt, dtype=np.float32)


class _FakeJunc:
    signature = ("JetPt",)

    def getUncertainty(self, JetPt):
        flat = ak.to_numpy(JetPt, allow_missing=False)
        unc = np.ones_like(flat, dtype=np.float32) * 0.02
        factors = np.stack([1 + unc, 1 - unc], axis=-1)
        return [
            ("FlavorQCD", ak.Array(factors)),
            ("Absolute", ak.Array(factors)),
        ]


class _FakeJER:
    signature = ("JetPt", "JetEta")

    def getResolution(self, JetPt, JetEta):
        return ak.ones_like(JetPt, dtype=np.float32) * 0.1


class _FakeJERSF:
    signature = ("JetPt", "JetEta")

    def getScaleFactor(self, JetPt, JetEta):
        flat = ak.to_numpy(JetPt, allow_missing=False)
        base = np.ones_like(flat, dtype=np.float32)
        stacked = np.stack([base, base * 1.01, base * 0.99], axis=-1)
        return ak.Array(stacked)


def _fake_stack():
    stack = JECStack.__new__(JECStack)  # bypass __init__
    stack.use_clib = False
    stack.jec = _FakeJEC()
    stack.junc = _FakeJunc()
    stack.jer = _FakeJER()
    stack.jersf = _FakeJERSF()
    stack.corrections = {}
    return stack


def _build_factories(allowed_variations=None):
    jets_factory = CorrectedJetsFactory(
        _example_name_map().copy(),
        _fake_stack(),
        allowed_variations=allowed_variations,
    )
    corrected_jets = jets_factory.build(_example_jets())
    met_factory = CorrectedMETFactory(
        _example_met_name_map(),
        allowed_variations=allowed_variations,
    )
    corrected_met = met_factory.build(_example_met(), corrected_jets)
    return corrected_jets, corrected_met, met_factory


def test_factories_build_all_variations_by_default():
    jets, met, met_factory = _build_factories()
    jet_fields = set(ak.fields(jets))
    met_fields = set(ak.fields(met))

    assert "JER" in jet_fields
    assert "JES_FlavorQCD" in jet_fields
    assert "JES_Absolute" in jet_fields
    assert "MET_UnclusteredEnergy" in met_fields
    assert "JER" in met_fields
    assert "JES_FlavorQCD" in met_fields
    assert met_factory.uncertainties() == ["MET_UnclusteredEnergy"]


def test_factories_filter_jes_components():
    jets, met, _ = _build_factories({"jes": {"components": ["FlavorQCD"]}})
    jet_fields = set(ak.fields(jets))
    met_fields = set(ak.fields(met))

    assert "JES_FlavorQCD" in jet_fields
    assert "JES_Absolute" not in jet_fields
    assert "JES_FlavorQCD" in met_fields
    assert "JES_Absolute" not in met_fields


def test_factories_disable_jer():
    jets, met, _ = _build_factories({"jer": False})
    assert "JER" not in ak.fields(jets)
    assert "JER" not in ak.fields(met)


def test_factories_disable_ues():
    jets_factory = CorrectedJetsFactory(_example_name_map().copy(), _fake_stack())
    corrected_jets = jets_factory.build(_example_jets())
    met_factory = CorrectedMETFactory(_example_met_name_map(), {"ues": False})
    corrected_met = met_factory.build(_example_met(), corrected_jets)

    assert "MET_UnclusteredEnergy" not in ak.fields(corrected_met)
    assert met_factory.uncertainties() == []


def test_factories_combined_filters():
    allowed = {
        "jes": {"components": ["FlavorQCD"]},
        "jer": False,
        "ues": False,
    }
    jets, met, met_factory = _build_factories(allowed)
    jet_fields = set(ak.fields(jets))
    met_fields = set(ak.fields(met))

    assert "JER" not in jet_fields
    assert "JER" not in met_fields
    assert "JES_FlavorQCD" in jet_fields
    assert "JES_Absolute" not in jet_fields
    assert "JES_FlavorQCD" in met_fields
    assert "MET_UnclusteredEnergy" not in met_fields
    assert met_factory.uncertainties() == []
