import awkward as ak
import numpy as np

from topcoffea.modules.CorrectedJetsFactory import CorrectedJetsFactory
from topcoffea.modules.JECStack import JECStack


def _example_name_map():
    return {
        "JetPt": "pt",
        "JetMass": "mass",
        "ptRaw": "pt_raw",
        "massRaw": "mass_raw",
        "JetEta": "eta",
        "JetPhi": "phi",
        "ptGenJet": "pt_gen",
    }


def test_corrected_jets_factory_build_without_cache():
    name_map = _example_name_map()
    jets = ak.Array(
        [
            [
                {
                    "pt": 20.0,
                    "mass": 2.5,
                    "pt_raw": 20.0,
                    "mass_raw": 2.5,
                    "eta": 0.1,
                    "phi": 0.2,
                    "pt_gen": 20.0,
                },
                {
                    "pt": 35.0,
                    "mass": 4.0,
                    "pt_raw": 35.0,
                    "mass_raw": 4.0,
                    "eta": -0.3,
                    "phi": -0.1,
                    "pt_gen": 35.0,
                },
            ]
        ]
    )

    factory = CorrectedJetsFactory(name_map, JECStack())
    corrected_jets = factory.build(jets)

    np.testing.assert_allclose(
        ak.to_numpy(ak.flatten(corrected_jets[name_map["JetPt"]])),
        ak.to_numpy(ak.flatten(jets[name_map["JetPt"]])),
    )
    np.testing.assert_allclose(
        ak.to_numpy(ak.flatten(corrected_jets[name_map["JetMass"]])),
        ak.to_numpy(ak.flatten(jets[name_map["JetMass"]])),
    )


def test_corrected_jets_factory_allows_corrections_without_cache():
    name_map = _example_name_map()
    jets = ak.Array(
        [
            [
                {
                    "pt": 25.0,
                    "mass": 3.0,
                    "pt_raw": 25.0,
                    "mass_raw": 3.0,
                    "eta": 0.0,
                    "phi": 0.0,
                    "pt_gen": 25.0,
                }
            ]
        ]
    )

    class FakeCorrection:
        signature = ("JetPt",)

        def __init__(self):
            self.calls = []

        def getCorrection(self, JetPt):
            self.calls.append(JetPt)
            return np.ones_like(JetPt)

    stack = JECStack()
    stack.jec = FakeCorrection()
    stack.junc = None
    stack.jer = None
    stack.jersf = None

    factory = CorrectedJetsFactory(name_map, stack)
    corrected_jets = factory.build(jets, lazy_cache={"ignored": True})

    assert corrected_jets[name_map["JetPt"]].to_list() == [[25.0]]
    assert len(stack.jec.calls) == 1


def test_corrected_jets_factory_handles_jer_without_cache():
    name_map = _example_name_map()
    jets = ak.Array(
        [
            [
                {
                    "pt": 30.0,
                    "mass": 3.0,
                    "pt_raw": 30.0,
                    "mass_raw": 3.0,
                    "eta": 0.4,
                    "phi": 0.0,
                    "pt_gen": 28.0,
                },
                {
                    "pt": 45.0,
                    "mass": 4.5,
                    "pt_raw": 45.0,
                    "mass_raw": 4.5,
                    "eta": -0.1,
                    "phi": 0.2,
                    "pt_gen": 44.0,
                },
            ]
        ]
    )

    class FakeResolution:
        signature = ("JetPt", "JetEta")

        def getResolution(self, JetPt, JetEta):
            assert isinstance(JetPt, ak.Array)
            return ak.ones_like(JetPt) * 0.1

    class FakeScaleFactor:
        signature = ("JetEta",)

        def getScaleFactor(self, JetEta):
            values = np.array([1.0, 1.05, 0.95], dtype=np.float32)
            tiled = np.tile(values, (ak.to_numpy(JetEta).size, 1))
            return ak.Array(tiled)

    stack = JECStack()
    stack.jec = None
    stack.junc = None
    stack.jer = FakeResolution()
    stack.jersf = FakeScaleFactor()

    factory = CorrectedJetsFactory(name_map, stack)
    corrected_jets = factory.build(jets)

    assert "JER" in ak.fields(corrected_jets)
    assert ak.to_list(ak.flatten(corrected_jets[name_map["JetPt"]]))[0] > 0.0
    jer_up = ak.to_list(ak.ravel(corrected_jets["JER"].up[name_map["JetPt"]]))
    assert jer_up[0] > 0.0
