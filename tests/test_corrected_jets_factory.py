import awkward as ak
import numpy as np

from topcoffea.modules.CorrectedJetsFactory import CorrectedJetsFactory
from topcoffea.modules.JECStack import JECStack


def test_corrected_jets_factory_build_without_cache():
    name_map = {
        "JetPt": "pt",
        "JetMass": "mass",
        "ptRaw": "pt_raw",
        "massRaw": "mass_raw",
        "JetEta": "eta",
        "JetPhi": "phi",
        "ptGenJet": "pt_gen",
    }

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
    corrected_jets = factory.build(jets, lazy_cache=None)

    np.testing.assert_allclose(
        ak.to_numpy(ak.flatten(corrected_jets[name_map["JetPt"]], axis=None)),
        ak.to_numpy(ak.flatten(jets[name_map["JetPt"]], axis=None)),
    )
    np.testing.assert_allclose(
        ak.to_numpy(ak.flatten(corrected_jets[name_map["JetMass"]], axis=None)),
        ak.to_numpy(ak.flatten(jets[name_map["JetMass"]], axis=None)),
    )
