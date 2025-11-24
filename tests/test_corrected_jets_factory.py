import awkward as ak
import numpy as np
import pytest

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


def test_corrected_jets_factory_defaults_to_cache(monkeypatch):
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
                }
            ]
        ]
    )

    caches = []
    original_virtual = ak.virtual

    def tracking_virtual(*args, **kwargs):
        caches.append(kwargs.get("cache"))
        return original_virtual(*args, **kwargs)

    monkeypatch.setattr(ak, "virtual", tracking_virtual)

    factory = CorrectedJetsFactory(name_map, JECStack())
    factory.build(jets, lazy_cache=None)

    assert len(caches) > 0
    assert all(cache is not None for cache in caches)
    assert len(set(map(id, caches))) == 1


def test_corrected_jets_factory_rewraps_without_awkward_util(monkeypatch):
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
                    "pt": 50.0,
                    "mass": 5.0,
                    "pt_raw": 50.0,
                    "mass_raw": 5.0,
                    "eta": 0.2,
                    "phi": 0.1,
                    "pt_gen": 50.0,
                },
                {
                    "pt": 30.0,
                    "mass": 3.0,
                    "pt_raw": 30.0,
                    "mass_raw": 3.0,
                    "eta": -0.2,
                    "phi": -0.1,
                    "pt_gen": 30.0,
                },
            ],
            [
                {
                    "pt": 40.0,
                    "mass": 4.0,
                    "pt_raw": 40.0,
                    "mass_raw": 4.0,
                    "eta": 0.5,
                    "phi": 0.3,
                    "pt_gen": 40.0,
                }
            ],
        ]
    )

    for attr in ("behaviorof", "recursively_apply", "wrap"):
        monkeypatch.delattr(ak._util, attr, raising=False)

    factory = CorrectedJetsFactory(name_map, JECStack())
    corrected_jets = factory.build(jets, lazy_cache=None)

    assert len(corrected_jets) == len(jets)
    assert ak.to_list(ak.num(corrected_jets, axis=1)) == ak.to_list(
        ak.num(jets, axis=1)
    )
