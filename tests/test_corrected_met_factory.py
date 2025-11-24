import awkward as ak

from topcoffea.modules.CorrectedMETFactory import CorrectedMETFactory


def test_corrected_met_factory_ignores_missing_mapping_proxy(monkeypatch):
    name_map = {
        "METpt": "pt",
        "METphi": "phi",
        "JetPt": "jet_pt",
        "JetPhi": "jet_phi",
        "ptRaw": "jet_pt_raw",
        "UnClusteredEnergyDeltaX": "unc_deltax",
        "UnClusteredEnergyDeltaY": "unc_deltay",
    }

    monkeypatch.delattr(ak._util, "MappingProxy", raising=False)

    met = ak.Array(
        [
            {
                "pt": 100.0,
                "phi": 0.1,
                "unc_deltax": 1.5,
                "unc_deltay": -0.5,
            }
        ]
    )

    corrected_jets = ak.Array(
        [
            {
                "jet_pt": ak.Array([50.0, 30.0]),
                "jet_phi": ak.Array([0.2, -0.1]),
                "jet_pt_raw": ak.Array([49.0, 29.0]),
            }
        ]
    )

    factory = CorrectedMETFactory(name_map)
    corrected_met = factory.build(met, corrected_jets, lazy_cache={})

    assert len(corrected_met) == len(met)
    assert corrected_met[name_map["METpt"] + "_orig"][0] == met[name_map["METpt"]][0]
