import json
import awkward as ak
import numpy as np
from unittest.mock import patch

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


def _write_clib_jec_file(tmp_path, scale=1.1, tail_scale=1.02):
    payload = {
        "schema_version": 2,
        "description": "test clib corrections",
        "corrections": [
            {
                "name": "TEST_L1_AK4PFchs",
                "description": "first level",
                "version": 1,
                "inputs": [{"name": "JetPt", "type": "real"}],
                "output": {"name": "weight", "type": "real"},
                "data": {
                    "nodetype": "formula",
                    "expression": "x*0 + " + str(scale),
                    "parser": "TFormula",
                    "variables": ["JetPt"],
                },
            },
            {
                "name": "TEST_L2_AK4PFchs",
                "description": "second level",
                "version": 1,
                "inputs": [{"name": "JetPt", "type": "real"}],
                "output": {"name": "weight", "type": "real"},
                "data": {
                    "nodetype": "formula",
                    "expression": "x*0 + " + str(tail_scale),
                    "parser": "TFormula",
                    "variables": ["JetPt"],
                },
            },
        ],
    }

    path = tmp_path / "jec_corrections.json"
    path.write_text(json.dumps(payload))
    return path


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
    corrected_jets = factory.build(jets)

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
    assert ak.num(corrected_jets["jet_resolution_rand_gauss"], axis=1).to_list() == [2]


def test_corrected_jets_factory_produces_consistent_shapes_for_jer():
    name_map = _example_name_map()
    jets = ak.Array(
        [
            [
                {
                    "pt": 30.0,
                    "mass": 3.5,
                    "pt_raw": 29.5,
                    "mass_raw": 3.5,
                    "eta": 0.2,
                    "phi": 0.0,
                    "pt_gen": 28.0,
                },
                {
                    "pt": 50.0,
                    "mass": 5.0,
                    "pt_raw": 48.0,
                    "mass_raw": 5.0,
                    "eta": -0.6,
                    "phi": 0.3,
                    "pt_gen": 49.0,
                },
            ],
            [
                {
                    "pt": 40.0,
                    "mass": 4.0,
                    "pt_raw": 39.0,
                    "mass_raw": 4.0,
                    "eta": 0.9,
                    "phi": -0.2,
                    "pt_gen": 38.5,
                }
            ],
        ]
    )

    class FakeResolution:
        signature = ("JetPt", "JetEta")

        def getResolution(self, JetPt, JetEta):
            return ak.ones_like(JetPt, dtype=np.float32) * 0.1

    class FakeScaleFactor:
        signature = ("JetEta",)

        def getScaleFactor(self, JetEta):
            values = np.array([1.0, 1.05, 0.95], dtype=np.float32)
            tiled = np.tile(values, (ak.to_numpy(ak.flatten(JetEta, axis=None)).size, 1))
            return ak.Array(tiled)

    stack = JECStack()
    stack.jec = None
    stack.junc = None
    stack.jer = FakeResolution()
    stack.jersf = FakeScaleFactor()

    factory = CorrectedJetsFactory(name_map, stack)
    corrected_jets = factory.build(jets)

    assert ak.num(corrected_jets[name_map["JetPt"]], axis=1).to_list() == [2, 1]
    assert ak.num(corrected_jets["JER"].up[name_map["JetPt"]], axis=1).to_list() == [2, 1]


def test_corrected_jets_factory_jes_and_nominal_shapes():
    name_map = _example_name_map()
    jets = ak.Array(
        [
            [
                {"pt": 20.0, "mass": 2.0, "pt_raw": 19.0, "mass_raw": 2.0, "eta": 0.2, "phi": 0.1, "pt_gen": 19.5},
                {"pt": 45.0, "mass": 4.0, "pt_raw": 44.0, "mass_raw": 4.0, "eta": -0.3, "phi": -0.2, "pt_gen": 44.5},
            ]
        ]
    )

    class FakeJEC:
        signature = ("JetPt",)

        def getCorrection(self, JetPt):
            return ak.ones_like(JetPt, dtype=np.float32) * 1.01

    class FakeJunc:
        signature = ("JetPt",)

        def getUncertainty(self, JetPt):
            unc = ak.ones_like(ak.flatten(JetPt, axis=None), dtype=np.float32) * 0.02
            unc_np = ak.to_numpy(unc)
            factors = ak.Array(np.stack([1 + unc_np, 1 - unc_np], axis=1))
            return [("Total", factors)]

    stack = JECStack()
    stack.jec = FakeJEC()
    stack.junc = FakeJunc()
    stack.jer = None
    stack.jersf = None

    factory = CorrectedJetsFactory(name_map, stack)
    corrected_jets = factory.build(jets)

    np.testing.assert_allclose(
        ak.to_numpy(ak.flatten(corrected_jets[name_map["JetPt"]])),
        np.array([19.19, 44.44], dtype=np.float32),
        rtol=1e-5,
    )
    jes = corrected_jets["JES_Total"]
    assert ak.num(jes.up[name_map["JetPt"]], axis=1).to_list() == [2]
    assert ak.num(jes.down[name_map["JetPt"]], axis=1).to_list() == [2]


def test_corrected_jets_factory_clib_jes_handles_multijet_events():
    name_map = _example_name_map()
    jets = ak.Array(
        [
            [
                {"pt": 30.0, "mass": 3.0, "pt_raw": 29.0, "mass_raw": 3.0, "eta": 0.1, "phi": 0.0, "pt_gen": 28.0},
                {"pt": 40.0, "mass": 4.0, "pt_raw": 39.0, "mass_raw": 4.0, "eta": -0.2, "phi": 0.2, "pt_gen": 39.0},
            ],
            [
                {"pt": 25.0, "mass": 2.5, "pt_raw": 24.5, "mass_raw": 2.5, "eta": 0.4, "phi": -0.1, "pt_gen": 24.0},
                {"pt": 35.0, "mass": 3.5, "pt_raw": 34.0, "mass_raw": 3.5, "eta": 0.6, "phi": 0.3, "pt_gen": 33.5},
                {"pt": 50.0, "mass": 5.0, "pt_raw": 49.0, "mass_raw": 5.0, "eta": -0.5, "phi": -0.2, "pt_gen": 49.5},
            ],
        ]
    )

    counts = ak.num(jets, axis=1)

    class FakeInput:
        def __init__(self, name):
            self.name = name

    class FakeClibCorrection:
        def __init__(self, inputs, value):
            self.inputs = [FakeInput(name) for name in inputs]
            self.value = value

        def evaluate(self, *args):
            target = args[0] if len(args) > 0 else ak.Array([self.value] * ak.sum(counts))
            return np.ones_like(ak.to_numpy(target), dtype=np.float32) * self.value

    class FakeJuncCorrection:
        def __init__(self, value):
            self.inputs = [FakeInput("JetPt")]
            self.value = value

        def evaluate(self, JetPt):
            return np.full(ak.sum(counts), self.value, dtype=np.float32)

    stack = JECStack.__new__(JECStack)
    stack.use_clib = True
    stack.corrections = {
        "Fake_L1_AK4": FakeClibCorrection(["JetPt"], 1.0),
        "Fake_Total_AK4": FakeJuncCorrection(0.05),
    }
    stack.jec_names_clib = ["Fake_L1_AK4"]
    stack.jer_names_clib = []
    stack.jec_uncsources_clib = ["Fake_Total_AK4"]
    stack.savecorr = False

    factory = CorrectedJetsFactory(name_map, stack)
    corrected_jets = factory.build(jets)

    assert ak.num(corrected_jets[name_map["JetPt"]], axis=1).to_list() == counts.to_list()
    jes_total = corrected_jets["JES_Total"]
    assert ak.num(jes_total.up[name_map["JetPt"]], axis=1).to_list() == counts.to_list()
    assert ak.num(jes_total.down[name_map["JetPt"]], axis=1).to_list() == counts.to_list()


def test_corrected_jets_factory_clib_preserves_jagged_shape_with_cumulative_jecs():
    name_map = _example_name_map()
    jets = ak.Array(
        [
            [
                {"pt": 20.0, "mass": 2.0, "pt_raw": 19.0, "mass_raw": 2.0, "eta": 0.2, "phi": 0.1, "pt_gen": 19.5},
                {"pt": 30.0, "mass": 3.0, "pt_raw": 29.5, "mass_raw": 3.0, "eta": -0.3, "phi": -0.2, "pt_gen": 29.0},
            ],
            [
                {"pt": 45.0, "mass": 4.5, "pt_raw": 44.0, "mass_raw": 4.5, "eta": 0.5, "phi": 0.3, "pt_gen": 44.5},
            ],
        ]
    )

    counts = ak.num(jets, axis=1)

    class FakeInput:
        def __init__(self, name):
            self.name = name

    class FakeClibCorrection:
        def __init__(self, value):
            self.inputs = [FakeInput("JetPt")]
            self.value = value

        def evaluate(self, JetPt):
            target = np.asarray(JetPt, dtype=np.float32)
            return np.ones_like(target, dtype=np.float32) * self.value

    stack = JECStack.__new__(JECStack)
    stack.use_clib = True
    stack.corrections = {
        "Fake_L1": FakeClibCorrection(1.05),
        "Fake_L2": FakeClibCorrection(0.97),
    }
    stack.jec_names_clib = ["Fake_L1", "Fake_L2"]
    stack.jer_names_clib = []
    stack.jec_uncsources_clib = []
    stack.savecorr = False

    factory = CorrectedJetsFactory(name_map, stack)
    corrected_jets = factory.build(jets)

    assert ak.num(corrected_jets[name_map["JetPt"]], axis=1).to_list() == counts.to_list()
    corrected_flat = ak.to_numpy(ak.flatten(corrected_jets[name_map["JetPt"]]))
    raw_flat = ak.to_numpy(ak.flatten(jets[name_map["ptRaw"]]))
    np.testing.assert_allclose(corrected_flat, raw_flat * 1.05 * 0.97)
    assert not np.array_equal(corrected_flat, raw_flat)


def test_corrected_jets_factory_clib_multiplies_with_awkward_defaults():
    name_map = _example_name_map()
    jets = ak.Array(
        [
            [
                {"pt": 22.0, "mass": 2.2, "pt_raw": 21.5, "mass_raw": 2.1, "eta": 0.2, "phi": 0.1, "pt_gen": 21.0},
                {"pt": 28.0, "mass": 2.8, "pt_raw": 27.0, "mass_raw": 2.7, "eta": -0.4, "phi": -0.2, "pt_gen": 27.5},
            ],
            [
                {"pt": 35.0, "mass": 3.5, "pt_raw": 34.0, "mass_raw": 3.3, "eta": 0.5, "phi": 0.0, "pt_gen": 34.5},
            ],
        ]
    )

    counts = ak.num(jets, axis=1)

    class FakeInput:
        def __init__(self, name):
            self.name = name

    class FakeClibCorrection:
        def __init__(self, scale):
            self.inputs = [FakeInput("JetPt")]
            self.scale = scale
            self.calls = []

        def evaluate(self, JetPt):
            self.calls.append(isinstance(JetPt, ak.Array))
            values = ak.Array(JetPt)
            return ak.ones_like(values, dtype=np.float32) * self.scale

    stack = JECStack.__new__(JECStack)
    stack.use_clib = True
    stack.corrections = {
        "Fake_L1": FakeClibCorrection(1.1),
        "Fake_L2": FakeClibCorrection(0.9),
    }
    stack.jec_names_clib = ["Fake_L1", "Fake_L2"]
    stack.jer_names_clib = []
    stack.jec_uncsources_clib = []
    stack.savecorr = False

    factory = CorrectedJetsFactory(name_map, stack)
    corrected_jets = factory.build(jets)

    corrected_flat = ak.to_numpy(ak.flatten(corrected_jets[name_map["JetPt"]]))
    raw_flat = ak.to_numpy(ak.flatten(jets[name_map["ptRaw"]]))

    np.testing.assert_allclose(corrected_flat, raw_flat * 1.1 * 0.9)
    assert not np.array_equal(corrected_flat, raw_flat)
    assert all(stack.corrections[name].calls for name in stack.jec_names_clib)
    assert ak.num(corrected_jets[name_map["JetPt"]], axis=1).to_list() == counts.to_list()


def test_corrected_jets_factory_preserves_jagged_shapes_with_all_corrections():
    name_map = _example_name_map()
    jets = ak.Array(
        [
            [
                {"pt": 30.0, "mass": 3.0, "pt_raw": 29.0, "mass_raw": 3.0, "eta": 0.1, "phi": 0.0, "pt_gen": 28.0},
                {"pt": 40.0, "mass": 4.0, "pt_raw": 39.0, "mass_raw": 4.0, "eta": -0.2, "phi": 0.2, "pt_gen": 39.0},
            ],
            [
                {"pt": 25.0, "mass": 2.5, "pt_raw": 24.5, "mass_raw": 2.5, "eta": 0.4, "phi": -0.1, "pt_gen": 24.0},
                {"pt": 35.0, "mass": 3.5, "pt_raw": 34.0, "mass_raw": 3.5, "eta": 0.6, "phi": 0.3, "pt_gen": 33.5},
                {"pt": 50.0, "mass": 5.0, "pt_raw": 49.0, "mass_raw": 5.0, "eta": -0.5, "phi": -0.2, "pt_gen": 49.5},
            ],
        ]
    )

    counts = ak.num(jets, axis=1)

    class FakeJEC:
        signature = ("JetPt",)

        def getCorrection(self, JetPt):
            values = 1.0 + np.linspace(0.0, 0.02, ak.to_numpy(JetPt).size, dtype=np.float32)
            return ak.Array(values)

    class FakeResolution:
        signature = ("JetPt", "JetEta")

        def getResolution(self, JetPt, JetEta):
            return ak.ones_like(JetPt, dtype=np.float32) * 0.05

    class FakeScaleFactor:
        signature = ("JetEta",)

        def getScaleFactor(self, JetEta):
            tiled = np.tile(np.array([1.0, 1.05, 0.95], dtype=np.float32), (ak.to_numpy(ak.flatten(JetEta)).size, 1))
            return ak.Array(tiled)

    class FakeJunc:
        signature = ("JetPt",)

        def getUncertainty(self, JetPt):
            factors = np.stack([1.0 + 0.01 * np.arange(ak.to_numpy(JetPt).size), 1.0 - 0.01 * np.arange(ak.to_numpy(JetPt).size)], axis=1)
            return [("Total", ak.Array(factors, with_name=None))]

    stack = JECStack()
    stack.jec = FakeJEC()
    stack.junc = FakeJunc()
    stack.jer = FakeResolution()
    stack.jersf = FakeScaleFactor()

    factory = CorrectedJetsFactory(name_map, stack)
    corrected_jets = factory.build(jets)

    assert ak.num(corrected_jets[name_map["JetPt"]], axis=1).to_list() == counts.to_list()
    assert ak.num(corrected_jets["JER"].up[name_map["JetPt"]], axis=1).to_list() == counts.to_list()
    assert ak.num(corrected_jets["JES_Total"].up[name_map["JetPt"]], axis=1).to_list() == counts.to_list()


def test_corrected_jets_factory_clib_keeps_multijet_shapes_for_all_corrections():
    name_map = _example_name_map()
    jets = ak.Array(
        [
            [
                {"pt": 20.0, "mass": 2.0, "pt_raw": 19.0, "mass_raw": 2.0, "eta": 0.2, "phi": 0.1, "pt_gen": 19.5},
                {"pt": 30.0, "mass": 3.0, "pt_raw": 29.5, "mass_raw": 3.0, "eta": -0.3, "phi": -0.2, "pt_gen": 29.0},
            ],
            [
                {"pt": 45.0, "mass": 4.5, "pt_raw": 44.0, "mass_raw": 4.5, "eta": 0.5, "phi": 0.3, "pt_gen": 44.5},
            ],
        ]
    )

    counts = ak.num(jets, axis=1)

    class FakeInput:
        def __init__(self, name):
            self.name = name

    class FakeClibCorrection:
        def __init__(self, inputs, scale):
            self.inputs = [FakeInput(name) for name in inputs]
            self.scale = scale

        def evaluate(self, *args):
            jet_pt = np.asarray(args[0], dtype=np.float32)
            return (1.0 + self.scale * np.arange(jet_pt.size, dtype=np.float32))

    class FakeJERResolution:
        def __init__(self):
            self.inputs = [FakeInput("JetPt"), FakeInput("JetEta")]

        def evaluate(self, *args):
            jet_pt = np.asarray(args[0], dtype=np.float32)
            return np.full(jet_pt.size, 0.1, dtype=np.float32)

    class FakeJERScaleFactor:
        def __init__(self):
            self.inputs = [FakeInput("JetEta")]

        def evaluate(self, *args):
            size = np.asarray(args[0], dtype=np.float32).size
            base = np.stack([np.ones(size), np.ones(size) * 1.05, np.ones(size) * 0.95], axis=1)
            which = args[-1] if isinstance(args[-1], str) else "nom"
            idx = {"nom": 0, "up": 1, "down": 2}[which]
            return base[:, idx]

    class FakeJuncCorrection:
        def __init__(self):
            self.inputs = [FakeInput("JetPt")]

        def evaluate(self, JetPt):
            jet_pt = np.asarray(JetPt, dtype=np.float32)
            return 0.02 * (1 + np.arange(jet_pt.size, dtype=np.float32))

    stack = JECStack.__new__(JECStack)
    stack.use_clib = True
    stack.corrections = {
        "Fake_L1": FakeClibCorrection(["JetPt"], 0.01),
        "Fake_JER": FakeJERResolution(),
        "Fake_JERScaleFactor": FakeJERScaleFactor(),
        "Fake_Total": FakeJuncCorrection(),
    }
    stack.jec_names_clib = ["Fake_L1"]
    stack.jer_names_clib = ["Fake_JER", "Fake_JERScaleFactor"]
    stack.jec_uncsources_clib = ["Fake_Total"]
    stack.savecorr = False

    factory = CorrectedJetsFactory(name_map, stack)
    corrected_jets = factory.build(jets)

    assert ak.num(corrected_jets[name_map["JetPt"]], axis=1).to_list() == counts.to_list()
    assert ak.num(corrected_jets["JER"].up[name_map["JetPt"]], axis=1).to_list() == counts.to_list()
    assert ak.num(corrected_jets["JES_Total"].up[name_map["JetPt"]], axis=1).to_list() == counts.to_list()

def test_corrected_jets_factory_avoids_ak_stack():
    name_map = _example_name_map()
    jets = ak.Array(
        [
            [
                {
                    "pt": 30.0,
                    "mass": 3.0,
                    "pt_raw": 30.0,
                    "mass_raw": 3.0,
                    "eta": 0.1,
                    "phi": 0.0,
                    "pt_gen": 29.0,
                }
            ]
        ]
    )

    class FakeResolution:
        signature = ("JetPt", "JetEta")

        def getResolution(self, JetPt, JetEta):
            return ak.ones_like(JetPt, dtype=np.float32) * 0.05

    class FakeScaleFactor:
        signature = ("JetEta",)

        def getScaleFactor(self, JetEta):
            factors = np.array([1.0, 1.1, 0.9], dtype=np.float32)
            tiled = np.tile(factors, (ak.to_numpy(ak.flatten(JetEta, axis=None)).size, 1))
            return ak.Array(tiled)

    class FakeJunc:
        signature = ("JetPt",)

        def getUncertainty(self, JetPt):
            values = ak.ones_like(ak.flatten(JetPt, axis=None), dtype=np.float32) * 0.02
            up = ak.Array(np.stack([ak.to_numpy(values) * 0 + 1 + 0.02, ak.to_numpy(values) * 0 + 1 - 0.02], axis=1))
            return [("Total", up)]

    stack = JECStack()
    stack.jec = None
    stack.junc = FakeJunc()
    stack.jer = FakeResolution()
    stack.jersf = FakeScaleFactor()

    def _fail_stack(*args, **kwargs):
        raise AssertionError("ak.stack should not be called in CorrectedJetsFactory")

    with patch.object(ak, "stack", side_effect=_fail_stack, create=True):
        factory = CorrectedJetsFactory(name_map, stack)
        corrected_jets = factory.build(jets)

    assert ak.num(corrected_jets[name_map["JetPt"]], axis=1).to_list() == [1]
    assert "JES_Total" in ak.fields(corrected_jets)


def test_corrected_jets_factory_handles_clib_jagged_layout(tmp_path):
    name_map = _example_name_map()
    jets = ak.Array(
        [
            [
                {
                    "pt": 50.0,
                    "mass": 5.0,
                    "pt_raw": 48.0,
                    "mass_raw": 4.8,
                    "eta": 0.1,
                    "phi": 1.2,
                    "pt_gen": 49.0,
                },
                {
                    "pt": 30.0,
                    "mass": 3.0,
                    "pt_raw": 29.0,
                    "mass_raw": 2.9,
                    "eta": -0.5,
                    "phi": -1.0,
                    "pt_gen": 28.0,
                },
            ],
            [
                {
                    "pt": 40.0,
                    "mass": 4.0,
                    "pt_raw": 38.0,
                    "mass_raw": 3.8,
                    "eta": 0.7,
                    "phi": 0.5,
                    "pt_gen": 39.0,
                }
            ],
        ]
    )

    corr_file = _write_clib_jec_file(tmp_path)
    stack = JECStack(
        corrections={},
        use_clib=True,
        jec_tag="TEST",
        jec_levels=["L1", "L2"],
        jet_algo="AK4PFchs",
        json_path=str(corr_file),
    )

    factory = CorrectedJetsFactory(name_map, stack)
    corrected_jets = factory.build(jets)

    expected_factor = 1.1 * 1.02
    expected_pt = expected_factor * jets[name_map["ptRaw"]]

    np.testing.assert_allclose(
        ak.to_numpy(ak.flatten(corrected_jets[name_map["JetPt"]])),
        ak.to_numpy(ak.flatten(expected_pt)),
    )
    assert not np.allclose(
        ak.to_numpy(ak.flatten(corrected_jets[name_map["JetPt"]])),
        ak.to_numpy(ak.flatten(jets[name_map["ptRaw"]])),
    )

    counts = ak.num(corrected_jets[name_map["JetPt"]], axis=-1)
    numeric_counts = ak.to_numpy(counts)
    assert numeric_counts.ndim == 1
    assert numeric_counts.dtype.kind in "iu"
