import awkward as ak
import numpy

from topcoffea.modules.CorrectedJetsFactory import CorrectedJetsFactory
from topcoffea.modules.JECStack import JECStack


class DummyJECStack(JECStack):
    def __post_init__(self):
        # Override to avoid loading external correction data
        pass


class FakeResolution:
    signature = ["JetEta", "ptGenJet"]

    def getResolution(self, **kwargs):
        arr = next(iter(kwargs.values()))
        return numpy.full(len(arr), 0.1, dtype=numpy.float32)


class FakeScaleFactor:
    signature = ["JetEta"]

    def __init__(self, counts):
        self.counts = counts

    def getScaleFactor(self, **kwargs):
        arr = next(iter(kwargs.values()))
        base = ak.unflatten(arr, self.counts, axis=0)
        central = ak.ones_like(base)
        up = central * 1.05
        down = central * 0.95
        return ak.concatenate([central[..., None], up[..., None], down[..., None]], axis=-1)


class FakeMultipleUncertainties:
    signature = ["JetPt"]

    def __init__(self, counts, names):
        self.counts = counts
        self.names = names

    def getUncertainty(self, **kwargs):
        jetpt = next(iter(kwargs.values()))
        base = ak.unflatten(jetpt, self.counts, axis=0)
        variations = []
        for idx, name in enumerate(self.names, start=1):
            spread = 0.01 * idx * ak.ones_like(base)
            up = 1 + spread
            down = 1 - spread
            variations.append((name, ak.concatenate([up[..., None], down[..., None]], axis=-1)))
        return variations


def test_corrected_jets_multiple_jes_sources_keep_axes():
    jets = ak.Array(
        [
            [
                {"pt": 50.0, "mass": 5.0, "eta": 1.1, "ptGenJet": 48.0, "pt_raw": 45.0, "mass_raw": 4.5},
                {"pt": 40.0, "mass": 4.0, "eta": -0.5, "ptGenJet": 39.0, "pt_raw": 38.0, "mass_raw": 3.8},
            ],
            [
                {"pt": 30.0, "mass": 3.0, "eta": 0.3, "ptGenJet": 28.0, "pt_raw": 28.0, "mass_raw": 2.8},
            ],
        ]
    )

    counts = ak.num(jets, axis=1)

    name_map = {
        "ptRaw": "pt_raw",
        "massRaw": "mass_raw",
        "JetPt": "pt",
        "JetMass": "mass",
        "JetEta": "eta",
        "ptGenJet": "ptGenJet",
    }

    jes_sources = ["Total", "Absolute", "Relative"]

    jec_stack = DummyJECStack()
    jec_stack.use_clib = False
    jec_stack.jec = None
    jec_stack.jer = FakeResolution()
    jec_stack.jersf = FakeScaleFactor(counts)
    jec_stack.junc = FakeMultipleUncertainties(counts, jes_sources)
    jec_stack.savecorr = False

    factory = CorrectedJetsFactory(name_map, jec_stack)
    cjets = factory.build(jets)

    central_counts = ak.num(cjets.pt, axis=1)

    # Central jets keep the same event->jet jagged structure
    assert ak.all(central_counts == counts)

    # JER variations keep the same jagged axes
    assert ak.all(ak.num(cjets.JER.up.pt, axis=1) == central_counts)
    assert ak.all(ak.num(cjets.JER.down.pt, axis=1) == central_counts)

    # JES variations (individual fields) follow the same jagged axes
    for source in jes_sources:
        assert ak.all(ak.num(cjets[f"JES_{source}"].up.pt, axis=1) == central_counts)
        assert ak.all(ak.num(cjets[f"JES_{source}"].down.pt, axis=1) == central_counts)
