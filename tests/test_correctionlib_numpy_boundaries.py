import awkward as ak
import correctionlib
import numpy as np

from topcoffea.modules import corrections
from topcoffea.modules.paths import topcoffea_path


class _RecordingCorrection:
    def __init__(self, correction):
        self.correction = correction
        self.calls = []

    def evaluate(self, *args):
        assert not any(isinstance(arg, ak.highlevel.Array) for arg in args)
        self.calls.append(args)
        return self.correction.evaluate(*args)


class _RecordingCorrectionSet:
    def __init__(self, correction_set, calls):
        self.correction_set = correction_set
        self.calls = calls

    def __getitem__(self, key):
        correction = _RecordingCorrection(self.correction_set[key])
        self.calls.append((key, correction))
        return correction


def _recording_factory(monkeypatch):
    original_correction_set = correctionlib.CorrectionSet
    recorded = []

    class RecordingFactory:
        @staticmethod
        def from_file(path):
            return _RecordingCorrectionSet(
                original_correction_set.from_file(path), recorded
            )

    monkeypatch.setattr(corrections.correctionlib, "CorrectionSet", RecordingFactory)
    return recorded


def test_btag_sf_evaluation_receives_numpy_and_preserves_jet_counts(monkeypatch):
    recorded = _recording_factory(monkeypatch)
    jets = ak.Array(
        [
            [{"pt": 50.0, "eta": 0.4, "hadronFlavour": 5}],
            [],
            [
                {"pt": 80.0, "eta": -1.2, "hadronFlavour": 4},
                {"pt": 1200.0, "eta": 2.0, "hadronFlavour": 5},
            ],
        ]
    )

    result = corrections.btag_sf_eval(
        jets, "M", "2018", "deepJet_comb", "central"
    )

    call = next(correction.calls[0] for _, correction in recorded if correction.calls)
    assert call[:2] == ("central", "M")
    for numeric in call[2:]:
        assert isinstance(numeric, np.ndarray)
    np.testing.assert_array_equal(call[2], np.array([5, 4, 5]))
    np.testing.assert_array_equal(call[4], np.array([50.0, 80.0, 1000.0]))
    assert ak.to_list(ak.num(result)) == [1, 0, 2]


def test_pileup_evaluation_receives_numpy_and_matches_direct_result(monkeypatch):
    recorded = _recording_factory(monkeypatch)
    ntrue = ak.Array([0.0, 10.5, 40.0])

    actual = corrections.GetPUSF(ntrue, "2018", "up")

    call = recorded[0][1].calls[0]
    assert isinstance(call[0], np.ndarray)
    assert call[1] == "up"
    np.testing.assert_array_equal(call[0], ak.to_numpy(ntrue))
    direct_set = correctionlib.CorrectionSet.from_file(
        topcoffea_path("data/POG/LUM/2018_UL/puWeights.json.gz")
    )
    expected = direct_set["Collisions18_UltraLegacy_goldenJSON"].evaluate(
        ak.to_numpy(ntrue), "up"
    )
    np.testing.assert_array_equal(actual, expected)
