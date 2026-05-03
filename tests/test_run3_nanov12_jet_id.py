import numpy as np
import pytest
import awkward as ak

import topcoffea.modules.object_selection as osel


class _FakeCorrection:
    def __init__(self, calls):
        self.calls = calls

    def evaluate(self, *inputs):
        self.calls.append(inputs)
        return np.array([1, 0, 1], dtype=np.int32)


class _FakeCorrectionSet:
    def __init__(self, calls, selected_keys):
        self.calls = calls
        self.selected_keys = selected_keys

    def __getitem__(self, key):
        self.selected_keys.append(key)
        return _FakeCorrection(self.calls)


def _jets():
    return ak.Array(
        [
            [
                {
                    "eta": 0.2,
                    "chHEF": 0.6,
                    "neHEF": 0.2,
                    "chEmEF": 0.1,
                    "neEmEF": 0.05,
                    "muEF": 0.01,
                    "chMultiplicity": 12,
                    "neMultiplicity": 4,
                },
                {
                    "eta": -1.7,
                    "chHEF": 0.5,
                    "neHEF": 0.3,
                    "chEmEF": 0.1,
                    "neEmEF": 0.08,
                    "muEF": 0.01,
                    "chMultiplicity": 8,
                    "neMultiplicity": 3,
                },
            ],
            [
                {
                    "eta": 2.1,
                    "chHEF": 0.4,
                    "neHEF": 0.4,
                    "chEmEF": 0.08,
                    "neEmEF": 0.1,
                    "muEF": 0.0,
                    "chMultiplicity": 5,
                    "neMultiplicity": 6,
                }
            ],
            [],
        ]
    )


def _skimmed_jets():
    return ak.Array(
        [
            [
                {
                    "eta": 0.2,
                    "jetId": 2,
                    "neHEF": 0.95,
                    "neEmEF": 0.95,
                    "muEF": 0.79,
                    "chEmEF": 0.79,
                },
                {
                    "eta": 2.85,
                    "jetId": 2,
                    "neHEF": 0.98,
                    "neEmEF": 0.95,
                    "muEF": 0.0,
                    "chEmEF": 0.0,
                },
                {
                    "eta": 2.95,
                    "jetId": 2,
                    "neHEF": 0.995,
                    "neEmEF": 0.95,
                    "muEF": 0.0,
                    "chEmEF": 0.0,
                },
            ],
            [
                {
                    "eta": 3.2,
                    "jetId": 2,
                    "neHEF": 0.4,
                    "neEmEF": 0.39,
                    "muEF": 0.0,
                    "chEmEF": 0.0,
                },
                {
                    "eta": 3.3,
                    "jetId": 2,
                    "neHEF": 0.4,
                    "neEmEF": 0.41,
                    "muEF": 0.0,
                    "chEmEF": 0.0,
                },
                {
                    "eta": -0.4,
                    "jetId": 0,
                    "neHEF": 0.0,
                    "neEmEF": 0.0,
                    "muEF": 0.0,
                    "chEmEF": 0.0,
                },
            ],
            [],
        ]
    )


@pytest.mark.parametrize(
    ("year", "payload_dir"),
    [
        ("2022", "2022_Summer22"),
        ("2022EE", "2022_Summer22EE"),
        ("2023", "2023_Summer23"),
        ("2023BPix", "2023_Summer23BPix"),
    ],
)
def test_run3_nanov12_jet_id_payload_dispatch(monkeypatch, year, payload_dir):
    paths = []
    selected_keys = []
    calls = []

    monkeypatch.setattr(osel, "topcoffea_path", lambda path: paths.append(path) or path)
    monkeypatch.setattr(
        osel.correctionlib.CorrectionSet,
        "from_file",
        lambda path: _FakeCorrectionSet(calls, selected_keys),
    )

    osel.run3_nanoV12_ak4puppi_jet_id(_jets(), year)

    assert paths == [f"data/POG/JME/{payload_dir}/jetid.json.gz"]
    assert len(calls) == 1


@pytest.mark.parametrize(
    ("working_point", "correction_key"),
    [
        ("tight", "AK4PUPPI_Tight"),
        ("tight_lepton_veto", "AK4PUPPI_TightLeptonVeto"),
    ],
)
def test_run3_nanov12_jet_id_working_point_mapping(monkeypatch, working_point, correction_key):
    selected_keys = []
    calls = []

    monkeypatch.setattr(osel, "topcoffea_path", lambda path: path)
    monkeypatch.setattr(
        osel.correctionlib.CorrectionSet,
        "from_file",
        lambda path: _FakeCorrectionSet(calls, selected_keys),
    )

    osel.run3_nanoV12_ak4puppi_jet_id(_jets(), "2022", working_point=working_point)

    assert selected_keys == [correction_key]


def test_run3_nanov12_jet_id_requires_all_fields():
    jets = ak.Array([[{"eta": 0.1, "chHEF": 0.5}]])

    with pytest.raises(ValueError, match="Missing correctionlib fields: .*neHEF"):
        osel.run3_nanoV12_ak4puppi_jet_id(jets, "2022")


def test_run3_nanov12_jet_id_returns_boolean_mask_with_input_shape(monkeypatch):
    calls = []
    selected_keys = []

    monkeypatch.setattr(osel, "topcoffea_path", lambda path: path)
    monkeypatch.setattr(
        osel.correctionlib.CorrectionSet,
        "from_file",
        lambda path: _FakeCorrectionSet(calls, selected_keys),
    )

    mask = osel.run3_nanoV12_ak4puppi_jet_id(_jets(), "2022")

    assert ak.to_list(mask) == [[True, False], [True], []]
    assert ak.to_list(ak.num(mask)) == [2, 1, 0]
    assert ak.to_numpy(ak.flatten(mask)).dtype == np.dtype("bool")
    assert ak.to_list(ak.Array(calls[0][-1])) == [16, 11, 11]


def test_run3_nanov12_jet_id_missing_multiplicities_uses_tight_recipe(monkeypatch):
    def _raise_if_correctionlib_is_used(path):
        raise AssertionError("skimmed fallback must not use correctionlib placeholders")

    monkeypatch.setattr(
        osel.correctionlib.CorrectionSet,
        "from_file",
        _raise_if_correctionlib_is_used,
    )

    mask = osel.run3_nanoV12_ak4puppi_jet_id(_skimmed_jets(), "2022")

    assert ak.to_list(mask) == [[True, True, False], [True, False, False], []]
    assert ak.to_numpy(ak.flatten(mask)).dtype == np.dtype("bool")


def test_run3_nanov12_jet_id_missing_multiplicities_uses_tight_lepton_veto_recipe():
    jets = ak.Array(
        [
            [
                {
                    "eta": 0.2,
                    "jetId": 2,
                    "neHEF": 0.0,
                    "neEmEF": 0.0,
                    "muEF": 0.79,
                    "chEmEF": 0.79,
                },
                {
                    "eta": 0.3,
                    "jetId": 2,
                    "neHEF": 0.0,
                    "neEmEF": 0.0,
                    "muEF": 0.8,
                    "chEmEF": 0.79,
                },
                {
                    "eta": 0.4,
                    "jetId": 2,
                    "neHEF": 0.0,
                    "neEmEF": 0.0,
                    "muEF": 0.79,
                    "chEmEF": 0.8,
                },
                {
                    "eta": 2.8,
                    "jetId": 2,
                    "neHEF": 0.98,
                    "neEmEF": 0.0,
                    "muEF": 0.99,
                    "chEmEF": 0.99,
                },
            ]
        ]
    )

    mask = osel.run3_nanoV12_ak4puppi_jet_id(
        jets, "2022", working_point="tight_lepton_veto"
    )

    assert ak.to_list(mask) == [[True, False, False, True]]


def test_run3_nanov12_jet_id_missing_multiplicities_requires_recipe_fields():
    jets = ak.Array([[{"eta": 0.1, "neHEF": 0.1, "neEmEF": 0.1}]])

    with pytest.raises(ValueError, match="Missing recipe fields: jetId"):
        osel.run3_nanoV12_ak4puppi_jet_id(jets, "2022")


def test_existing_flag_based_tight_jet_selection_is_unchanged():
    mask = osel.is_tight_jet(
        np.array([29.0, 31.0, 31.0, 31.0]),
        np.array([0.1, 0.1, 2.5, 0.1]),
        np.array([2, 1, 2, 2]),
        pt_cut=30.0,
        eta_cut=2.4,
        id_cut=1,
    )

    assert mask.tolist() == [False, False, False, True]
