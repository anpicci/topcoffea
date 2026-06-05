from types import SimpleNamespace

import awkward as ak
import pytest

import topcoffea.modules.muon_momentum_corrections as mmc


class _FakeBackend:
    def __init__(self):
        self.calls = []

    def pt_scale(self, is_data, pt, eta, phi, charge, cset, nested):
        self.calls.append(("scale", is_data, cset, nested))
        return pt + 1.0

    def pt_resol(
        self, pt, eta, phi, n_layers, events, lumis, cset, nested
    ):
        self.calls.append(("resolution", cset, nested))
        return pt + 2.0

    def pt_scale_var(
        self, pt, eta, phi, charge, direction, cset, nested
    ):
        self.calls.append(("scale_var", direction, cset, nested))
        return pt + (0.1 if direction == "up" else -0.1)

    def pt_resol_var(
        self, pt_without_resolution, pt_with_resolution, eta,
        direction, cset, nested
    ):
        self.calls.append(("resolution_var", direction, cset, nested))
        return pt_with_resolution + (0.2 if direction == "up" else -0.2)


def _muons():
    return ak.Array(
        [
            [
                {
                    "pt": 30.0,
                    "eta": 0.2,
                    "phi": 0.1,
                    "charge": 1,
                    "nTrackerLayers": 12,
                },
                {
                    "pt": 45.0,
                    "eta": -1.1,
                    "phi": -0.2,
                    "charge": -1,
                    "nTrackerLayers": 14,
                },
            ],
            [],
            [
                {
                    "pt": 60.0,
                    "eta": 1.7,
                    "phi": 2.1,
                    "charge": 1,
                    "nTrackerLayers": 10,
                }
            ],
        ]
    )


def _apply(variation="nominal", is_data=False, backend=None):
    return mmc.apply_muon_momentum_corrections(
        _muons(),
        "2022",
        is_data,
        variation,
        event_numbers=ak.Array([101, 102, 103]),
        luminosity_blocks=ak.Array([11, 12, 13]),
        correction_set=object(),
        backend=backend,
    )


@pytest.mark.parametrize(
    ("year", "campaign"),
    [
        ("2022", "2022_Summer22"),
        ("2022EE", "2022_Summer22EE"),
        ("2023", "2023_Summer23"),
        ("2023BPix", "2023_Summer23BPix"),
    ],
)
def test_campaign_routing(year, campaign):
    assert mmc.get_run3_muon_campaign(year) == campaign
    assert (
        mmc.get_scarekit_payload_path(year, "/payloads")
        == mmc.Path("/payloads") / f"{campaign}.json"
    )


def test_unsupported_campaign_fails_loudly():
    with pytest.raises(ValueError, match="Unsupported Run 3.*2024"):
        mmc.get_run3_muon_campaign("2024")


def test_data_nominal_dispatches_scale_only():
    backend = _FakeBackend()
    corrected = _apply(is_data=True, backend=backend)

    assert [call[0] for call in backend.calls] == ["scale"]
    assert ak.to_list(corrected) == [[31.0, 46.0], [], [61.0]]


def test_data_rejects_object_variations():
    with pytest.raises(ValueError, match="not applicable to data"):
        _apply("MuonResolutionUp", is_data=True, backend=_FakeBackend())


def test_mc_nominal_dispatches_scale_then_resolution():
    backend = _FakeBackend()
    corrected = _apply(backend=backend)

    assert [call[0] for call in backend.calls] == ["scale", "resolution"]
    assert ak.to_list(corrected) == [[33.0, 48.0], [], [63.0]]


@pytest.mark.parametrize(
    ("variation", "direction"),
    [
        ("MuonScaleUp", "up"),
        ("MuonScaleDown", "dn"),
    ],
)
def test_mc_scale_variation_dispatch(variation, direction):
    backend = _FakeBackend()
    _apply(variation, backend=backend)

    assert [call[0] for call in backend.calls] == [
        "scale",
        "resolution",
        "scale_var",
    ]
    assert backend.calls[-1][1] == direction


@pytest.mark.parametrize(
    ("variation", "direction"),
    [
        ("MuonResolutionUp", "up"),
        ("MuonResolutionDown", "dn"),
    ],
)
def test_mc_resolution_variation_dispatch(variation, direction):
    backend = _FakeBackend()
    _apply(variation, backend=backend)

    assert [call[0] for call in backend.calls] == [
        "scale",
        "resolution",
        "resolution_var",
    ]
    assert backend.calls[-1][1] == direction


def test_nested_shape_is_preserved():
    corrected = _apply("MuonResolutionUp", backend=_FakeBackend())

    assert ak.to_list(ak.num(corrected)) == [2, 0, 1]
    assert ak.to_list(corrected) == [[33.2, 48.2], [], [63.2]]


def test_missing_backend_fails_with_actionable_error(monkeypatch):
    def _missing_backend(name):
        raise ModuleNotFoundError(name)

    monkeypatch.setattr(mmc.importlib, "import_module", _missing_backend)

    with pytest.raises(RuntimeError, match="external ScaReKit"):
        _apply(is_data=True)


def test_missing_payload_source_fails_loudly():
    with pytest.raises(RuntimeError, match="correction_set or payload_directory"):
        mmc.apply_muon_momentum_corrections(
            _muons(),
            "2022",
            True,
            backend=SimpleNamespace(),
        )
