import pickle

import cloudpickle
import hist
import numpy as np
import pytest

from topcoffea.modules.histEFT import HistEFT
from topcoffea.modules.sparseHist import SparseHist


def make_sparse(*, track_raw_counts=True):
    return SparseHist(
        hist.axis.StrCategory([], name="process", growth=True),
        hist.axis.StrCategory([], name="systematic", growth=True),
        hist.axis.Regular(2, 0.0, 2.0, name="x"),
        storage="Double",
        track_raw_counts=track_raw_counts,
    )


def only_raw(histogram, *, flow=True):
    values = histogram.raw_counts(flow=flow)
    assert len(values) == 1
    return next(iter(values.values()))


def fill_recorded(histogram, process, values, weights):
    histogram.fill(
        process=process,
        systematic="nominal",
        x=np.asarray(values),
        weight=np.asarray(weights),
        record_raw_count=True,
    )


def test_disabled_tracking_preserves_fill_and_serializes_no_raw_payload():
    histogram = make_sparse(track_raw_counts=False)
    histogram.fill(
        process="mc",
        systematic="nominal",
        x=np.asarray([0.25, 1.25]),
        weight=np.asarray([2.0, -3.0]),
    )

    assert histogram.track_raw_counts is False
    assert not hasattr(histogram, "_raw_counts")
    assert len(histogram.__reduce__()[1]) == 4
    with pytest.raises(RuntimeError, match="disabled"):
        histogram.raw_counts()

    restored = pickle.loads(pickle.dumps(histogram))
    assert restored.track_raw_counts is False
    assert not hasattr(restored, "_raw_counts")
    np.testing.assert_array_equal(
        next(iter(restored.view(flow=True, as_dict=True).values())),
        np.asarray([0.0, 2.0, -3.0, 0.0]),
    )


def test_enabled_fill_is_explicit_and_counts_weight_independent_events_with_flow():
    histogram = make_sparse()
    with pytest.raises(RuntimeError, match="explicitly true or false"):
        histogram.fill(
            process="mc",
            systematic="nominal",
            x=np.asarray([0.25]),
        )
    assert histogram._dense_hists == {}
    assert histogram._raw_counts == {}

    fill_recorded(
        histogram,
        "mc",
        [-1.0, 0.25, 0.25, 1.25, 3.0],
        [-100.0, 0.5, -2.0, 7.0, 0.0],
    )
    histogram.fill(
        process="mc",
        systematic="up",
        x=np.asarray([0.25]),
        weight=np.asarray([11.0]),
        record_raw_count=False,
    )

    raw = histogram.raw_counts(flow=True)
    assert set(raw) == {("mc", "nominal")}
    assert raw[("mc", "nominal")].dtype == np.dtype(np.uint64)
    np.testing.assert_array_equal(raw[("mc", "nominal")], [1, 2, 1, 1])
    np.testing.assert_array_equal(
        histogram.raw_counts(flow=False)[("mc", "nominal")],
        [2, 1],
    )
    assert histogram._raw_counts[histogram.categories_to_index(("mc", "up"))] is None


def test_zero_missing_and_malformed_raw_state_are_distinct():
    histogram = make_sparse()
    fill_recorded(histogram, "zero", [], [])
    np.testing.assert_array_equal(only_raw(histogram), np.zeros(4, dtype=np.uint64))

    key = next(iter(histogram._dense_hists))
    state = histogram._raw_counts.pop(key)
    with pytest.raises(RuntimeError, match="coverage is incomplete"):
        histogram.raw_counts()

    histogram._raw_counts[key] = state.astype(np.int64)
    with pytest.raises(TypeError, match="dtype uint64"):
        histogram.raw_counts()

    histogram._raw_counts[key] = np.zeros(3, dtype=np.uint64)
    with pytest.raises(ValueError, match="expected"):
        histogram.raw_counts()


@pytest.mark.parametrize("serializer", [pickle, cloudpickle])
def test_serialization_preserves_exact_integer_state(serializer):
    histogram = make_sparse()
    fill_recorded(histogram, "mc", [-1.0, 0.25, 1.25, 3.0], [2.0, -4.0, 0.5, 9.0])

    restored = serializer.loads(serializer.dumps(histogram))

    np.testing.assert_array_equal(only_raw(restored), [1, 1, 1, 1])
    assert only_raw(restored).dtype == np.dtype(np.uint64)
    np.testing.assert_array_equal(
        next(iter(restored.view(flow=True, as_dict=True).values())),
        next(iter(histogram.view(flow=True, as_dict=True).values())),
    )


def test_merge_group_slice_remove_prune_scale_and_reset_transport_raw_state():
    left = make_sparse()
    fill_recorded(left, "a", [-1.0, 0.25], [2.0, -3.0])
    fill_recorded(left, "b", [1.25, 3.0], [5.0, 7.0])
    right = make_sparse()
    fill_recorded(right, "a", [0.25, 1.25], [-11.0, 13.0])

    merged = left + right
    np.testing.assert_array_equal(
        merged.raw_counts(flow=True)[("a", "nominal")],
        [1, 2, 1, 0],
    )

    grouped = merged.group("process", {"all_mc": ["a", "b"]})
    np.testing.assert_array_equal(only_raw(grouped), [1, 2, 2, 1])

    selected = merged[{"process": "a"}]
    integrated = merged.integrate("process", "a")
    removed = merged.remove("process", ["b"])
    pruned = merged.prune("process", ["a"])
    for transformed in (selected, integrated, removed, pruned):
        np.testing.assert_array_equal(only_raw(transformed), [1, 2, 1, 0])

    raw_before_scale = {
        key: np.array(value, copy=True)
        for key, value in merged.raw_counts(flow=True).items()
    }
    weighted_before = {
        key: np.array(value, copy=True)
        for key, value in merged.view(flow=True, as_dict=True).items()
    }
    merged.scale(-2.0)
    for key, value in merged.raw_counts(flow=True).items():
        np.testing.assert_array_equal(value, raw_before_scale[key])
    for key, value in merged.view(flow=True, as_dict=True).items():
        np.testing.assert_array_equal(value, -2.0 * weighted_before[key])

    merged.reset()
    for value in merged.raw_counts(flow=True).values():
        np.testing.assert_array_equal(value, np.zeros(4, dtype=np.uint64))


def test_incompatible_arithmetic_dense_slice_and_mixed_tracking_fail_closed():
    tracked = make_sparse()
    fill_recorded(tracked, "mc", [0.25], [2.0])
    untracked = make_sparse(track_raw_counts=False)
    untracked.fill(process="mc", systematic="nominal", x=[0.25], weight=[2.0])

    with pytest.raises(RuntimeError, match="tracked and untracked"):
        tracked += untracked
    with pytest.raises(RuntimeError, match="Only additive"):
        tracked -= tracked
    with pytest.raises(RuntimeError, match="arithmetic operation"):
        tracked += 1
    with pytest.raises(RuntimeError, match="Dense-axis selection"):
        tracked[{"x": slice(0, 1)}]


def test_checked_merge_raises_before_uint64_wraparound():
    left = make_sparse()
    right = make_sparse()
    fill_recorded(left, "mc", [0.25], [2.0])
    fill_recorded(right, "mc", [0.25], [3.0])
    key = next(iter(left._raw_counts))
    left._raw_counts[key][1] = np.iinfo(np.uint64).max
    weighted_before = np.array(left._dense_hists[key].view(flow=True), copy=True)

    with pytest.raises(OverflowError, match="overflow"):
        left += right

    assert left._raw_counts[key][1] == np.iinfo(np.uint64).max
    np.testing.assert_array_equal(left._dense_hists[key].view(flow=True), weighted_before)


def test_histeft_counts_source_events_once_before_coefficient_expansion():
    histogram = HistEFT(
        hist.axis.StrCategory([], name="process", growth=True),
        hist.axis.StrCategory([], name="systematic", growth=True),
        hist.axis.Regular(2, 0.0, 2.0, name="x"),
        wc_names=["ctG", "cpt"],
        track_raw_counts=True,
    )
    coefficients = np.asarray(
        [
            [1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
            [7.0, 8.0, 9.0, 10.0, 11.0, 12.0],
        ]
    )
    histogram.fill(
        process="eft",
        systematic="nominal",
        x=np.asarray([0.25, 1.25]),
        weight=np.asarray([-2.0, 0.5]),
        eft_coeff=coefficients,
        record_raw_count=True,
    )

    np.testing.assert_array_equal(only_raw(histogram), [0, 1, 1, 0])
    assert only_raw(histogram).dtype == np.dtype(np.uint64)
    np.testing.assert_allclose(
        histogram.eval({})[("eft", "nominal")],
        [0.0, -2.0, 3.5, 0.0],
    )
