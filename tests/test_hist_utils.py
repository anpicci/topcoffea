from pathlib import Path
import pickle

import pytest

hist = pytest.importorskip("hist")

from topcoffea.modules.hist_utils import (
    LazyHist,
    dict_comp,
    dump_to_pkl,
    get_diff_between_dicts,
    get_diff_between_nested_dicts,
    get_hist_dict_non_empty,
    get_hist_from_pkl,
    print_yld_dicts,
)


class DummyHist:
    def __init__(self, is_empty):
        self._is_empty = is_empty

    def empty(self):
        return self._is_empty


def test_get_hist_dict_non_empty_filters():
    mapping = {"full": DummyHist(False), "empty": DummyHist(True)}
    result = get_hist_dict_non_empty(mapping)
    assert "full" in result and "empty" not in result


def test_dump_and_load_roundtrip(tmp_path):
    payload = {"value": 42}
    output = tmp_path / "hist"
    dump_to_pkl(str(output), payload)
    stored = Path(str(output) + ".pkl.gz")
    assert stored.exists()
    loaded = get_hist_from_pkl(str(stored))
    assert loaded == payload


def _make_hist(fill: bool) -> hist.Hist:
    h = hist.Hist(hist.axis.StrCategory([], name="cat", growth=True))
    if fill:
        h.fill(cat="nominal")
    return h


def test_get_hist_from_pkl_tuple_keys_lazy(tmp_path):
    full = _make_hist(True)
    empty = _make_hist(False)
    payload = {
        ("sample", "nominal"): {"hist": pickle.dumps(full), "empty": full.empty()},
        ("sample", "empty"): {"hist": pickle.dumps(empty), "empty": empty.empty()},
    }
    output = tmp_path / "tuple_hist"
    dump_to_pkl(str(output), payload)
    stored = Path(str(output) + ".pkl.gz")

    lazy_loaded = get_hist_from_pkl(str(stored), materialize=False)
    assert isinstance(lazy_loaded[("sample", "nominal")], LazyHist)
    assert lazy_loaded[("sample", "nominal")].sum() == full.sum()
    assert lazy_loaded[("sample", "empty")].empty()

    filtered_lazy = get_hist_from_pkl(str(stored), allow_empty=False, materialize=False)
    assert ("sample", "empty") not in filtered_lazy

    eager = get_hist_from_pkl(str(stored), allow_empty=False, materialize=True)
    assert isinstance(eager[("sample", "nominal")], hist.Hist)


def test_get_hist_from_pkl_tuple_keys_hist_payload(tmp_path):
    full = _make_hist(True)
    payload = {("sample", "nominal"): full}
    output = tmp_path / "tuple_hist_direct"
    dump_to_pkl(str(output), payload)
    stored = Path(str(output) + ".pkl.gz")

    loaded = get_hist_from_pkl(str(stored))
    assert isinstance(loaded[("sample", "nominal")], hist.Hist)
    assert loaded[("sample", "nominal")].sum() == full.sum()


def test_get_hist_from_pkl_legacy_warning(tmp_path):
    legacy = _make_hist(True)
    output = tmp_path / "legacy_hist"
    dump_to_pkl(str(output), {"legacy": legacy})
    stored = Path(str(output) + ".pkl.gz")

    with pytest.warns(UserWarning, match="legacy categorical-axis"):
        loaded = get_hist_from_pkl(str(stored))
    assert isinstance(loaded["legacy"], hist.Hist)


def test_get_diff_between_dicts_percent():
    hist_a = {"bin": (2.0, None)}
    hist_b = {"bin": (1.0, None)}
    result = get_diff_between_dicts(hist_a, hist_b, "percent_diff", inpercent=True)
    assert pytest.approx(result["bin"][0]) == 100.0


def test_get_diff_between_nested_dicts_warns(capsys):
    nested_a = {"proc": {"bin": (2.0, 1.0)}}
    nested_b = {"proc": {"bin": (1.0, 0.5)}}
    result = get_diff_between_nested_dicts(nested_a, nested_b, "absolute_diff")
    assert "proc" in result and "bin" in result["proc"]
    captured = capsys.readouterr()
    # Should not warn for matching keys
    assert "WARNING" not in captured.out


def test_dict_comp_strict_failure():
    with pytest.raises(Exception):
        dict_comp({"a": [1]}, {"b": [2]}, strict=True)


def test_print_yld_dicts_tolerance(capsys):
    ylds = {"proc": {"cat": (2.0, 0.5)}}
    ok = print_yld_dicts(ylds, "tag", tolerance=3.0)
    assert ok
    fail = print_yld_dicts(ylds, "tag", tolerance=1.0)
    assert not fail
    captured = capsys.readouterr()
    assert "NOTE: This is larger than tolerance" in captured.out
