from pathlib import Path

import pytest

from topcoffea.modules.hist_utils import (
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
