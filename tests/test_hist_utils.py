import gzip
import pickle

import pytest

from topcoffea.modules import hist_utils


class _DummyHist:
    def __init__(self, name: str, empty_flag: bool):
        self.name = name
        self._empty_flag = empty_flag

    def empty(self):
        return self._empty_flag

    def __eq__(self, other):
        return isinstance(other, _DummyHist) and (self.name, self._empty_flag) == (
            other.name,
            other._empty_flag,
        )


@pytest.mark.skipif(
    not hist_utils.HAS_STREAMING_SUPPORT,
    reason="Streaming helpers are unavailable on this Python version",
)
def test_streaming_unpickler_filters_empty(tmp_path):
    hist_map = {
        "keep": _DummyHist("keep", False),
        "drop": _DummyHist("drop", True),
    }

    path = tmp_path / "hist.pkl.gz"
    with gzip.open(path, "wb") as fout:
        pickle.dump(hist_map, fout, protocol=pickle.HIGHEST_PROTOCOL)

    loaded = hist_utils.iterate_hist_from_pkl(
        str(path), allow_empty=False, materialize=True
    )

    assert loaded == {"keep": hist_map["keep"]}
