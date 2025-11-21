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
def test_streaming_iterator_stops_after_close(monkeypatch, tmp_path):
    path = tmp_path / "hist.pkl.gz"

    payload = {str(i): _DummyHist(str(i), False) for i in range(5)}
    with gzip.open(path, "wb") as fout:
        pickle.dump(payload, fout, protocol=pickle.HIGHEST_PROTOCOL)

    reads_after_stop = 0

    class RecordingStopAwareReader(hist_utils._StopAwareReader):
        def read(self, size):
            nonlocal reads_after_stop
            if self._stop_event.is_set():
                reads_after_stop += 1
            return super().read(size)

        def readinto(self, buffer):  # pragma: no cover - small wrapper
            nonlocal reads_after_stop
            if self._stop_event.is_set():
                reads_after_stop += 1
            return super().readinto(buffer)

    monkeypatch.setattr(hist_utils, "_StopAwareReader", RecordingStopAwareReader)

    iterator = hist_utils.iterate_hist_from_pkl(str(path), materialize=False)

    next(iterator)
    iterator.close()

    assert reads_after_stop == 0


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
