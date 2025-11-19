import gc
import gzip
import io
import os
import pickle
import tempfile
import unittest

from topcoffea.modules import hist_utils as hist_utils_module
from topcoffea.modules.utils import (
    get_hist_from_pkl,
    iterate_histograms_from_pkl,
)
from pickle import UnpicklingError

HAS_STREAMING_SUPPORT = hist_utils_module.HAS_STREAMING_SUPPORT
LazyHist = hist_utils_module.LazyHist
iterate_hist_from_pkl = hist_utils_module.iterate_hist_from_pkl
iterate_histograms_from_pkl_module = (
    hist_utils_module.iterate_histograms_from_pkl
)
if HAS_STREAMING_SUPPORT:
    _StreamingHistUnpickler = hist_utils_module._StreamingHistUnpickler
else:  # pragma: no cover - exercised when streaming helpers unavailable
    _StreamingHistUnpickler = None


class TrackingHist:
    live_instances = 0
    max_live_instances = 0

    def __init__(self, payload):
        self.payload = payload
        self._bump()

    def __del__(self):
        type(self).live_instances -= 1

    def empty(self):
        return False

    @classmethod
    def _bump(cls):
        cls.live_instances += 1
        cls.max_live_instances = max(cls.max_live_instances, cls.live_instances)

    def __getstate__(self):
        return self.payload

    def __setstate__(self, state):
        self.payload = state
        self._bump()


class AlwaysEmptyHist:
    def empty(self):
        return True


if HAS_STREAMING_SUPPORT:

    class _StopAwareBytesIO(io.BytesIO):
        """Bytes buffer that records reads performed after stop is requested."""

        def __init__(self, payload: bytes):
            super().__init__(payload)
            self.stop_event = None
            self.read_after_stop = 0

        def read(self, size: int = -1) -> bytes:  # pragma: no cover - used indirectly
            if self.stop_event is not None and self.stop_event.is_set():
                self.read_after_stop += 1
            return super().read(size)


def _write_hist_file(mapping):
    fd, raw_path = tempfile.mkstemp(suffix=".pkl.gz")
    os.close(fd)
    path = raw_path
    with gzip.open(path, "wb") as fout:
        pickle.dump(mapping, fout)
    return path


@unittest.skipUnless(
    HAS_STREAMING_SUPPORT, "Streaming iterator requires Python 3.11+"
)
class HistUtilsStreamingTests(unittest.TestCase):
    def test_iterate_hist_from_pkl_streams(self):
        payloads = {f"hist_{i}": TrackingHist(i) for i in range(3)}
        path = _write_hist_file(payloads)
        payloads.clear()
        gc.collect()
        TrackingHist.live_instances = 0
        TrackingHist.max_live_instances = 0

        try:
            seen = []
            iterator = iterate_hist_from_pkl(path)
            for key, hist in iterator:
                seen.append((key, hist.payload))
                del hist
                gc.collect()

            self.assertEqual([value for _, value in seen], [0, 1, 2])
            gc.collect()
            self.assertEqual(TrackingHist.live_instances, 0)
        finally:
            os.remove(path)

    def test_iterate_hist_from_pkl_materialize_and_filter(self):
        mapping = {
            "filled": TrackingHist("filled"),
            "empty": AlwaysEmptyHist(),
        }
        path = _write_hist_file(mapping)
        mapping.clear()
        gc.collect()

        try:
            eager = iterate_hist_from_pkl(path, materialize=True)
            self.assertEqual(eager["filled"].payload, "filled")
            self.assertIn("empty", eager)

            filtered = iterate_hist_from_pkl(
                path, allow_empty=False, materialize=True
            )
            self.assertNotIn("empty", filtered)

            utils_filtered = get_hist_from_pkl(path, allow_empty=False)
            self.assertEqual(utils_filtered.keys(), filtered.keys())
        finally:
            os.remove(path)

    def test_streaming_iterator_stops_after_close(self):
        payload = pickle.dumps({f"hist_{i}": i for i in range(5)}, protocol=pickle.HIGHEST_PROTOCOL)
        backing = _StopAwareBytesIO(payload)
        streamer = _StreamingHistUnpickler(backing)
        backing.stop_event = streamer._stop_event

        iterator = streamer.iterate()
        next(iterator)
        iterator.close()

        self.assertEqual(
            backing.read_after_stop,
            0,
            "Streaming unpickler continued reading after cancellation",
        )


class HistUtilsLazyIteratorTests(unittest.TestCase):
    def test_lazy_iterator_limits_live_histograms(self):
        payloads = {f"hist_{i}": TrackingHist(i) for i in range(4)}
        path = _write_hist_file(payloads)
        payloads.clear()
        gc.collect()
        TrackingHist.live_instances = 0
        TrackingHist.max_live_instances = 0

        try:
            lazies = list(iterate_histograms_from_pkl(path))
            gc.collect()
            self.assertEqual(TrackingHist.live_instances, 0)

            seen = []
            for key, lazy in lazies:
                hist = lazy.materialize()
                seen.append((key, hist.payload))
                lazy.release()
                del hist
                gc.collect()

            self.assertEqual([value for _, value in seen], [0, 1, 2, 3])
            gc.collect()
            self.assertEqual(TrackingHist.live_instances, 0)
        finally:
            os.remove(path)

    def test_lazy_iterator_filters_empty(self):
        mapping = {
            "filled": TrackingHist("filled"),
            "empty": AlwaysEmptyHist(),
        }
        path = _write_hist_file(mapping)
        mapping.clear()
        gc.collect()

        try:
            eager = list(iterate_histograms_from_pkl(path, allow_empty=False))
            keys = [key for key, _ in eager]
            self.assertEqual(keys, ["filled"])
        finally:
            os.remove(path)

    def test_lazy_iterator_module_export(self):
        mapping = {f"hist_{i}": TrackingHist(i) for i in range(2)}
        path = _write_hist_file(mapping)
        mapping.clear()
        gc.collect()

        try:
            module_keys = [
                key for key, _ in iterate_histograms_from_pkl_module(path)
            ]
            utils_keys = [key for key, _ in iterate_histograms_from_pkl(path)]
            self.assertEqual(module_keys, utils_keys)
        finally:
            os.remove(path)


class HistUtilsValidationTests(unittest.TestCase):
    def test_iterate_hist_from_pkl_rejects_non_dict(self):
        fd, raw_path = tempfile.mkstemp(suffix=".pkl.gz")
        os.close(fd)
        path = raw_path
        try:
            with gzip.open(path, "wb") as fout:
                pickle.dump([1, 2, 3], fout)

            with self.assertRaises(UnpicklingError):
                iterate_hist_from_pkl(path, materialize=True)
        finally:
            os.remove(path)


if __name__ == "__main__":
    unittest.main()
