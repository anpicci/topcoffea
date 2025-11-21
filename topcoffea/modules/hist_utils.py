"""Utilities for working with histogram pickle files."""

from __future__ import annotations

import gzip
import importlib
import importlib.util
import pickle
import queue
import sys
import threading
import typing
from typing import Dict, Iterator, Tuple, Union

from pickle import UnpicklingError

try:  # pragma: no cover - exercised in environments without cloudpickle
    import cloudpickle
except ModuleNotFoundError:  # pragma: no cover - fallback for minimal envs
    cloudpickle = pickle  # type: ignore[assignment]

try:  # pragma: no cover - exercised via versioned environments
    from pickle import DICT, EMPTY_DICT, _Unpickler
except ImportError:  # Python < 3.11 lacks the streaming helpers
    _STREAMING_SUPPORT = False
else:
    _STREAMING_SUPPORT = True

_FALLBACK_HIST_UTILS = typing.cast(object, sys.modules[__name__])

_PATCH_TARGET = "ArrayLike | Mapping | None"
_PATCH_REPLACEMENT = "Union[ArrayLike, Mapping, None]"

HAS_STREAMING_SUPPORT = _STREAMING_SUPPORT

__all__ = [
    "HAS_STREAMING_SUPPORT",
    "LazyHist",
    "get_hist_dict_non_empty",
    "ensure_histEFT_py39_compat",
    "ensure_hist_utils",
    "iterate_hist_from_pkl",
    "iterate_histograms_from_pkl",
]


def _patched_histEFT_source(source: str) -> str | None:
    if _PATCH_TARGET not in source:
        return None
    return source.replace(_PATCH_TARGET, _PATCH_REPLACEMENT)


def ensure_histEFT_py39_compat() -> None:
    """Load ``topcoffea.modules.histEFT`` with Python 3.9 friendly annotations."""

    if sys.version_info >= (3, 10):
        return

    fullname = "topcoffea.modules.histEFT"
    if fullname in sys.modules:
        return

    spec = importlib.util.find_spec(fullname)
    if spec is None or spec.loader is None or not hasattr(spec.loader, "get_source"):
        return

    source = spec.loader.get_source(fullname)
    if source is None:
        return

    patched_source = _patched_histEFT_source(source)
    if patched_source is None:
        importlib.import_module(fullname)
        return

    module = importlib.util.module_from_spec(spec)
    module.__dict__.setdefault("Union", typing.Union)
    try:
        sys.modules[fullname] = module
        exec(compile(patched_source, spec.origin or fullname, "exec"), module.__dict__)
    except Exception:
        sys.modules.pop(fullname, None)
        raise

    package_name, _, attr = fullname.rpartition(".")
    package = importlib.import_module(package_name)
    setattr(package, attr, module)


def ensure_hist_utils() -> None:
    """Ensure ``topcoffea.modules.hist_utils`` is importable."""

    try:
        importlib.import_module("topcoffea.modules.hist_utils")
    except ModuleNotFoundError:
        modules_pkg = importlib.import_module("topcoffea.modules")
        module_name = "topcoffea.modules.hist_utils"
        sys.modules[module_name] = _FALLBACK_HIST_UTILS
        setattr(modules_pkg, "hist_utils", _FALLBACK_HIST_UTILS)


def get_hist_dict_non_empty(h: Dict[str, object]) -> Dict[str, object]:
    """Return a shallow copy of *h* that omits entries with empty histograms."""

    return {k: v for k, v in h.items() if not _is_hist_empty(v)}


class _StreamingHistDict(dict):
    """Dictionary subclass that forwards assignments to a sink function."""

    __slots__ = ("_sink",)

    def __init__(self, sink):
        super().__init__()
        self._sink = sink

    def __setitem__(self, key, value):  # type: ignore[override]
        self._sink(key, value)


_QUEUE_END = object()


class _StopStreaming(EOFError):
    """Sentinel exception used to cancel streaming unpickling early."""


class _StopAwareReader:
    __slots__ = ("_file", "_stop_event")

    def __init__(self, file, stop_event: threading.Event):
        self._file = file
        self._stop_event = stop_event

    def __getattr__(self, name):  # pragma: no cover - passthrough for file attrs
        return getattr(self._file, name)

    def _check(self):
        if self._stop_event.is_set():
            raise _StopStreaming()

    def read(self, size):  # pragma: no cover - small, exercised indirectly
        self._check()
        return self._file.read(size)

    def readinto(self, buffer):  # pragma: no cover - small, exercised indirectly
        self._check()
        return self._file.readinto(buffer)



if HAS_STREAMING_SUPPORT:

    class _StreamingHistUnpickler(_Unpickler):
        """Unpickler that emits items as the top-level histogram dict is filled."""

        dispatch = _Unpickler.dispatch.copy()

        def __init__(self, file, *, allow_empty=True, **kwargs):
            self._stop_event = threading.Event()
            stop_aware_file = _StopAwareReader(file, self._stop_event)
            super().__init__(stop_aware_file, **kwargs)
            self._allow_empty = allow_empty
            self._root_dict = None
            self._queue: "queue.Queue[Tuple[str, object] | object]" = queue.Queue(
                maxsize=1
            )
            self._worker_exc: Exception | None = None

        def _should_emit(self, hist) -> bool:
            if self._allow_empty:
                return True
            return not _is_hist_empty(hist)

        def _emit(self, key, hist):
            if self._should_emit(hist):
                self._push_queue((key, hist))

        def iterate(self) -> Iterator[Tuple[str, object]]:
            worker = threading.Thread(target=self._consume_pickle, daemon=True)
            worker.start()
            try:
                while True:
                    item = self._queue.get()
                    if item is _QUEUE_END:
                        if self._worker_exc is not None:
                            raise self._worker_exc
                        return
                    yield item  # type: ignore[misc]
            finally:
                self._stop_event.set()
                worker.join()

        def _push_queue(self, value):
            while not self._stop_event.is_set():
                try:
                    self._queue.put(value, timeout=0.1)
                    return
                except queue.Full:
                    continue

        def _consume_pickle(self):
            try:
                self._run()
                if self._root_dict is None:
                    raise UnpicklingError(
                        "Histogram pickle did not contain a dictionary"
                    )
            except _StopStreaming:
                pass
            except Exception as exc:  # pragma: no cover - propagated to caller
                self._worker_exc = exc
            finally:
                self._push_queue(_QUEUE_END)

        def _run(self):
            self.load()

        def _is_root_context(self) -> bool:
            return self._root_dict is None and not self.stack and not self.metastack

        def load_empty_dictionary(self):  # type: ignore[override]
            if self._is_root_context():
                root = _StreamingHistDict(self._emit)
                self._root_dict = root
                self.append(root)
            else:
                super().load_empty_dictionary()

        dispatch[EMPTY_DICT[0]] = load_empty_dictionary

        def load_dict(self):  # type: ignore[override]
            if self._is_root_context():
                items = self.pop_mark()
                root = _StreamingHistDict(self._emit)
                self._root_dict = root
                self.append(root)
                for i in range(0, len(items), 2):
                    root[items[i]] = items[i + 1]
            else:
                super().load_dict()

        dispatch[DICT[0]] = load_dict


def _is_hist_empty(hist: object) -> bool:
    empty_method = getattr(hist, "empty", None)
    if callable(empty_method):
        try:
            return bool(empty_method())
        except TypeError:
            return bool(empty_method)
    return False


class LazyHist:
    """Wrapper that defers histogram materialization until explicitly requested."""

    __slots__ = ("_payload", "_value")

    def __init__(self, payload: bytes):
        self._payload = payload
        self._value = _QUEUE_END  # sentinel reused privately

    @classmethod
    def from_hist(cls, hist: object) -> "LazyHist":
        payload = cloudpickle.dumps(hist, protocol=pickle.HIGHEST_PROTOCOL)
        return cls(payload)

    def materialize(self) -> object:
        if self._value is _QUEUE_END:
            self._value = cloudpickle.loads(self._payload)
        return self._value

    def release(self) -> None:
        if self._value is not _QUEUE_END:
            self._value = _QUEUE_END

    def empty(self) -> bool:
        hist = self.materialize()
        return _is_hist_empty(hist)

    def unwrap(self) -> object:
        return self.materialize()


def _iterate_hist_entries(
    path_to_pkl: str, allow_empty: bool
) -> Iterator[Tuple[str, object]]:
    with gzip.open(path_to_pkl, "rb") as fin:
        if HAS_STREAMING_SUPPORT:
            streamer = _StreamingHistUnpickler(fin, allow_empty=allow_empty)
            yield from streamer.iterate()
        else:
            mapping = pickle.load(fin)
            if not isinstance(mapping, dict):
                raise UnpicklingError("Histogram pickle did not contain a dictionary")
            for key, hist in mapping.items():
                if allow_empty or not _is_hist_empty(hist):
                    yield key, hist


def iterate_histograms_from_pkl(
    path_to_pkl: str, *, allow_empty: bool = True
) -> Iterator[Tuple[str, LazyHist]]:
    """Yield ``(key, LazyHist)`` pairs for the histograms stored in *path_to_pkl*."""

    for key, hist in _iterate_hist_entries(
        path_to_pkl, allow_empty=allow_empty
    ):
        lazy = LazyHist.from_hist(hist)
        del hist
        yield key, lazy


def iterate_hist_from_pkl(
    path_to_pkl: str,
    *,
    allow_empty: bool = True,
    materialize: Union[bool, str] = False,
) -> Union[Iterator[Tuple[str, object]], Dict[str, object]]:
    """Iterate over histogram pickle entries, materializing as requested."""

    if isinstance(materialize, str):
        normalized = materialize.lower()
        if normalized not in {"lazy", "eager"}:
            raise ValueError(
                "materialize must be a boolean or one of 'lazy'/'eager'"
            )
        materialize = normalized == "eager"

    iterator = _iterate_hist_entries(path_to_pkl, allow_empty=allow_empty)
    if materialize:
        return {key: hist for key, hist in iterator}
    return iterator
