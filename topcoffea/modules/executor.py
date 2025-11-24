"""Lightweight executor factory with processor compatibility checks.

The executor helpers shipped in ``topeft`` expect the sibling ``topcoffea``
checkout on the ``ch_update_calcoffea`` branch to expose a simple way to build
``coffea.processor.Runner`` instances.  This module provides that surface while
keeping the processor type guard flexible enough to accept both classic Coffea
processors and the CalCoffea variant used in newer releases.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, MutableMapping, Optional
import importlib


@dataclass(frozen=True)
class RunnerConfig:
    """Configuration used to construct :class:`coffea.processor.Runner` objects."""

    executor: str = "futures"
    chunksize: int = 100_000
    maxchunks: Optional[int] = None
    schema: Any = None
    executor_options: Mapping[str, Any] = field(default_factory=dict)
    runner_options: Mapping[str, Any] = field(default_factory=dict)


class ExecutorFactory:
    """Create Coffea runners with processor interface validation."""

    def __init__(self, config: RunnerConfig) -> None:
        self._config = config

    def _coffea_processor(self):
        return importlib.import_module("coffea.processor")

    def _nano_schema(self):
        schema = self._config.schema
        if schema is not None:
            return schema
        nanoevents = importlib.import_module("coffea.nanoevents")
        return nanoevents.NanoAODSchema

    def _validate_processor(self, processor_mod: Any, processor_instance: Any) -> None:
        candidates = []
        for attr in ("ProcessorABC", "CalProcessorABC"):
            base = getattr(processor_mod, attr, None)
            if base is not None:
                candidates.append(base)

        if candidates and not isinstance(processor_instance, tuple(candidates)):
            allowed = ", ".join(base.__name__ for base in candidates)
            raise TypeError(
                "Processor must inherit from one of: %s" % allowed
            )

        if not hasattr(processor_instance, "process"):
            raise TypeError(
                "Processor objects must define a 'process' method returning histograms"
            )

    def _build_executor(self, processor_mod: Any) -> Any:
        executor_name = (self._config.executor or "futures").lower()
        if executor_name == "futures":
            return processor_mod.FuturesExecutor(**dict(self._config.executor_options))
        if executor_name == "iterative":
            try:
                return processor_mod.IterativeExecutor(**dict(self._config.executor_options))
            except AttributeError:  # pragma: no cover - depends on coffea build
                return processor_mod.iterative_executor(**dict(self._config.executor_options))
        raise ValueError(
            f"Unknown executor '{executor_name}'. Expected 'futures' or 'iterative'."
        )

    def create_runner(self, processor_instance: Any) -> Any:
        """Return a configured ``coffea.processor.Runner``.

        The processor instance is validated against the available Coffea or
        CalCoffea ABCs before the runner is constructed so downstream analyses
        can fail fast with a clear error message when the wrong base class is
        used.
        """

        processor_mod = self._coffea_processor()
        self._validate_processor(processor_mod, processor_instance)

        runner_kwargs: MutableMapping[str, Any] = dict(self._config.runner_options)
        runner_kwargs.setdefault("schema", self._nano_schema())
        runner_kwargs.setdefault("chunksize", self._config.chunksize)
        runner_kwargs.setdefault("maxchunks", self._config.maxchunks)

        return processor_mod.Runner(
            executor=self._build_executor(processor_mod),
            **runner_kwargs,
        )


__all__ = ["RunnerConfig", "ExecutorFactory"]
