# Tuple-keyed histogram schema

Tuple-keyed histogram outputs avoid relying on categorical axes to preserve
analysis metadata. Each histogram entry is stored under a five-element tuple:

1. **Variable** – the observable being histogrammed.
2. **Channel** – the analysis channel or region grouping.
3. **Application region** – tags such as `isSR_*`, `isCR_*`, or other
   application labels used during non-prompt or control-region projections.
4. **Sample** – the physics sample name.
5. **Systematic** – the systematic variation label (including `nominal`).

The order is fixed so downstream tools can safely parse tuple identifiers
without inspecting histogram axes. Helpers in
`topcoffea.modules.runner_output` (e.g. :func:`normalise_runner_output` and
:func:`materialise_tuple_dict`) keep tuple-keyed payloads deterministic during
serialisation, and :func:`topcoffea.modules.hist_utils.dump_to_pkl` uses these
utilities automatically when writing pickle artifacts. Pickles remain
structured as ``{tuple_key: HistEFT|Hist, ...}`` mappings—the tuple ordering is
the only change. Downstream code can keep consuming the histogram objects
directly while relying on deterministic tuple ordering for reproducible
artefacts. Loader utilities validate tuple keys and will raise if a histogram
entry omits the application-region element; downstream consumers should update
any legacy 4-tuples to the five-element schema above.
