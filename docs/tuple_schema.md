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

## topeft entry points and plotting tools

The refreshed `analysis/topeft_run2` entry points—including
`run_analysis.py`, its TaskVine-aware quickstart wrapper, and the plotting
scripts that read TaskVine outputs—expect pickles that follow the 5-tuple
layout verbatim. When producing output from a `topcoffea` runner (for example
inside a TaskVine job launched by the new CLI helper) call
:func:`topcoffea.modules.runner_output.normalise_runner_output` before writing
the pickle or, more simply, rely on :func:`topcoffea.modules.hist_utils.dump_to_pkl`
which already invokes the normaliser and enforces the tuple layout.【F:topcoffea/modules/runner_output.py†L1-L116】【F:topcoffea/modules/hist_utils.py†L34-L63】
Plotting utilities (such as `make_cr_and_sr_plots.py` in `topeft`) should load
the resulting artifact with :func:`topcoffea.modules.hist_utils.get_hist_from_pkl`
to benefit from the same schema validation. When analysts need deterministic,
serialisable summaries—e.g. the new plotting backends that iterate over tuple
keys—use :func:`topcoffea.modules.runner_output.materialise_tuple_dict` to turn
lazy histogram handles into ordered payloads before shipping them to plotting
processes.【F:topcoffea/modules/runner_output.py†L64-L111】【F:topcoffea/modules/hist_utils.py†L63-L132】

These helpers guarantee that future topeft entry points and plotting updates
remain compatible with the shared tuple schema without re-implementing tuple
validation or TaskVine-specific serialisation steps.
