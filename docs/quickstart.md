# Quickstart

This short guide summarises the minimum steps to get a development checkout of
`topcoffea` running and points to the configuration reference for more detail.

1. **Clone and install**
   ```bash
   git clone https://github.com/TopEFT/topcoffea.git
   cd topcoffea
   pip install -e .
   ```
   The editable install matches what our TaskVine workers expect and keeps your
   local checkout in sync with the cached remote environment.【F:README.md†L1-L14】

2. **Reuse the Conda environment shipped with analyses**
   The `environment.yml` file now matches the `ttbarEFT` `coffea2025` baseline
   (`coffea=2025.7.3`, `awkward=2.8.7`, `ndcctools`, `conda-pack`, etc.) so the
   same solver inputs are used locally, in CI, and by downstream users. Create
   or update that Conda environment before running processors, then rebuild
   cached worker tarballs so the refreshed toolchain is distributed alongside
   submissions:

   ```bash
   conda env create -f environment.yml  # or: conda env update -f environment.yml --prune
   conda activate coffea2025
   pip install -e .
   python -c "import topcoffea"
   ```

   The final command mirrors the CI smoke test and guarantees that downstream
   `topeft` checkouts still resolve the namespace import.【F:README.md†L7-L28】

3. **Pick an executor**
   The `RunConfig`/`ExecutorCLIConfig` dataclass lets you switch between the
   `futures`, `taskvine`, and `work_queue` backends.  See the
   [configuration guide](configuration.md) for the field-by-field description
   and YAML profile examples.【F:docs/configuration.md†L10-L32】

4. **Launch a processor**
   Downstream analyses such as `topeft` reuse this quickstart; for example,
   `analysis/topeft_run2/run_analysis.py` accepts the same executor arguments and
   optional YAML profiles.  Review the [configuration guide](configuration.md)
   for the runtime merging rules before launching large campaigns.【F:docs/configuration.md†L34-L58】

For TaskVine usage and datacard-generation workflows refer to the dedicated
sections in each analysis repository, which now link back to the shared
configuration guide.

## Aligning with topeft's new entry point

The refreshed `topeft` entry point (`analysis/topeft_run2/run_analysis.py` and
its quickstart wrapper) calls back into shared `topcoffea` helpers so both
repositories expose the same TaskVine-focused workflow. When extending the
entry point or adding new plotting/CLI front-ends:

- Reuse :func:`topcoffea.modules.executor_cli.register_executor_arguments` so the
  TaskVine defaults (`executor=taskvine`, port range `9123-9130`, `--environment-file`
  support) remain identical across scripts.【F:topcoffea/modules/executor_cli.py†L1-L135】
- Normalise user-provided values with
  :func:`topcoffea.modules.executor_cli.executor_config_from_values`; when the
  executor resolves to TaskVine and `environment_file` is set to `auto`, the
  helper transparently invokes
  :func:`topcoffea.modules.remote_environment.get_environment` so editable
  `topcoffea`/`topeft` checkouts are packaged and cached before workers start.【F:topcoffea/modules/executor_cli.py†L137-L197】【F:topcoffea/modules/remote_environment.py†L1-L101】
- Keep TaskVine tarballs reproducible by leaving
  :data:`topcoffea.modules.executor_cli.TASKVINE_EXTRA_PIP_LOCAL` untouched; it
  tracks both repositories' watch paths and triggers cache rebuilds whenever the
  entry-point code changes.【F:topcoffea/modules/executor_cli.py†L30-L60】【F:topcoffea/modules/remote_environment.py†L24-L60】
- Emit histogram pickles through
  :func:`topcoffea.modules.hist_utils.dump_to_pkl`, which enforces the shared
  five-element tuple schema documented in :doc:`tuple_schema` so plotting updates
  (e.g. the control/signal-region overlays) can consume TaskVine outputs without
  schema-specific adapters.【F:topcoffea/modules/hist_utils.py†L34-L69】

Following these conventions keeps the topeft entry points compatible with the
TaskVine executor options, cached remote environments, and tuple-keyed pickles
that topcoffea's utilities already validate.

## End-to-end: run a toy processor and plot tuple outputs

The snippets below walk through a minimal workflow that exercises both
executors and the tuple-keyed pickle layout used by the plotting utilities. The
same tuple schema is consumed by `topeft` plotting scripts, so outputs produced
here can be loaded by either repository without conversion. Refer back to the
[configuration guide](configuration.md) for the full list of runtime switches
and to the [tuple-schema reference](tuple_schema.md) for the tuple layout.

1. **Define a tiny processor that writes tuple-keyed histograms**

   Save the following as `examples/toy_processor.py` in a working directory. It
   fills a single-bin histogram keyed by the
   `(variable, channel, application, sample, systematic)` tuple expected by the
   pickle readers and writes the result with
   :func:`topcoffea.modules.hist_utils.dump_to_pkl`:

   ```python
   from __future__ import annotations

   import argparse
   import yaml
   from pathlib import Path

   from coffea import processor
   from hist import Hist

   from topcoffea.modules import executor_cli
   from topcoffea.modules.hist_utils import dump_to_pkl


   class ToyProcessor(processor.ProcessorABC):
       def __init__(self):
           self._hist = Hist.new.Reg(1, 0, 1, name="ones", label="unit weights").Double()

       def process(self, events):
           # Pretend every chunk has one event; real analyses would use columns from `events`.
           hist = self._hist.copy()
           hist.fill([1.0])
           return {("ones", "SR", "isSR", "data", "nominal"): hist}

       def postprocess(self, accumulator):
           return accumulator


   if __name__ == "__main__":
       parser = argparse.ArgumentParser(description="Toy tuple-keyed processor")
       parser.add_argument("--options", help="YAML RunConfig overlay", default=None)
       executor_cli.register_executor_arguments(parser)
       args = parser.parse_args()

       profile = {}
       if args.options:
           profile = yaml.safe_load(Path(args.options).read_text())

       config = executor_cli.executor_config_from_values(
           executor=profile.get("executor", args.executor),
           nworkers=profile.get("nworkers", args.nworkers),
           chunksize=profile.get("chunksize", args.chunksize),
           nchunks=profile.get("nchunks", args.nchunks),
           port=profile.get("port", args.port),
           environment_file=profile.get("environment_file", args.environment_file),
       )

       if config.executor == "taskvine":
           from ndcctools.taskvine.futures import FuturesExecutor as TaskVineExecutor

           port_low, _ = config.port if config.port else executor_cli.parse_port_range(None)
           executor = TaskVineExecutor(port=port_low, factory=config.environment_file or False)
       else:
           executor = processor.FuturesExecutor(workers=config.nworkers)

       runner = processor.Runner(
           executor=executor,
           chunksize=config.chunksize,
           savemetrics=False,
       )
       output = runner(
           {
               "dummy.root": {"treename": "Events", "metadata": {"dataset": "data"}},
           },
           treename="Events",
           processor_instance=ToyProcessor(),
       )
       dump_to_pkl("outputs/hists", output)
   ```

   The call to :func:`dump_to_pkl` enforces tuple ordering and validates that the
   application-region entry is populated, matching the safeguards documented in
   :doc:`tuple_schema` and implemented by
   :func:`topcoffea.modules.runner_output.normalise_runner_output` before the
   pickle is written.【F:topcoffea/modules/hist_utils.py†L34-L69】【F:topcoffea/modules/runner_output.py†L1-L116】

2. **Choose executor and chunking via configuration**

   Two YAML fragments toggle between a local futures run and a TaskVine
   submission. Both map directly onto the fields described in the
   [configuration guide](configuration.md) and are parsed through
   :func:`topcoffea.modules.executor_cli.executor_config_from_values` when wired
   into a CLI.

   ```yaml
   # options_futures.yml
   executor: futures
   nworkers: 2
   chunksize: 10_000

   # options_taskvine.yml
   executor: taskvine
   nworkers: 4
   port: 9123-9130
   environment_file: auto  # builds/reuses the cached env for workers
   chunksize: 10_000
   ```

   `environment_file` is intentionally absent from the futures profile and set
   to `auto` for TaskVine so the remote environment cache is rebuilt or reused
   before workers start. The `port` range is ignored when `executor=futures` but
   reserves a manager port when running with TaskVine, mirroring the behaviour
   of :class:`topcoffea.modules.executor_cli.ExecutorCLIConfig`.

3. **Run locally with the futures executor**

   ```bash
   python examples/toy_processor.py \
     --options options_futures.yml \
     --executor futures
   ```

     The command above exercises `processor.FuturesExecutor` inside the script
     and keeps everything on the login node. Swap `dummy.root` in the example
     input mapping for a small uproot-readable file from your analysis. The
     output pickle at `outputs/hists.pkl.gz` contains the same five-element tuple
     schema that `topeft` plotting scripts expect, so it can be moved directly
     into a plotting workflow without reformatting.

4. **Distribute the same processor with TaskVine**

   ```bash
   python examples/toy_processor.py \
     --options options_taskvine.yml \
     --executor taskvine \
     --environment-file auto
   ```

   When the executor resolves to TaskVine the `--environment-file auto` flag
   packages the active conda environment (including editable `topcoffea`/`topeft`
   installs) and ships it to workers before processing begins—no extra flag is
   required for the futures backend. The port range from the YAML is parsed by
   :func:`topcoffea.modules.executor_cli.parse_port_range` and handed to the
   TaskVine manager automatically.【F:topcoffea/modules/executor_cli.py†L29-L115】

5. **Load the tuple pickle and plot**

   The tuple-keyed pickle can be visualised with the same helper both
   repositories use:

   ```python
   from topcoffea.modules.hist_utils import get_hist_from_pkl

   hists = get_hist_from_pkl("outputs/hists.pkl.gz")
   for key, hist in hists.items():
       print("Loaded tuple", key, "with entries", hist.sum())
       # Downstream plotting backends can now iterate over `hists` directly.
   ```

   Because `get_hist_from_pkl` validates tuple length and application-region
   entries, it will raise on legacy 4-tuples and keep the schema aligned with
   the plotting contracts captured in :doc:`tuple_schema`. The same loader is
   used by `topeft` plotting utilities, so analysts can swap between futures and
   TaskVine executors without changing plotting code.【F:topcoffea/modules/hist_utils.py†L34-L132】【F:docs/tuple_schema.md†L1-L40】
