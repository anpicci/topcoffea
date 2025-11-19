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
   The `environment.yml` file mirrors the versions baked into cached TaskVine
   environments (Python 3.13, `coffea==2025.7.3`, `awkward==2.8.7`,
   `ndcctools>=7.14.11`, `setuptools=80.9.0`, etc.) and defines the shared
   `coffea20250703` environment.  Create or update that Conda environment before
   running processors, then rebuild cached worker tarballs so the refreshed
   toolchain is distributed alongside submissions:

   ```bash
   conda env create -f environment.yml  # or: conda env update -f environment.yml --prune
   conda activate coffea20250703
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
