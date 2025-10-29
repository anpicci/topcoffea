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
   environments (`coffea==2025.7.3`, `awkward==2.8.7`, etc.).  Create or update
   a Conda environment with those pins before running processors.【F:README.md†L7-L14】

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
