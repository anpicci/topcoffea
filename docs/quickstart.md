# Quickstart

This guide focuses on the `topcoffea` pieces that downstream analyses reuse.
For end-to-end workflows (running processors, building datacards, and plotting)
follow the [`topeft` documentation](https://github.com/TopEFT/topeft) and use the
sections below to look up the supporting APIs referenced there.

## Install `topcoffea`

Use an editable install so the namespace import (`import topcoffea`) resolves in
the same way CI and downstream repositories expect:

```bash
git clone https://github.com/TopEFT/topcoffea.git
cd topcoffea
pip install -e .
python -c "import topcoffea"
```

The repository ships an `environment.yml` aligned with the shared
`coffea2025` toolchain (`coffea=2025.7.3`, `awkward=2.8.7`, `ndcctools`,
`conda-pack`, etc.). Create or update that environment before running
downstream entry points, then rebuild the TaskVine cache if workers rely on the
packaged environment.

## Configure executors and options

`topcoffea` centralises executor configuration in
`topcoffea.modules.executor_cli` and the accompanying [configuration
guide](configuration.md). Use these references when wiring up CLI flags or YAML
overlays inside analysis scripts so the `futures`, `taskvine`, and
`work_queue` backends share the same defaults and validation.

## Tuple schema helpers

Histogram pickles produced by the helpers in
`topcoffea.modules.hist_utils` follow the five-element
`(variable, channel, application, sample, systematic)` tuple schema documented
in [tuple_schema.md](tuple_schema.md). Downstream plotting utilities rely on the
same layout; prefer the provided dump/load helpers to enforce the ordering and
validation.

## Using with `topeft`

When developing both repositories together, pair `ch_update_calcoffea` here with
`format_update_anpicci_calcoffea` in `topeft` so shared helpers stay in sync.
Refer to the `topeft` quickstart and workflow guides for authoritative run and
plotting instructions; use this document to keep the underlying `topcoffea`
installation and configuration consistent with those workflows.
