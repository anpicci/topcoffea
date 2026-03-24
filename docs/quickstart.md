# Quickstart

This guide focuses on the `topcoffea` pieces that downstream analyses reuse.
For end-to-end workflows (running processors, building datacards, and plotting),
start with
[`topeft/docs/workflow_and_yaml_hub.md`](https://github.com/TopEFT/topeft/blob/master/docs/workflow_and_yaml_hub.md),
[`topeft/docs/quickstart_run2.md`](https://github.com/TopEFT/topeft/blob/master/docs/quickstart_run2.md),
and
[`topeft/docs/taskvine_workflow.md`](https://github.com/TopEFT/topeft/blob/master/docs/taskvine_workflow.md).
Use the sections below to look up the supporting `topcoffea` APIs referenced
there. For a complete `topcoffea` docs map, see [index.md](index.md).
For smoke tests and common troubleshooting, continue with
[testing.md](testing.md). For TaskVine cache naming, rebuild policy, and
downstream worker-environment handoff, continue with
[remote_environment.md](remote_environment.md).

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
packaged environment. Jet corrections now run eagerly without awkward virtual
caches, so keep `awkward>=2` and coffea `>=0.7` in sync with the pinned
environment to avoid AttributeError crashes when applying JEC/JER variations.
Detailed cache naming, rebuild policy, and TaskVine handoff live in
[remote_environment.md](remote_environment.md).
If the editable install or namespace import does not behave as expected, use
[testing.md](testing.md) for smoke checks and
[topeft_integration.md](topeft_integration.md) for coordinated-repo debugging.

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

When developing both repositories together, use coordinated refs (release tags
or matched feature branches) across repos. For current compatibility guidance,
see [`topeft_integration.md`](topeft_integration.md). Refer to
[`topeft/docs/workflow_and_yaml_hub.md`](https://github.com/TopEFT/topeft/blob/master/docs/workflow_and_yaml_hub.md)
and
[`topeft/docs/quickstart_run2.md`](https://github.com/TopEFT/topeft/blob/master/docs/quickstart_run2.md)
for authoritative run and plotting instructions; use this document to keep the
underlying `topcoffea` installation and configuration consistent with those
workflows.

## Where to go next

- Need smoke tests or first-pass troubleshooting: [testing.md](testing.md)
- Need worker-cache or TaskVine environment details:
  [remote_environment.md](remote_environment.md)
- Need shared plotting examples rather than end-to-end analysis plotting:
  [plotting.md](plotting.md)
- Need coordinated-ref or shared-environment guidance:
  [topeft_integration.md](topeft_integration.md)
