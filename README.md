# topcoffea

`topcoffea` is the shared-library layer used by TopEFT Coffea analyses. It
packages common corrections, executor helpers, histogram utilities, and
cross-repository integration surfaces.

## Start here

Use `topcoffea` as your starting point if you are maintaining shared helpers,
working on corrections or executor internals, or coordinating integration
between repositories. Use `topeft` as your starting point if you want
end-to-end analysis instructions, operator workflows, or newcomer guidance.

- Shared-library maintainer / integration developer: start with
  [docs/index.md](docs/index.md), [Quickstart](docs/quickstart.md), and
  [`topeft` integration](docs/topeft_integration.md).
- Analyst / operator / newcomer: start in `topeft` with
  [`topeft/docs/workflow_and_yaml_hub.md`](https://github.com/TopEFT/topeft/blob/master/docs/workflow_and_yaml_hub.md),
  [`topeft/docs/quickstart_run2.md`](https://github.com/TopEFT/topeft/blob/master/docs/quickstart_run2.md),
  and
  [`topeft/docs/taskvine_workflow.md`](https://github.com/TopEFT/topeft/blob/master/docs/taskvine_workflow.md).

`docs/index.md` is the canonical documentation hub for `topcoffea`.

## Documentation map

- [Documentation index](docs/index.md) – canonical docs hub for `topcoffea`
- [Quickstart](docs/quickstart.md) – installation and shared-library usage
- [Testing and troubleshooting](docs/testing.md) – smoke tests, pytest entry
  points, and common integration drift checks
- [Remote environment guide](docs/remote_environment.md) – TaskVine cache,
  rebuild, and downstream worker-environment notes
- [Plotting guide](docs/plotting.md) – shared plotting example surfaces and
  boundaries with `topeft`
- [Configuration guide](docs/configuration.md) – workflow guide and configuration
  reference for shared dataclasses and executors
- [Tuple schema](docs/tuple_schema.md) – histogram tuple-key reference used by
  downstream pickle outputs
- [`topeft` integration](docs/topeft_integration.md) – cross-repo integration
  guidance and exact `topeft` start links
- [Release notes](docs/release_notes.md) – canonical release history

## Minimal install smoke test

Use an editable install so the namespace import (`import topcoffea`) resolves in
the same way CI and downstream repositories expect:

```bash
git clone https://github.com/TopEFT/topcoffea.git
cd topcoffea
pip install -e .
python -c "import topcoffea; topcoffea.modules.histEFT.HistEFT"
```

For the full installation/update guidance, the shared `coffea2025`
environment recipe, and shared-helper usage patterns, continue with
[docs/quickstart.md](docs/quickstart.md). For TaskVine cache naming, rebuild
policy, and downstream worker-environment handoff, continue with
[docs/remote_environment.md](docs/remote_environment.md). When
environment-policy wording in docs and implementation differ, follow
`tests/test_environment_spec.py`.

## Testing and troubleshooting

Use [docs/testing.md](docs/testing.md) for the canonical smoke-test commands,
pytest selectors, and common coordinated-repo troubleshooting steps. Use
[docs/topeft_integration.md](docs/topeft_integration.md) when the issue is
specifically about coordinated refs, shared environments, or namespace-import
drift with `topeft`. Use [docs/remote_environment.md](docs/remote_environment.md)
for worker-cache rebuild questions and [docs/plotting.md](docs/plotting.md)
for shared plotting examples.
