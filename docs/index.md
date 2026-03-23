# topcoffea documentation map

Use this page as the canonical hub for `topcoffea` documentation.

## Start here

- [Quickstart](quickstart.md) – **Start here** if you are maintaining shared
  helpers or installing `topcoffea` for downstream workflows.
- Analyst / operator / newcomer: start in `topeft` with
  [`topeft/docs/workflow_and_yaml_hub.md`](https://github.com/TopEFT/topeft/blob/master/docs/workflow_and_yaml_hub.md),
  [`topeft/docs/quickstart_run2.md`](https://github.com/TopEFT/topeft/blob/master/docs/quickstart_run2.md),
  and
  [`topeft/docs/taskvine_workflow.md`](https://github.com/TopEFT/topeft/blob/master/docs/taskvine_workflow.md).

## Shared-library usage

- [Quickstart](quickstart.md) – Installation and usage conventions for shared
  helpers.
- [Configuration guide](configuration.md) – Workflow guide for executor helpers,
  dataclasses, and configuration overlays.

## Cross-repo integration

- [`topeft` integration](topeft_integration.md) – Coordinated refs, namespace
  import expectations, and integration-specific notes.
- End-to-end workflow owners should use the exact `topeft` landing pages listed
  above rather than treating `topcoffea` as the operator guide.

## Reference

- [Tuple schema](tuple_schema.md) – Histogram tuple-key conventions and
  compatibility notes.
- [Configuration guide](configuration.md) – Detailed configuration reference for
  shared helpers and executor settings.

## Testing / troubleshooting

- [Quickstart](quickstart.md) – First stop for editable installs and smoke-test
  setup.
- [`topeft` integration](topeft_integration.md) – Use this when coordinated
  refs, namespace imports, or shared-environment expectations drift.
- [README testing section](../README.md#testing) – Current `pytest` entry
  points and integration-test selectors.

## Release history

- [Release notes](release_notes.md) – Canonical change log and release history.
