# Remote environment guide

Use this page for the cached worker environment built by
`topcoffea.modules.remote_environment`. This is the operational guide for
TaskVine-ready archives; it is not the main install quickstart or the
coordinated-ref guide.

If you need installation/update steps or shared-helper usage context first,
start with [quickstart.md](quickstart.md). If you need smoke tests, use
[testing.md](testing.md). If you need cross-repo compatibility guidance,
continue with [topeft_integration.md](topeft_integration.md).

## What the cache is for

`topcoffea.modules.remote_environment.get_environment` builds and caches Conda
environment tarballs that include editable installs of `topcoffea`. Downstream
TaskVine workflows use those archives so workers receive the same dependency
stack and editable checkouts as the local submission environment.

The cache naming is handled by `topcoffea.modules.env_cache`. In the current
workflow, tarballs typically appear next to the run as:

```text
topeft-envs/env_spec_<hash>_edit_<commit>.tar.gz
```

That archive is the same asset paired with downstream TaskVine workers in
`topeft` workflows.

## Typical invocation

Build or refresh the cache from the active checkout and environment:

```bash
python -m topcoffea.modules.remote_environment
```

The helper prints the resulting archive path. Downstream TaskVine helpers can
use that path directly, or submission tools can preload it with
`--python-env`.

## When to rebuild

Rebuild the cached environment archive when:

- the Conda environment changes
- editable `topcoffea` or `topeft` checkouts change and workers should see the
  updated code
- TaskVine workers appear to be using stale dependencies
- coordinated refs between `topcoffea` and `topeft` have changed

The helper accepts an `unstaged` policy of either `rebuild` (default) or `fail`
when it detects local editable-checkout changes. The cache key also tracks
editable `topeft` checkouts, so modifying a local `topeft` repository forces an
environment rebuild when `unstaged="rebuild"` is used.

## Downstream `topeft` and TaskVine usage

For end-to-end analyst/operator workflows, `topeft` remains the authoritative
entrypoint. Use:

- [`topeft/docs/workflow_and_yaml_hub.md`](https://github.com/TopEFT/topeft/blob/master/docs/workflow_and_yaml_hub.md)
- [`topeft/docs/quickstart_run2.md`](https://github.com/TopEFT/topeft/blob/master/docs/quickstart_run2.md)
- [`topeft/docs/taskvine_workflow.md`](https://github.com/TopEFT/topeft/blob/master/docs/taskvine_workflow.md)

Those pages cover the full TaskVine run path. Use this page when you need the
cache-specific details behind that workflow.

## Related troubleshooting routes

- Editable install or namespace import looks wrong:
  [testing.md](testing.md)
- Coordinated refs or shared-environment expectations drift between repos:
  [topeft_integration.md](topeft_integration.md)
- End-to-end worker submission and operator workflow:
  [`topeft/docs/taskvine_workflow.md`](https://github.com/TopEFT/topeft/blob/master/docs/taskvine_workflow.md)
