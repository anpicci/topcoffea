# Testing and troubleshooting

Use this page for lightweight verification and first-pass troubleshooting in
`topcoffea`.

## Start here

- Need installation/update steps or shared-helper usage context: start with
  [quickstart.md](quickstart.md)
- Need coordinated-repo or environment-drift guidance: continue with
  [topeft_integration.md](topeft_integration.md)
- Need worker-cache rebuild behavior or TaskVine archive semantics: continue
  with [remote_environment.md](remote_environment.md)
- Need the full docs map: go back to [index.md](index.md)

## Smoke tests

Confirm the plain namespace import succeeds in the active environment:

```bash
python -c "import topcoffea"
```

For downstream-facing checks, confirm the namespace exposes the helpers that
`topeft` expects:

```bash
python -c "import topcoffea; topcoffea.modules.histEFT.HistEFT"
```

## Pytest entry points

Default `pytest` runs exclude integration tests (`addopts = -m "not integration"`):

```bash
pytest -q
pytest -q -k "not taskvine"
pytest -q -m integration
pytest -q -m integration -k "taskvine or vine"
TOPCOFFEA_TASKVINE_TIMEOUT_SECONDS=30 pytest -q -m integration tests/test_taskvine_cli.py::test_minimal_taskvine_cli
```

## Common troubleshooting routes

- `import topcoffea` fails:
  return to [quickstart.md](quickstart.md) and confirm the editable install was
  performed in the same environment used by downstream analysis code.
- `topeft` cannot see the expected `topcoffea` helpers:
  use [topeft_integration.md](topeft_integration.md) to check coordinated refs,
  namespace-import expectations, and shared-environment assumptions.
- Worker environments look stale after dependency updates:
  see [remote_environment.md](remote_environment.md) for cache naming and
  rebuild behavior, then rebuild the cached environment archive with
  `python -m topcoffea.modules.remote_environment` and re-run the smoke tests
  above.
