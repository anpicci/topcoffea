# Coordinating `topcoffea` with `topeft`

Use compatible snapshots of both repositories (release tags or coordinated
branches) in the same Python environment. Keep this page evergreen by avoiding
hard-coded branch pairings in workflow docs.

If you need end-to-end analysis instructions, start in `topeft` with
[`topeft/docs/workflow_and_yaml_hub.md`](https://github.com/TopEFT/topeft/blob/master/docs/workflow_and_yaml_hub.md),
[`topeft/docs/quickstart_run2.md`](https://github.com/TopEFT/topeft/blob/master/docs/quickstart_run2.md),
and
[`topeft/docs/taskvine_workflow.md`](https://github.com/TopEFT/topeft/blob/master/docs/taskvine_workflow.md).
Use this page for compatibility and environment coordination rather than as the
primary operator guide.

If you only need install smoke tests or pytest entry points, start with
[quickstart.md](quickstart.md) and [testing.md](testing.md) instead.

## Dependency and environment policy

`topcoffea` and `topeft` share a dependency stack. The authoritative constraints
are defined in:

- `pyproject.toml` (`[project].dependencies`)
- `environment.yml`

The environment policy is validated by tests (policy-as-tests), not by doc
prose. When in doubt, follow `tests/test_environment_spec.py`.

At the time of writing, key constraints include:

- `coffea==2025.7.3`
- `awkward==2.8.7`
- `numpy>=2.3,<2.4`
- `hist>=2.9,<3.0`
- `pandas>=2.2,<2.3`

After dependency updates, refresh the environment and rebuild the TaskVine
archive so workers receive consistent wheels:

```bash
conda env update -f environment.yml --prune
python -m topcoffea.modules.remote_environment
```

For cache naming, archive location, and rebuild-policy details, continue with
[remote_environment.md](remote_environment.md). This page stays focused on
coordinated-repo expectations rather than general worker-cache operations.

## Keep the namespace import available

`topeft` relies on plain namespace imports:

```bash
python -c "import topcoffea"
```

Install `topcoffea` into the same environment used for `topeft`.

- Editable install from local checkout:

```bash
python -m pip install -e .
python -c "import topcoffea"
```

- Direct install from a chosen Git ref:

```bash
python -m pip install "git+https://github.com/TopEFT/topcoffea.git@<ref>"
python -c "import topcoffea"
```

Re-run the smoke test after pulling updates.

## Optional dependency helper

If you use the `topeft` optional dependency extra, install from this checkout:

```bash
python -m pip install -e ".[topeft]"
python -m topcoffea.modules.remote_environment
```

That extra follows the dependency ref currently declared in `pyproject.toml`.

## Suggested workflow

1. Clone `topcoffea` and `topeft` side-by-side.
2. Activate the shared analysis environment.
3. Install `topcoffea` in editable mode.
4. Run `topeft` commands and keep both repos updated together.

## Avoid loading vendored copies

Importing `topcoffea` from a vendored copy nested inside a `topeft` checkout can
shadow the real package and trigger startup errors. If that happens, remove the
nested copy and reinstall the standalone `topcoffea` checkout.
