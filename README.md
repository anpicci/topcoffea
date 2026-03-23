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

## Install `topcoffea`

Use an editable install so the namespace import (`import topcoffea`) resolves in
the same way CI and downstream repositories expect:

```bash
git clone https://github.com/TopEFT/topcoffea.git
cd topcoffea
pip install -e .
python -c "import topcoffea; topcoffea.modules.histEFT.HistEFT"
```

The shared `coffea2025` Conda environment distributed with `topcoffea` and
`topeft` tracks a TaskVine-ready dependency set (`coffea=2025.7.3`,
`awkward=2.8.7`, `ndcctools`, `conda-pack`, etc.) so local installs mirror the
remote cache. Provision or refresh that environment before running processors:

```bash
conda env create -f environment.yml  # or: conda env update -f environment.yml --prune
conda activate coffea2025
pip install -e .
python -c "import topcoffea"
```

Rebuild the cached worker tarball with
`python -m topcoffea.modules.remote_environment` after dependency updates so
downstream workflows pick up the refreshed pins. When environment-policy wording
in docs and implementation differ, follow `tests/test_environment_spec.py`.

Supported Coffea range: the 2025 release series, tested against
`coffea==2025.7.3`.

## Using with `topeft`

Projects such as [`topeft`](https://github.com/TopEFT/topeft) expect that the
plain namespace import (`import topcoffea`) succeeds without extra
`PYTHONPATH` tweaks. When testing a feature branch together with `topeft`, make
sure the coordinated ref is installed in the environment that runs the analysis.
For compatibility guidance, namespace-import smoke tests, and branch-pairing
notes, see [docs/topeft_integration.md](docs/topeft_integration.md).

For end-to-end workflows, plotting, and campaign-level instructions, use the
specific `topeft` landing pages above rather than treating this repository as an
operator manual.

## Documentation

- [Documentation index](docs/index.md) – canonical docs hub for `topcoffea`
- [Quickstart](docs/quickstart.md) – installation and usage conventions for
  shared helpers
- [Configuration guide](docs/configuration.md) – workflow guide and configuration
  reference for shared dataclasses and executors
- [Tuple schema](docs/tuple_schema.md) – histogram tuple-key reference used by
  downstream pickle outputs
- [`topeft` integration](docs/topeft_integration.md) – cross-repo integration
  guidance and exact `topeft` start links
- [Release notes](docs/release_notes.md) – canonical release history

## Testing

Default `pytest` runs exclude integration tests (`addopts = -m "not integration"`).

```bash
# Default unit-ish suite (integration excluded by default)
pytest -q

# Equivalent explicit selector
pytest -q -k "not taskvine"

# Run all integration tests
pytest -q -m integration

# Run only TaskVine/Vine integration tests
pytest -q -m integration -k "taskvine or vine"

# Override TaskVine CLI timeout (seconds)
TOPCOFFEA_TASKVINE_TIMEOUT_SECONDS=30 pytest -q -m integration tests/test_taskvine_cli.py::test_minimal_taskvine_cli
```

## Histogram plotting

`topcoffea` supports modern `hist` objects only. The legacy Coffea histogram
namespace is not supported.

```python
import hist
import mplhep as hep
import matplotlib.pyplot as plt

h2 = hist.Hist(
    hist.axis.Regular(20, -5, 5, name="x"),
    hist.axis.Regular(20, -5, 5, name="y"),
)
h2.fill(x=[-1.0, 0.2, 1.7], y=[0.5, -0.8, 1.1])

hep.hist2dplot(h2, xaxis="x")
plt.tight_layout()
plt.show()
```

## Remote environment cache

`topcoffea.modules.remote_environment.get_environment` builds and caches Conda
environments that include editable installs of `topcoffea`. The cache tarballs,
named via `topcoffea.modules.env_cache`, live next to your workflow as
`topeft-envs/env_spec_<hash>_edit_<commit>.tar.gz`, and the helper function
accepts an `unstaged` policy of either `rebuild` (default) or `fail` when it
detects local changes in editable checkouts. The cache key tracks editable
`topeft` checkouts so modifying a local `topeft` repository forces an
environment rebuild when `unstaged="rebuild"` is used. Pair the resulting
tarball with TaskVine workers submitted via
[`vine_submit_workers`](https://github.com/cooperative-computing-lab/taskvine/blob/main/doc/man/vine_submit_workers.md)
to avoid repeatedly transferring large environments.
