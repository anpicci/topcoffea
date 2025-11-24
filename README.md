# topcoffea

Tools that sit on top of coffea to facilitate CMS analyses. The repository is set up as a pip installable package. To install this package into a conda environment:
```
git clone https://github.com/TopEFT/topcoffea.git
cd topcoffea
pip install -e .

# Confirm the namespace import that downstream projects rely on
python -c "import topcoffea; topcoffea.modules.HistEFT.HistEFT"
```

The shared `coffea2025` Conda environment distributed with `topcoffea` and
`topeft` now matches the TaskVine-ready spec used in the `ttbarEFT`
`coffea2025` branch (`coffea=2025.7.3`, `awkward=2.8.7`, `ndcctools`,
`conda-pack`, etc.) so local installs mirror the remote cache. CI hashes
`environment.yml` against the stored upstream baseline to catch drift. Provision
or refresh the environment with the commands below before running processors so
downstream projects see the same toolchain that CI exercises:

```bash
conda env create -f environment.yml  # or: conda env update -f environment.yml --prune
conda activate coffea2025
pip install -e .
python -c "import topcoffea"
```

Rebuild the cached worker tarball with `python -m
topcoffea.modules.remote_environment` after pulling these changes so downstream
workflows pick up the refreshed pins.

Supported Coffea range: the 2025 release series, tested against `coffea==2025.7.3`.

## Using `topcoffea` from downstream projects

Projects such as [`topeft`](https://github.com/TopEFT/topeft) expect that the
plain namespace import (`import topcoffea`) succeeds without extra
`PYTHONPATH` tweaks. When testing a feature branch together with `topeft`, make
sure the branch is installed in the environment that runs the analysis. See
[the `topeft` integration guide](docs/topeft_integration.md) for branch pairing
details when working on `ch_update_calcoffea` in `topcoffea` alongside
`format_update_anpicci_calcoffea` in `topeft`.

```bash
# Option 1: install directly from GitHub
python -m pip install --upgrade pip
python -m pip install "git+https://github.com/TopEFT/topcoffea.git@<branch>"

# Option 2: editable install from a local checkout
git clone https://github.com/TopEFT/topcoffea.git
cd topcoffea
git checkout <branch>
python -m pip install -e .

# Smoke test to confirm the namespace import works for downstream users
python -c "import topcoffea"
```

Branches such as `format_update_anpicci_calcoffea` in the `topeft` repository
require the editable install above so helpers like
`topcoffea.modules.HistEFT` and `topcoffea.scripts.make_html` resolve via plain
attribute access. When developing both repositories side-by-side, activate the
environment used for `topeft`, run `pip install -e ../topcoffea` from the
`topeft` checkout, and re-run `python -c "import topcoffea"` (optionally adding
`topcoffea.modules.HistEFT.HistEFT` to the smoke test) before invoking the
analysis scripts. This matches the CI installation check and guarantees that
`import topcoffea` succeeds anywhere the sibling repository runs.

Running the smoke test mirrors the CI check and guarantees that modules such as
`topcoffea.modules.utils` can be imported by downstream repositories.



## Documentation

* [Quickstart](docs/quickstart.md) – installation and executor conventions for
  the `topcoffea` helpers shared across analyses.
* [Configuration guide](docs/configuration.md) – details on `RunConfig`, YAML
  overlays, and the dataclass helpers powering executors and jet corrections.
* [Tuple schema](docs/tuple_schema.md) – description of the
  `(variable, channel, application, sample, systematic)` histogram keys used in
  pickle outputs.
* [`topeft` integration](docs/topeft_integration.md) – branch coordination tips
  for keeping shared helpers aligned.

End-to-end run and plotting workflows now live in the
[`topeft`](https://github.com/TopEFT/topeft) documentation; follow those guides
for campaign-level instructions and use the `topcoffea` references above for API
and configuration details.

## Using with `topeft`

When testing changes against `topeft`, pair the `ch_update_calcoffea` branch
here with `format_update_anpicci_calcoffea` in the `topeft` repository so the
shared helpers and CLI flags stay in sync. The authoritative run and plotting
instructions live in the `topeft` quickstart and workflow guides; refer to those
documents for end-to-end steps and use the `topcoffea` references above to look
up configuration and tuple-schema details.

Branch compatibility is now enforced at runtime: Coffea 2025 executors raise if
the supplied processor does not derive from `coffea.processor.ProcessorABC`.
`topcoffea` `ch_update_calcoffea` and `topeft` `format_update_anpicci_calcoffea`
carry matching processor definitions and coffea 2025 pinning so the executor
accepts `AnalysisProcessor` instances without downgrades. The
`tests/test_topeft_analysis_processor_executor.py` smoke test imports the
`topeft` processor and exercises the coffea runtime check to catch branch
mismatches before they reach data access.

## Remote environment cache

`topcoffea.modules.remote_environment.get_environment` builds and caches
Conda environments that include editable installs of `topcoffea`. The
cache tarballs, named via `topcoffea.modules.env_cache`, live next to
your workflow as `topeft-envs/env_spec_<hash>_edit_<commit>.tar.gz`, and
the helper function accepts an `unstaged` policy of either `rebuild`
(default) or `fail` when it detects local changes in editable
checkouts. The cache key
tracks editable `topeft` checkouts so modifying a local `topeft`
repository forces an environment rebuild when `unstaged="rebuild"` is
used. Pair the resulting tarball with TaskVine workers submitted via
[`vine_submit_workers`](https://github.com/cooperative-computing-lab/taskvine/blob/main/doc/man/vine_submit_workers.md)
to avoid repeatedly transferring large environments.
