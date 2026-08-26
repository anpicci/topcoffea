# topeft integration

The consuming [`topeft`](https://github.com/TopEFT/topeft) workflow owns the
exact compatible `topcoffea` ref through its installation or environment
configuration. Record both resolved commits for a campaign; do not infer a
compatible pair from similarly named branches.

## Quick setup checklist

From a `topeft` checkout that lives next to `topcoffea`:

1. Activate the desired analysis environment (for example `conda activate <env>`).
2. Install `topcoffea` in editable mode from the sibling checkout: `pip install -e ../topcoffea`.
3. Confirm the package is discoverable before running analysis code: `python -c "import topcoffea"`.
4. Record `git rev-parse HEAD` in both repositories with campaign evidence.

## Package data and import paths

The `topcoffea.modules.paths.topcoffea_path` helper must be used to locate packaged data and JSON payloads so the correct files are found regardless of where `topcoffea` is installed. Avoid keeping duplicate or modified copies of those resources inside a `topeft` checkout, since local overlays can mask the installed package and lead to stale data being used at runtime.

## Installation flow reference

`topeft` installs `topcoffea` through `scripts/install_topcoffea.sh`, which
clones the repository (or updates an existing checkout) and performs an
editable install. Set `TOPCOFFEA_GIT_REF` to the exact ref required by the
consuming workflow. These instructions mirror that flow so manual setups stay
in sync with the automated installation. For remote-executor archive identity
and validation, see
[Remote environment archive contract](environment_archive_contract.md).
