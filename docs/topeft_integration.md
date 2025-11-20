# topeft integration

The `run3_test_mmerged` branch of `topcoffea` is maintained for side-by-side use with the `run3_test_mmerged_anpicci` branch of [`topeft`](https://github.com/TopEFT/topeft). Keep both repositories on those branches to match the validated workflow and shared configurations.

## Quick setup checklist

From a `topeft` checkout that lives next to `topcoffea`:

1. Activate the desired analysis environment (for example `conda activate <env>`).
2. Install `topcoffea` in editable mode from the sibling checkout: `pip install -e ../topcoffea`.
3. Confirm the package is discoverable before running analysis code: `python -c "import topcoffea"`.

## Package data and import paths

The `topcoffea.modules.paths.topcoffea_path` helper must be used to locate packaged data and JSON payloads so the correct files are found regardless of where `topcoffea` is installed. Avoid keeping duplicate or modified copies of those resources inside a `topeft` checkout, since local overlays can mask the installed package and lead to stale data being used at runtime.

## Compatibility helpers shared with `topeft`

`topcoffea.modules.hist_utils` now ships the shims previously hosted inside
`topeft/topeft/compat`. Projects running on Python 3.9 should invoke
`hist_utils.ensure_histEFT_py39_compat()` before importing
`topcoffea.modules.histEFT` to patch PEP 604 union annotations, and use
`hist_utils.ensure_hist_utils()` when `topcoffea.modules.hist_utils` needs to be
available despite missing optional dependencies.

## Installation flow reference

`topeft` installs `topcoffea` through [`scripts/install_topcoffea.sh`](https://github.com/TopEFT/topeft/blob/run3_test_mmerged_anpicci/scripts/install_topcoffea.sh), which clones the repository (or updates an existing checkout) and performs an editable install. Set `TOPCOFFEA_GIT_REF=run3_test_mmerged` when invoking that script to pin the supported branch. These instructions mirror that flow so manual setups stay in sync with the automated installation.
