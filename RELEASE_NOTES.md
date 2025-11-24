# Release notes

## Unreleased

### Infrastructure
- Standardised remote environment cache naming via `topcoffea.modules.env_cache` so helper scripts shared with `topeft` build tarballs like `env_spec_<spec-hash>_edit_<editable-hash>.tar.gz` and rely on a single source for the format.
- Replaced the legacy `coffea-env` specification with the shared `coffea2025` environment (`coffea=2025.7.3`, `awkward=2.8.7`, `ndcctools`, `conda-pack`, `xrootd`, `git`, `pyyaml`, conda-forge Python). Downstream projects must rebuild cached worker tarballs via `python -m topcoffea.modules.remote_environment` so TaskVine workers and CI both pick up the refreshed toolchain.
- CI now runs a dedicated Conda smoke test of the README snippet (`conda env create -f environment.yml && conda run -n coffea2025 pip install -e . && conda run -n coffea2025 python -c "import topcoffea"`) to catch environment regressions before they reach downstream repositories.
- Added `boost-histogram>=1.4` to `environment.yml` so the Conda spec matches the Python dependency set (`boost-histogram`/`hist`/`numpy`/`pandas`) used in `pyproject.toml` and the aligned `topeft` environment.

### Histogram payloads
- Tuple-keyed histogram pickles now require five-element `(variable, channel, application, sample, systematic)` identifiers. Loader and writer utilities raise on legacy 4-tuples to ensure the application-region element is always recorded.
- HistEFT pickle helpers now error when the application-region entry is missing or `None`, removing the legacy fallback that silently permitted ambiguous tuple layouts.

## ch_update_calcoffea

- Aligns `HistEFT` storage handling with the `hist`/`boost-histogram` API used by Coffea 2025 by normalizing the storage configuration to `hist.storage.Double()`, while preserving compatibility with legacy `"Double"` inputs. This keeps the EFT histogram API in sync with `main` while tracking upcoming Coffea/runtime changes.
- Validates Wilson coefficient inputs during evaluation to guard against shape mismatches and unknown names. Evaluations now provide clearer errors when the coefficient vector does not match the histogram definition.
- Refreshes the default environment to target Coffea 2025-era dependencies (`coffea 2025.7.3`, `awkward 2.8.7`, `numpy 2`, `hist 2.9+`, `boost-histogram 1.4+`, `conda-pack`, `ndcctools`).

Compatibility target: Coffea 2025 series with the corresponding `hist`/`boost-histogram` releases. Runtime expectations follow the conda-forge Python packaged in the shared environment and NumPy 2.x.
