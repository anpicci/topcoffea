# Release notes

## Unreleased

### Infrastructure
- Standardised remote environment cache naming via `topcoffea.modules.env_cache` so helper scripts shared with `topeft` build tarballs like `env_spec_<spec-hash>_edit_<editable-hash>.tar.gz` and rely on a single source for the format.
- Replaced the legacy `coffea-env` specification with the shared `coffea20250703` environment (`python 3.13`, `coffea==2025.7.3`, `awkward==2.8.7`, `ndcctools>=7.14.11`, `setuptools>=72`). Downstream projects must rebuild cached worker tarballs via `python -m topcoffea.modules.remote_environment` so TaskVine workers and CI both pick up the refreshed toolchain.
- CI now runs a dedicated Conda smoke test of the README snippet (`conda env create -f environment.yml && conda run -n coffea20250703 pip install -e . && conda run -n coffea20250703 python -c "import topcoffea"`) to catch environment regressions before they reach downstream repositories.

### Histogram payloads
- Tuple-keyed histogram pickles now require five-element `(variable, channel, application, sample, systematic)` identifiers. Loader and writer utilities raise on legacy 4-tuples to ensure the application-region element is always recorded.
- HistEFT pickle helpers now error when the application-region entry is missing or `None`, removing the legacy fallback that silently permitted ambiguous tuple layouts.

## ch_update_calcoffea

- Aligns `HistEFT` storage handling with the `hist`/`boost-histogram` API used by Coffea 2025 by normalizing the storage configuration to `hist.storage.Double()`, while preserving compatibility with legacy `"Double"` inputs. This keeps the EFT histogram API in sync with `main` while tracking upcoming Coffea/runtime changes.
- Validates Wilson coefficient inputs during evaluation to guard against shape mismatches and unknown names. Evaluations now provide clearer errors when the coefficient vector does not match the histogram definition.
- Refreshes the default environment to target Coffea 2025-era dependencies (`python 3.11`, `numpy 2`, `hist 2.9+`, `boost-histogram 1.4+`, `setuptools`, `coffea 2025.7.3`).

Compatibility target: Coffea 2025 series with the corresponding `hist`/`boost-histogram` releases. Runtime expectations are Python 3.11+ and NumPy 2.x.
