# Release notes

## Unreleased

### Infrastructure
- Standardised remote environment cache naming via `topcoffea.modules.env_cache`
  so helper scripts shared with `topeft` build tarballs like
  `env_spec_<spec-hash>_edit_<editable-hash>.tar.gz` and rely on a
  single source for the format.
- Updated the shared environment spec and packaging helper to pin Python 3.13
  together with Coffea 2025.7.3 (`awkward==2.8.7`, `ndcctools>=7.14.11`,
  `setuptools=80.9.0`).  Downstream projects should rebuild cached worker
  tarballs via `python -m topcoffea.modules.remote_environment` so workers pick
  up the refreshed toolchain.
