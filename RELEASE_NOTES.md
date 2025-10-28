# Release notes

## Unreleased

### Infrastructure
- Standardised remote environment cache naming via `topcoffea.modules.env_cache`
  so helper scripts shared with `topeft` build tarballs like
  `env_spec_<spec-hash>_edit_<editable-hash>.tar.gz` and rely on a
  single source for the format.
