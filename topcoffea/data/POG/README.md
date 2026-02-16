# CMS POG correctionlib payloads

## Usage guidance

For how these payloads fit into the broader corrections setup, see
[`docs/configuration.md`](../../../docs/configuration.md#payloads-and-corrections-overview).
For the full documentation map, see
[`docs/index.md`](../../../docs/index.md).

## Provenance

The upstream JSON payload release and documentation are maintained in
`jsonpog-integration`:
https://gitlab.cern.ch/cms-nanoAOD/jsonpog-integration

This directory is synced from CVMFS, for example:

```bash
cp -r /cvmfs/cms.cern.ch/rsync/cms-nanoAOD/jsonpog-integration/POG /path/to/topcoffea/data/
```

## What's in here

Subdirectories follow POG families and campaign labels (for example `BTV`,
`EGM`, `JME`, `LUM`, `MUO`, `TAU`), each with campaign-specific JSON payloads.

For Run 2, only UL campaign corrections are retained.
For Run 3, the recommended sets are:

- `2022_Summer22` for 2022
- `2022_Summer22EE` for 2022EE
- `2023_Summer23` for 2023
- `2023_Summer23BPix` for 2023BPix
