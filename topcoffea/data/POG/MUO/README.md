# Muon POG payloads (MUO)

## Usage guidance

For general correctionlib usage patterns and how `topcoffea` consumes payloads,
see
[`docs/configuration.md`](../../../../docs/configuration.md#payloads-and-corrections-overview).
For the full documentation map, see [docs/index.md](../../../../docs/index.md).

## Provenance

Muon payload recommendations and method details are documented on the Muon POG
TWiki:
https://twiki.cern.ch/twiki/bin/view/CMS/MuonRun32022

## What's in here

Under each campaign folder, the usual files are:

- `muon_JPsi.json` (J/Psi tag-and-probe, low-pt)
- `muon_Z.json` (Z tag-and-probe, medium-pt)
- `muon_HighPt.json` (high-mass DY cut-and-count, high-pt)

| Correction file | Method | Typical pt range | TWiki section |
| --- | --- | --- | --- |
| `muon_JPsi` | TnP on J/Psi peak | `pt < 30 GeV` | [low-pt](https://twiki.cern.ch/twiki/bin/view/CMS/MuonRun32022#Low_pT_below_30_GeV) |
| `muon_Z` | TnP on Z peak | `15 < pt < 200 GeV` | [medium-pt](https://twiki.cern.ch/twiki/bin/view/CMS/MuonRun32022#Medium_pT_15_GeV_to_200_GeV) |
| `muon_HighPt` | Cut-and-count on high-mass DY | `pt > 200 GeV` | [high-pt](https://twiki.cern.ch/twiki/bin/view/CMS/MuonRun32022#High_pT_above_200_GeV) |

## Payload-specific conventions

For Run 3 2023, some Z-peak scale factors are binned in signed `eta` (with
more granularity) instead of `abs(eta)`.

For earlier years (Run 2 UL + 2022), payload definitions use `abs(eta)`, but
`eta` can still be provided as input for consistency with 2023 workflows.
