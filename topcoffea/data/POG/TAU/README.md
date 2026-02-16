# Tau POG payloads (TAU)

## Usage guidance

For general correctionlib loading/evaluation patterns and how `topcoffea`
consumes payloads, see
[`docs/configuration.md`](../../../../docs/configuration.md#payloads-and-corrections-overview).
For the full documentation map, see [docs/index.md](../../../../docs/index.md).

## Provenance

Tau recommendations:
https://twiki.cern.ch/twiki/bin/viewauth/CMS/TauIDRecommendationForRun2

JSON production reference:
https://github.com/cms-tau-pog/correctionlib

## What's in here

This directory contains Tau POG scale factors and energy-scale payloads (for
example `tau.json.gz`) used for tau ID, trigger, and TES/FES workflows.

### DeepTau2017v2p1 summary

| Tau component | `genmatch` | `VSjet` | `VSe` | `VSmu` | Energy scale |
| --- | --- | --- | --- | --- | --- |
| Real tau | `5` | vs `pt` or vs `dm` | n/a | n/a | vs `dm` |
| `e -> tau` fake | `1`, `3` | n/a | vs `eta` | n/a | vs `dm` and `eta` |
| `mu -> tau` fake | `2`, `4` | n/a | n/a | vs `eta` | n/a (plus/minus 1 percent uncertainty) |

If you use a working-point combination different from the measurement setup,
apply the extra uncertainty recommended on the TWiki.

### Gen-match coding

- `1`: prompt electrons
- `2`: prompt muons
- `3`: electrons from tau decay
- `4`: muons from tau decay
- `5`: real taus
- `6`: no match or jets faking taus

In NanoAOD, this mapping is exposed via `Tau_GenPartFlav`, with jet/no-match
represented by `Tau_GenPartFlav == 0`.

### Campaign mapping

| Year label | MC campaign | Data campaign |
| --- | --- | --- |
| `2016Legacy` | `RunIISummer16MiniAODv3` | `17Jul2018` |
| `2017ReReco` | `RunIIFall17MiniAODv2` | `31Mar2018` |
| `2018ReReco` | `RunIIAutumn18MiniAOD` | `17Sep2018` / `22Jan2019` |
