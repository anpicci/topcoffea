# Tau scale factors in JSON format (TauSF)

## Usage guidance

For general correctionlib usage patterns and how `topcoffea` consumes payloads,
see
[`docs/configuration.md`](../../../docs/configuration.md#payloads-and-corrections-overview).
For the full documentation map, see [docs/index.md](../../../docs/index.md).

## Provenance

Primary source:
https://github.com/cms-tau-pog/TauIDSFs

These payloads were converted from the original ROOT-based distribution into
JSON for coffea-friendly use while preserving binning granularity and values.

For files not prefixed with `TauFakeSF_*`, source documentation follows the Tau
POG repository above.  Where needed for TOP-22-006 workflows, payloads were
translated to the JSON structure used in this analysis setup.

## What's in here

- `TauSF*`, `TauTES*`, `TauFES*`: scale-factor and energy-scale payload
  families by year and era.
- `TauFake*` and `TauFakeSF_*`: fake-rate correction payloads for jets
  misidentified as hadronic taus at fixed DeepTau working points.

The `TauFakeSF_*` payloads correspond to the procedure presented to Tau POG:
https://indico.cern.ch/event/1326438/#preview:4727537
