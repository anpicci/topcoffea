# JetMET POG payloads (JME)

## Usage guidance

For how these payloads fit into the broader corrections setup, see
[`docs/configuration.md`](../../../../docs/configuration.md#payloads-and-corrections-overview).
For the full documentation map, see
[`docs/index.md`](../../../../docs/index.md).

## Provenance

Primary recommendations:

- JetMET overview:
  https://twiki.cern.ch/twiki/bin/viewauth/CMS/JetMET#Quick_links_to_current_recommend
- JEC/JER version mapping:
  https://twiki.cern.ch/twiki/bin/viewauth/CMS/JECDataMC
- JER details:
  https://twiki.cern.ch/twiki/bin/view/CMS/JetResolution#JER_Scaling_factors_and_Uncertai

JSON production references:

- JMAR JSONs: https://github.com/cms-jet/JSON_Format
- JERC JSONs:
  https://github.com/cms-jet/JECDatabase/tree/master/scripts/JERC2JSON

## What's in here

This directory contains JME correction payloads, including:

- Tagging scale factors (`*_jmar.json.gz`)
- Jet energy corrections/resolution payloads (`jet_jerc.json.gz`,
  `fatJet_jerc.json.gz`)
- Full JEC uncertainty sources
- JER scale factors and JER parameterizations (currently AK4-focused)
- MET phi correction payloads (`met.json.gz` in relevant campaign folders)

Payload naming conventions:

- Run 2: `jet` corresponds to `AK4PFchs`, `fatJet` to `AK8PFPuppi`
- Run 3: `jet` corresponds to `AK4PFPuppi`, `fatJet` to `AK8PFPuppi`

## Campaign mapping

| Year folder | MC campaign | Data campaign |
| --- | --- | --- |
| `2016_EOY` | `RunIISummer16MiniAODv3` | `17Jul2018` |
| `2017_EOY` | `RunIIFall17MiniAODv2` | `31Mar2018` |
| `2018_EOY` | `RunIIAutumn18MiniAOD` | `17Sep2018` / `22Jan2019` |
| `2016preVFP_UL` | `RunIISummer20UL16MiniAODAPVv2` | `21Feb2020` |
| `2016postVFP_UL` | `RunIISummer20UL16MiniAODv2` | `21Feb2020` |
| `2017_UL` | `RunIISummer20UL17MiniAODv2` | `09Aug2019` |
| `2018_UL` | `RunIISummer20UL18MiniAODv2` | `12Nov2019` |
| `2022_Prompt` | `Winter22` | `Prompt RunCDE` |
| `2022_Summer22` | `Summer22` | `22Sep2023` (ReReco CD) |
| `2022_Summer22EE` | `Summer22EE` | `22Sep2023` (ReReco E + Prompt RunFG, EE leak veto) |
| `2023_Summer23` | `Summer23` | `Prompt23 RunC` (Cv123 and Cv4) |
| `2023_Summer23BPix` | `Summer23BPix` | `Prompt23 RunD` |
