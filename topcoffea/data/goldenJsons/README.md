# Certified luminosity JSON text files (goldenJsons)

## Usage guidance

For how these payloads fit into the broader corrections setup, see
[`docs/configuration.md`](../../../docs/configuration.md#payloads-and-corrections-overview).
For the full documentation map, see
[`docs/index.md`](../../../docs/index.md).

## Provenance

Reference TWikis:

- Run 2 luminosity certification:
  https://twiki.cern.ch/twiki/bin/view/CMS/TWikiLUM
- Run 3 certification and luminosity context:
  https://twiki.cern.ch/twiki/bin/viewauth/CMS/PdmVRun3Analysis
- Top-systematics luminosity summary:
  https://twiki.cern.ch/twiki/bin/viewauth/CMS/TopSystematics#Luminosity

### Run 2 copy sources

```text
/afs/cern.ch/cms/CAF/CMSCOMM/COMM_DQM/certification/Collisions16/13TeV/Legacy_2016/Cert_271036-284044_13TeV_Legacy2016_Collisions16_JSON.txt
/afs/cern.ch/cms/CAF/CMSCOMM/COMM_DQM/certification/Collisions17/13TeV/Legacy_2017/Cert_294927-306462_13TeV_UL2017_Collisions17_GoldenJSON.txt
/afs/cern.ch/cms/CAF/CMSCOMM/COMM_DQM/certification/Collisions18/13TeV/Legacy_2018/Cert_314472-325175_13TeV_Legacy2018_Collisions18_JSON.txt
```

### Run 3 copy sources

```text
/afs/cern.ch/cms/CAF/certification/Collisions22/Cert_Collisions2022_355100_362760_Golden.json
/afs/cern.ch/cms/CAF/certification/Collisions23/Cert_Collisions2023_366442_370790_Golden.json
```

Run 3 JSON copies are stored in this directory as `.txt` files.

## What's in here

- Run 2 certified JSON text files for 2016/2017/2018
- Run 3 certified JSON text files for 2022/2023

The integrated luminosity values consumed in `topcoffea/json/lumi.json` are
tracked from the TWiki sources above.
