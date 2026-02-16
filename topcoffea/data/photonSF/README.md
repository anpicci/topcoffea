# Photon scale-factor inputs (photonSF)

## Usage guidance

For how these payloads fit into the broader corrections setup, see
[`docs/configuration.md`](../../../docs/configuration.md#payloads-and-corrections-overview).
For the full documentation map, see
[`docs/index.md`](../../../docs/index.md).

## Provenance

Photon SF recommendations come from EGM TWiki pages:

- 2016 preVFP:
  https://twiki.cern.ch/twiki/bin/view/CMS/EgammaUL2016To2018#SFs_for_Photons_UL_2016_preVFP
- 2016 postVFP:
  https://twiki.cern.ch/twiki/bin/view/CMS/EgammaUL2016To2018#SFs_for_Photons_UL_2016_postVFP
- 2017:
  https://twiki.cern.ch/twiki/bin/view/CMS/EgammaUL2016To2018#SFs_for_Photons_UL_2017
- 2018:
  https://twiki.cern.ch/twiki/bin/view/CMS/EgammaUL2016To2018#SFs_for_Photons_UL_2018

## What's in here

### 2016APV

- Medium ID: `egammaEffi_EGM2D_Pho_Medium_UL16.root`
- Tight ID: `egammaEffi_EGM2D_Pho_Tight_UL16.root`
- Conversion-safe electron veto: `CSEV_SummaryPlot_UL16_preVFP.root`
- Pixel veto: `HasPix_SummaryPlot_UL16_preVFP.root`

### 2016

- Medium ID: `egammaEffi_EGM2D_Pho_Medium_UL16_postVFP.root`
- Tight ID: `egammaEffi_EGM2D_Pho_Tight_UL16_postVFP.root`
- Conversion-safe electron veto: `CSEV_SummaryPlot_UL16_postVFP.root`
- Pixel veto: `HasPix_SummaryPlot_UL16_postVFP.root`

### 2017

- Medium ID: `egammaEffi_EGM2D_PHO_Medium_UL17.root`
- Tight ID: `egammaEffi_EGM2D_PHO_Tight_UL17.root`
- Conversion-safe electron veto: `CSEV_SummaryPlot_UL17.root`
- Pixel veto: `HasPix_SummaryPlot_UL17.root`

### 2018

- Medium ID: `egammaEffi_EGM2D_Pho_Med_UL18.root`
- Tight ID: `egammaEffi_EGM2D_Pho_Tight_UL18.root`
- Conversion-safe electron veto: `CSEV_SummaryPlot_UL18.root`
- Pixel veto: `HasPix_SummaryPlot_UL18.root`

## Helper script

`add_err.py` adds error histograms (`EGamma_SF2D_err`) by reading variances from
EGM POG SF histograms (`EGamma_SF2D`) and writing them to a second histogram.
