import numpy as np
import awkward as ak
np.seterr(divide='ignore', invalid='ignore', over='ignore')

from coffea import hist, processor

from topcoffea.modules.HistEFT import HistEFT
import topcoffea.modules.eft_helper as efth


class AnalysisProcessor(processor.ProcessorABC):

    def __init__(self, samples, wc_names_lst=[], do_errors=False, dtype=np.float32):

        self._samples = samples
        self._wc_names_lst = wc_names_lst
        self._dtype = dtype
        self._do_errors = do_errors # Whether to calculate and store the w**2 coefficients

        # Create the histogram
        self._accumulator = processor.dict_accumulator({
            "SumOfWeights": HistEFT("SumOfWeights", wc_names_lst, hist.Cat("sample", "sample"), hist.Bin("SumOfWeights", "sow", 1, 0, 2))
        })

    @property
    def accumulator(self):
        return self._accumulator

    @property
    def columns(self):
        return self._columns

    # Main function: run on a given dataset
    def process(self, events):

        # Dataset parameters
        dataset = events.metadata["dataset"]    # This should be the name of the .json file (without the .json part)
        isData  = self._samples[dataset]["isData"]

        eft_coeffs = None
        eft_w2_coeffs = None
        
        if hasattr(events, "EFTfitCoefficients"):
            eft_coeffs = ak.to_numpy(events["EFTfitCoefficients"])
            # Check to see if the ordering of WCs for this sample matches what want
            if self._samples[dataset]["WCnames"] != self._wc_names_lst:
                eft_coeffs = efth.remap_coeffs(self._samples[dataset]["WCnames"], self._wc_names_lst, eft_coeffs)
            if self._do_errors:
                eft_w2_coeffs = efth.calc_w2_coeffs(eft_coeffs,self._dtype)
    
        counts = np.ones_like(events['event'])
        wgts = np.ones_like(events['event'])

        if not isData and eft_coeffs is None:
            # Basically any central MC samples
            wgts = events["genWeight"]

        hout = self.accumulator.identity()
        hout["SumOfWeights"].fill(sample=dataset, SumOfWeights=counts, weight=wgts, eft_coeff=eft_coeffs, eft_err_coeff=eft_w2_coeffs)

        return hout

    def postprocess(self, accumulator):
        return accumulator