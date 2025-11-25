import numpy
import awkward as ak
import warnings
from functools import reduce

from topcoffea.modules.JECStack import JECStack

_stack_parts = ["jec", "junc", "jer", "jersf"]
_MIN_JET_ENERGY = numpy.array(1e-2, dtype=numpy.float32)


def _random_gauss(counts, rng):
    total = int(ak.sum(counts))
    draws = rng.standard_normal(size=total).astype(numpy.float32)
    return ak.unflatten(draws, counts, axis=0)


def _ensure_jagged(arr, counts, total):
    array = arr if isinstance(arr, ak.Array) else ak.Array(arr)
    layout = ak.to_layout(array)
    if isinstance(layout, ak.contents.RegularArray):
        if len(array) == total:
            return ak.unflatten(array, counts, axis=0)
        if len(array) == len(counts):
            return ak.unflatten(array, counts, axis=0)
        return array
    if getattr(layout, "is_list", False):
        return array
    if len(array) == total:
        return ak.unflatten(array, counts, axis=0)
    if len(array) == len(counts):
        return ak.unflatten(array, counts, axis=0)
    return array


def jer_smear(
    variation,
    forceStochastic,
    pt_gen,
    jetPt,
    etaJet,
    jet_energy_resolution,
    jet_resolution_rand_gauss,
    jet_energy_resolution_scale_factor,
):
    if not isinstance(jetPt, ak.Array):
        raise Exception("'jetPt' must be an awkward array of some kind!")

    jetPt, pt_gen, etaJet, jet_energy_resolution, jet_resolution_rand_gauss = ak.broadcast_arrays(
        jetPt,
        ak.zeros_like(jetPt) if forceStochastic else pt_gen,
        etaJet,
        jet_energy_resolution,
        jet_resolution_rand_gauss,
    )

    jersf = ak.broadcast_arrays(jet_energy_resolution_scale_factor[..., variation], jetPt)[0]
    deltaPtRel = (jetPt - pt_gen) / jetPt
    doHybrid = (pt_gen > 0) & (numpy.abs(deltaPtRel) < 3 * jet_energy_resolution)
    detSmear = 1 + (jersf - 1) * deltaPtRel
    stochSmear = 1 + numpy.sqrt(numpy.maximum(jersf**2 - 1, 0)) * (jet_energy_resolution * jet_resolution_rand_gauss)

    min_jet_pt = _MIN_JET_ENERGY / numpy.cosh(etaJet)
    min_jet_pt_corr = min_jet_pt / jetPt
    smearfact = ak.where(doHybrid, detSmear, stochSmear)
    smearfact = ak.where((smearfact * jetPt) < min_jet_pt, min_jet_pt_corr, smearfact)
    return smearfact


def get_corr_inputs(jets, corr_obj, name_map, corrections=None):
    """Helper function for getting values of input variables."""

    input_values = []
    for inp in corr_obj.inputs:
        if inp.name == "systematic":
            continue
        if corrections is not None and inp.name == "JetPt":
            rawvar = ak.flatten(jets[name_map[inp.name]])
            input_values.append(corrections * rawvar)
        else:
            input_values.append(ak.flatten(jets[name_map[inp.name]]))
    return input_values


class CorrectedJetsFactory(object):
    def __init__(self, name_map, jec_stack):
        if not isinstance(jec_stack, JECStack):
            raise TypeError("jec_stack must be an instance of JECStack")

        self.tool = "clib" if jec_stack.use_clib else "jecstack"
        self.forceStochastic = False

        if "ptRaw" not in name_map or name_map["ptRaw"] is None:
            warnings.warn(
                "There is no name mapping for ptRaw," " CorrectedJets will assume that <object>.pt is raw pt!"
            )
            name_map["ptRaw"] = name_map["JetPt"] + "_raw"
        self.treat_pt_as_raw = "ptRaw" not in name_map

        if "massRaw" not in name_map or name_map["massRaw"] is None:
            warnings.warn(
                "There is no name mapping for massRaw," " CorrectedJets will assume that <object>.mass is raw mass!"
            )
            name_map["massRaw"] = name_map["JetMass"] + "_raw"

        self.jec_stack = jec_stack
        self.name_map = name_map
        self.real_sig = [v for v in name_map.values()]

        if self.jec_stack.use_clib:
            self.load_corrections_clib()
        else:
            self.load_corrections_jecstack()

        if "ptGenJet" not in name_map:
            warnings.warn(
                'Input JaggedCandidateArray must have "ptGenJet" in order to apply hybrid JER smearing method. Stochastic smearing will be applied.'
            )
            self.forceStochastic = True

    def load_corrections_clib(self):
        self.corrections = self.jec_stack.corrections

    def load_corrections_jecstack(self):
        self.corrections = self.jec_stack.corrections

        total_signature = set()
        for part in _stack_parts:
            attr = getattr(self.jec_stack, part)
            if attr is not None:
                total_signature.update(attr.signature)

        missing = total_signature - set(self.name_map.keys())
        if len(missing) > 0:
            raise Exception(
                f"Missing mapping of {missing} in name_map!" + " Cannot evaluate jet corrections!" + " Please supply mappings for these variables!"
            )

    def build(self, jets):
        jets = ak.Array(jets)

        fields = ak.fields(jets)
        if len(fields) == 0:
            raise Exception("Empty record, please pass a jet object with at least {self.real_sig} defined!")

        counts = ak.num(jets, axis=1)
        total_jets = int(ak.sum(counts))
        flat_jets = ak.flatten(jets)
        parameters = dict(ak.parameters(flat_jets) or {})
        parameters["corrected"] = True

        in_dict = {field: flat_jets[field] for field in fields}
        out_dict = dict(in_dict)

        out_dict[self.name_map["JetPt"] + "_orig"] = out_dict[self.name_map["JetPt"]]
        out_dict[self.name_map["JetMass"] + "_orig"] = out_dict[self.name_map["JetMass"]]
        if self.treat_pt_as_raw:
            out_dict[self.name_map["ptRaw"]] = out_dict[self.name_map["JetPt"]]
            out_dict[self.name_map["massRaw"]] = out_dict[self.name_map["JetMass"]]

        jec_name_map = dict(self.name_map)
        jec_name_map["JetPt"] = jec_name_map["ptRaw"]
        jec_name_map["JetMass"] = jec_name_map["massRaw"]

        total_correction = None
        if self.tool == "jecstack":
            if self.jec_stack.jec is not None:
                jec_args = {k: out_dict[jec_name_map[k]] for k in self.jec_stack.jec.signature}
                total_correction = ak.Array(self.jec_stack.jec.getCorrection(**jec_args))
            else:
                total_correction = ak.ones_like(out_dict[self.name_map["JetPt"]])

        elif self.tool == "clib":
            corrections_list = []
            for lvl in self.jec_stack.jec_names_clib:
                cumCorr = None
                if len(corrections_list) > 0:
                    ones = numpy.ones_like(corrections_list[-1], dtype=numpy.float32)
                    cumCorr = reduce(lambda x, y: y * x, corrections_list, ones).astype(dtype=numpy.float32)

                    # cmssw multiplies each successive correction by all previous corrections
                    # as part of the correction inputs
                sf = self.corrections.get(lvl, None)
                if sf is None:
                    raise ValueError(f"Correction {lvl} not found in self.corrections")

                inputs = get_corr_inputs(jets=jets, corr_obj=sf, name_map=jec_name_map, corrections=cumCorr)
                correction = sf.evaluate(*inputs).astype(dtype=numpy.float32)
                corrections_list.append(correction)
                if total_correction is None:
                    total_correction = numpy.ones_like(correction, dtype=numpy.float32)
                total_correction *= correction

                if self.jec_stack.savecorr:
                    jec_lvl_tag = "_jec_" + lvl

                    out_dict[f"jet_energy_correction_{lvl}"] = correction
                    out_dict[self.name_map["JetPt"] + f"_{lvl}"] = correction * out_dict[self.name_map["ptRaw"]]
                    out_dict[self.name_map["JetMass"] + f"_{lvl}"] = correction * out_dict[self.name_map["massRaw"]]

                    out_dict[self.name_map["JetPt"] + jec_lvl_tag] = out_dict[self.name_map["JetPt"] + f"_{lvl}"]
                    out_dict[self.name_map["JetMass"] + jec_lvl_tag] = out_dict[self.name_map["JetMass"] + f"_{lvl}"]

        out_dict["jet_energy_correction"] = total_correction

        out_dict[self.name_map["JetPt"]] = out_dict["jet_energy_correction"] * out_dict[self.name_map["ptRaw"]]
        out_dict[self.name_map["JetMass"]] = out_dict["jet_energy_correction"] * out_dict[self.name_map["massRaw"]]

        out_dict[self.name_map["JetPt"] + "_jec"] = out_dict[self.name_map["JetPt"]]
        out_dict[self.name_map["JetMass"] + "_jec"] = out_dict[self.name_map["JetMass"]]

        jagged_out = {key: _ensure_jagged(val, counts, total_jets) for key, val in out_dict.items()}

        has_jer = False
        if self.tool == "jecstack":
            if self.jec_stack.jer is not None and self.jec_stack.jersf is not None:
                has_jer = True
        elif self.tool == "clib":
            has_jer = len(self.jec_stack.jer_names_clib) > 0

        if has_jer:
            jer_name_map = dict(self.name_map)
            jer_name_map["JetPt"] = jer_name_map["JetPt"] + "_jec"
            jer_name_map["JetMass"] = jer_name_map["JetMass"] + "_jec"

            if self.tool == "jecstack":
                jer_args = {k: out_dict[jer_name_map[k]] for k in self.jec_stack.jer.signature}
                jet_energy_resolution = ak.Array(self.jec_stack.jer.getResolution(**jer_args))

                jersf_args = {k: out_dict[jer_name_map[k]] for k in self.jec_stack.jersf.signature}
                jet_energy_resolution_scale_factor = ak.Array(self.jec_stack.jersf.getScaleFactor(**jersf_args))

            elif self.tool == "clib":
                jer_out_parms = dict(parameters)
                jer_out_parms["corrected"] = True
                jer_out = ak.zip(jagged_out, depth_limit=1, parameters=jer_out_parms, behavior=flat_jets.behavior)
                jerjets = jer_out

                jet_energy_resolution = None
                jet_energy_resolution_scale_factor = None
                for jer_entry in self.jec_stack.jer_names_clib:
                    outtag = "jet_energy_resolution"
                    jer_entry = jer_entry.replace("SF", "ScaleFactor")
                    sf = self.corrections[jer_entry]
                    inputs = get_corr_inputs(jets=jerjets, corr_obj=sf, name_map=jer_name_map)
                    if "ScaleFactor" in jer_entry:
                        outtag += "_scale_factor"
                        nom = ak.values_astype(sf.evaluate(*inputs, "nom"), numpy.float32)
                        up = ak.values_astype(sf.evaluate(*inputs, "up"), numpy.float32)
                        down = ak.values_astype(sf.evaluate(*inputs, "down"), numpy.float32)
                        stacked = ak.concatenate([nom[..., None], up[..., None], down[..., None]], axis=-1)
                        correction = ak.Array(stacked)
                        jet_energy_resolution_scale_factor = correction
                    else:
                        correction = ak.values_astype(sf.evaluate(*inputs), numpy.float32)
                        jet_energy_resolution = ak.Array(correction)
                if jet_energy_resolution is None:
                    jet_energy_resolution = ak.zeros_like(jagged_out[jer_name_map["JetPt"]])
                if jet_energy_resolution_scale_factor is None:
                    jet_energy_resolution_scale_factor = ak.ones_like(jagged_out[jer_name_map["JetPt"]])[..., None]

            jet_energy_resolution = _ensure_jagged(jet_energy_resolution, counts, total_jets)
            jet_energy_resolution_scale_factor = _ensure_jagged(jet_energy_resolution_scale_factor, counts, total_jets)

            rng = numpy.random.default_rng()
            jet_resolution_rand_gauss = _random_gauss(counts, rng)

            jer_correction = jer_smear(
                variation=0,
                forceStochastic=self.forceStochastic,
                pt_gen=ak.values_astype(_ensure_jagged(out_dict[jer_name_map.get("ptGenJet", "ptGenJet")], counts, total_jets), numpy.float32),
                jetPt=ak.values_astype(jagged_out[jer_name_map["JetPt"]], numpy.float32),
                etaJet=ak.values_astype(jagged_out[jer_name_map["JetEta"]], numpy.float32),
                jet_energy_resolution=ak.values_astype(jet_energy_resolution, numpy.float32),
                jet_resolution_rand_gauss=ak.values_astype(jet_resolution_rand_gauss, numpy.float32),
                jet_energy_resolution_scale_factor=ak.values_astype(jet_energy_resolution_scale_factor, numpy.float32),
            )

            jagged_out["jet_energy_resolution"] = jet_energy_resolution
            jagged_out["jet_energy_resolution_scale_factor"] = jet_energy_resolution_scale_factor
            jagged_out["jet_resolution_rand_gauss"] = jet_resolution_rand_gauss
            jagged_out["jet_energy_resolution_correction"] = jer_correction

            jagged_out[self.name_map["JetPt"]] = jer_correction * jagged_out[jer_name_map["JetPt"]]
            jagged_out[self.name_map["JetMass"]] = jer_correction * jagged_out[jer_name_map["JetMass"]]

            jagged_out[self.name_map["JetPt"] + "_jer"] = jagged_out[self.name_map["JetPt"]]
            jagged_out[self.name_map["JetMass"] + "_jer"] = jagged_out[self.name_map["JetMass"]]

            def build_jer_variant(variation_index):
                correction = jer_smear(
                    variation=variation_index,
                    forceStochastic=self.forceStochastic,
                    pt_gen=ak.values_astype(_ensure_jagged(out_dict[jer_name_map.get("ptGenJet", "ptGenJet")], counts, total_jets), numpy.float32),
                    jetPt=ak.values_astype(jagged_out[jer_name_map["JetPt"]], numpy.float32),
                    etaJet=ak.values_astype(jagged_out[jer_name_map["JetEta"]], numpy.float32),
                    jet_energy_resolution=ak.values_astype(jet_energy_resolution, numpy.float32),
                    jet_resolution_rand_gauss=ak.values_astype(jet_resolution_rand_gauss, numpy.float32),
                    jet_energy_resolution_scale_factor=ak.values_astype(jet_energy_resolution_scale_factor, numpy.float32),
                )

                var_dict = {field: jagged_out[field] for field in in_dict}
                var_dict[self.name_map["JetPt"]] = correction * jagged_out[jer_name_map["JetPt"]]
                var_dict[self.name_map["JetMass"]] = correction * jagged_out[jer_name_map["JetMass"]]
                return ak.zip(var_dict, depth_limit=1, parameters=parameters, behavior=flat_jets.behavior)

            jagged_out["JER"] = ak.zip({"up": build_jer_variant(1), "down": build_jer_variant(2)}, depth_limit=1, with_name="JetSystematic")

        has_junc = self.jec_stack.junc is not None
        if self.tool == "clib":
            has_junc = len(self.jec_stack.jec_uncsources_clib) > 0

        if has_junc:
            junc_name_map = dict(self.name_map)
            if has_jer:
                junc_name_map["JetPt"] = junc_name_map["JetPt"] + "_jer"
                junc_name_map["JetMass"] = junc_name_map["JetMass"] + "_jer"
            else:
                junc_name_map["JetPt"] = junc_name_map["JetPt"] + "_jec"
                junc_name_map["JetMass"] = junc_name_map["JetMass"] + "_jec"

            if self.tool == "jecstack":
                junc_args = {k: ak.flatten(jagged_out[junc_name_map[k]]) for k in self.jec_stack.junc.signature}
                juncs = self.jec_stack.junc.getUncertainty(**junc_args)

            elif self.tool == "clib":
                junc_out_parms = dict(parameters)
                junc_out_parms["corrected"] = True
                junc_out = ak.zip(jagged_out, depth_limit=1, parameters=junc_out_parms, behavior=flat_jets.behavior)
                juncjets = junc_out

                uncnames, uncvalues = [], []
                for junc_name in self.jec_stack.jec_uncsources_clib:
                    sf = self.corrections[junc_name]
                    if sf is None:
                        raise ValueError(f"Correction {junc_name} not found in self.corrections")

                    inputs = get_corr_inputs(jets=juncjets, corr_obj=sf, name_map=junc_name_map)
                    unc = ak.values_astype(sf.evaluate(*inputs), numpy.float32)
                    central = ak.ones_like(jagged_out[self.name_map["JetPt"]])
                    unc_up = central + _ensure_jagged(unc, counts, total_jets)
                    unc_down = central - _ensure_jagged(unc, counts, total_jets)
                    uncnames.append(junc_name.split("_")[-2])
                    uncvalues.append(ak.concatenate([unc_up[..., None], unc_down[..., None]], axis=-1))
                del juncjets

                juncs = zip(uncnames, uncvalues)

            def build_variation(unc, jetpt, jetpt_orig, jetmass, jetmass_orig, updown):
                factor = unc[..., updown]
                var_dict = {field: jagged_out[field] for field in in_dict}
                var_dict[jetpt] = factor * jetpt_orig
                var_dict[jetmass] = factor * jetmass_orig
                return ak.zip(var_dict, depth_limit=1, parameters=parameters, behavior=flat_jets.behavior)

            def build_variant(unc, jetpt, jetpt_orig, jetmass, jetmass_orig):
                up = build_variation(unc, jetpt, jetpt_orig, jetmass, jetmass_orig, 0)
                down = build_variation(unc, jetpt, jetpt_orig, jetmass, jetmass_orig, 1)
                return ak.zip({"up": up, "down": down}, depth_limit=1, with_name="JetSystematic")

            template_pt = jagged_out[junc_name_map["JetPt"]]
            template_mass = jagged_out[junc_name_map["JetMass"]]
            for name, func in juncs:
                jagged_unc = _ensure_jagged(func, counts, total_jets)
                jagged_out[f"jet_energy_uncertainty_{name}"] = jagged_unc
                jagged_out[f"JES_{name}"] = build_variant(
                    jagged_unc,
                    self.name_map["JetPt"],
                    template_pt,
                    self.name_map["JetMass"],
                    template_mass,
                )

        return ak.zip(jagged_out, depth_limit=1, parameters=parameters, behavior=flat_jets.behavior)
