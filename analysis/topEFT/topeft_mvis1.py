#!/usr/bin/env python
import lz4.frame as lz4f
import cloudpickle
import json
import pprint
import copy
import coffea
import numpy as np
import awkward as ak
from coffea import hist, processor
from coffea.util import load, save
from optparse import OptionParser
from coffea.analysis_tools import PackedSelection
from coffea.lumi_tools import LumiMask
import numba
import random 


from topcoffea.modules.GetValuesFromJsons import get_param, get_lumi
from topcoffea.modules.objects import *
from topcoffea.modules.corrections import SFevaluator, GetBTagSF, ApplyJetCorrections, GetBtagEff, AttachMuonSF, AttachElectronSF, AttachPerLeptonFR, GetPUSF, ApplyRochesterCorrections, ApplyJetSystematics, AttachPSWeights, AttachPdfWeights, AttachScaleWeights, GetTriggerSF, AttachTauSF, ApplyTES
from topcoffea.modules.selection import *
from topcoffea.modules.HistEFT import HistEFT
from topcoffea.modules.paths import topcoffea_path
import topcoffea.modules.eft_helper as efth


# Takes strings as inputs, constructs a string for the full channel name
# Try to construct a channel name like this: [n leptons]_[lepton flavors]_[p or m charge]_[on or off Z]_[n b jets]_[n jets]
    # chan_str should look something like "3l_p_offZ_1b", NOTE: This function assumes nlep comes first
    # njet_str should look something like "atleast_5j",   NOTE: This function assumes njets comes last
    # flav_str should look something like "emm"
def construct_cat_name(chan_str,njet_str=None,flav_str=None):

    # Get the component strings
    nlep_str = chan_str.split("_")[0] # Assumes n leps comes first in the str
    chan_str = "_".join(chan_str.split("_")[1:]) # The rest of the channel name is everything that comes after nlep
    if chan_str == "": chan_str = None # So that we properly skip this in the for loop below
    if flav_str is not None:
        flav_str = flav_str
    if njet_str is not None:
        njet_str = njet_str[-2:] # Assumes number of n jets comes at the end of the string
        if "j" not in njet_str:
            # The njet string should really have a "j" in it
            raise Exception(f"Something when wrong while trying to consturct channel name, is \"{njet_str}\" an njet string?")

    # Put the component strings into the channel name
    ret_str = nlep_str
    for component in [flav_str,chan_str,njet_str]:
        if component is None: continue
        ret_str = "_".join([ret_str,component])
    return ret_str

boson_dict = {
    9: "Gluon",
    21: "Gluon",
    22: "Photon",
    23: "Z",
    24: "W",
    25: "Higgs"
}

def trace_boson_ancestor(pdgId, motherIdx, genparticles):
    while motherIdx >= 0:
        pdgId = abs(genparticles[motherIdx].pdgId)
        if pdgId in boson_dict:
            return boson_dict[pdgId], motherIdx
        motherIdx = genparticles[motherIdx].genPartIdxMother
    return "Unknown", -1


def find_boson_ancestors_for_event(genparticles_for_event):
    # Initialize an array of "None" strings with the same length as genparticles_for_event
    ancestors = ak.Array(["None"] * len(genparticles_for_event))

    masks = []
    values = []

    # Loop over each genparticle in the event
    for idx, particle in enumerate(genparticles_for_event):
        mother_idx = particle.genPartIdxMother
        while mother_idx >= 0:
            mother = genparticles_for_event[mother_idx]
            if abs(mother.pdgId) in boson_dict:
                mask = (ak.from_iter(range(len(ancestors))) == idx)
                masks.append(mask)
                values.append(boson_dict[abs(mother.pdgId)])
                break
            mother_idx = mother.genPartIdxMother

    # After the loop
    combined_mask = ak.concatenate(masks, axis=1)
    combined_values = ak.concatenate(values, axis=0)
    ancestors = ak.where(combined_mask, combined_values, ancestors)

    return ancestors

def deltaR(eta1, phi1, eta2, phi2):
    deta = eta1 - eta2
    dphi = np.remainder(phi1 - phi2 + np.pi, 2*np.pi) - np.pi
    return np.sqrt(deta**2 + dphi**2)

def trace_ancestors(particle, genparticles):
    ancestors = []
    while particle.genPartIdxMother > 0:
        mother = genparticles[particle.genPartIdxMother]
        ancestors.append(particle.genPartIdxMother)
        particle = mother
    return ancestors

def GenLeptonTauPairs(genparticles, electrons, muons, taus, depth=20):
    tele_mask = (electrons.genPartFlav == 15) | (electrons.genPartFlav == 1)
    tmu_mask = (muons.genPartFlav == 15) | (muons.genPartFlav == 1)
    htau_mask = taus.genPartFlav == 5

    gens_electrons = electrons.matched_gen
    gens_muons = muons.matched_gen
    gentau_mask = abs(genparticles.pdgId)==15
    gens_taus = genparticles[gentau_mask]
    gele_mask = abs(gens_taus.distinctChildren.pdgId)==11
    gmu_mask = abs(gens_taus.distinctChildren.pdgId)==13
    lglep_mask = gele_mask | gmu_mask
    lglep_mask = ~ak.any(lglep_mask, axis=-1)
    gens_taus = gens_taus[lglep_mask]

    dP_gens_electrons = gens_electrons.distinctParent
    dP_gens_muons = gens_muons.distinctParent                                                                      
    dP_gens_taus = gens_taus.distinctParent

    comm_ele_mask = ak.zeros_like(electrons.pt, dtype=bool)
    comm_mu_mask = ak.zeros_like(muons.pt, dtype=bool)
    comm_tau_mask = ak.zeros_like(taus.pt, dtype=bool)

    ele_anc = ak.zeros_like(electrons.pt)
    print("ele_anc", ak.to_list(ele_anc))

    ### INSERIRE UNA MASK PER VETARE LE COPPIE DI LEP E TAU CHE IN REALTA SONO LA STESSA PARTICELLA
    for lev in range(depth):
        ele_anc = ak.concatenate([ele_anc, dP_gens_electrons.pdgId], axis=-1)
        ele_anc_mask = ~ak.is_none(ele_anc, axis=-1)
        ele_anc = ele_anc[ele_anc_mask]
        dP_etau_pairs = ak.cartesian({"a": dP_gens_electrons, "b": dP_gens_taus})                                  
        dP_etau_pairs_idx = ak.argcartesian({"a": dP_gens_electrons, "b": dP_gens_taus})                         
        dP_mutau_pairs = ak.cartesian({"a": dP_gens_muons, "b": dP_gens_taus})                                  
        dP_mutau_pairs_idx = ak.argcartesian({"a": dP_gens_muons, "b": dP_gens_taus})                              
        dP_etau_same_objects = (dP_etau_pairs["a"].pdgId == dP_etau_pairs["b"].pdgId) & (dP_etau_pairs["a"].pt == dP_etau_pairs["b"].pt)
        dP_mutau_same_objects = (dP_mutau_pairs["a"].pdgId == dP_mutau_pairs["b"].pdgId) & (dP_mutau_pairs["a"].pt == dP_mutau_pairs["b"].pt)
        dP_etau_same_objects = ak.fill_none(dP_etau_same_objects, False)                                         
        dP_mutau_same_objects = ak.fill_none(dP_mutau_same_objects, False)                    
        
        # Transform them in arrays to avoid bad::alloc problems
        dP_etau_same_objects_array = ak.Array(dP_etau_same_objects)
        dP_mutau_same_objects_array = ak.Array(dP_mutau_same_objects)
        ##dP_etau_same_objects = ak.fill_none(ak.pad_none(dP_etau_same_objects, target=100, axis=1, clip=True), False)
        ##dP_mutau_same_objects = ak.fill_none(ak.pad_none(dP_mutau_same_objects, target=100, axis=1, clip=True), False)

        # Create a replacement array of the same shape as dP_etau_pairs_idx but with all values set to (-1, -1)
        replacement_etau_array, dP_etau_pairs_idx = ak.broadcast_arrays({"a": -1, "b": -1}, dP_etau_pairs_idx)
        replacement_mutau_array, dP_mutau_pairs_idx = ak.broadcast_arrays({"a": -1, "b": -1}, dP_mutau_pairs_idx)
        
        # Use ak.where to get the desired result, corresponding to ak.where but giving -1 when the condition is False
        result_etau = ak.where(dP_etau_same_objects_array, dP_etau_pairs_idx, replacement_etau_array)
        result_mutau = ak.where(dP_mutau_same_objects_array, dP_mutau_pairs_idx, replacement_mutau_array)

        ele_indices = result_etau['a']
        mu_indices = result_mutau['a']
        tau_eindices = result_etau['b']
        tau_mindices = result_mutau['b']



        # Create masks of the same shape as electrons and taus with all False values                     
        ele_mask = ak.zeros_like(electrons.pt, dtype=bool)
        mu_mask = ak.zeros_like(muons.pt, dtype=bool)
        tau_emask = ak.zeros_like(taus.pt, dtype=bool)
        tau_mmask = ak.zeros_like(taus.pt, dtype=bool)
        tau_mask = ak.zeros_like(taus.pt, dtype=bool)

        # Identify subarrays with only -1        # Extract the maximum value from each subarray        #Replace subarrays with only -1 with a single -1 and replace other subarrays with their max value

        ele_indices = ak.to_list(ak.fill_none(ak.where(ak.all(ele_indices == -1, axis=1), -1, ak.max(ele_indices, axis=1)), -1))
        mu_indices = ak.to_list(ak.fill_none(ak.where(ak.all(mu_indices == -1, axis=1), -1, ak.max(mu_indices, axis=1)), -1))
        tau_eindices = ak.to_list(ak.fill_none(ak.where(ak.all(tau_eindices == -1, axis=1), -1, ak.max(tau_eindices, axis=1)), -1))
        tau_mindices = ak.to_list(ak.fill_none(ak.where(ak.all(tau_mindices == -1, axis=1), -1, ak.max(tau_mindices, axis=1)), -1))

        ##print("ele_indices, tau_eindices", ele_indices, tau_eindices)#, ak.type(ele_indices), ak.type(tau_eindices))
        ##print("mu_indices, tau_mindices", mu_indices, tau_mindices)#, ak.type(mu_indices), ak.type(tau_mindices))
        ##print("\n\n\n\n\n")

        # Set True values at the indices with common objects                 
        ele_mask = ak.local_index(ele_mask) == ele_indices               
        tau_emask = ak.local_index(tau_emask) == tau_eindices            
        mu_mask = ak.local_index(mu_mask) == mu_indices                  
        tau_mmask = ak.local_index(tau_mmask) == tau_mindices  
        
        # OR the masks for taus
        tau_mask = tau_emask | tau_mmask 
        
        # Updating global masks
        comm_ele_mask = comm_ele_mask | ele_mask
        comm_mu_mask = comm_mu_mask | mu_mask
        comm_tau_mask = comm_tau_mask | tau_mask
        
        # Preparing for next depth level in GenParts
        dP_gens_electrons = dP_gens_electrons.distinctParent
        dP_gens_muons = dP_gens_muons.distinctParent
        dP_gens_taus = dP_gens_taus.distinctParent


    #print("comm_ele_mask", comm_ele_mask, ak.any(comm_ele_mask, axis=1), ak.any(comm_ele_mask))
    #print("comm_mu_mask", comm_mu_mask, ak.any(comm_mu_mask, axis=1), ak.any(comm_mu_mask))
    #print("comm_tau_mask", comm_tau_mask, ak.any(comm_tau_mask, axis=1), ak.any(comm_tau_mask))

    ##anyele = ak.any(comm_ele_mask, axis=1)
    ##anymu = ak.any(comm_mu_mask, axis=1)
    
    comm_tau_mask = comm_tau_mask #& htau_mask #& (anyele | anymu)   
    #anytau = ak.any(comm_tau_mask, axis=1)
    #print("comm_ele_mask", comm_ele_mask, ak.any(comm_ele_mask, axis=1), ak.any(comm_ele_mask))
    #print("comm_mu_mask", comm_mu_mask, ak.any(comm_mu_mask, axis=1), ak.any(comm_mu_mask))
    #print("comm_tau_mask", comm_tau_mask, ak.any(comm_tau_mask, axis=1), ak.any(comm_tau_mask))

    comm_ele_mask = comm_ele_mask & tele_mask #& anytau
    comm_mu_mask = comm_mu_mask & tmu_mask #& anytau

    ##print("comm_ele_mask", comm_ele_mask, ak.any(comm_ele_mask, axis=1), ak.any(ak.any(comm_ele_mask, axis=1)))
    ##print("comm_mu_mask", comm_mu_mask, ak.any(comm_mu_mask, axis=1), ak.any(ak.any(comm_mu_mask, axis=1)))
    ##print("comm_tau_mask", comm_tau_mask, ak.any(comm_tau_mask, axis=1), ak.any(ak.any(comm_tau_mask, axis=1)))

    # Add the masks as new fields to electrons and taus
    electrons["isTauPaired"] = comm_ele_mask #& tele_mask
    muons["isTauPaired"] = comm_mu_mask #& tmu_mask
    taus["isLeptonPaired"] = comm_tau_mask #& htau_mask
    

    
def find_lepton_tau_pairs(ev_genparticles, leptons, taus):
    # Define masks for leptons based on genPartFlav
    lepton_mask = (leptons.genPartFlav == 15) | (leptons.genPartFlav == 1)
    
    # Filter leptons using the mask
    selected_leptons = leptons[lepton_mask]

    # Get associated GenPart for these leptons
    associated_genparts = ev_genparticles[selected_leptons.genPartIdx]

    # Define mask for ev_genparticles to identify taus
    gen_tau_mask = abs(ev_genparticles.pdgId) == 15
    
    # Filter ev_genparticles using the tau mask
    gen_taus = ev_genparticles[gen_tau_mask]

    # Filter taus with genPartFlav == 5
    taus = taus[taus.genPartFlav == 5]

    lepton_tau_pairs = None
    genmatched_lepton = ak.Array([[]])
    genmatched_tau = ak.Array([[]])
    lepton_tau_masses = [-25.]
    common_ancestors_list = []

    # Find common ancestors for each lepton and check if there's a tau with the same ancestor
    for idx, lepton in enumerate(associated_genparts):

        lepton_ancestors = trace_ancestors(lepton, ev_genparticles)
        
        # Create a mask for gen_taus that have common ancestors with the lepton
        common_ancestor_mask = [any(ancestor in lepton_ancestors for ancestor in trace_ancestors(gen_tau, ev_genparticles)) for gen_tau in gen_taus]
        
        # Filter gen_taus using the common_ancestor_mask
        common_gen_taus = gen_taus[common_ancestor_mask]
        
        if ak.any(common_gen_taus):
            # Calculate deltaR for each tau in taus to common_gen_taus, because genPartIdx does not work for hadronic taus
            dRs = deltaR(taus.eta, taus.phi, common_gen_taus.eta[0], common_gen_taus.phi[0])

            # Check if dRs is not empty
            if len(dRs) > 0:
                # Find the index of the tau with the smallest deltaR
                closest_tau_idx = np.argmin(dRs)

                # Select the closest tau
                closest_tau = taus[closest_tau_idx]

                # Fetch the corresponding lepton object from the 'leptons' collection using the index
                corresponding_lepton = selected_leptons[idx]
                
                lepton_tau_dR = deltaR(corresponding_lepton.eta, corresponding_lepton.phi, closest_tau.eta, closest_tau.phi)

                lepton_tau = corresponding_lepton + closest_tau
                mvis = lepton_tau.mass                

                if lepton_tau_pairs is None:
                    lepton_tau_pairs = []

                if lepton_tau_masses == [-25.]:
                    lepton_tau_masses = []

                lepton_tau_pairs.append([corresponding_lepton, closest_tau, lepton_tau_dR])
                lepton_tau_masses.append(mvis)

                # Find the common ancestors between the lepton and the gen_tau
                tau_ancestors = trace_ancestors(common_gen_taus[0], ev_genparticles)
                actual_common_ancestors = list(set(lepton_ancestors) & set(tau_ancestors))
            
                # Append the common ancestors to the list
                common_ancestors_list.append(actual_common_ancestors)
    
    max_ancestor_idx = None
    
    if lepton_tau_pairs is not None and len(lepton_tau_pairs) >= 1:
        # Find the index of the pair with the highest value of the first common ancestor
        max_ancestor_idx = np.argmax([ancestors[0] if ancestors else -np.inf for ancestors in common_ancestors_list])
        # Retain only the pair with the highest value of the first common ancestor
        genmatched_lepton = ak.Array([[lepton_tau_pairs[max_ancestor_idx][0]]])
        genmatched_tau = ak.Array([[lepton_tau_pairs[max_ancestor_idx][1]]])
        lepton_tau_masses = ak.Array([lepton_tau_masses[max_ancestor_idx]])

        ##common_ancestors_list = [common_ancestors_list[max_ancestor_idx]]

    ##return lepton_tau_pairs, lepton_tau_masses
    return genmatched_lepton, genmatched_tau, lepton_tau_masses

def get_closest_taus_for_all_events(genparticles, leptons, taus):
    ##closest_taus = []
    genmatched_leps = ak.Array([])
    genmatched_taus = ak.Array([])
    mvis_values = ak.Array([])

    for i, ev_genparticles in enumerate(genparticles):
        ##closest_tau, mvis = find_lepton_tau_pairs(ev_genparticles, leptons[i], taus[i])
        genmatched_lep, genmatched_tau, mvis = find_lepton_tau_pairs(ev_genparticles, leptons[i], taus[i])
        
        ##closest_taus.append(closest_tau)
        ##genmatched_leps.append(genmatched_lep)
        genmatched_leps = ak.concatenate([genmatched_leps, genmatched_lep], axis=0)
        genmatched_taus = ak.concatenate([genmatched_taus, genmatched_tau], axis=0)
        ##genmatched_taus.append(genmatched_tau)
        ##mvis_values.append(mvis)
        mvis_values = ak.concatenate([mvis_values, mvis], axis=0)
    
    ##return ak.Array(genmatched_leps), ak.Array(genmatched_taus), ak.Array(mvis_values)
    return genmatched_leps, genmatched_taus, mvis_values


def calculate_M1T(reco_obj0, reco_obj1, met):
    # Convert the reconstructed objects to PtEtaPhiMLorentzVector structure
    reco_obj0_vector = ak.zip({
        "pt": reco_obj0.pt,
        "eta": reco_obj0.eta,
        "phi": reco_obj0.phi,
        "mass": reco_obj0.mass if hasattr(reco_obj0, 'mass') else ak.zeros_like(reco_obj0.pt),
    }, with_name="PtEtaPhiMLorentzVector")
    
    reco_obj1_vector = ak.zip({
        "pt": reco_obj1.pt,
        "eta": reco_obj1.eta,
        "phi": reco_obj1.phi,
        "mass": reco_obj1.mass if hasattr(reco_obj1, 'mass') else ak.zeros_like(reco_obj1.pt),
    }, with_name="PtEtaPhiMLorentzVector")
    
    # Combine the two reconstructed objects into a single system
    visible_system = reco_obj0_vector + reco_obj1_vector

    # Compute the transverse energy for each object
    ET_reco_obj0 = np.sqrt(reco_obj0_vector.pt**2 + reco_obj0_vector.mass**2)
    ET_reco_obj1 = np.sqrt(reco_obj1_vector.pt**2 + reco_obj1_vector.mass**2)
    
    # Compute the combined transverse energy
    #ET_visible_system = ET_reco_obj0 + ET_reco_obj1
    ET_visible_system = np.sqrt(visible_system.pt**2 + visible_system.mass**2)
    
    # Compute the combined transverse momentum components for the visible system
    px_visible_system = visible_system.pt * np.cos(visible_system.phi)
    py_visible_system = visible_system.pt * np.sin(visible_system.phi)

    met_px = met.pt*np.cos(met.phi)
    met_py = met.pt*np.sin(met.phi)

    #ET_tot = (ET_visible_system + met.pt)**2
    #pT_tot = (px_visible_system + met.pt*np.cos(met.phi))**2 + (py_visible_system + met.pt*np.sin(met.phi))**2
    #px_tot = (px_visible_system + met.pt*np.cos(met.phi))
    #py_tot = (py_visible_system + met.pt*np.sin(met.phi))
    
    # Compute the M1T using the combined visible system and MET
    M1T_squared = (ET_visible_system + met.pt)**2 - (px_visible_system + met_px)**2 - (py_visible_system + met_py)**2
    M1T = np.sqrt(M1T_squared)

    '''
    with open("object_attributes.txt", "w") as file:
        for idx in range(len(reco_obj0_vector.pt)):
            file.write(f"Event Index: {idx}\n")
            file.write(f"reco_obj0_vector px: {reco_obj0_vector.px[idx]}\n")
            file.write(f"reco_obj0_vector py: {reco_obj0_vector.py[idx]}\n")
            file.write(f"reco_obj0_vector pz: {reco_obj0_vector.pz[idx]}\n")
            file.write(f"reco_obj0_vector Energy: {reco_obj0_vector.energy[idx]}\n")
            file.write(f"reco_obj0_vector Mass: {reco_obj0_vector.mass[idx]}\n")
            file.write(f"reco_obj1_vector px: {reco_obj1_vector.px[idx]}\n")
            file.write(f"reco_obj1_vector py: {reco_obj1_vector.py[idx]}\n")
            file.write(f"reco_obj1_vector pz: {reco_obj1_vector.pz[idx]}\n")
            file.write(f"reco_obj1_vector Energy: {reco_obj1_vector.energy[idx]}\n")
            file.write(f"reco_obj1_vector Mass: {reco_obj1_vector.mass[idx]}\n")
            file.write(f"visible_system pt: {visible_system.pt[idx]}\n")
            file.write(f"visible_system px: {visible_system.px[idx]}\n")
            file.write(f"visible_system py: {visible_system.py[idx]}\n")
            file.write(f"visible_system pz: {visible_system.pz[idx]}\n")
            file.write(f"visible_system Energy: {visible_system.energy[idx]}\n")
            file.write(f"visible_system Mass: {visible_system.mass[idx]}\n")
            file.write(f"MET px: {met.pt[idx] * np.cos(met.phi[idx])}\n")
            file.write(f"MET py: {met.pt[idx] * np.sin(met.phi[idx])}\n")
            file.write(f"MET pt: {met.pt[idx]}\n")
            file.write(f"MET phi: {met.phi[idx]}\n")
            file.write(f"ET tot: {ET_tot[idx]}\n")
            file.write(f"pT tot: {pT_tot[idx]}\n")
            file.write(f"px tot: {px_tot[idx]}\n")
            file.write(f"py tot: {py_tot[idx]}\n")
            file.write("-----\n")
            if idx > 99:
                break
    '''

    return M1T



def calculate_Mo1(reco_obj0, reco_obj1, met):
    # Convert the reconstructed objects to massless PtEtaPhiMLorentzVector structure
    reco_obj0_massless = ak.zip({
        "pt": reco_obj0.pt,
        "eta": reco_obj0.eta,
        "phi": reco_obj0.phi,
        "mass": ak.zeros_like(reco_obj0.pt),
    }, with_name="PtEtaPhiMLorentzVector")
    
    reco_obj1_massless = ak.zip({
        "pt": reco_obj1.pt,
        "eta": reco_obj1.eta,
        "phi": reco_obj1.phi,
        "mass": ak.zeros_like(reco_obj1.pt),
    }, with_name="PtEtaPhiMLorentzVector")
    
    # For massless objects, ET is simply pt
    ET_reco_obj0 = reco_obj0_massless.pt
    ET_reco_obj1 = reco_obj1_massless.pt
    
    # Compute the combined transverse momentum components for the visible objects
    px_reco_obj0 = ET_reco_obj0 * np.cos(reco_obj0_massless.phi)
    py_reco_obj0 = ET_reco_obj0 * np.sin(reco_obj0_massless.phi)
    
    px_reco_obj1 = ET_reco_obj1 * np.cos(reco_obj1_massless.phi)
    py_reco_obj1 = ET_reco_obj1 * np.sin(reco_obj1_massless.phi)
    
    # Compute the Mo1 using the separated visible objects and MET
    Mo1_squared = (ET_reco_obj0 + ET_reco_obj1 + met.pt)**2 - (px_reco_obj0 + px_reco_obj1 + met.pt*np.cos(met.phi))**2 - (py_reco_obj0 + py_reco_obj1 + met.pt*np.sin(met.phi))**2
    Mo1 = np.sqrt(Mo1_squared)
    #Mo1 = Mo1_squared
    
    return Mo1

class AnalysisProcessor(processor.ProcessorABC):

    def __init__(self, samples, wc_names_lst=[], hist_lst=None, ecut_threshold=None, do_errors=False, do_systematics=False, split_by_lepton_flavor=False, skip_signal_regions=False, skip_control_regions=False, muonSyst='nominal', dtype=np.float32):

        self._samples = samples
        self._wc_names_lst = wc_names_lst
        self._dtype = dtype

        # Create the histograms
        self._accumulator = processor.dict_accumulator({
            ##"invmass" : HistEFT("Events", wc_names_lst, hist.Cat("sample", "sample"), hist.Cat("channel", "channel"), hist.Cat("systematic", "Systematic Uncertainty"),hist.Cat("appl", "AR/SR"), hist.Bin("invmass", "$m_{\ell\ell}$ (GeV) ", 100, 0, 200)),
            "mvis_gentaulep": HistEFT("Events", wc_names_lst, hist.Cat("sample", "sample"), hist.Cat("channel", "channel"), hist.Cat("systematic", "Systematic Uncertainty"), hist.Cat("appl", "AR/SR"), hist.Bin("mvis_gentaulep", "Invariant Mass of gne tau-lepton pair (GeV)", 10, 0, 500)),
            "mvis_gentaulep0": HistEFT("Events", wc_names_lst, hist.Cat("sample", "sample"), hist.Cat("channel", "channel"), hist.Cat("systematic", "Systematic Uncertainty"), hist.Cat("appl", "AR/SR"), hist.Bin("mvis_gentaulep0", "Invariant Mass of gne tau-lepton pair (GeV)", 10, 0, 500)),
            "mvis_nogentaulep0": HistEFT("Events", wc_names_lst, hist.Cat("sample", "sample"), hist.Cat("channel", "channel"), hist.Cat("systematic", "Systematic Uncertainty"), hist.Cat("appl", "AR/SR"), hist.Bin("mvis_nogentaulep0", "Invariant Mass of gne tau-lepton pair (GeV)", 10, 0, 500)),
            "mvis_gentaulep1": HistEFT("Events", wc_names_lst, hist.Cat("sample", "sample"), hist.Cat("channel", "channel"), hist.Cat("systematic", "Systematic Uncertainty"), hist.Cat("appl", "AR/SR"), hist.Bin("mvis_gentaulep1", "Invariant Mass of gne tau-lepton pair (GeV)", 10, 0, 500)),
            "mvis_nogentaulep1": HistEFT("Events", wc_names_lst, hist.Cat("sample", "sample"), hist.Cat("channel", "channel"), hist.Cat("systematic", "Systematic Uncertainty"), hist.Cat("appl", "AR/SR"), hist.Bin("mvis_nogentaulep1", "Invariant Mass of gne tau-lepton pair (GeV)", 10, 0, 500)),
            "mvis_gentaulepc": HistEFT("Events", wc_names_lst, hist.Cat("sample", "sample"), hist.Cat("channel", "channel"), hist.Cat("systematic", "Systematic Uncertainty"), hist.Cat("appl", "AR/SR"), hist.Bin("mvis_gentaulepc", "Invariant Mass of gne tau-lepton pair (GeV)", 10, 0, 500)),
            "mvis_nogentaulepc": HistEFT("Events", wc_names_lst, hist.Cat("sample", "sample"), hist.Cat("channel", "channel"), hist.Cat("systematic", "Systematic Uncertainty"), hist.Cat("appl", "AR/SR"), hist.Bin("mvis_nogentaulepc", "Invariant Mass of gne tau-lepton pair (GeV)", 10, 0, 500)),
            "mvis_taulep0": HistEFT("Events", wc_names_lst, hist.Cat("sample", "sample"), hist.Cat("channel", "channel"), hist.Cat("systematic", "Systematic Uncertainty"), hist.Cat("appl", "AR/SR"), hist.Bin("mvis_taulep0", "Invariant Mass of tau-lepton1 pair (GeV)", 10, 0, 500)),
            "mvis_taulep1": HistEFT("Events", wc_names_lst, hist.Cat("sample", "sample"), hist.Cat("channel", "channel"), hist.Cat("systematic", "Systematic Uncertainty"), hist.Cat("appl", "AR/SR"), hist.Bin("mvis_taulep1", "Invariant Mass of closest tau-lepton pair (GeV)", 10, 0, 500)),
            "mvis_taulep_dR0": HistEFT("Events", wc_names_lst, hist.Cat("sample", "sample"), hist.Cat("channel", "channel"), hist.Cat("systematic", "Systematic Uncertainty"), hist.Cat("appl", "AR/SR"), hist.Bin("mvis_taulep_dR0", "Invariant Mass of tau-lepton0 pair (GeV)", 10, 0, 500)),

            #"M1T_taulep0": HistEFT("Events", wc_names_lst, hist.Cat("sample", "sample"), hist.Cat("channel", "channel"), hist.Cat("systematic", "Systematic Uncertainty"), hist.Cat("appl", "AR/SR"), hist.Bin("M1T_taulep0", "Invariant Mass of tau-lepton0 pair (GeV)", 25, 0, 500)),
            #"M1T_taulep1": HistEFT("Events", wc_names_lst, hist.Cat("sample", "sample"), hist.Cat("channel", "channel"), hist.Cat("systematic", "Systematic Uncertainty"), hist.Cat("appl", "AR/SR"), hist.Bin("M1T_taulep1", "Invariant Mass of tau-lepton0 pair (GeV)", 25, 0, 500)),

            #"M1T_taulep_dR0": HistEFT("Events", wc_names_lst, hist.Cat("sample", "sample"), hist.Cat("channel", "channel"), hist.Cat("systematic", "Systematic Uncertainty"), hist.Cat("appl", "AR/SR"), hist.Bin("M1T_taulep_dR0", "Invariant Mass of tau-lepton0 pair (GeV)", 25, 0, 500)),

            #"Mo1_taulep0": HistEFT("Events", wc_names_lst, hist.Cat("sample", "sample"), hist.Cat("channel", "channel"), hist.Cat("systematic", "Systematic Uncertainty"), hist.Cat("appl", "AR/SR"), hist.Bin("Mo1_taulep0", "Invariant Mass of tau-lepton0 pair (GeV)", 25, 0, 500)),
            #"Mo1_taulep1": HistEFT("Events", wc_names_lst, hist.Cat("sample", "sample"), hist.Cat("channel", "channel"), hist.Cat("systematic", "Systematic Uncertainty"), hist.Cat("appl", "AR/SR"), hist.Bin("Mo1_taulep1", "Invariant Mass of tau-lepton0 pair (GeV)", 25, 0, 500)),

            #"Mo1_taulep_dR0": HistEFT("Events", wc_names_lst, hist.Cat("sample", "sample"), hist.Cat("channel", "channel"), hist.Cat("systematic", "Systematic Uncertainty"), hist.Cat("appl", "AR/SR"), hist.Bin("Mo1_taulep_dR0", "Invariant Mass of tau-lepton0 pair (GeV)", 25, 0, 500)),

            #"puppiM1T_taulep0": HistEFT("Events", wc_names_lst, hist.Cat("sample", "sample"), hist.Cat("channel", "channel"), hist.Cat("systematic", "Systematic Uncertainty"), hist.Cat("appl", "AR/SR"), hist.Bin("puppiM1T_taulep0", "Invariant Mass of tau-lepton0 pair (GeV)", 25, 0, 500)),
            #"puppiM1T_taulep1": HistEFT("Events", wc_names_lst, hist.Cat("sample", "sample"), hist.Cat("channel", "channel"), hist.Cat("systematic", "Systematic Uncertainty"), hist.Cat("appl", "AR/SR"), hist.Bin("puppiM1T_taulep1", "Invariant Mass of tau-lepton0 pair (GeV)", 25, 0, 500)),

            #"puppiM1T_taulep_dR0": HistEFT("Events", wc_names_lst, hist.Cat("sample", "sample"), hist.Cat("channel", "channel"), hist.Cat("systematic", "Systematic Uncertainty"), hist.Cat("appl", "AR/SR"), hist.Bin("puppiM1T_taulep_dR0", "Invariant Mass of tau-lepton0 pair (GeV)", 25, 0, 500)),

            #"puppiMo1_taulep0": HistEFT("Events", wc_names_lst, hist.Cat("sample", "sample"), hist.Cat("channel", "channel"), hist.Cat("systematic", "Systematic Uncertainty"), hist.Cat("appl", "AR/SR"), hist.Bin("puppiMo1_taulep0", "Invariant Mass of tau-lepton0 pair (GeV)", 25, 0, 500)),
            #"puppiMo1_taulep1": HistEFT("Events", wc_names_lst, hist.Cat("sample", "sample"), hist.Cat("channel", "channel"), hist.Cat("systematic", "Systematic Uncertainty"), hist.Cat("appl", "AR/SR"), hist.Bin("puppiMo1_taulep1", "Invariant Mass of tau-lepton0 pair (GeV)", 25, 0, 500)),

            #"puppiMo1_taulep_dR0": HistEFT("Events", wc_names_lst, hist.Cat("sample", "sample"), hist.Cat("channel", "channel"), hist.Cat("systematic", "Systematic Uncertainty"), hist.Cat("appl", "AR/SR"), hist.Bin("puppiMo1_taulep_dR0", "Invariant Mass of tau-lepton0 pair (GeV)", 25, 0, 500)),


            ##"ptbl"    : HistEFT("Events", wc_names_lst, hist.Cat("sample", "sample"), hist.Cat("channel", "channel"), hist.Cat("systematic", "Systematic Uncertainty"),hist.Cat("appl", "AR/SR"), hist.Bin("ptbl",    "$p_{T}^{b\mathrm{-}jet+\ell_{min(dR)}}$ (GeV) ", 40, 0, 1000)),
            ##"ptz"     : HistEFT("Events", wc_names_lst, hist.Cat("sample", "sample"), hist.Cat("channel", "channel"), hist.Cat("systematic", "Systematic Uncertainty"),hist.Cat("appl", "AR/SR"), hist.Bin("ptz",     "$p_{T}$ Z (GeV)", 12, 0, 600)),
            "njets"   : HistEFT("Events", wc_names_lst, hist.Cat("sample", "sample"), hist.Cat("channel", "channel"), hist.Cat("systematic", "Systematic Uncertainty"),hist.Cat("appl", "AR/SR"), hist.Bin("njets",   "Jet multiplicity ", 10, 0, 10)),
            ##"nbtagsl" : HistEFT("Events", wc_names_lst, hist.Cat("sample", "sample"), hist.Cat("channel", "channel"), hist.Cat("systematic", "Systematic Uncertainty"),hist.Cat("appl", "AR/SR"), hist.Bin("nbtagsl", "Loose btag multiplicity ", 5, 0, 5)),
            "l0pt"    : HistEFT("Events", wc_names_lst, hist.Cat("sample", "sample"), hist.Cat("channel", "channel"), hist.Cat("systematic", "Systematic Uncertainty"),hist.Cat("appl", "AR/SR"), hist.Bin("l0pt",    "Leading lep $p_{T}$ (GeV)", 10, 0, 500)),
            ##"l1pt"    : HistEFT("Events", wc_names_lst, hist.Cat("sample", "sample"), hist.Cat("channel", "channel"), hist.Cat("systematic", "Systematic Uncertainty"),hist.Cat("appl", "AR/SR"), hist.Bin("l1pt",    "Subleading lep $p_{T}$ (GeV)", 10, 0, 100)),
            ##"l1eta"   : HistEFT("Events", wc_names_lst, hist.Cat("sample", "sample"), hist.Cat("channel", "channel"), hist.Cat("systematic", "Systematic Uncertainty"),hist.Cat("appl", "AR/SR"), hist.Bin("l1eta",   "Subleading $\eta$", 20, -2.5, 2.5)),
            ##"j0pt"    : HistEFT("Events", wc_names_lst, hist.Cat("sample", "sample"), hist.Cat("channel", "channel"), hist.Cat("systematic", "Systematic Uncertainty"),hist.Cat("appl", "AR/SR"), hist.Bin("j0pt",    "Leading jet  $p_{T}$ (GeV)", 10, 0, 500)),
            ##"b0pt"    : HistEFT("Events", wc_names_lst, hist.Cat("sample", "sample"), hist.Cat("channel", "channel"), hist.Cat("systematic", "Systematic Uncertainty"),hist.Cat("appl", "AR/SR"), hist.Bin("b0pt",    "Leading b jet  $p_{T}$ (GeV)", 10, 0, 500)),
            ##"l0eta"   : HistEFT("Events", wc_names_lst, hist.Cat("sample", "sample"), hist.Cat("channel", "channel"), hist.Cat("systematic", "Systematic Uncertainty"),hist.Cat("appl", "AR/SR"), hist.Bin("l0eta",   "Leading lep $\eta$", 20, -2.5, 2.5)),
            ##"j0eta"   : HistEFT("Events", wc_names_lst, hist.Cat("sample", "sample"), hist.Cat("channel", "channel"), hist.Cat("systematic", "Systematic Uncertainty"),hist.Cat("appl", "AR/SR"), hist.Bin("j0eta",   "Leading jet  $\eta$", 30, -3.0, 3.0)),
            ##"ht"      : HistEFT("Events", wc_names_lst, hist.Cat("sample", "sample"), hist.Cat("channel", "channel"), hist.Cat("systematic", "Systematic Uncertainty"),hist.Cat("appl", "AR/SR"), hist.Bin("ht",      "H$_{T}$ (GeV)", 20, 0, 1000)),
            #"met"     : HistEFT("Events", wc_names_lst, hist.Cat("sample", "sample"), hist.Cat("channel", "channel"), hist.Cat("systematic", "Systematic Uncertainty"),hist.Cat("appl", "AR/SR"), hist.Bin("met",     "MET (GeV)", 20, 0, 400)),
            #"puppimet"     : HistEFT("Events", wc_names_lst, hist.Cat("sample", "sample"), hist.Cat("channel", "channel"), hist.Cat("systematic", "Systematic Uncertainty"),hist.Cat("appl", "AR/SR"), hist.Bin("puppimet",     "PuppiMET (GeV)", 20, 0, 400)),
            ##"ljptsum" : HistEFT("Events", wc_names_lst, hist.Cat("sample", "sample"), hist.Cat("channel", "channel"), hist.Cat("systematic", "Systematic Uncertainty"),hist.Cat("appl", "AR/SR"), hist.Bin("ljptsum", "S$_{T}$ (GeV)", 11, 0, 1100)),
            ##"o0pt"    : HistEFT("Events", wc_names_lst, hist.Cat("sample", "sample"), hist.Cat("channel", "channel"), hist.Cat("systematic", "Systematic Uncertainty"),hist.Cat("appl", "AR/SR"), hist.Bin("o0pt",    "Leading l or b jet $p_{T}$ (GeV)", 10, 0, 500b )),
            ##"bl0pt"   : HistEFT("Events", wc_names_lst, hist.Cat("sample", "sample"), hist.Cat("channel", "channel"), hist.Cat("systematic", "Systematic Uncertainty"),hist.Cat("appl", "AR/SR"), hist.Bin("bl0pt",   "Leading (b+l) $p_{T}$ (GeV)", 10, 0, 500)),
            ##"lj0pt"   : HistEFT("Events", wc_names_lst, hist.Cat("sample", "sample"), hist.Cat("channel", "channel"), hist.Cat("systematic", "Systematic Uncertainty"),hist.Cat("appl", "AR/SR"), hist.Bin("lj0pt",   "Leading pt of pair from l+j collection (GeV)", 12, 0, 600)),
            "taupt"   : HistEFT("Events", wc_names_lst, hist.Cat("sample", "sample"), hist.Cat("channel", "channel"), hist.Cat("systematic", "Systematic Uncertainty"),hist.Cat("appl", "AR/SR"), hist.Bin("taupt",   "Leading pt of tau (GeV)", 20, 0, 200)),
            ##"nVLtau"  :  HistEFT("Events", wc_names_lst, hist.Cat("sample", "sample"), hist.Cat("channel", "channel"), hist.Cat("systematic", "Systematic Uncertainty"),hist.Cat("appl", "AR/SR"), hist.Bin("nVLtau",  "Number of VL WP taus", 3, 0, 3)),
            ##"nLtau"   :  HistEFT("Events", wc_names_lst, hist.Cat("sample", "sample"), hist.Cat("channel", "channel"), hist.Cat("systematic", "Systematic Uncertainty"),hist.Cat("appl", "AR/SR"), hist.Bin("nLtau",  "Number of L WP taus", 3, 0, 3)),
            ##"nMtau"   :  HistEFT("Events", wc_names_lst, hist.Cat("sample", "sample"), hist.Cat("channel", "channel"), hist.Cat("systematic", "Systematic Uncertainty"),hist.Cat("appl", "AR/SR"), hist.Bin("nMtau",  "Number of M WP taus", 3, 0, 3)),
            ##"nTtau"   :  HistEFT("Events", wc_names_lst, hist.Cat("sample", "sample"), hist.Cat("channel", "channel"), hist.Cat("systematic", "Systematic Uncertainty"),hist.Cat("appl", "AR/SR"), hist.Bin("nTtau",  "Number of T WP taus", 3, 0, 3)),
            ##"nVTtau"  :  HistEFT("Events", wc_names_lst, hist.Cat("sample", "sample"), hist.Cat("channel", "channel"), hist.Cat("systematic", "Systematic Uncertainty"),hist.Cat("appl", "AR/SR"), hist.Bin("nVTtau",  "Number of VT WP taus", 3, 0, 3)),
        })

        # Set the list of hists to fill
        if hist_lst is None:
            # If the hist list is none, assume we want to fill all hists
            self._hist_lst = list(self._accumulator.keys())
        else:
            # Otherwise, just fill the specified subset of hists
            for hist_to_include in hist_lst:
                if hist_to_include not in self._accumulator.keys():
                    raise Exception(f"Error: Cannot specify hist \"{hist_to_include}\", it is not defined in the processor.")
            self._hist_lst = hist_lst # Which hists to fill

        # Set the energy threshold to cut on
        self._ecut_threshold = ecut_threshold

        # Set the booleans
        self._do_errors = do_errors # Whether to calculate and store the w**2 coefficients
        self._do_systematics = do_systematics # Whether to process systematic samples
        self._split_by_lepton_flavor = split_by_lepton_flavor # Whether to keep track of lepton flavors individually
        self._skip_signal_regions = skip_signal_regions # Whether to skip the SR categories
        self._skip_control_regions = skip_control_regions # Whether to skip the CR categories


    @property
    def accumulator(self):
        return self._accumulator

    @property
    def columns(self):
        return self._columns

    # Main function: run on a given dataset
    def process(self, events):

        # Dataset parameters
        dataset = events.metadata["dataset"]

        isData             = self._samples[dataset]["isData"]
        histAxisName       = self._samples[dataset]["histAxisName"]
        year               = self._samples[dataset]["year"]
        xsec               = self._samples[dataset]["xsec"]
        sow                = self._samples[dataset]["nSumOfWeights"]

        # Get up down weights from input dict
        if (self._do_systematics and not isData):
            if histAxisName in get_param("lo_xsec_samples"):
                # We have a LO xsec for these samples, so for these systs we will have e.g. xsec_LO*(N_pass_up/N_gen_nom)
                # Thus these systs will cover the cross section uncty and the acceptance and effeciency and shape
                # So no NLO rate uncty for xsec should be applied in the text data card
                sow_ISRUp          = self._samples[dataset]["nSumOfWeights"]
                sow_ISRDown        = self._samples[dataset]["nSumOfWeights"]
                sow_FSRUp          = self._samples[dataset]["nSumOfWeights"]
                sow_FSRDown        = self._samples[dataset]["nSumOfWeights"]
                sow_renormUp       = self._samples[dataset]["nSumOfWeights"]
                sow_renormDown     = self._samples[dataset]["nSumOfWeights"]
                sow_factUp         = self._samples[dataset]["nSumOfWeights"]
                sow_factDown       = self._samples[dataset]["nSumOfWeights"]
                sow_renormfactUp   = self._samples[dataset]["nSumOfWeights"]
                sow_renormfactDown = self._samples[dataset]["nSumOfWeights"]
            else:
                # Otherwise we have an NLO xsec, so for these systs we will have e.g. xsec_NLO*(N_pass_up/N_gen_up)
                # Thus these systs should only affect acceptance and effeciency and shape
                # The uncty on xsec comes from NLO and is applied as a rate uncty in the text datacard
                sow_ISRUp          = self._samples[dataset]["nSumOfWeights_ISRUp"          ]
                sow_ISRDown        = self._samples[dataset]["nSumOfWeights_ISRDown"        ]
                sow_FSRUp          = self._samples[dataset]["nSumOfWeights_FSRUp"          ]
                sow_FSRDown        = self._samples[dataset]["nSumOfWeights_FSRDown"        ]
                sow_renormUp       = self._samples[dataset]["nSumOfWeights_renormUp"       ]
                sow_renormDown     = self._samples[dataset]["nSumOfWeights_renormDown"     ]
                sow_factUp         = self._samples[dataset]["nSumOfWeights_factUp"         ]
                sow_factDown       = self._samples[dataset]["nSumOfWeights_factDown"       ]
                sow_renormfactUp   = self._samples[dataset]["nSumOfWeights_renormfactUp"   ]
                sow_renormfactDown = self._samples[dataset]["nSumOfWeights_renormfactDown" ]
        else: 
            sow_ISRUp          = -1
            sow_ISRDown        = -1
            sow_FSRUp          = -1
            sow_FSRDown        = -1
            sow_renormUp       = -1
            sow_renormDown     = -1
            sow_factUp         = -1
            sow_factDown       = -1        
            sow_renormfactUp   = -1
            sow_renormfactDown = -1

        datasets = ["SingleMuon", "SingleElectron", "EGamma", "MuonEG", "DoubleMuon", "DoubleElectron", "DoubleEG"]
        for d in datasets: 
            if d in dataset: dataset = dataset.split('_')[0]

        # Set the sampleType (used for MC matching requirement)
        sampleType = "prompt"
        if isData:
            sampleType = "data"
        elif histAxisName in get_param("conv_samples"):
            sampleType = "conversions"
        elif histAxisName in get_param("prompt_and_conv_samples"):
            # Just DY (since we care about prompt DY for Z CR, and conv DY for 3l CR)
            sampleType = "prompt_and_conversions"
        
        # Initialize objects
        met  = events.MET
        puppimet  = events.PuppiMET
        e    = events.Electron
        mu   = events.Muon
        tau  = events.Tau
        jets = events.Jet
        genparticles = events.GenPart



        ##GenLeptonTauPairs(genparticles, e, mu, tau)

        # Assuming you have already loaded the genparticles, muons, and electrons                                                         
        leptons = ak.concatenate([mu, e], axis=1)

        ##print("leptons[leptons.isTauPaired]", leptons[leptons.isTauPaired], ak.type(leptons[leptons.isTauPaired]), ak.sum(ak.any(leptons.isTauPaired, axis=1)))
        ##print("tau", tau, ak.type(tau))
        ##print("tau.isLeptonPaired\t", ak.to_list(tau.isLeptonPaired), ak.type(tau.isLeptonPaired), ak.sum(ak.any((tau.isLeptonPaired), axis=1)))
        ##print("tau.genPartFlav == 5\t", ak.to_list(tau.genPartFlav == 5), ak.type(tau.genPartFlav == 5), ak.sum(ak.any((tau.genPartFlav == 5), axis=1)))
        ##print("tau.isLeptonPaired & tau.genPartFlav == 5", ak.to_list(tau.isLeptonPaired & tau.genPartFlav == 5), ak.type(tau.isLeptonPaired & tau.genPartFlav == 5), ak.sum(ak.any((tau.isLeptonPaired & tau.genPartFlav == 5), axis=1)))
        ##print("tau[tau.genPartFlav == 5]", tau[tau.genPartFlav == 5], ak.type(tau[tau.genPartFlav == 5]))
        ##print("tau[tau.isLeptonPaired]", tau[tau.isLeptonPaired], ak.type(tau[tau.isLeptonPaired]))

        ##mvis_gentaulep = (genleptons + gentaus).mass
        ##mvis_gentaulep_nonone = mvis_gentaulep[~ak.is_none(mvis_gentaulep)]
        ##print("non null mvis_gentaulep", mvis_gentaulep_nonone, ak.type(mvis_gentaulep_nonone)) #, ak.num(mvis_gentaulep_nonone)) #ak.any(ak.is_none(mvis_gentaulep)), ak.any(~ak.is_none(mvis_gentaulep)), ak.is_none(mvis_gentaulep), ~ak.is_none(mvis_gentaulep))
        
        genmatch_lep, genmatch_tau, mvis_gentaulep = get_closest_taus_for_all_events(genparticles, leptons, tau)
        
        '''
        # Open a file to write the information
        filename = f"particles_info.txt"
        with open(filename, "w") as file:

            for i, event in enumerate(genparticles):
                file.write(f"Event {i}:\n")

                # Loop over electrons in the current event
                file.write("Electrons:\n")
                for j, electron in enumerate(e[i]):
                    file.write(f"Electron {j}: genPartIdx: {electron.genPartIdx}, genPartFlav: {electron.genPartFlav}, charge: {electron.charge}, pt: {electron.pt}, eta: {electron.eta}, phi: {electron.phi}, mass: {electron.mass}, dxy: {electron.dxy}, dz: {electron.dz}\n")
                file.write("-" * 40 + "\n")

                # Loop over muons in the current event
                file.write("Muons:\n")
                for j, muon in enumerate(mu[i]):
                    file.write(f"Muon {j}: genPartIdx: {muon.genPartIdx}, genPartFlav: {muon.genPartFlav}, charge: {muon.charge}, pt: {muon.pt}, eta: {muon.eta}, phi: {muon.phi}, mass: {muon.mass}, dxy: {muon.dxy}, dz: {muon.dz}\n")
                file.write("-" * 40 + "\n")

                # Loop over taus in the current event
                file.write("Taus:\n")
                for j, tau_particle in enumerate(tau[i]):
                    file.write(f"Tau {j}: genPartIdx: {tau_particle.genPartIdx}, genPartFlav: {tau_particle.genPartFlav}, charge: {tau_particle.charge}, pt: {tau_particle.pt}, eta: {tau_particle.eta}, phi: {tau_particle.phi}, mass: {tau_particle.mass}, idDeepTau2017v2p1VSjet: {tau_particle.idDeepTau2017v2p1VSjet}, dxy: {tau_particle.dxy}, dz: {tau_particle.dz}, DM: {tau_particle.decayMode}\n")
                file.write("-" * 40 + "\n")

                # Write information about genparticles
                file.write("GenParticles:\n")
                for genparticle_idx, genparticle in enumerate(genparticles[i]):
                    if True:#genparticle.status in [1, 2]:
                        file.write(f"GenParticle {genparticle_idx}: pdgId: {genparticle.pdgId}, genPartIdxMother: {genparticle.genPartIdxMother}, status: {genparticle.status}, statusFlags: {genparticle.statusFlags}, pt: {genparticle.pt}, eta: {genparticle.eta}, phi: {genparticle.phi}, mass: {genparticle.mass}\n")
                file.write("-" * 40 + "\n")

        
                # Write information about gen_taulep_pairs
                file.write("Lepton-Tau Pairs:\n")
                if genmatch_lep[i] is None or genmatch_tau[i] is None:
                    file.write("No gen-level matched pair")
                    continue

                for j, lepton in enumerate(genmatch_lep[i]):
                    ttau = genmatch_tau[i][j]
                    if lepton is not None and ttau is not None:
                        file.write(f"Pair {j}:\n")
                        file.write(f"Lepton: genPartIdx: {lepton.genPartIdx}, genPartFlav: {lepton.genPartFlav}, charge: {lepton.charge}, pt: {lepton.pt}, eta: {lepton.eta}, phi: {lepton.phi}, mass: {lepton.mass}, dxy: {lepton.dxy}, dz: {lepton.dz}\n")
                        file.write(f"Tau: genPartIdx: {ttau.genPartIdx}, genPartFlav: {ttau.genPartFlav}, charge: {ttau.charge}, pt: {ttau.pt}, eta: {ttau.eta}, phi: {ttau.phi}, mass: {ttau.mass}, idDeepTau2017v2p1VSjet: {ttau.idDeepTau2017v2p1VSjet}, dxy: {ttau.dxy}, dz: {ttau.dz}\n")

                    else:
                        file.write(f"Pair {j}: No valid Lepton-Tau pair found for this event.\n")
                    file.write("-" * 40 + "\n")

                file.write("=" * 80 + "\n")
        '''
        # An array of lenght events that is just 1 for each event
        # Probably there's a better way to do this, but we use this method elsewhere so I guess why not..
        events.nom = ak.ones_like(events.MET.pt)

        e["idEmu"] = ttH_idEmu_cuts_E3(e.hoe, e.eta, e.deltaEtaSC, e.eInvMinusPInv, e.sieie)
        e["conept"] = coneptElec(e.pt, e.mvaTTHUL, e.jetRelIso)
        mu["conept"] = coneptMuon(mu.pt, mu.mvaTTHUL, mu.jetRelIso, mu.mediumId)
        e["btagDeepFlavB"] = ak.fill_none(e.matched_jet.btagDeepFlavB, -99)
        mu["btagDeepFlavB"] = ak.fill_none(mu.matched_jet.btagDeepFlavB, -99)
        if not isData:
            e["gen_pdgId"] = ak.fill_none(e.matched_gen.pdgId, 0)
            mu["gen_pdgId"] = ak.fill_none(mu.matched_gen.pdgId, 0)

        # Get the lumi mask for data
        if year == "2016" or year == "2016APV":
            golden_json_path = topcoffea_path("data/goldenJsons/Cert_271036-284044_13TeV_Legacy2016_Collisions16_JSON.txt")
        elif year == "2017":
            golden_json_path = topcoffea_path("data/goldenJsons/Cert_294927-306462_13TeV_UL2017_Collisions17_GoldenJSON.txt")
        elif year == "2018":
            golden_json_path = topcoffea_path("data/goldenJsons/Cert_314472-325175_13TeV_Legacy2018_Collisions18_JSON.txt")
        else:
            raise ValueError(f"Error: Unknown year \"{year}\".")
        lumi_mask = LumiMask(golden_json_path)(events.run,events.luminosityBlock)

        ######### EFT coefficients ##########

        # Extract the EFT quadratic coefficients and optionally use them to calculate the coefficients on the w**2 quartic function
        # eft_coeffs is never Jagged so convert immediately to numpy for ease of use.
        eft_coeffs = ak.to_numpy(events["EFTfitCoefficients"]) if hasattr(events, "EFTfitCoefficients") else None
        if eft_coeffs is not None:
            # Check to see if the ordering of WCs for this sample matches what want
            if self._samples[dataset]["WCnames"] != self._wc_names_lst:
                eft_coeffs = efth.remap_coeffs(self._samples[dataset]["WCnames"], self._wc_names_lst, eft_coeffs)
        eft_w2_coeffs = efth.calc_w2_coeffs(eft_coeffs,self._dtype) if (self._do_errors and eft_coeffs is not None) else None
        # Initialize the out object
        hout = self.accumulator.identity()

        ################### Electron selection ####################

        e["isPres"] = isPresElec(e.pt, e.eta, e.dxy, e.dz, e.miniPFRelIso_all, e.sip3d, getattr(e,"mvaFall17V2noIso_WPL"))
        e["isLooseE"] = isLooseElec(e.miniPFRelIso_all,e.sip3d,e.lostHits) #loose selection
        e["isFO"] = isFOElec(e.pt, e.conept, e.btagDeepFlavB, e.idEmu, e.convVeto, e.lostHits, e.mvaTTHUL, e.jetRelIso, e.mvaFall17V2noIso_WP90, year) # fakeable object
        e["isTightLep"] = tightSelElec(e.isFO, e.mvaTTHUL)      # tight selection

        ################### Muon selection ####################

        mu["pt"] = ApplyRochesterCorrections(year, mu, isData) # Need to apply corrections before doing muon selection
        mu["isPres"] = isPresMuon(mu.dxy, mu.dz, mu.sip3d, mu.eta, mu.pt, mu.miniPFRelIso_all)
        mu["isLooseM"] = isLooseMuon(mu.miniPFRelIso_all,mu.sip3d,mu.looseId)
        mu["isFO"] = isFOMuon(mu.pt, mu.conept, mu.btagDeepFlavB, mu.mvaTTHUL, mu.jetRelIso, year)
        mu["isTightLep"]= tightSelMuon(mu.isFO, mu.mediumId, mu.mvaTTHUL)

        ################### Loose selection ####################

        m_loose = mu[mu.isPres & mu.isLooseM]
        e_loose = e[e.isPres & e.isLooseE]
        l_loose = ak.with_name(ak.concatenate([e_loose, m_loose], axis=1), 'PtEtaPhiMCandidate') #gives array with lorentz vector structure

        ################### Tau selection ####################

        # Compute pair invariant masses, for all flavors all signes
        llpairs = ak.combinations(l_loose, 2, fields=["l0","l1"])
        events["minMllAFAS"] = ak.min( (llpairs.l0+llpairs.l1).mass, axis=-1)

        # Build FO collection
        m_fo = mu[mu.isPres & mu.isLooseM & mu.isFO]
        e_fo = e[e.isPres & e.isLooseE & e.isFO]

        # Attach the lepton SFs to the electron and muons collections
        AttachElectronSF(e_fo,year=year)
        AttachMuonSF(m_fo,year=year)

        # Attach per lepton fake rates
        AttachPerLeptonFR(e_fo, flavor = "Elec", year=year)
        AttachPerLeptonFR(m_fo, flavor = "Muon", year=year)
        m_fo['convVeto'] = ak.ones_like(m_fo.charge); 
        m_fo['lostHits'] = ak.zeros_like(m_fo.charge); 
        l_fo = ak.pad_none(ak.with_name(ak.concatenate([e_fo, m_fo], axis=1), 'PtEtaPhiMCandidate'), 3)
        l_fo_conept_sorted = l_fo[ak.argsort(l_fo.conept, axis=-1,ascending=False)]

        tau["pt"], tau["mass"]      = ApplyTES(year, tau, isData)
        tau["isPres"]  = isPresTau(tau.pt, tau.eta, tau.dxy, tau.dz, tau.idDeepTau2017v2p1VSjet, minpt=20)
        tau["isClean"] = isClean(tau, l_fo, drmin=0.3)
        #tau["isClean"] = isClean(tau, l_loose, drmin=0.3)
        tau["isGood"]  =  tau["isClean"] & tau["isPres"]
        tau = tau[tau.isGood] # use these to clean jets
        tau["isVLoose"]  = isVLooseTau(tau.idDeepTau2017v2p1VSjet) # use these to veto
        tau["isLoose"]   = isLooseTau(tau.idDeepTau2017v2p1VSjet)
        tau["isMedium"]  = isMediumTau(tau.idDeepTau2017v2p1VSjet)
        tau["isTight"]   = isTightTau(tau.idDeepTau2017v2p1VSjet)
        tau["isVTight"]  = isVTightTau(tau.idDeepTau2017v2p1VSjet)
        tau["isVVTight"] = isVVTightTau(tau.idDeepTau2017v2p1VSjet)
        
        tau['DMflag'] = ((tau.decayMode==0) | (tau.decayMode==1) | (tau.decayMode==10) | (tau.decayMode==11))
        tau = tau[tau['DMflag']]
        nVLtau = ak.num(tau[tau["isVLoose"]>0])
        nLtau  = ak.num(tau[tau["isLoose"]>0] )
        nMtau  = ak.num(tau[tau["isMedium"]>0])
        nTtau  = ak.num(tau[tau["isTight"]>0] )
        nVTtau = ak.num(tau[tau["isVTight"]>0])
                        
        if not isData:
            AttachTauSF(events, tau, year=year)
        tau_padded = ak.pad_none(tau, 1)
        tau0 = tau_padded[:,0]

        l_fo_conept_sorted_padded = ak.pad_none(l_fo_conept_sorted, 3)
        leading_lepton = l_fo_conept_sorted[:, 0]
        subleading_lepton = l_fo_conept_sorted[:, 1]

        # Visible mass for any lepton-tau pair
        leading_tau = tau_padded[:, 0]

        
        gen_leadtau_mask = leading_tau["pt"] == genmatch_tau["pt"]
        gen_leadlep_mask = leading_lepton["pt"] == genmatch_lep["pt"]
        gen_subleadlep_mask = subleading_lepton["pt"] == genmatch_lep["pt"]
        gen_leadtau_mask = ak.pad_none(gen_leadtau_mask, 1)
        gen_leadlep_mask = ak.pad_none(gen_leadlep_mask, 1)
        gen_subleadlep_mask = ak.pad_none(gen_subleadlep_mask, 1)

        gen_lep0tau_mask = gen_leadtau_mask & gen_leadlep_mask
        gen_lep1tau_mask = gen_leadtau_mask & gen_subleadlep_mask
        

        conv_mask_l0t = ak.num(gen_lep0tau_mask) == 0
        conv_mask_l1t = ak.num(gen_lep1tau_mask) == 0
        gen_lep0tau_mask = ak.where(conv_mask_l0t, [[False]], gen_lep0tau_mask)
        gen_lep1tau_mask = ak.where(conv_mask_l1t, [[False]], gen_lep1tau_mask)
        gen_lep0tau_mask = ak.where(ak.is_none(gen_lep0tau_mask), [[False]], gen_lep0tau_mask)
        gen_lep1tau_mask = ak.where(ak.is_none(gen_lep1tau_mask), [[False]], gen_lep1tau_mask)
        gen_lep0tau_mask = ak.flatten(gen_lep0tau_mask)
        gen_lep1tau_mask = ak.flatten(gen_lep1tau_mask)
        gen_lep0tau_mask = ak.fill_none(gen_lep0tau_mask, False)
        gen_lep1tau_mask = ak.fill_none(gen_lep1tau_mask, False)
        nogen_lep0tau_mask = ~gen_lep0tau_mask
        nogen_lep1tau_mask = ~gen_lep1tau_mask

        #print("flat gen_lep0tau_mask", ak.to_list(gen_lep0tau_mask), ak.type(gen_lep0tau_mask))
        #print("flat ~gen_lep0tau_mask", ak.to_list(~gen_lep0tau_mask), ak.type(~gen_lep0tau_mask))
        #print("flat gen_lep1tau_mask", ak.to_list(gen_lep1tau_mask), ak.type(gen_lep1tau_mask))
        #print("flat ~gen_lep1tau_mask", ak.to_list(~gen_lep1tau_mask), ak.type(~gen_lep1tau_mask))
        #print("\n\n\n\n\n\n")

        mvis_gentaulep0 = ak.where(gen_lep0tau_mask, mvis_gentaulep, -25)
        mvis_gentaulep1 = ak.where(gen_lep1tau_mask, mvis_gentaulep, -25)
        mvis_nogentaulep0 = ak.where(nogen_lep0tau_mask, mvis_gentaulep, -25)
        mvis_nogentaulep1 = ak.where(nogen_lep1tau_mask, mvis_gentaulep, -25)

        mvis_taulep = (leading_tau + l_fo_conept_sorted).mass
        charge_taulep = leading_tau.charge + l_fo_conept_sorted.charge
        
        leading_tau_jagged = ak.singletons(leading_tau)
        
        closest_taulep = leading_tau_jagged.nearest(l_fo_conept_sorted_padded)
        closest_taulep = ak.pad_none(closest_taulep, 1)
        
        mvis_taulep_dR0 = (leading_tau + closest_taulep).mass

        genmatch_lep = ak.pad_none(genmatch_lep, 1)
        gen_closlep_mask = closest_taulep["pt"] == genmatch_lep["pt"]
        
        gen_lepctau_mask = gen_leadtau_mask & gen_closlep_mask
        conv_mask_lct = ak.num(gen_lepctau_mask) == 0
        gen_lepctau_mask = ak.where(conv_mask_lct, [[False]], gen_lepctau_mask)
        gen_lepctau_mask = ak.where(ak.is_none(gen_lepctau_mask), [[False]], gen_lepctau_mask)
        gen_lepctau_mask = ak.flatten(gen_lepctau_mask)
        gen_lepctau_mask = ak.fill_none(gen_lepctau_mask, False)
        nogen_lepctau_mask = ~gen_lepctau_mask

        mvis_gentaulepc = ak.where(gen_lepctau_mask, mvis_gentaulep, -25)
        mvis_nogentaulepc = ak.where(nogen_lepctau_mask, mvis_gentaulep, -25)
        taulepOS_mask = ak.any(charge_taulep == 0, axis=1) #(charge_taulep == 0)

        M1T_taulep0 = calculate_M1T(leading_tau, l_fo_conept_sorted[:, 0], met)
        M1T_taulep1 = calculate_M1T(leading_tau, l_fo_conept_sorted[:, 1], met)
        M1T_taulep_dR0 = calculate_M1T(leading_tau, closest_taulep, met)

        puppiM1T_taulep0 = calculate_M1T(leading_tau, l_fo_conept_sorted[:, 0], puppimet)
        puppiM1T_taulep1 = calculate_M1T(leading_tau, l_fo_conept_sorted[:, 1], puppimet)
        puppiM1T_taulep_dR0 = calculate_M1T(leading_tau, closest_taulep, puppimet)

        Mo1_taulep0 = calculate_Mo1(leading_tau, l_fo_conept_sorted[:, 0], met)
        Mo1_taulep1 = calculate_Mo1(leading_tau, l_fo_conept_sorted[:, 1], met)
        Mo1_taulep_dR0 = calculate_Mo1(leading_tau, closest_taulep, met)

        puppiMo1_taulep0 = calculate_Mo1(leading_tau, l_fo_conept_sorted[:, 0], puppimet)
        puppiMo1_taulep1 = calculate_Mo1(leading_tau, l_fo_conept_sorted[:, 1], puppimet)
        puppiMo1_taulep_dR0 = calculate_Mo1(leading_tau, closest_taulep, puppimet)

        leading_tau_genPartIdx = leading_tau.genPartIdx
        leading_lepton_genPartIdx = l_fo_conept_sorted[:, 0].genPartIdx

        '''        
        # Dictionary to translate pdgId to particle name
        pdgId_dict = {
            11: "Electron",
            -11: "Positron",
            13: "Muon",
            -13: "Anti-muon",
            15: "Tau",
            -15: "Anti-tau"
        }

        # Open the file for writing
        with open(f'leading_tau_and_leptons_info_{random_number}.txt', 'w') as file:
    
            # Loop over events
            for i, event in enumerate(events):
                file.write(f"Event {i}:\n")
                # Information for leading_tau
                if leading_tau is not None:
                    file.write("=== leading_tau ===\n")
                    file.write(f"Charge: {leading_tau.charge[i]}\n")
                    file.write(f"PT: {leading_tau.pt[i]}, Eta: {leading_tau.eta[i]}, Phi: {leading_tau.phi[i]}, Mass: {leading_tau.mass[i]}\n")
                    file.write(f"idDeepTau2017v2p1VSjet: {leading_tau.idDeepTau2017v2p1VSjet[i]}\n")
                    file.write(f"genPartIdx: {leading_tau.genPartIdx[i]}, genPartFlav: {leading_tau.genPartFlav[i]}\n")
                    file.write("\n")
                else:
                    file.write("leading_tau object is None for this event.\n")
        
                # Information for leptons in l_fo_conept_sorted
                for j, lepton in enumerate(l_fo_conept_sorted[i]):
                    if lepton is not None:
                        file.write(f"=== lepton {j} ===\n")
                        file.write(f"Flavor: {pdgId_dict.get(lepton.pdgId, 'Unknown')}\n")
                        file.write(f"Charge: {lepton.charge}\n")
                        file.write(f"PT: {lepton.pt}, Eta: {lepton.eta}, Phi: {lepton.phi}, Mass: {lepton.mass}\n")
                        file.write(f"genPartIdx: {lepton.genPartIdx}, genPartFlav: {lepton.genPartFlav}\n")
                        file.write("\n")
                    else:
                        file.write("Lepton object is None for this event.\n")
                if i > 49:
                    break
                file.write("=====================================\n\n")
        '''

        ######### Systematics ###########

        # Define the lists of systematics we include
        obj_correction_syst_lst = [
            f'JER_{year}Up',f'JER_{year}Down', # Systs that affect the kinematics of objects
            'JES_FlavorQCDUp', 'JES_AbsoluteUp', 'JES_RelativeBalUp', 'JES_BBEC1Up', 'JES_RelativeSampleUp', 'JES_FlavorQCDDown', 'JES_AbsoluteDown', 'JES_RelativeBalDown', 'JES_BBEC1Down', 'JES_RelativeSampleDown'
        ]
        wgt_correction_syst_lst = [
            "lepSF_muonUp","lepSF_muonDown","lepSF_elecUp","lepSF_elecDown","lepSF_tausUp","lepSF_tausDown",f"btagSFbc_{year}Up",f"btagSFbc_{year}Down","btagSFbc_corrUp","btagSFbc_corrDown",f"btagSFlight_{year}Up",f"btagSFlight_{year}Down","btagSFlight_corrUp","btagSFlight_corrDown","PUUp","PUDown","PreFiringUp","PreFiringDown",f"triggerSF_{year}Up",f"triggerSF_{year}Down", # Exp systs
            "FSRUp","FSRDown","ISRUp","ISRDown","renormfactUp","renormfactDown", "renormUp","renormDown","factUp","factDown", # Theory systs
        ]
        data_syst_lst = [
            "FFUp","FFDown","FFptUp","FFptDown","FFetaUp","FFetaDown",f"FFcloseEl_{year}Up",f"FFcloseEl_{year}Down",f"FFcloseMu_{year}Up",f"FFcloseMu_{year}Down"
        ]

        # These weights can go outside of the outside sys loop since they do not depend on pt of mu or jets
        # We only calculate these values if not isData
        # Note: add() will generally modify up/down weights, so if these are needed for any reason after this point, we should instead pass copies to add()
        # Note: Here we will to the weights object the SFs that do not depend on any of the forthcoming loops
        weights_obj_base = coffea.analysis_tools.Weights(len(events),storeIndividual=True)
        if not isData:

            # If this is no an eft sample, get the genWeight
            if eft_coeffs is None: genw = events["genWeight"]
            else: genw= np.ones_like(events["event"])

            # Normalize by (xsec/sow)*genw where genw is 1 for EFT samples
            # Note that for theory systs, will need to multiply by sow/sow_wgtUP to get (xsec/sow_wgtUp)*genw and same for Down
            lumi = 1000.0*get_lumi(year)
            weights_obj_base.add("norm",(xsec/sow)*genw*lumi)

            # Attach PS weights (ISR/FSR) and scale weights (renormalization/factorization) and PDF weights
            AttachPSWeights(events)
            AttachScaleWeights(events)
            #AttachPdfWeights(events) # TODO
            # FSR/ISR weights
            weights_obj_base.add('ISR', events.nom, events.ISRUp*(sow/sow_ISRUp), events.ISRDown*(sow/sow_ISRDown))
            weights_obj_base.add('FSR', events.nom, events.FSRUp*(sow/sow_FSRUp), events.FSRDown*(sow/sow_FSRDown))
            # renorm/fact scale
            weights_obj_base.add('renormfact', events.nom, events.renormfactUp*(sow/sow_renormfactUp), events.renormfactDown*(sow/sow_renormfactDown))
            weights_obj_base.add('renorm', events.nom, events.renormUp*(sow/sow_renormUp), events.renormDown*(sow/sow_renormDown))
            weights_obj_base.add('fact', events.nom, events.factUp*(sow/sow_factUp), events.factDown*(sow/sow_factDown))
            # Prefiring and PU (note prefire weights only available in nanoAODv9)
            weights_obj_base.add('PreFiring', events.L1PreFiringWeight.Nom,  events.L1PreFiringWeight.Up,  events.L1PreFiringWeight.Dn)
            weights_obj_base.add('PU', GetPUSF((events.Pileup.nTrueInt), year), GetPUSF(events.Pileup.nTrueInt, year, 'up'), GetPUSF(events.Pileup.nTrueInt, year, 'down'))


        ######### The rest of the processor is inside this loop over systs that affect object kinematics  ###########

        # If we're doing systematics and this isn't data, we will loop over the obj_correction_syst_lst list
        if self._do_systematics and not isData: syst_var_list = ["nominal"] + obj_correction_syst_lst
        # Otherwise loop juse once, for nominal
        else: syst_var_list = ['nominal']

        # Loop over the list of systematic variations we've constructed
        met_raw=met
        for syst_var in syst_var_list:
            # Make a copy of the base weights object, so that each time through the loop we do not double count systs
            # In this loop over systs that impact kinematics, we will add to the weights objects the SFs that depend on the object kinematics
            weights_obj_base_for_kinematic_syst = copy.deepcopy(weights_obj_base)

            #################### Jets ####################

            # Jet cleaning, before any jet selection
            vetos_tocleanjets = ak.with_name( ak.concatenate([tau, l_fo], axis=1), "PtEtaPhiMCandidate")
            #vetos_tocleanjets = ak.with_name( l_fo, "PtEtaPhiMCandidate")
            tmp = ak.cartesian([ak.local_index(jets.pt), vetos_tocleanjets.jetIdx], nested=True)
            cleanedJets = jets[~ak.any(tmp.slot0 == tmp.slot1, axis=-1)] # this line should go before *any selection*, otherwise lep.jetIdx is not aligned with the jet index

            # Selecting jets and cleaning them
            jetptname = "pt_nom" if hasattr(cleanedJets, "pt_nom") else "pt"

            # Jet energy corrections
            if not isData:
                cleanedJets["pt_raw"] = (1 - cleanedJets.rawFactor)*cleanedJets.pt
                cleanedJets["mass_raw"] = (1 - cleanedJets.rawFactor)*cleanedJets.mass
                cleanedJets["pt_gen"] =ak.values_astype(ak.fill_none(cleanedJets.matched_gen.pt, 0), np.float32)
                cleanedJets["rho"] = ak.broadcast_arrays(events.fixedGridRhoFastjetAll, cleanedJets.pt)[0]
                events_cache = events.caches[0]
                cleanedJets = ApplyJetCorrections(year, corr_type='jets').build(cleanedJets, lazy_cache=events_cache)
                # SYSTEMATICS
                cleanedJets=ApplyJetSystematics(year,cleanedJets,syst_var)
                met=ApplyJetCorrections(year, corr_type='met').build(met_raw, cleanedJets, lazy_cache=events_cache)
            cleanedJets["isGood"] = isTightJet(getattr(cleanedJets, jetptname), cleanedJets.eta, cleanedJets.jetId, jetPtCut=30.) # temporary at 25 for synch, TODO: Do we want 30 or 25?
            goodJets = cleanedJets[cleanedJets.isGood]

            # Count jets
            njets = ak.num(goodJets)
            ht = ak.sum(goodJets.pt,axis=-1)
            j0 = goodJets[ak.argmax(goodJets.pt,axis=-1,keepdims=True)]

            # Loose DeepJet WP
            if year == "2017":
                btagwpl = get_param("btag_wp_loose_UL17")
            elif year == "2018":
                btagwpl = get_param("btag_wp_loose_UL18")
            elif year=="2016":
                btagwpl = get_param("btag_wp_loose_UL16")          
            elif year=="2016APV":
                btagwpl = get_param("btag_wp_loose_UL16APV")
            else:
                raise ValueError(f"Error: Unknown year \"{year}\".")
            isBtagJetsLoose = (goodJets.btagDeepFlavB > btagwpl)
            isNotBtagJetsLoose = np.invert(isBtagJetsLoose)
            nbtagsl = ak.num(goodJets[isBtagJetsLoose])

            # Medium DeepJet WP
            if year == "2017": 
                btagwpm = get_param("btag_wp_medium_UL17")
            elif year == "2018":
                btagwpm = get_param("btag_wp_medium_UL18")
            elif year=="2016":
                btagwpm = get_param("btag_wp_medium_UL16")
            elif year=="2016APV":
                btagwpm = get_param("btag_wp_medium_UL16APV")
            else:
                raise ValueError(f"Error: Unknown year \"{year}\".")
            isBtagJetsMedium = (goodJets.btagDeepFlavB > btagwpm)
            isNotBtagJetsMedium = np.invert(isBtagJetsMedium)
            nbtagsm = ak.num(goodJets[isBtagJetsMedium])


            #################### Add variables into event object so that they persist ####################

            # Put njets and l_fo_conept_sorted into events
            events["njets"] = njets
            events["l_fo_conept_sorted"] = l_fo_conept_sorted

            # The event selection
            add1lMaskAndSFs(events, year, isData, sampleType)
            add2lMaskAndSFs(events, year, isData, sampleType)
            add3lMaskAndSFs(events, year, isData, sampleType)
            add4lMaskAndSFs(events, year, isData)
            addLepCatMasks(events)

            # Convenient to have l0, l1, l2 on hand

            l0 = l_fo_conept_sorted_padded[:,0]
            l1 = l_fo_conept_sorted_padded[:,1]
            l2 = l_fo_conept_sorted_padded[:,2]


            ######### Event weights that do not depend on the lep cat ##########

            if not isData:

                # Btag SF following 1a) in https://twiki.cern.ch/twiki/bin/viewauth/CMS/BTagSFMethods    
                isBtagJetsLooseNotMedium = (isBtagJetsLoose & isNotBtagJetsMedium)
                bJetSF   = [GetBTagSF(goodJets, year, 'LOOSE'),GetBTagSF(goodJets, year, 'MEDIUM')]
                bJetEff  = [GetBtagEff(goodJets, year, 'loose'),GetBtagEff(goodJets, year, 'medium')]
                bJetEff_data   = [bJetEff[0]*bJetSF[0],bJetEff[1]*bJetSF[1]]
                pMC     = ak.prod(bJetEff[1]       [isBtagJetsMedium], axis=-1) * ak.prod((bJetEff[0]       [isBtagJetsLooseNotMedium] - bJetEff[1]       [isBtagJetsLooseNotMedium]), axis=-1) * ak.prod((1-bJetEff[0]       [isNotBtagJetsLoose]), axis=-1)
                pMC     = ak.where(pMC==0,1,pMC) # removeing zeroes from denominator...
                pData   = ak.prod(bJetEff_data[1]  [isBtagJetsMedium], axis=-1) * ak.prod((bJetEff_data[0]  [isBtagJetsLooseNotMedium] - bJetEff_data[1]  [isBtagJetsLooseNotMedium]), axis=-1) * ak.prod((1-bJetEff_data[0]  [isNotBtagJetsLoose]), axis=-1)
                weights_obj_base_for_kinematic_syst.add("btagSF", pData/pMC)

                if self._do_systematics and syst_var=='nominal':
                    for b_syst in ["bc_corr","light_corr",f"bc_{year}",f"light_{year}"]:
                        bJetSFUp = [GetBTagSF(goodJets, year, 'LOOSE', sys=b_syst)[0],GetBTagSF(goodJets, year, 'MEDIUM', sys=b_syst)[0]]
                        bJetSFDo = [GetBTagSF(goodJets, year, 'LOOSE', sys=b_syst)[1],GetBTagSF(goodJets, year, 'MEDIUM', sys=b_syst)[1]]
                        bJetEff_dataUp = [bJetEff[0]*bJetSFUp[0],bJetEff[1]*bJetSFUp[1]]
                        bJetEff_dataDo = [bJetEff[0]*bJetSFDo[0],bJetEff[1]*bJetSFDo[1]]
                        pDataUp = ak.prod(bJetEff_dataUp[1][isBtagJetsMedium], axis=-1) * ak.prod((bJetEff_dataUp[0][isBtagJetsLooseNotMedium] - bJetEff_dataUp[1][isBtagJetsLooseNotMedium]), axis=-1) * ak.prod((1-bJetEff_dataUp[0][isNotBtagJetsLoose]), axis=-1)
                        pDataDo = ak.prod(bJetEff_dataDo[1][isBtagJetsMedium], axis=-1) * ak.prod((bJetEff_dataDo[0][isBtagJetsLooseNotMedium] - bJetEff_dataDo[1][isBtagJetsLooseNotMedium]), axis=-1) * ak.prod((1-bJetEff_dataDo[0][isNotBtagJetsLoose]), axis=-1)           
                        weights_obj_base_for_kinematic_syst.add(f"btagSF{b_syst}", events.nom, (pDataUp/pMC)/(pData/pMC),(pDataDo/pMC)/(pData/pMC))

                # Trigger SFs 
                GetTriggerSF(year,events,l0,l1)                
                weights_obj_base_for_kinematic_syst.add(f"triggerSF_{year}", events.trigger_sf, copy.deepcopy(events.trigger_sfUp), copy.deepcopy(events.trigger_sfDown))            # In principle does not have to be in the lep cat loop


            ######### Event weights that do depend on the lep cat ###########

            # Loop over categories and fill the dict
            weights_dict = {}
            for ch_name in ["2l", "2l_4t", "3l", "4l", "2l_CR", "2l_CRflip", "3l_CR", "2los_CRtt", "2los_CRZ", "2los_CR"]:

                # For both data and MC
                weights_dict[ch_name] = copy.deepcopy(weights_obj_base_for_kinematic_syst)
                #if ch_name.startswith("1l"):
                #    weights_dict[ch_name].add("FF", events.fakefactor_2l, copy.deepcopy(events.fakefactor_2l_up), copy.deepcopy(events.fakefactor_2l_down))
                #    weights_dict[ch_name].add("FFpt",  events.nom, copy.deepcopy(events.fakefactor_2l_pt1/events.fakefactor_2l), copy.deepcopy(events.fakefactor_2l_pt2/events.fakefactor_2l))
                #    weights_dict[ch_name].add("FFeta", events.nom, copy.deepcopy(events.fakefactor_2l_be1/events.fakefactor_2l), copy.deepcopy(events.fakefactor_2l_be2/events.fakefactor_2l))
                #    weights_dict[ch_name].add(f"FFcloseEl_{year}", events.nom, copy.deepcopy(events.fakefactor_2l_elclosureup/events.fakefactor_2l), copy.deepcopy(events.fakefactor_2l_elclosuredown/events.fakefactor_2l))
                #    weights_dict[ch_name].add(f"FFcloseMu_{year}", events.nom, copy.deepcopy(events.fakefactor_2l_muclosureup/events.fakefactor_2l), copy.deepcopy(events.fakefactor_2l_muclosuredown/events.fakefactor_2l))
                if ch_name.startswith("2l"):
                    weights_dict[ch_name].add("FF", events.fakefactor_2l, copy.deepcopy(events.fakefactor_2l_up), copy.deepcopy(events.fakefactor_2l_down))
                    weights_dict[ch_name].add("FFpt",  events.nom, copy.deepcopy(events.fakefactor_2l_pt1/events.fakefactor_2l), copy.deepcopy(events.fakefactor_2l_pt2/events.fakefactor_2l))
                    weights_dict[ch_name].add("FFeta", events.nom, copy.deepcopy(events.fakefactor_2l_be1/events.fakefactor_2l), copy.deepcopy(events.fakefactor_2l_be2/events.fakefactor_2l))
                    weights_dict[ch_name].add(f"FFcloseEl_{year}", events.nom, copy.deepcopy(events.fakefactor_2l_elclosureup/events.fakefactor_2l), copy.deepcopy(events.fakefactor_2l_elclosuredown/events.fakefactor_2l))
                    weights_dict[ch_name].add(f"FFcloseMu_{year}", events.nom, copy.deepcopy(events.fakefactor_2l_muclosureup/events.fakefactor_2l), copy.deepcopy(events.fakefactor_2l_muclosuredown/events.fakefactor_2l))
                elif ch_name.startswith("3l"):
                    weights_dict[ch_name].add("FF", events.fakefactor_3l, copy.deepcopy(events.fakefactor_3l_up), copy.deepcopy(events.fakefactor_3l_down))
                    weights_dict[ch_name].add("FFpt",  events.nom, copy.deepcopy(events.fakefactor_3l_pt1/events.fakefactor_3l), copy.deepcopy(events.fakefactor_3l_pt2/events.fakefactor_3l))
                    weights_dict[ch_name].add("FFeta", events.nom, copy.deepcopy(events.fakefactor_3l_be1/events.fakefactor_3l), copy.deepcopy(events.fakefactor_3l_be2/events.fakefactor_3l))
                    weights_dict[ch_name].add(f"FFcloseEl_{year}", events.nom, copy.deepcopy(events.fakefactor_3l_elclosureup/events.fakefactor_3l), copy.deepcopy(events.fakefactor_3l_elclosuredown/events.fakefactor_3l))
                    weights_dict[ch_name].add(f"FFcloseMu_{year}", events.nom, copy.deepcopy(events.fakefactor_3l_muclosureup/events.fakefactor_3l), copy.deepcopy(events.fakefactor_3l_muclosuredown/events.fakefactor_3l))

                # For data only
                if isData:
                    if ch_name in ["2l","2l_4t","2l_CR","2l_CRflip"]:
                        weights_dict[ch_name].add("fliprate", events.flipfactor_2l)

                # For MC only
                if not isData:
                    #if ch_name.startswith("1l"):
                    #    weights_dict[ch_name].add("lepSF_muon", events.sf_2l_muon, copy.deepcopy(events.sf_2l_hi_muon), copy.deepcopy(events.sf_2l_lo_muon))
                    #    weights_dict[ch_name].add("lepSF_elec", events.sf_2l_elec, copy.deepcopy(events.sf_2l_hi_elec), copy.deepcopy(events.sf_2l_lo_elec))
                    if ch_name.startswith("2l"):
                        weights_dict[ch_name].add("lepSF_muon", events.sf_2l_muon, copy.deepcopy(events.sf_2l_hi_muon), copy.deepcopy(events.sf_2l_lo_muon))
                        weights_dict[ch_name].add("lepSF_elec", events.sf_2l_elec, copy.deepcopy(events.sf_2l_hi_elec), copy.deepcopy(events.sf_2l_lo_elec))
                        weights_dict[ch_name].add("lepSF_taus", events.sf_2l_taus, copy.deepcopy(events.sf_2l_taus_hi), copy.deepcopy(events.sf_2l_taus_lo))
                    elif ch_name.startswith("3l"):
                        weights_dict[ch_name].add("lepSF_muon", events.sf_3l_muon, copy.deepcopy(events.sf_3l_hi_muon), copy.deepcopy(events.sf_3l_lo_muon))
                        weights_dict[ch_name].add("lepSF_elec", events.sf_3l_elec, copy.deepcopy(events.sf_3l_hi_elec), copy.deepcopy(events.sf_3l_lo_elec))
                    elif ch_name.startswith("4l"):
                        weights_dict[ch_name].add("lepSF_muon", events.sf_4l_muon, copy.deepcopy(events.sf_4l_hi_muon), copy.deepcopy(events.sf_4l_lo_muon))
                        weights_dict[ch_name].add("lepSF_elec", events.sf_4l_elec, copy.deepcopy(events.sf_4l_hi_elec), copy.deepcopy(events.sf_4l_lo_elec))
                    else:
                        raise Exception(f"Unknown channel name: {ch_name}")


            ######### Masks we need for the selection ##########

            # Get mask for events that have two sf os leps close to z peak
            sfosz_3l_mask = get_Z_peak_mask(l_fo_conept_sorted_padded[:,0:3],pt_window=10.0)
            sfosz_2l_mask = get_Z_peak_mask(l_fo_conept_sorted_padded[:,0:2],pt_window=10.0)
            sfasz_2l_mask = get_Z_peak_mask(l_fo_conept_sorted_padded[:,0:2],pt_window=30.0,flavor="as") # Any sign (do not enforce ss or os here)

            # Pass trigger mask
            pass_trg = trgPassNoOverlap(events,isData,dataset,str(year))

            # b jet masks
            bmask_atleast1med_atleast2loose = ((nbtagsm>=1)&(nbtagsl>=2)) # Used for 2lss and 4l
            bmask_exactly0med = (nbtagsm==0) # Used for 3l CR and 2los Z CR
            bmask_exactly1med = (nbtagsm==1) # Used for 3l SR and 2lss CR
            bmask_exactly2med = (nbtagsm==2) # Used for CRtt
            bmask_atleast2med = (nbtagsm>=2) # Used for 3l SR
            bmask_atmost2med  = (nbtagsm< 3) # Used to make 2lss mutually exclusive from tttt enriched
            bmask_atleast3med = (nbtagsm>=3) # Used for tttt enriched

            # Charge masks
            chargel0_p = ak.fill_none(((l0.charge)>0),False)
            chargel0_m = ak.fill_none(((l0.charge)<0),False)
            charge2l_0 = ak.fill_none(((l0.charge+l1.charge)==0),False)
            charge2l_1 = ak.fill_none(((l0.charge+l1.charge)!=0),False)
            charge3l_p = ak.fill_none(((l0.charge+l1.charge+l2.charge)>0),False)
            charge3l_m = ak.fill_none(((l0.charge+l1.charge+l2.charge)<0),False)

            #tau mask
            tau_2lss_0tau_mask = (ak.num(tau[tau["isVLoose"]>0])==0)
            tau_2lss_1tau_mask = (ak.num(tau[tau["isVLoose"]>0])==1)
            tau_2los_1tau_mask = (ak.num(tau[tau["isVTight"]>0])==1)
            #print("isVTight=", tau["isVTight"])
            #print("VTight Mask=", tau_2los_1tau_mask)
            tau_3l_0tau_mask   = (ak.num(tau[tau["isVLoose"]>0])==0)
            tau_3l_1tau_mask   = (ak.num(tau[tau["isVLoose"]>0])==1)
            tau_2los_2tau_mask = (ak.num(tau[tau["isVLoose"]>0])==2)
            tau_2lss_2tau_mask = (ak.num(tau[tau["isVLoose"]>0])==2)

            tau_Fake_mask   = (ak.num(tau)>0)
            no_tau_mask = (ak.num(tau[tau["isVLoose"]>0])==0)
            tau_VL_mask = (ak.num(tau[tau["isVLoose"]>0])==1)
            tau_L_mask  = (ak.num(tau[tau["isLoose"]>0]) ==1)
            tau_M_mask  = (ak.num(tau[tau["isMedium"]>0])==1)
            tau_T_mask  = (ak.num(tau[tau["isTight"]>0]) ==1)
            tau_VT_mask = (ak.num(tau[tau["isVTight"]>0])==1)

            if not isData:
                tau_emufake_mask = ((tau0.genPartFlav == 1) | (tau0.genPartFlav == 2) | (tau0.genPartFlav == 3) | (tau0.genPartFlav == 4))
                tau_jetfake_mask = ((tau0.genPartFlav == 0) | (tau0.genPartFlav == 6))
                tau_real_mask    = (tau0.genPartFlav == 5)

            ######### Store boolean masks with PackedSelection ##########

            selections = PackedSelection(dtype='uint64')

            # Lumi mask (for data)
            selections.add("is_good_lumi",lumi_mask)

            #selections.add("1l_2tau", (events.is1l & bmask_atleast1med_atleast2loose & pass_trg & bmask_atmost2med & tau_2lss_2tau_mask))

            # 2lss selection (drained of 4 top)
            #selections.add("2lss_p", (events.is2l & chargel0_p & bmask_atleast1med_atleast2loose & pass_trg & bmask_atmost2med))  # Note: The ss requirement has NOT yet been made at this point! We take care of it later with the appl axis
            #selections.add("2lss_m", (events.is2l & chargel0_m & bmask_atleast1med_atleast2loose & pass_trg & bmask_atmost2med))  # Note: The ss requirement has NOT yet been made at this point! We take care of it later with the appl axis
            ##selections.add("2lss_p_1tau_VL", (events.is2l & chargel0_p & bmask_atleast1med_atleast2loose & pass_trg & bmask_atmost2med & tau_VL_mask))
            ##selections.add("2lss_m_1tau_VL", (events.is2l & chargel0_m & bmask_atleast1med_atleast2loose & pass_trg & bmask_atmost2med & tau_VL_mask))
            ##selections.add("2lss_p_1tau_VT", (events.is2l & chargel0_p & bmask_atleast1med_atleast2loose & pass_trg & bmask_atmost2med & tau_VT_mask))
            ##selections.add("2lss_m_1tau_VT", (events.is2l & chargel0_m & bmask_atleast1med_atleast2loose & pass_trg & bmask_atmost2med & tau_VT_mask))
            selections.add("2lss_1tau_VL", (events.is2l & pass_trg & tau_VL_mask & charge2l_1))
            selections.add("2lss_1tau_VL_OStaul", (events.is2l & pass_trg & tau_VL_mask & charge2l_1 & taulepOS_mask))
            selections.add("2lss_1tau_VL_SStaul", (events.is2l & pass_trg & tau_VL_mask & charge2l_1 & ~taulepOS_mask))
            selections.add("2los_onZ_1tau", (events.is2l & charge2l_0 & sfosz_2l_mask & bmask_atleast1med_atleast2loose & pass_trg & tau_2los_1tau_mask))
            #selections.add("2los_offZ_1tau", (events.is2l & charge2l_0 & ~sfosz_2l_mask & bmask_atleast1med_atleast2loose & pass_trg & tau_2los_1tau_mask))
            #selections.add("2los_2tau", (events.is2l & charge2l_0 & bmask_atleast1med_atleast2loose & pass_trg & tau_2los_2tau_mask))
            #selections.add("2lss_2tau", (events.is2l & bmask_atleast1med_atleast2loose & pass_trg & tau_2lss_2tau_mask))

            # 2lss selection (enriched in 4 top)
            ##selections.add("2lss_4t_p", (events.is2l & chargel0_p & bmask_atleast1med_atleast2loose & pass_trg & bmask_atleast3med))  # Note: The ss requirement has NOT yet been made at this point! We take care of it later with the appl axis
            ##selections.add("2lss_4t_m", (events.is2l & chargel0_m & bmask_atleast1med_atleast2loose & pass_trg & bmask_atleast3med))  # Note: The ss requirement has NOT yet been made at this point! We take care of it later with the appl axis
		
            # 2lss selection for CR
            ##selections.add("2lss_CR", (events.is2l & (chargel0_p | chargel0_m) & bmask_exactly1med & pass_trg)) # Note: The ss requirement has NOT yet been made at this point! We take care of it later with the appl axis
            ##selections.add("2lss_CRflip", (events.is2l_nozeeveto & events.is_ee & sfasz_2l_mask & pass_trg)) # Note: The ss requirement has NOT yet been made at this point! We take care of it later with the appl axis, also note explicitly include the ee requirement here, so we don't have to rely on running with _split_by_lepton_flavor turned on to enforce this requirement

            # 2los selection
            ##selections.add("2los_CRtt", (events.is2l_nozeeveto & charge2l_0 & events.is_em & bmask_exactly2med & pass_trg)) # Explicitly add the em requirement here, so we don't have to rely on running with _split_by_lepton_flavor turned on to enforce this requirement
            ##selections.add("2los_CRZ", (events.is2l_nozeeveto & charge2l_0 & sfosz_2l_mask & bmask_exactly0med & pass_trg))
            #selections.add("2los_CRZ_VLtau", (events.is2l_nozeeveto & charge2l_0 & sfosz_2l_mask & bmask_exactly0med & pass_trg & tau_VL_mask))
            #selections.add("2los_CRZ_Ltau", (events.is2l_nozeeveto & charge2l_0 & sfosz_2l_mask & bmask_exactly0med & pass_trg & tau_L_mask))
            #selections.add("2los_CRZ_Mtau", (events.is2l_nozeeveto & charge2l_0 & sfosz_2l_mask & bmask_exactly0med & pass_trg & tau_M_mask))
            #selections.add("2los_CRZ_Ttau", (events.is2l_nozeeveto & charge2l_0 & sfosz_2l_mask & bmask_exactly0med & pass_trg & tau_T_mask))
            #selections.add("2los_CRZ_VTtau", (events.is2l_nozeeveto & charge2l_0 & sfosz_2l_mask & bmask_exactly0med & pass_trg & tau_VT_mask))
            #selections.add("2los_CR_VLtau", (events.is2l_nozeeveto & charge2l_0 & ~sfosz_2l_mask & bmask_exactly0med & pass_trg & tau_VL_mask))
            #selections.add("2los_CR", (events.is2l_nozeeveto & charge2l_0 & ~sfosz_2l_mask & bmask_exactly0med & pass_trg))
            #selections.add("2los_CR_Ltau", (events.is2l_nozeeveto & charge2l_0 & ~sfosz_2l_mask & bmask_exactly0med & pass_trg & tau_L_mask))
            #selections.add("2los_CR_Mtau", (events.is2l_nozeeveto & charge2l_0 & ~sfosz_2l_mask & bmask_exactly0med & pass_trg & tau_M_mask))
            #selections.add("2los_CR_Ttau", (events.is2l_nozeeveto & charge2l_0 & ~sfosz_2l_mask & bmask_exactly0med & pass_trg & tau_T_mask))
            #selections.add("2los_CR_VTtau", (events.is2l_nozeeveto & charge2l_0 & ~sfosz_2l_mask & bmask_exactly0med & pass_trg & tau_VT_mask))
            #selections.add("1l_1tau_CR", (events.is1l & tau_VL_mask))

            # 3l selection
            selections.add("3l_p_offZ_1b", (events.is3l & charge3l_p & ~sfosz_3l_mask & bmask_exactly1med & pass_trg & tau_3l_0tau_mask))
            selections.add("3l_m_offZ_1b", (events.is3l & charge3l_m & ~sfosz_3l_mask & bmask_exactly1med & pass_trg & tau_3l_0tau_mask))
            selections.add("3l_p_offZ_2b", (events.is3l & charge3l_p & ~sfosz_3l_mask & bmask_atleast2med & pass_trg & tau_3l_0tau_mask))
            selections.add("3l_m_offZ_2b", (events.is3l & charge3l_m & ~sfosz_3l_mask & bmask_atleast2med & pass_trg & tau_3l_0tau_mask))
            selections.add("3l_onZ_1b", (events.is3l & sfosz_3l_mask & bmask_exactly1med & pass_trg & tau_3l_0tau_mask))
            selections.add("3l_onZ_2b", (events.is3l & sfosz_3l_mask & bmask_atleast2med & pass_trg & tau_3l_0tau_mask))
            selections.add("3l_CR", (events.is3l & bmask_exactly0med & pass_trg))
            selections.add("3l_1tau_1b_VL", (events.is3l & bmask_exactly1med & pass_trg & tau_VL_mask))
            selections.add("3l_1tau_VL", (events.is3l & pass_trg & tau_VL_mask))
            selections.add("3l_1tau_2b_VL", (events.is3l & bmask_exactly2med & pass_trg & tau_VL_mask))
            selections.add("3l_1tau_1b_VT", (events.is3l & bmask_exactly1med & pass_trg & tau_VT_mask))
            selections.add("3l_1tau_2b_VT", (events.is3l & bmask_exactly2med & pass_trg & tau_VT_mask))

            # 4l selection
            selections.add("4l", (events.is4l & bmask_atleast1med_atleast2loose & pass_trg))

            if not isData:
                selections.add("tau_emufake", tau_emufake_mask)
                selections.add("tau_jetfake", tau_jetfake_mask)
                selections.add("tau_real", tau_real_mask)

            # Lep flavor selection
            selections.add("e",   events.is_e)
            selections.add("m",   events.is_m)
            selections.add("ee",  events.is_ee)
            selections.add("em",  events.is_em)
            selections.add("mm",  events.is_mm)
            selections.add("eee", events.is_eee)
            selections.add("eem", events.is_eem)
            selections.add("emm", events.is_emm)
            selections.add("mmm", events.is_mmm)
            selections.add("llll", (events.is_eeee | events.is_eeem | events.is_eemm | events.is_emmm | events.is_mmmm | events.is_gr4l)) # Not keepting track of these separately

            # Njets selection
            selections.add("exactly_0j", (njets==0))
            selections.add("exactly_1j", (njets==1))
            selections.add("exactly_2j", (njets==2))
            selections.add("exactly_3j", (njets==3))
            selections.add("exactly_4j", (njets==4))
            selections.add("exactly_5j", (njets==5))
            selections.add("exactly_6j", (njets==6))
            selections.add("atleast_1j", (njets>=1))
            selections.add("atleast_4j", (njets>=4))
            selections.add("atleast_5j", (njets>=5))
            selections.add("atleast_7j", (njets>=7))
            selections.add("atleast_0j", (njets>=0))
            selections.add("atmost_3j" , (njets<=3))

            selections.add("isSR_1l", (events.is1l))

            # AR/SR categories
            selections.add("isSR_2lSS",    ( events.is2l_SR) & charge2l_1) 
            selections.add("isAR_2lSS",    (~events.is2l_SR) & charge2l_1) 
            selections.add("isAR_2lSS_OS", ( events.is2l_SR) & charge2l_0) # Sideband for the charge flip
            selections.add("isSR_2lOS",    ( events.is2l_SR) & charge2l_0) 
            selections.add("isAR_2lOS",    (~events.is2l_SR) & charge2l_0)
            
            selections.add("isSR_3l",  events.is3l_SR)
            selections.add("isAR_3l", ~events.is3l_SR)
            selections.add("isSR_4l",  events.is4l_SR)


            ######### Variables for the dense axes of the hists ##########

            # Calculate ptbl
            ptbl_bjet = goodJets[(isBtagJetsMedium | isBtagJetsLoose)]
            ptbl_bjet = ptbl_bjet[ak.argmax(ptbl_bjet.pt,axis=-1,keepdims=True)] # Only save hardest b-jet
            ptbl_lep = l_fo_conept_sorted
            ptbl = (ptbl_bjet.nearest(ptbl_lep) + ptbl_bjet).pt
            ptbl = ak.values_astype(ak.fill_none(ptbl, -1), np.float32)

            # Z pt (pt of the ll pair that form the Z for the onZ categories)
            ptz = get_Z_pt(l_fo_conept_sorted_padded[:,0:3],10.0)

            # Leading (b+l) pair pt
            bjetsl = goodJets[isBtagJetsLoose][ak.argsort(goodJets[isBtagJetsLoose].pt, axis=-1, ascending=False)]
            bl_pairs = ak.cartesian({"b":bjetsl,"l":l_fo_conept_sorted})
            blpt = (bl_pairs["b"] + bl_pairs["l"]).pt
            bl0pt = ak.flatten(blpt[ak.argmax(blpt,axis=-1,keepdims=True)])

            # Collection of all objects (leptons and jets)
            l_j_collection = ak.with_name(ak.concatenate([l_fo_conept_sorted,goodJets], axis=1),"PtEtaPhiMCollection")

            # Leading object (j or l) pt
            o0pt = ak.max(l_j_collection.pt,axis=-1)

            # Pairs of l+j
            l_j_pairs = ak.combinations(l_j_collection,2,fields=["o0","o1"])
            l_j_pairs_pt = (l_j_pairs.o0 + l_j_pairs.o1).pt
            l_j_pairs_mass = (l_j_pairs.o0 + l_j_pairs.o1).mass
            lj0pt = ak.max(l_j_pairs_pt,axis=-1)

            # Define invariant mass hists
            mll_0_1 = (l0+l1).mass # Invmass for leading two leps


            '''
            with open('tau_lepton_pairs.txt', 'a') as output_file:
                # Write the header for the file
                output_file.write("Event | Tau | Pair | mvis_taulep | Total Charge | Lepton Flavor | Lepton origin | Tau pt | Tau eta | Tau phi | Lepton pt | Lepton eta | Lepton phi\n")
                output_file.write("-------------------------------------------------------------------------------------------------------------------------------------------------\n")
                
                # Loop over events
                for event_index in range(len(tau)):
        
                    # Loop over each tau within the event
                    for tau_index, tau_event in enumerate(tau[event_index]):
            
                        # Loop over the sorted leptons
                        for pair_index in range(len(l_fo_conept_sorted[event_index])):
                            lepton = l_fo_conept_sorted[event_index, pair_index]
                
                            # Check if either the tau or the lepton is None
                            if tau_event is None or lepton is None:
                                continue  # Skip this pair

                            # Compute the invariant mass
                            mvis = (tau_event + lepton).mass
                
                            # Compute the total charge
                            total_charge = tau_event.charge + lepton.charge
                
                            # Determine the lepton flavor using pdgId
                            if abs(lepton.pdgId) == 11:
                                lepton_flavor = "Electron"
                            elif abs(lepton.pdgId) == 13:
                                lepton_flavor = "Muon"
                            else:
                                lepton_flavor = "Unknown"

                            # Write the information to the file
                            output_file.write(f"{event_index} | {tau_index} | {pair_index} | {mvis:.4f} | {total_charge} | {lepton_flavor} | {lepton.genPartFlav} | {tau_event.pt:.4f} | {tau_event.eta:.4f} | {tau_event.phi:.4f} | {lepton.pt:.4f} | {lepton.eta:.4f} | {lepton.phi:.4f}\n")
            '''

            # ST (but "st" is too hard to search in the code, so call it ljptsum)
            ljptsum = ak.sum(l_j_collection.pt,axis=-1)
            if self._ecut_threshold is not None:
                ecut_mask = (ljptsum<self._ecut_threshold)

            # Counts
            counts = np.ones_like(events['event'])

            # Variables we will loop over when filling hists
            varnames = {}
            varnames["ht"]      = ht
            varnames["met"]     = met.pt
            varnames["puppimet"]= puppimet.pt
            varnames["ljptsum"] = ljptsum
            varnames["l0pt"]    = l0.conept
            varnames["l0eta"]   = l0.eta
            varnames["l1pt"]    = l1.conept
            varnames["l1eta"]   = l1.eta
            #varnames["j0pt"]    = ak.flatten(j0.pt)
            #varnames["j0eta"]   = ak.flatten(j0.eta)
            varnames["njets"]   = njets
            varnames["nbtagsl"] = nbtagsl
            varnames["invmass"] = mll_0_1
            varnames["ptbl"]    = ak.flatten(ptbl)
            varnames["ptz"]     = ptz
            varnames["b0pt"]    = ak.flatten(ptbl_bjet.pt)
            varnames["bl0pt"]   = bl0pt
            varnames["o0pt"]    = o0pt
            varnames["lj0pt"]   = lj0pt
            varnames["taupt"]   = tau0.pt
            varnames["nVLtau"]  = nVLtau 
            varnames["nLtau"]   = nLtau
            varnames["nMtau"]   = nMtau
            varnames["nTtau"]   = nTtau
            varnames["nVTtau"]  = nVTtau
            #print("met", ak.num(met.pt, axis=0), met.pt)
            ##print("mvis_gentaulep", ak.num(mvis_gentaulep, axis=0), mvis_gentaulep, ak.flatten(mvis_gentaulep))
            #print("mvis_taulep_dR0", ak.num(mvis_taulep_dR0, axis=0), mvis_taulep_dR0)
            varnames["mvis_gentaulep"] = ak.fill_none(mvis_gentaulep, -100) #ak.flatten(mvis_gentaulep)
            varnames["mvis_gentaulep0"] = ak.fill_none(mvis_gentaulep0, -100) #ak.flatten(mvis_gentaulep)
            varnames["mvis_nogentaulep0"] = ak.fill_none(mvis_nogentaulep0, -100) #ak.flatten(mvis_gentaulep)
            varnames["mvis_gentaulep1"] = ak.fill_none(mvis_gentaulep1, -100) #ak.flatten(mvis_gentaulep)
            varnames["mvis_nogentaulep1"] = ak.fill_none(mvis_nogentaulep1, -100) #ak.flatten(mvis_gentaulep)
            varnames["mvis_gentaulepc"] = ak.fill_none(mvis_gentaulepc, -100) #ak.flatten(mvis_gentaulep)
            varnames["mvis_nogentaulepc"] = ak.fill_none(mvis_nogentaulepc, -100) #ak.flatten(mvis_gentaulep)

            varnames["mvis_taulep0"] = mvis_taulep[:, 0]
            varnames["mvis_taulep1"] = mvis_taulep[:, 1]
            varnames["mvis_taulep_dR0"] = mvis_taulep_dR0

            varnames["M1T_taulep0"] = M1T_taulep0
            varnames["M1T_taulep1"] = M1T_taulep1
            varnames["M1T_taulep_dR0"] = M1T_taulep_dR0

            varnames["Mo1_taulep0"] = Mo1_taulep0
            varnames["Mo1_taulep1"] = Mo1_taulep1
            varnames["Mo1_taulep_dR0"] = Mo1_taulep_dR0


            varnames["puppiM1T_taulep0"] = puppiM1T_taulep0
            varnames["puppiM1T_taulep1"] = puppiM1T_taulep1
            varnames["puppiM1T_taulep_dR0"] = puppiM1T_taulep_dR0

            varnames["puppiMo1_taulep0"] = puppiMo1_taulep0
            varnames["puppiMo1_taulep1"] = puppiMo1_taulep1
            varnames["puppiMo1_taulep_dR0"] = puppiMo1_taulep_dR0

            ########## Fill the histograms ##########

            # This dictionary keeps track of which selections go with which SR categories
            sr_cat_dict = {
              #"1l" : {
              #    "exactly_3j" : {
              #        "lep_chan_lst" : ["1l_2tau"],
              #        "lep_flav_lst" : ["e", "m"],
              #        "appl_lst"     : ["isSR_1l"] + (["isAR_2lSS_OS"] if isData else []),
              #    },
              #    "exactly_4j" : {
              #        "lep_chan_lst" : ["1l_2tau"],
              #        "lep_flav_lst" : ["e", "m"],
              #        "appl_lst"     : ["isSR_1l"] + (["isAR_2lSS_OS"] if isData else []),
              #    },
              #    "exactly_5j" : {
              #        "lep_chan_lst" : ["1l_2tau"],
              #        "lep_flav_lst" : ["e", "m"],
              #        "appl_lst"     : ["isSR_1l"] + (["isAR_2lSS_OS"] if isData else []),
              #    },
              #    "exactly_6j" : {
              #        "lep_chan_lst" : ["1l_2tau"],
              #        "lep_flav_lst" : ["e", "m"],
              #        "appl_lst"     : ["isSR_1l"] + (["isAR_2lSS_OS"] if isData else []),
              #    },
              #    "atleast_7j" : {
              #        "lep_chan_lst" : ["1l_2tau"],
              #        "lep_flav_lst" : ["e", "m"],
              #        "appl_lst"     : ["isSR_1l"] + (["isAR_2lSS_OS"] if isData else []),
              #    },
              #},
              "2l" : {
                  #"exactly_2j" : {
                  #    "lep_chan_lst" :
                  #    #['2lss_1tau_VL'],
                  #    ["2lss_p" , "2lss_m", "2lss_4t_p", "2lss_4t_m", "2lss_p_1tau_VL", "2lss_m_1tau_VL", "2lss_p_1tau_VT", "2lss_m_1tau_VT", "2los_onZ_1tau", "2los_offZ_1tau"],#, "2los_2tau", "2lss_2tau"],
                  #    "lep_flav_lst" : ["ee" , "em" , "mm"],
                  #    "appl_lst"     : ["isSR_2lSS" , "isAR_2lSS", "isSR_2lOS"] + (["isAR_2lSS_OS"] if isData else []),
                  #},
                  #"exactly_3j" : {
                  #    "lep_chan_lst" : 
                  #    ["2lss_p" , "2lss_m", "2lss_4t_p", "2lss_4t_m", "2lss_p_1tau_VL", "2lss_m_1tau_VL", "2lss_p_1tau_VT", "2lss_m_1tau_VT", "2los_onZ_1tau", "2los_offZ_1tau"], #, "2los_2tau", "2lss_2tau"],
                  #    "lep_flav_lst" : ["ee" , "em" , "mm"],
                  #    "appl_lst"     : ["isSR_2lSS" , "isAR_2lSS", "isSR_2lOS"] + (["isAR_2lSS_OS"] if isData else []),
                  #},
                  #"exactly_4j" : {
                  #    "lep_chan_lst" : 
                  #    ["2lss_p" , "2lss_m", "2lss_4t_p", "2lss_4t_m", "2lss_p_1tau_VL", "2lss_m_1tau_VL", "2lss_p_1tau_VT", "2lss_m_1tau_VT", "2los_onZ_1tau", "2los_offZ_1tau"], #, "2los_2tau", "2lss_2tau"],
                  #    "lep_flav_lst" : ["ee" , "em" , "mm"],
                  #    "appl_lst"     : ["isSR_2lSS" , "isAR_2lSS", "isSR_2lOS"] + (["isAR_2lSS_OS"] if isData else []),
                  #},
                  #"exactly_5j" : {
                  #    "lep_chan_lst" : 
                  #    ["2lss_p" , "2lss_m", "2lss_4t_p", "2lss_4t_m", "2lss_p_1tau_VL", "2lss_m_1tau_VL", "2lss_p_1tau_VT", "2lss_m_1tau_VT", "2los_onZ_1tau", "2los_offZ_1tau"], #, "2los_2tau", "2lss_2tau"],
                  #    "lep_flav_lst" : ["ee" , "em" , "mm"],
                  #    "appl_lst"     : ["isSR_2lSS" , "isAR_2lSS", "isSR_2lOS"] + (["isAR_2lSS_OS"] if isData else []),
                  #},
                  #"exactly_6j" : {
                  #    "lep_chan_lst" : 
                  #    ["2lss_p" , "2lss_m", "2lss_4t_p", "2lss_4t_m", "2lss_p_1tau_VL", "2lss_m_1tau_VL", "2lss_p_1tau_VT", "2lss_m_1tau_VT", "2los_onZ_1tau", "2los_offZ_1tau"], #, "2los_2tau", "2lss_2tau"],
                  #    "lep_flav_lst" : ["ee" , "em" , "mm"],
                  #    "appl_lst"     : ["isSR_2lSS" , "isAR_2lSS", "isSR_2lOS"] + (["isAR_2lSS_OS"] if isData else []),
                  #},
                  #"atleast_7j" : {
                  #    "lep_chan_lst" : 
                  #    ["2lss_p" , "2lss_m", "2lss_4t_p", "2lss_4t_m", "2lss_p_1tau_VL", "2lss_m_1tau_VL", "2lss_p_1tau_VT", "2lss_m_1tau_VT", "2los_onZ_1tau", "2los_offZ_1tau"], #, "2los_2tau", "2lss_2tau"],
                  #    "lep_flav_lst" : ["ee" , "em" , "mm"],
                  #    "appl_lst"     : ["isSR_2lSS" , "isAR_2lSS", "isSR_2lOS"] + (["isAR_2lSS_OS"] if isData else []),
                  #},
                  "atleast_0j" : {
                      "lep_chan_lst" : ['2lss_1tau_VL', '2lss_1tau_VL_OStaul', '2lss_1tau_VL_SStaul'],
                      "lep_flav_lst" : ["ee" , "em" , "mm"],
                      "appl_lst"     : ["isSR_2lSS" , "isAR_2lSS", "isSR_2lOS"] + (["isAR_2lSS_OS"] if isData else []),
                  },
              },
              "3l" : {
                  #"exactly_2j" : {
                  #    "lep_chan_lst" : [
                  #        "3l_p_offZ_1b" , "3l_m_offZ_1b" , "3l_p_offZ_2b" , "3l_m_offZ_2b" , "3l_onZ_1b" , "3l_onZ_2b", "3l_1tau_1b_VL", "3l_1tau_2b_VL", "3l_1tau_1b_VT", "3l_1tau_2b_VT"
                  #    ],
                  #    "lep_flav_lst" : ["eee" , "eem" , "emm", "mmm"],
                  #    "appl_lst"     : ["isSR_3l", "isAR_3l"],
                  #},
                  #"exactly_3j" : {
                  #    "lep_chan_lst" : [
                  #        "3l_p_offZ_1b" , "3l_m_offZ_1b" , "3l_p_offZ_2b" , "3l_m_offZ_2b" , "3l_onZ_1b" , "3l_onZ_2b", "3l_1tau_1b_VL", "3l_1tau_2b_VL", "3l_1tau_1b_VT", "3l_1tau_2b_VT"
                  #    ],
                  #    "lep_flav_lst" : ["eee" , "eem" , "emm", "mmm"],
                  #    "appl_lst"     : ["isSR_3l", "isAR_3l"],
                  #},
                  #"exactly_4j" : {
                  #    "lep_chan_lst" : [
                  #        "3l_p_offZ_1b" , "3l_m_offZ_1b" , "3l_p_offZ_2b" , "3l_m_offZ_2b" , "3l_onZ_1b" , "3l_onZ_2b", "3l_1tau_1b_VL", "3l_1tau_2b_VL", "3l_1tau_1b_VT", "3l_1tau_2b_VT"
                  #    ],
                  #    "lep_flav_lst" : ["eee" , "eem" , "emm", "mmm"],
                  #    "appl_lst"     : ["isSR_3l", "isAR_3l"],
                  #},
                  #"atleast_5j" : {
                  #    "lep_chan_lst" : [
                  #        "3l_p_offZ_1b" , "3l_m_offZ_1b" , "3l_p_offZ_2b" , "3l_m_offZ_2b" , "3l_onZ_1b" , "3l_onZ_2b", "3l_1tau_1b_VL", "3l_1tau_2b_VL", "3l_1tau_1b_VT", "3l_1tau_2b_VT"
                  #    ],
                  #    "lep_flav_lst" : ["eee" , "eem" , "emm", "mmm"],
                  #    "appl_lst"     : ["isSR_3l", "isAR_3l"],
                  #},
                  #"atleast_0j" : {
                  #    "lep_chan_lst" : ["3l_1tau_VL"],
                  #    "lep_flav_lst" : ["eee" , "eem" , "emm", "mmm"],
                  #    "appl_lst"     : ["isSR_3l", "isAR_3l"],
                  #},
              },
              #"4l" : {
              #        "exactly_2j" : {
              #            "lep_chan_lst" : ["4l"],
              #            "lep_flav_lst" : ["llll"], # Not keeping track of these separately
              #            "appl_lst"     : ["isSR_4l"],
              #        },
              #        "exactly_3j" : {
              #            "lep_chan_lst" : ["4l"],
              #            "lep_flav_lst" : ["llll"], # Not keeping track of these separately
              #            "appl_lst"     : ["isSR_4l"],
              #        },
              #        "atleast_4j" : {
              #            "lep_chan_lst" : ["4l"],
              #            "lep_flav_lst" : ["llll"], # Not keeping track of these separately
              #            "appl_lst"     : ["isSR_4l"],
              #        },
              #},
            }
            # This dictionary keeps track of which selections go with which CR categories
            cr_cat_dict = {
              #"1l_1tau_CR": {
              #    "exactly_2j": {
              #        "lep_chan_lst" : ["1l_1tau_CR"],
              #        "lep_flav_lst" : ["e", "m"],
              #        "appl_lst"     : ["isSR_1l"] + (["isAR_2lSS_OS"] if isData else []),
              #    },
              #    "exactly_3j": {
              #        "lep_chan_lst" : ["1l_1tau_CR"],
              #        "lep_flav_lst" : ["e", "m"],
              #        "appl_lst"     : ["isSR_1l"] + (["isAR_2lSS_OS"] if isData else []),
              #    },
              #    "atleast_4j": {
              #        "lep_chan_lst" : ["1l_1tau_CR"],
              #        "lep_flav_lst" : ["e", "m"],
              #        "appl_lst"     : ["isSR_1l"] + (["isAR_2lSS_OS"] if isData else []),
              #    },
              #},
              ##"2l_CRflip" : {
              ##    "atmost_3j" : {
              ##        "lep_chan_lst" : ["2lss_CRflip"],
              ##        "lep_flav_lst" : ["ee"],
              ##        "appl_lst"     : ["isSR_2lSS" , "isAR_2lSS"] + (["isAR_2lSS_OS"] if isData else []),
              ##    },
              ##},
              ##"2l_CR" : {
              ##    "exactly_1j" : {
              ##        "lep_chan_lst" : ["2lss_CR"],
              ##        "lep_flav_lst" : ["ee" , "em" , "mm"],
              ##        "appl_lst"     : ["isSR_2lSS" , "isAR_2lSS"] + (["isAR_2lSS_OS"] if isData else []),
              ##    },
              ##    "exactly_2j" : {
              ##        "lep_chan_lst" : ["2lss_CR"],
              ##        "lep_flav_lst" : ["ee" , "em" , "mm"],
              ##        "appl_lst"     : ["isSR_2lSS" , "isAR_2lSS"] + (["isAR_2lSS_OS"] if isData else []),
              ##    },
              ##    "exactly_3j" : {
              ##        "lep_chan_lst" : ["2lss_CR"],
              ##        "lep_flav_lst" : ["ee" , "em" , "mm"],
              ##        "appl_lst"     : ["isSR_2lSS" , "isAR_2lSS"] + (["isAR_2lSS_OS"] if isData else []),
              ##    },
              ##},
              ##"3l_CR" : {
              ##    "exactly_0j" : {
              ##        "lep_chan_lst" : ["3l_CR"],
              ##        "lep_flav_lst" : ["eee" , "eem" , "emm", "mmm"],
              ##        "appl_lst"     : ["isSR_3l" , "isAR_3l"],
              ##    },
              ##    "atleast_1j" : {
              ##        "lep_chan_lst" : ["3l_CR"],
              ##        "lep_flav_lst" : ["eee" , "eem" , "emm", "mmm"],
              ##        "appl_lst"     : ["isSR_3l" , "isAR_3l"],
              ##    },
              ##},
            
              #"2los_CRtt" : {
              #    "exactly_2j"   : {
              #        "lep_chan_lst" : ["2los_CRtt"],
              #        "lep_flav_lst" : ["em"],
              #        "appl_lst"     : ["isSR_2lOS" , "isAR_2lOS"],
              #    },
                #},
              #"2los_CRZ" : {
              #    "atleast_0j"   : {
              #        "lep_chan_lst" : ["2los_CRZ"],#, "2los_CRZ_VLtau", "2los_CRZ_Ltau", "2los_CRZ_Mtau", "2los_CRZ_Ttau", "2los_CRZ_VTtau"],
              #        "lep_flav_lst" : ["ee", "mm"],
              #        "appl_lst"     : ["isSR_2lOS" , "isAR_2lOS"],
              #    },
                #},
              #"2los_CR" : {
              #    "atleast_0j"   : {
              #        "lep_chan_lst" : ["2los_CR_Ltau", "2los_CR_Mtau", "2los_CR_Ttau"],
              #        "lep_flav_lst" : ["ee", "mm"],
              #        "appl_lst"     : ["isSR_2lOS" , "isAR_2lOS"],
              #    },
              #},
            }

            # Include SRs and CRs unless we asked to skip them
            cat_dict = {}
            if not self._skip_signal_regions:
              cat_dict.update(sr_cat_dict)
            if not self._skip_control_regions:
              cat_dict.update(cr_cat_dict)
            if (not self._skip_signal_regions and not self._skip_control_regions):
              for k in sr_cat_dict:
                  if k in cr_cat_dict:
                      raise Exception(f"The key {k} is in both CR and SR dictionaries.")




            # Loop over the hists we want to fill
            for dense_axis_name, dense_axis_vals in varnames.items():
                if dense_axis_name not in self._hist_lst:
                    print(f"Skipping \"{dense_axis_name}\", it is not in the list of hists to include.")
                    continue

                # Set up the list of syst wgt variations to loop over
                wgt_var_lst = ["nominal"]
                if self._do_systematics:
                    if not isData:
                        if (syst_var != "nominal"):
                            # In this case, we are dealing with systs that change the kinematics of the objs (e.g. JES)
                            # So we don't want to loop over up/down weight variations here
                            wgt_var_lst = [syst_var]
                        else:
                            # Otherwise we want to loop over the up/down weight variations
                            wgt_var_lst = wgt_var_lst + wgt_correction_syst_lst + data_syst_lst
                    else:
                        # This is data, so we want to loop over just up/down variations relevant for data (i.e. FF up and down)
                        wgt_var_lst = wgt_var_lst + data_syst_lst

                # Loop over the systematics
                for wgt_fluct in wgt_var_lst:

                    # Loop over nlep categories "2l", "3l", "4l"
                    for nlep_cat in cat_dict.keys():

                        # Get the appropriate Weights object for the nlep cat and get the weight to be used when filling the hist
                        # Need to do this inside of nlep cat loop since some wgts depend on lep cat
                        weights_object = weights_dict[nlep_cat]
                        if (wgt_fluct == "nominal") or (wgt_fluct in obj_correction_syst_lst):
                            # In the case of "nominal", or the jet energy systematics, no weight systematic variation is used
                            weight = weights_object.weight(None)
                        else:
                            # Otherwise get the weight from the Weights object
                            if wgt_fluct in weights_object.variations:                                
                                weight = weights_object.weight(wgt_fluct)
                            else:
                                # Note in this case there is no up/down fluct for this cateogry, so we don't want to fill a hist for it
                                continue

                        # This is a check ot make sure we guard against any unintentional variations being applied to data
                        if self._do_systematics and isData:
                            # Should not have any up/down variations for data in 4l (since we don't estimate the fake rate there)
                            if nlep_cat == "4l":
                                if weights_object.variations != set([]): raise Exception(f"Error: Unexpected wgt variations for data! Expected \"{[]}\" but have \"{weights_object.variations}\".")
                            # In all other cases, the up/down variations should correspond to only the ones in the data list
                            else:
                                if weights_object.variations != set(data_syst_lst): raise Exception(f"Error: Unexpected wgt variations for data! Expected \"{set(data_syst_lst)}\" but have \"{weights_object.variations}\".")

                        # Get a mask for events that pass any of the njet requiremens in this nlep cat
                        # Useful in cases like njets hist where we don't store njets in a sparse axis
                        njets_any_mask = selections.any(*cat_dict[nlep_cat].keys())

                        # Loop over the njets list for each channel
                        for njet_val in cat_dict[nlep_cat].keys():

                            # Loop over the appropriate AR and SR for this channel
                            for appl in cat_dict[nlep_cat][njet_val]["appl_lst"]:

                                # We don't want or need to fill SR histos with the FF variations
                                if appl.startswith("isSR") and wgt_fluct in data_syst_lst: continue

                                # Loop over the channels in each nlep cat (e.g. "3l_m_offZ_1b")
                                for lep_chan in cat_dict[nlep_cat][njet_val]["lep_chan_lst"]:

                                    # Loop over the lep flavor list for each channel
                                    for lep_flav in cat_dict[nlep_cat][njet_val]["lep_flav_lst"]:
                                        
                                        tau_list = ["tau_emufake", "tau_jetfake", "tau_real", "all"]
                                        for tau_item in tau_list:

                                            # Construct the hist name
                                            flav_ch = None
                                            njet_ch = None
                                            cuts_lst = [appl,lep_chan]
                                            if isData:
                                                cuts_lst.append("is_good_lumi")
                                            if self._split_by_lepton_flavor:
                                                flav_ch = lep_flav
                                                cuts_lst.append(lep_flav)
                                            if dense_axis_name != "njets":
                                                njet_ch = njet_val
                                                cuts_lst.append(njet_val)
                                            ch_name = construct_cat_name(lep_chan,njet_str=njet_ch,flav_str=flav_ch)

                                            if (not isData) and ("tau" in ch_name) and (tau_item != "all"):
                                                cuts_lst.append(tau_item)
                                                temp_histAxisName = histAxisName
                                                histAxisName = histAxisName + "_" + tau_item

                                            # Get the cuts mask for all selections
                                            if dense_axis_name == "njets":
                                                all_cuts_mask = (selections.all(*cuts_lst) & njets_any_mask)
                                            else:
                                                all_cuts_mask = selections.all(*cuts_lst)

                                            # Apply the optional cut on energy of the event
                                            if self._ecut_threshold is not None:
                                                all_cuts_mask = (all_cuts_mask & ecut_mask)

                                            # Weights and eft coeffs
                                            weights_flat = weight[all_cuts_mask]
                                            eft_coeffs_cut = eft_coeffs[all_cuts_mask] if eft_coeffs is not None else None
                                            eft_w2_coeffs_cut = eft_w2_coeffs[all_cuts_mask] if eft_w2_coeffs is not None else None

                                            # Fill the histos
                                            axes_fill_info_dict = {
                                                dense_axis_name : dense_axis_vals[all_cuts_mask],
                                                "channel"       : ch_name,
                                                "appl"          : appl,
                                                "sample"        : histAxisName,
                                                "systematic"    : wgt_fluct,
                                                "weight"        : weights_flat,
                                                "eft_coeff"     : eft_coeffs_cut,
                                                "eft_err_coeff" : eft_w2_coeffs_cut,
                                            }

                                            #for ka, va in axes_fill_info_dict.items():
                                                #if ka == "channel":
                                                    #print(ka, va)

                                            if ("tau" in histAxisName):
                                                histAxisName = temp_histAxisName
                                            # Skip histos that are not defined (or not relevant) to given categories
                                            if ((("j0" in dense_axis_name) and ("lj0pt" not in dense_axis_name)) & (("CRZ" in ch_name) or ("CRflip" in ch_name))): continue
                                            if ((("j0" in dense_axis_name) and ("lj0pt" not in dense_axis_name)) & ("0j" in ch_name)): continue
                                            if (("ptz" in dense_axis_name) & ("onZ" not in lep_chan)): continue
                                            if ((dense_axis_name in ["o0pt","b0pt","bl0pt"]) & ("CR" in ch_name)): continue
                                            if ((("tau" in dense_axis_name)) and (("tau" not in ch_name))): continue
                                            #print("Filling", dense_axis_name)
                                            hout[dense_axis_name].fill(**axes_fill_info_dict)
                                        
                                            if (isData) or ("tau" not in ch_name): 
                                                break
                                             

                                        # Do not loop over lep flavors if not self._split_by_lepton_flavor, it's a waste of time and also we'd fill the hists too many times
                                        if not self._split_by_lepton_flavor: break

                            # Do not loop over njets if hist is njets (otherwise we'd fill the hist too many times)
                            if dense_axis_name == "njets": break

        return hout

    def postprocess(self, accumulator):
        return accumulator

if __name__ == '__main__':
    # Load the .coffea files
    outpath= './coffeaFiles/'
    samples     = load(outpath+'samples.coffea')
    topprocessor = AnalysisProcessor(samples)
