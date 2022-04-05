## Summary of topeft processor

This document summarizes topeft, focusing on parts where we access info from the "events" object, or put new info into "events", or access additional external files (e.g. txt, csv, root)

* [L117](https://github.com/TopEFT/topcoffea/blob/3ba04eb74314f3a5ad10e2727522a386ebec3bca/analysis/topEFT/topeft.py#L117): Get dataset name from "events", accesses 1 column from events.metadata
* [L151-156](): Get the physics objects we care about from the “events” object, accesses 5 columns (E.g.: mu = events.Muon, note that the rest of the code mainly uses these copies, e.g. from here forward we use "mu", not “events.Muon”)
* [L158-162](): Calculate variables to be used in objects section, putting these into the "e" and "mu" objects (but this does not touch the "events" object)
* [L173](): Access 1 column from “events” (events.luminosityBlock) to get a mask that specifies "good” data taking conditions (note this uses an external txt file)
* [L179](): Access 1 column to get EFT coefficients (events["EFTfitCoefficients"])
* [L190-193](): Object selection for electrons (i.e. make make masks to specify electrons we want to keep), this uses the "e" object (not the "events.Electron" object)
* [L214-234](): Get weights for some of the scale factors and systematics
    * From the “events” object, we access 10 columns (events, genWeight, nominal/up/down weights for “L1PreFiringWeight” and “Pileup”, also “LHEScaleWeight” and “PSWeight”)
    * This step also puts 9 new columns into the events object
    * Then access 5 of these new columns
* [L243-802](): The rest of the processor is inside of a for loop over some systematics (so everything after this is repeated multiple times when running with systematics):
    * [L245-247](): Apply muon pt corrections, uses external txt files
    * [L249-252](): Muon object selection 
    * [L260](): Put in 1 column into “events” for an invariant mass cut we use later on
    * [L266-272](): Calculate lepton scale factors and fake rates (does not use “events”), note this accesses values from external root and json files
    * [L275-276](): Build collection of leptons we care about ("fakable leptons") from selected e and mu objects
    * [L287-343](): Jet selection: Access 2 columns from “events” (events.caches[0], events.fixedGridRhoFastjetAll), also use info from external txt files
    * [L349-350](): Next put 2 columns into “events” (the collection of leptons we care about (“l_fo_conept_sorted”) and number of jets “njets”) 
    * [L353-355](): Event selection: construct masks to keep track of which events pass, this accesses 3 columns from “events” 3 times (two of these we put in ourselves), and puts 19 columns (the masks related to the selections) into “events”
    * [L356](): Build masks for keeping track of the flavors of the leptons, accesses 1 column (which was put in ourselves), and puts 13 columns into events
    * [L375-378](): Get systematics for btag jets, uses external csv and pkl files
    * [L394](): Calculate trigger scale factors: Access 7 columns from “events” (all of which we put in ourselves), and puts in 3 columns
    * [L395-418](): More systematics and scale factors: Access 19 columns (all of these we put into “events” ourselves)
    * [L429](): Construct masks for the trigger selection: Accesses 2 columns (events.HLT and (unnecessarily) events.MET.pt)
    * [L451-515](): Using all of the information calculated above, construct the masks we will use for the final selection for each category: Access 20 columns from “events” (all of which are columns we have added ourselves)
    * [L705-801](): Finally we loop through categories, apply masks, fill histograms