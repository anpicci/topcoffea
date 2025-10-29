"""Helpers for interacting with ROOT files via uproot."""

from __future__ import annotations

import uproot


def get_info(fname, tree_name="Events"):
    raw_events = 0
    gen_events = 0
    sow_events = 0
    sow_lhe_wgts = 0
    is_data = False
    print(f"Opening with uproot: {fname}")
    with uproot.open(fname) as f:
        tree = f[tree_name]
        is_data = "genWeight" not in tree

        raw_events = int(tree.num_entries)
        if is_data:
            gen_events = raw_events
            sow_events = raw_events
        else:
            gen_events = raw_events
            sow_events = sum(tree["genWeight"])
            if "Runs" in f:
                runs = f["Runs"]
                gen_key = "genEventCount" if "genEventCount" in runs else "genEventCount_"
                sow_key = "genEventSumw" if "genEventSumw" in runs else "genEventSumw_"
                gen_events = sum(runs[gen_key].array())
                sow_events = sum(runs[sow_key].array())

                sow_arr = runs[sow_key].array()
                LHEScaleSumw_arr = runs["LHEScaleSumw"].array()
                sow_lhe_wgts = sum(sow_arr * LHEScaleSumw_arr)

    return [raw_events, gen_events, sow_events, sow_lhe_wgts, is_data]


def get_list_of_wc_names(fname):
    wc_names_lst = []
    tree = uproot.open(f"{fname}:Events")
    if "WCnames" not in tree.keys():
        wc_names_lst = []
    else:
        wc_info = tree["WCnames"].array(entry_stop=1)[0]
        for idx, i in enumerate(wc_info):
            h = hex(i)[2:]
            wc_fragment = bytes.fromhex(h).decode("utf-8")
            if not wc_fragment.startswith("-"):
                wc_names_lst.append(wc_fragment)
            else:
                leftover = wc_fragment[1:]
                wc_names_lst[-1] = wc_names_lst[-1] + leftover
    return wc_names_lst


__all__ = ["get_info", "get_list_of_wc_names"]
