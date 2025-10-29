"""Utilities for working with histogram dictionaries and yield summaries."""

from __future__ import annotations

import gzip
import pickle
from typing import Dict, Iterable, List, Mapping, Tuple

import cloudpickle


############## Floats manipulations and tools ##############

def get_pdiff(a, b, in_percent: bool = False):
    if (a is None) or (b is None):
        return None
    if b == 0:
        return None
    p = (float(a) - float(b)) / float(b)
    if in_percent:
        p *= 100.0
    return p


############## Pickle manipulations and tools ##############

def dump_to_pkl(out_name: str, out_file) -> None:
    if not out_name.endswith(".pkl.gz"):
        out_name = out_name + ".pkl.gz"
    print(f"\nSaving output to {out_name}...")
    with gzip.open(out_name, "wb") as fout:
        cloudpickle.dump(out_file, fout)
    print("Done.\n")


def get_hist_dict_non_empty(h: Mapping) -> Dict:
    return {k: v for k, v in h.items() if not v.empty()}


def get_hist_from_pkl(path_to_pkl: str, allow_empty: bool = True):
    h = pickle.load(gzip.open(path_to_pkl))
    if not allow_empty:
        h = get_hist_dict_non_empty(h)
    return h


############## Dictionary manipulations and tools ##############

def get_common_keys(dict1: Mapping, dict2: Mapping):
    common_lst: List = []
    unique_1_lst: List = []
    unique_2_lst: List = []

    for k1 in dict1.keys():
        if k1 in dict2.keys():
            common_lst.append(k1)
        else:
            unique_1_lst.append(k1)

    for k2 in dict2.keys():
        if k2 not in common_lst:
            unique_2_lst.append(k2)

    return [common_lst, unique_1_lst, unique_2_lst]


def dict_comp(in_dict1: Mapping[str, Iterable], in_dict2: Mapping[str, Iterable], strict: bool = False) -> bool:
    def all_d1_in_d2(d1, d2):
        agree = True
        for k1, v1 in d1.items():
            if k1 not in d2:
                agree = False
                break
            for i1 in v1:
                if i1 not in d2[k1]:
                    agree = False
                    break
        return agree

    dicts_match = all_d1_in_d2(in_dict1, in_dict2) and all_d1_in_d2(in_dict2, in_dict1)
    print_str = f"The two dictionaries do not agree.\n\tDict 1:{in_dict1}\n\tDict 2:{in_dict2}"

    if not dicts_match:
        if strict:
            raise Exception("Error: " + print_str)
        else:
            print("Warning: " + print_str)

    return dicts_match


def strip_errs(in_dict: Mapping[str, Mapping[str, Tuple]]) -> Dict[str, Dict[str, float]]:
    out_dict: Dict[str, Dict[str, float]] = {}
    for k in in_dict.keys():
        out_dict[k] = {}
        for subk in in_dict[k]:
            out_dict[k][subk] = in_dict[k][subk][0]
    return out_dict


def put_none_errs(in_dict: Mapping[str, Mapping[str, float]]) -> Dict[str, Dict[str, List]]:
    out_dict: Dict[str, Dict[str, List]] = {}
    for k in in_dict.keys():
        out_dict[k] = {}
        for subk in in_dict[k]:
            out_dict[k][subk] = [in_dict[k][subk], None]
    return out_dict


def print_yld_dicts(ylds_dict: Mapping[str, Mapping[str, Tuple]], tag: str, show_errs: bool = False, tolerance=None):
    ret = True
    print(f"\n--- {tag} ---\n")
    for proc in ylds_dict.keys():
        print(proc)
        for cat in ylds_dict[proc].keys():
            print(f"    {cat}")
            val, err = ylds_dict[proc][cat]

            if tolerance is None:
                if show_errs:
                    print(f"\t{val} +- {err} -> {err/val}")
                else:
                    print(f"\t{val}")
            else:
                if (val is None) or (abs(val) < abs(tolerance)):
                    print(f"\t{val}")
                else:
                    print(f"\t{val} -> NOTE: This is larger than tolerance ({tolerance})!")
                    ret = False
    return ret


def get_diff_between_nested_dicts(dict1: Mapping, dict2: Mapping, difftype: str, inpercent: bool = False):
    common_keys, d1_keys, d2_keys = get_common_keys(dict1, dict2)
    if len(d1_keys + d2_keys) > 0:
        print(f"\nWARNING, keys {d1_keys + d2_keys} are not in both dictionaries.")

    ret_dict: Dict = {}
    for k in common_keys:
        ret_dict[k] = get_diff_between_dicts(dict1[k], dict2[k], difftype, inpercent)

    return ret_dict


def get_diff_between_dicts(dict1: Mapping[str, Tuple], dict2: Mapping[str, Tuple], difftype: str, inpercent: bool = False):
    common_keys, d1_keys, d2_keys = get_common_keys(dict1, dict2)
    if len(d1_keys + d2_keys) > 0:
        print(f"\tWARNING, sub keys {d1_keys + d2_keys} are not in both dictionaries.")

    ret_dict: Dict[str, Tuple] = {}
    for k in common_keys:
        v1, e1 = dict1[k]
        v2, e2 = dict2[k]
        if difftype == "percent_diff":
            ret_diff = get_pdiff(v1, v2, in_percent=inpercent)
            ret_err = None
        elif difftype == "absolute_diff":
            ret_diff = v1 - v2
            if (e1 is not None) and (e2 is not None):
                ret_err = e1 - e2
            else:
                ret_err = None
        elif difftype == "sum":
            ret_diff = v1 + v2
            if (e1 is not None) and (e2 is not None):
                ret_err = e1 + e2
            else:
                ret_err = None
        else:
            raise Exception(f"Unknown diff type: {difftype}. Exiting...")

        ret_dict[k] = [ret_diff, ret_err]

    return ret_dict


__all__ = [
    "dict_comp",
    "dump_to_pkl",
    "get_common_keys",
    "get_diff_between_dicts",
    "get_diff_between_nested_dicts",
    "get_hist_dict_non_empty",
    "get_hist_from_pkl",
    "get_pdiff",
    "print_yld_dicts",
    "put_none_errs",
    "strip_errs",
]
