"""Utility helpers for filesystem access and configuration loading."""

from __future__ import annotations

import json
import os
import re
from typing import Dict, Iterable, List, Optional

pjoin = os.path.join


############## Strings manipulations and tools ##############

def regex_match(lst: Iterable[str], regex_lst: Iterable[str]) -> List[str]:
    """Return the subset of *lst* that matches any of the provided regex patterns."""

    patterns = list(regex_lst)
    if len(patterns) == 0:
        return list(lst)

    def _normalize(pattern: str) -> str:
        try:
            # ``unicode_escape`` interprets sequences such as ``\\.`` into the
            # expected ``\.``, which matches the behaviour that most of the
            # legacy configuration files relied on.
            return pattern.encode().decode("unicode_escape")
        except UnicodeDecodeError:
            return pattern

    compiled = [re.compile(_normalize(str(pattern))) for pattern in patterns]
    matches: List[str] = []
    for candidate in lst:
        for pat in compiled:
            if pat.search(candidate) is not None:
                matches.append(candidate)
                break
    return matches


def filter_lst_of_strs(
    in_lst: Iterable[str],
    substr_whitelist: Optional[Iterable[str]] = None,
    substr_blacklist: Optional[Iterable[str]] = None,
) -> List[str]:
    """Filter *in_lst* by requiring whitelist substrings and rejecting blacklist substrings."""

    substr_whitelist = [] if substr_whitelist is None else list(substr_whitelist)
    substr_blacklist = [] if substr_blacklist is None else list(substr_blacklist)

    if not (
        all(isinstance(x, str) for x in in_lst)
        and all(isinstance(x, str) for x in substr_whitelist)
        and all(isinstance(x, str) for x in substr_blacklist)
    ):
        raise Exception(
            "Error: This function only filters lists of strings, one of the elements in one of the input lists is not a str."
        )

    for elem in substr_whitelist:
        if elem in substr_blacklist:
            raise Exception(f"Error: Cannot whitelist and blacklist the same element (\"{elem}\").")

    out_lst: List[str] = []
    for element in in_lst:
        blacklisted = False
        whitelisted = True
        for substr in substr_blacklist:
            if substr in element:
                blacklisted = True
        for substr in substr_whitelist:
            if substr not in element:
                whitelisted = False
        if whitelisted and not blacklisted:
            out_lst.append(element)

    return out_lst


############## Dirs and root files manipulations and tools ##############

def get_files(top_dir: str, **kwargs) -> List[str]:
    """Walk a directory tree searching for matching files.

    The recognised keyword arguments are:

    ``ignore_dirs``
        Regex patterns describing directories to be skipped.
    ``match_files``
        Regex patterns describing which files to keep.
    ``ignore_files``
        Regex patterns describing files to skip.
    ``recursive``
        Whether to recursively walk sub-directories.
    ``verbose``
        Emit verbose logging describing the traversal.
    """

    ignore_dirs = kwargs.pop("ignore_dirs", [])
    match_files = kwargs.pop("match_files", [])
    ignore_files = kwargs.pop("ignore_files", [])
    recursive = kwargs.pop("recursive", False)
    verbose = kwargs.pop("verbose", False)

    found: List[str] = []
    if verbose:
        print(f"Searching in {top_dir}")
        print(f"\tRecurse: {recursive}")
        print(f"\tignore_dirs: {ignore_dirs}")
        print(f"\tmatch_files: {match_files}")
        print(f"\tignore_files: {ignore_files}")
    for root, dirs, files in os.walk(top_dir):
        if recursive:
            if ignore_dirs:
                dir_matches = regex_match(dirs, regex_lst=ignore_dirs)
                for match in dir_matches:
                    if verbose:
                        print(f"\tSkipping directory: {match}")
                    dirs.remove(match)
        else:
            dirs.clear()
        files = regex_match(files, match_files)
        if ignore_files:
            file_matches = regex_match(files, regex_lst=ignore_files)
            for match in file_matches:
                if verbose:
                    print(f"\tSkipping file: {match}")
                files.remove(match)
        for fname in files:
            fpath = os.path.join(root, fname)
            found.append(fpath)
    return found


def move_files(files: Iterable[str], target: str) -> None:
    width = len(max(files, key=len)) if files else 0
    _ = width  # The width is kept for backwards compatibility with previous behaviour.
    for src in files:
        dst = os.path.join(target, src)
        os.rename(src, dst)


def clean_dir(tdir: str, targets: Iterable[str], dry_run: bool = False) -> None:
    fnames = regex_match(get_files(tdir), targets)
    if len(fnames) == 0:
        return
    print(f"Removing files from: {tdir}")
    print(f"\tTargets: {targets}")
    for fn in fnames:
        fpath = os.path.join(tdir, fn)
        if not dry_run:
            print(f"\tRemoving {fn}")
            os.remove(fpath)
        else:
            print(f"\tRemoving {fpath}")


############## Jason and config manipulations and tools ##############

def load_sample_json_file(fpath: str) -> Dict[str, object]:
    if not os.path.exists(fpath):
        raise RuntimeError(f"fpath '{fpath}' does not exist!")
    with open(fpath) as f:
        jsn = json.load(f)
    jsn["redirector"] = None
    for i, fn in enumerate(jsn["files"]):
        fn = fn.replace("//", "/")
        jsn["files"][i] = fn
    jsn["xsec"] = float(jsn["xsec"])
    jsn["nEvents"] = int(jsn["nEvents"])
    jsn["nGenEvents"] = int(jsn["nGenEvents"])
    jsn["nSumOfWeights"] = float(jsn["nSumOfWeights"])
    return jsn


def update_cfg(jsn: Dict[str, object], name: str, **kwargs) -> Dict[str, object]:
    cfg = kwargs.pop("cfg", {})
    max_files = kwargs.pop("max_files", 0)
    cfg[name] = {}
    cfg[name].update(jsn)
    if max_files:
        del cfg[name]["files"][max_files:]
    for key, value in kwargs.items():
        cfg[name][key] = value
    return cfg


def read_cfg_file(fpath: str, cfg: Optional[Dict[str, object]] = None, max_files: int = 0) -> Dict[str, object]:
    cfg_dir, fname = os.path.split(fpath)
    if not cfg_dir:
        raise RuntimeError(f"No cfg directory in {fpath}")
    if not os.path.exists(cfg_dir):
        raise RuntimeError(f"{cfg_dir} does not exist!")
    xrd_src = None
    cfg = {} if cfg is None else cfg
    with open(fpath) as f:
        print(" >> Reading json from cfg file...")
        for line in f:
            line = line.strip().split("#")[0]
            if not len(line):
                continue
            if line.startswith("root:") or line.startswith("http:") or line.startswith("https:"):
                xrd_src = line
            elif line.startswith("file://"):
                xrd_src = line.replace("file://", "")
            else:
                sample = os.path.basename(line)
                sample = sample.replace(".json", "")
                full_path = pjoin(cfg_dir, line)
                jsn = load_sample_json_file(full_path)
                cfg = update_cfg(jsn, sample, cfg=cfg, max_files=max_files, redirector=xrd_src)
    return cfg


__all__ = [
    "clean_dir",
    "filter_lst_of_strs",
    "get_files",
    "load_sample_json_file",
    "move_files",
    "read_cfg_file",
    "regex_match",
    "update_cfg",
]
