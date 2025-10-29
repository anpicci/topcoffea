import json
import os

import pytest

from topcoffea.modules.io_utils import (
    filter_lst_of_strs,
    get_files,
    load_sample_json_file,
    read_cfg_file,
    regex_match,
    update_cfg,
)


def test_regex_match_basic():
    files = ["foo.txt", "bar.root", "baz.log"]
    matches = regex_match(files, [r".*\\.root"])
    assert matches == ["bar.root"]


def test_filter_lst_of_strs_whitelist_blacklist():
    items = ["alpha", "alphabet", "beta", "gamma"]
    filtered = filter_lst_of_strs(items, substr_whitelist=["al"], substr_blacklist=["bet"])
    assert filtered == ["alpha"]


def test_get_files_matches(tmp_path):
    (tmp_path / "sub").mkdir()
    keep_file = tmp_path / "sub" / "keep.root"
    keep_file.write_text("root data")
    skip_file = tmp_path / "skip.txt"
    skip_file.write_text("skip")

    results = get_files(str(tmp_path), match_files=[r".*\\.root"], recursive=True)
    assert str(keep_file) in results
    assert str(skip_file) not in results


def test_load_and_update_cfg(tmp_path):
    sample_json = {
        "files": ["path//to/file.root"],
        "xsec": "1.0",
        "nEvents": "10",
        "nGenEvents": "5",
        "nSumOfWeights": "2.5",
    }
    json_path = tmp_path / "sample.json"
    with open(json_path, "w") as handle:
        json.dump(sample_json, handle)

    loaded = load_sample_json_file(str(json_path))
    assert loaded["files"] == ["path/to/file.root"]
    assert loaded["redirector"] is None
    assert isinstance(loaded["xsec"], float)
    assert isinstance(loaded["nEvents"], int)

    cfg = update_cfg(loaded, "sample", redirector="root://", max_files=1)
    assert cfg["sample"]["redirector"] == "root://"
    assert len(cfg["sample"]["files"]) == 1

    cfg_dir = tmp_path / "cfg"
    cfg_dir.mkdir()
    cfg_file = cfg_dir / "samples.cfg"
    # Copy the json into the cfg directory since read_cfg_file expects relative paths
    json_path_in_cfg = cfg_dir / json_path.name
    os.replace(json_path, json_path_in_cfg)
    with open(cfg_file, "w") as handle:
        handle.write(f"{json_path_in_cfg.name}\n")

    cfg_from_file = read_cfg_file(str(cfg_file), max_files=1)
    assert "sample" in cfg_from_file
    assert cfg_from_file["sample"]["files"] == ["path/to/file.root"]


def test_filter_lst_of_strs_invalid_types():
    with pytest.raises(Exception):
        filter_lst_of_strs(["valid", 5], substr_whitelist=["v"])
