"""Ensure ``import topcoffea`` exposes downstream helpers."""

from __future__ import annotations


def test_modules_are_accessible_via_attribute() -> None:
    import topcoffea

    hist_eft = topcoffea.modules.HistEFT
    assert hasattr(hist_eft, "HistEFT"), "HistEFT module should expose the factory class"

    utils = topcoffea.modules.utils
    assert hasattr(utils, "get_files")


def test_scripts_are_accessible_via_attribute() -> None:
    import topcoffea

    script = topcoffea.scripts.make_html
    assert hasattr(script, "make_html")


def test_import_module_shim_is_available() -> None:
    import topcoffea

    module = topcoffea.import_module("topcoffea.modules.paths")
    assert hasattr(module, "topcoffea_path")
