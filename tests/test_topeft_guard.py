import importlib.util
from pathlib import Path

import pytest

import topcoffea


def test_vendored_import_raises_runtime_error(tmp_path):
    source_init = Path(topcoffea.__file__).resolve()
    vendored_init = tmp_path / "topeft" / "topcoffea" / "__init__.py"
    vendored_init.parent.mkdir(parents=True)
    vendored_init.write_text(source_init.read_text())

    spec = importlib.util.spec_from_file_location("vendored_topcoffea", vendored_init)
    assert spec and spec.loader

    vendored_module = importlib.util.module_from_spec(spec)

    with pytest.raises(RuntimeError, match="vendored copy inside a topeft checkout"):
        spec.loader.exec_module(vendored_module)


def test_non_vendored_topeft_parent_allows_import(tmp_path):
    source_init = Path(topcoffea.__file__).resolve()
    non_vendored_init = (
        tmp_path / "topeft" / "env" / "site-packages" / "topcoffea" / "__init__.py"
    )
    non_vendored_init.parent.mkdir(parents=True)
    non_vendored_init.write_text(source_init.read_text())

    spec = importlib.util.spec_from_file_location(
        "nonvendored_topcoffea", non_vendored_init
    )
    assert spec and spec.loader

    non_vendored_module = importlib.util.module_from_spec(spec)

    spec.loader.exec_module(non_vendored_module)

    assert non_vendored_module.__name__ == "nonvendored_topcoffea"
