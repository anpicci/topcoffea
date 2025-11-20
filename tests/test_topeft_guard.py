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
