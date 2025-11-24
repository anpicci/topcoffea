import copy
import sys
import types
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))
sys.modules.setdefault("coffea", types.SimpleNamespace(__version__="0.0"))

from topcoffea.modules.remote_environment import _sanitize_spec


def test_sanitize_spec_relaxes_unavailable_pip_pin():
    # Simulate a spec assembled from a host environment export with strict pins
    spec = {
        "conda": {
            "channels": ["conda-forge"],
            "packages": [
                "python=3.10.14=h955ad1f_0",
                "pip=25.1=py310h06a4308_0",
                "conda=24.7.1=h5eee18b_0",
            ],
        },
        "pip": ["topcoffea"],
    }

    sanitized = _sanitize_spec(copy.deepcopy(spec))

    # Ensure the pip constraint is relaxed to a conda-forge compatible range
    assert "pip>=24,<25" in sanitized["conda"]["packages"]
    # Build strings should be removed for conda packages
    assert "python=3.10.14" in sanitized["conda"]["packages"]
    # Original spec should remain unchanged
    assert "pip=25.1=py310h06a4308_0" in spec["conda"]["packages"]
