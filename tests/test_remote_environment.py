import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from topcoffea.modules import remote_environment


def test_default_modules_pip_requirements():
    assert remote_environment.DEFAULT_MODULES["pip"] == [
        "coffea==2025.7.3",
        "awkward==2.8.7",
        "topcoffea",
    ]
