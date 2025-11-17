"""Pytest configuration used across the test-suite.

The goal is to make the tests hermetic: they should work even when the
surrounding environment does not provide the heavy scientific dependencies that
``topcoffea`` relies on.  We keep the logic here so every test file benefits
from it while remaining focused on its actual assertions.
"""

from __future__ import annotations

import sys
from pathlib import Path

# Ensure that ``import topcoffea`` works even when tests are executed from the
# repository root (or even its parent).  Pytest inserts the directory that
# contains this file into ``sys.path`` so we can leverage that to inject the
# project root as well.
_PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from topcoffea._ensure_deps import ensure_runtime_dependencies
from topcoffea._coffea_hist_shim import ensure_coffea_hist_module

# Install the scientific Python stack if it is missing.
ensure_runtime_dependencies()
ensure_coffea_hist_module()

