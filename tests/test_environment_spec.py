from pathlib import Path
import hashlib

UPSTREAM_DIGEST = "f48088dc786c70552262b198c56c39830b4ac1af30583eee1b559f5804ff9020"
UPSTREAM_SPEC = Path(__file__).resolve().parent / "data" / "ttbareft_coffea2025.yml"


def test_environment_spec_matches_ttbareft():
    env_path = Path(__file__).resolve().parents[1] / "environment.yml"

    upstream_bytes = UPSTREAM_SPEC.read_bytes()
    upstream_digest = hashlib.sha256(upstream_bytes).hexdigest()
    env_digest = hashlib.sha256(env_path.read_bytes()).hexdigest()

    assert upstream_digest == UPSTREAM_DIGEST, (
        "Stored ttbarEFT baseline drifted; refresh UPSTREAM_DIGEST to match the "
        f"updated spec (observed {upstream_digest})."
    )
    assert env_digest == upstream_digest, (
        "environment.yml drifted from the ttbarEFT coffea2025 specification. "
        "Update the file to match upstream or refresh the stored baseline." \
        f" Observed {env_digest}."
    )
