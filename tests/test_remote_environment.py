import copy
import io
import json
import sys
import tarfile
import types
from pathlib import Path

import pytest


def test_sanitize_spec_relaxes_unavailable_pip_pin(monkeypatch):
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[1]))
    monkeypatch.setitem(sys.modules, "coffea", types.SimpleNamespace(__version__="0.0"))

    from topcoffea.modules.remote_environment import _sanitize_spec

    # Simulate a spec assembled from a host environment export with strict pins
    spec = {
        "conda": {
            "channels": ["conda-forge"],
            "packages": [
                "python=3.10.14=h955ad1f_0",
                "pip=25.1=py310h06a4308_0",
                "conda=25.0=h5eee18b_0",
            ],
        },
        "pip": ["topcoffea"],
    }

    sanitized = _sanitize_spec(copy.deepcopy(spec))

    # Ensure the pip constraint is relaxed to a conda-forge compatible range
    assert "pip>=24,<25" in sanitized["conda"]["packages"]
    # Python pins should be relaxed to the major.minor ABI when host-specific patches are present
    assert "python=3.10" in sanitized["conda"]["packages"]
    # conda 25.x is not yet provided on conda-forge, so fall back to the supported series
    assert "conda>=24,<25" in sanitized["conda"]["packages"]
    # Build strings should be removed for conda packages
    assert all("=" not in pkg.split("=")[-1] for pkg in sanitized["conda"]["packages"] if "=" in pkg)
    # Original spec should remain unchanged
    assert "pip=25.1=py310h06a4308_0" in spec["conda"]["packages"]


def _synthetic_archive(path):
    with tarfile.open(path, "w:gz") as archive:
        payload = b"synthetic environment"
        info = tarfile.TarInfo("environment.txt")
        info.size = len(payload)
        archive.addfile(info, io.BytesIO(payload))


def _request(fingerprint="a" * 64, commit="commit-a", source="source-a"):
    return {
        "environment_fingerprint": fingerprint,
        "python_version": "3.11.0",
        "resolved_environment_spec": {"conda": {"packages": ["python=3.11"]}, "pip": ["topcoffea"]},
        "resolved_environment_spec_fingerprint": "spec-a",
        "editable_packages": [
            {"package_name": "topcoffea", "git_commit": commit, "watched_source_fingerprint": source, "clean_or_dirty": "clean"}
        ],
        "dirty_packages": [],
    }


def test_create_env_returns_path_after_successful_cache_miss(tmp_path, monkeypatch):
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[1]))
    from topcoffea.modules import remote_environment as remote

    target = tmp_path / "new.tar.gz"

    def create(_command, **_kwargs):
        _synthetic_archive(target)
        return b"created"

    monkeypatch.setattr(remote, "_check_current_env", lambda spec: spec)
    monkeypatch.setattr(remote.subprocess, "check_output", create)
    assert remote._create_env(str(target), {"pip": [], "conda": {"packages": []}}) == str(target)
    assert remote._create_env(str(target), {"pip": [], "conda": {"packages": []}}) == str(target)


def test_create_env_propagates_packaging_failure(tmp_path, monkeypatch):
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[1]))
    from topcoffea.modules import remote_environment as remote

    monkeypatch.setattr(remote, "_check_current_env", lambda spec: spec)
    failure = remote.subprocess.CalledProcessError(1, ["poncho_package_create"], output=b"synthetic failure")
    monkeypatch.setattr(remote.subprocess, "check_output", lambda *_args, **_kwargs: (_ for _ in ()).throw(failure))
    with pytest.raises(remote.subprocess.CalledProcessError):
        remote._create_env(str(tmp_path / "failed.tar.gz"), {"pip": [], "conda": {"packages": []}})


def test_manifest_validation_distinguishes_integrity_and_snapshot_compatibility(tmp_path, monkeypatch):
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[1]))
    from topcoffea.modules import remote_environment as remote

    archive = tmp_path / "synthetic.tar.gz"
    _synthetic_archive(archive)
    current = _request()
    remote.write_archive_manifest(str(archive), current)

    assert remote.validate_environment_archive(str(archive), current)["status"] == "valid"
    stale = remote.validate_environment_archive(str(archive), _request("b" * 64))
    assert stale["status"] == "stale"
    assert not stale["usable"]
    snapshot = remote.validate_environment_archive(str(archive), _request("b" * 64), snapshot=True)
    assert snapshot["status"] == "stale"
    assert snapshot["usable"]

    archive.write_bytes(b"not a tarball")
    invalid = remote.validate_environment_archive(str(archive), current, snapshot=True)
    assert invalid["status"] == "invalid_archive"
    assert not invalid["usable"]


def test_missing_manifest_is_snapshot_only_unverifiable(tmp_path, monkeypatch):
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[1]))
    from topcoffea.modules import remote_environment as remote

    archive = tmp_path / "unmanifested.tar.gz"
    _synthetic_archive(archive)
    assert not remote.validate_environment_archive(str(archive), _request())["usable"]
    snapshot = remote.validate_environment_archive(str(archive), _request(), snapshot=True)
    assert snapshot["status"] == "unverifiable"
    assert snapshot["usable"]


def test_effective_fingerprint_includes_resolved_spec_and_editable_source(monkeypatch):
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[1]))
    from topcoffea.modules import remote_environment as remote

    monkeypatch.setattr(remote, "_check_current_env", lambda spec: spec)
    states = [{"package_name": "topcoffea", "git_commit": "one", "watched_source_fingerprint": "source-one", "clean_or_dirty": "clean"}]
    monkeypatch.setattr(remote, "_editable_package_states", lambda _watch: states)
    first = remote.resolve_environment_request(extra_pip=["example=1"])
    states[0] = {**states[0], "watched_source_fingerprint": "source-two"}
    second = remote.resolve_environment_request(extra_pip=["example=1"])
    third = remote.resolve_environment_request(extra_pip=["example=2"])
    assert first["environment_fingerprint"] != second["environment_fingerprint"]
    assert second["environment_fingerprint"] != third["environment_fingerprint"]
