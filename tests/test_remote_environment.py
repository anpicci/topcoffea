import copy
import io
import json
import sys
import subprocess
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


def _git(arguments, cwd):
    subprocess.check_call(["git", *arguments], cwd=cwd)


def _synthetic_watched_repository(tmp_path):
    repository = tmp_path / "repository"
    watched = repository / "package"
    watched.mkdir(parents=True)
    (watched / "tracked.py").write_text("value = 1\n", encoding="utf-8")
    (repository / "setup.py").write_text("# synthetic\n", encoding="utf-8")
    (repository / ".gitignore").write_text("package/ignored.txt\n", encoding="utf-8")
    _git(["init"], repository)
    _git(["config", "user.email", "test@example.invalid"], repository)
    _git(["config", "user.name", "Test User"], repository)
    _git(["add", "package/tracked.py", "setup.py", ".gitignore"], repository)
    _git(["commit", "-m", "initial"], repository)
    return repository


def test_watched_untracked_files_change_source_and_environment_identity(tmp_path, monkeypatch):
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[1]))
    from topcoffea.modules import remote_environment as remote

    repository = _synthetic_watched_repository(tmp_path)
    watched_paths = ["package", "setup.py"]
    baseline, baseline_dirty, baseline_untracked = remote._watched_source_fingerprint(
        str(repository), watched_paths
    )
    assert not baseline_dirty
    assert baseline_untracked == []

    generated = repository / "package" / "generated.py"
    generated.write_text("answer = 1\n", encoding="utf-8")
    added, added_dirty, added_untracked = remote._watched_source_fingerprint(str(repository), watched_paths)
    assert not added_dirty
    assert added != baseline
    assert added_untracked == [{"path": "package/generated.py", "sha256": remote._sha256_file(generated)}]

    monkeypatch.setattr(remote, "_check_current_env", lambda spec: spec)
    state = [{"package_name": "synthetic", "git_commit": "commit-a", "watched_source_fingerprint": baseline, "clean_or_dirty": "clean"}]
    monkeypatch.setattr(remote, "_editable_package_states", lambda _watch: state)
    baseline_request = remote.resolve_environment_request()
    state[0] = {**state[0], "watched_source_fingerprint": added}
    added_request = remote.resolve_environment_request()
    assert added_request["environment_fingerprint"] != baseline_request["environment_fingerprint"]

    generated.write_text("answer = 2\n", encoding="utf-8")
    modified, _, _ = remote._watched_source_fingerprint(str(repository), watched_paths)
    assert modified != added

    renamed = repository / "package" / "renamed.py"
    generated.rename(renamed)
    renamed_fingerprint, _, renamed_untracked = remote._watched_source_fingerprint(str(repository), watched_paths)
    assert renamed_fingerprint != modified
    assert renamed_untracked[0]["path"] == "package/renamed.py"

    renamed.unlink()
    restored, _, restored_untracked = remote._watched_source_fingerprint(str(repository), watched_paths)
    assert restored == baseline
    assert restored_untracked == []

    (repository / "package" / "small.json.gz").write_bytes(b"tiny gzip-like payload")
    payload_fingerprint, _, _ = remote._watched_source_fingerprint(str(repository), watched_paths)
    assert payload_fingerprint != baseline


def test_untracked_scope_ignored_rules_and_strict_staleness(tmp_path, monkeypatch):
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[1]))
    from topcoffea.modules import remote_environment as remote

    repository = _synthetic_watched_repository(tmp_path)
    watched_paths = ["package", "setup.py"]
    baseline, _, _ = remote._watched_source_fingerprint(str(repository), watched_paths)
    (repository / "outside.log").write_text("irrelevant\n", encoding="utf-8")
    outside, _, _ = remote._watched_source_fingerprint(str(repository), watched_paths)
    assert outside == baseline
    (repository / "package" / "ignored.txt").write_text("ignored\n", encoding="utf-8")
    ignored, _, ignored_files = remote._watched_source_fingerprint(str(repository), watched_paths)
    assert ignored == baseline
    assert ignored_files == []

    tracked = repository / "package" / "tracked.py"
    tracked.write_text("value = 2\n", encoding="utf-8")
    tracked_changed, dirty, untracked_files = remote._watched_source_fingerprint(str(repository), watched_paths)
    assert dirty
    assert not untracked_files
    assert tracked_changed != baseline

    package_state = {
        "package_name": "synthetic",
        "git_commit": "commit-a",
        "watched_source_fingerprint": baseline,
        "clean_or_dirty": "clean",
        "untracked_relevant_count": 1,
        "untracked_relevant_fingerprint": "untracked-a",
    }
    archive = tmp_path / "synthetic.tar.gz"
    _synthetic_archive(archive)
    old_request = _request("a" * 64)
    old_request["editable_packages"] = [package_state]
    remote.write_archive_manifest(str(archive), old_request)
    manifest = json.loads(Path(f"{archive}.manifest.json").read_text(encoding="utf-8"))
    assert manifest["editable_packages"][0]["untracked_relevant_count"] == 1
    assert manifest["editable_packages"][0]["untracked_relevant_fingerprint"] == "untracked-a"

    changed_request = _request("b" * 64)
    strict = remote.validate_environment_archive(str(archive), changed_request)
    assert strict["status"] == "stale"
    snapshot = remote.validate_environment_archive(str(archive), changed_request, snapshot=True)
    assert snapshot["usable"]
