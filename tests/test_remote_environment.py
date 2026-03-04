import subprocess
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from topcoffea.modules import remote_environment


def _create_topeft_repo(tmp_path: Path) -> Path:
    repo = tmp_path / "topeft"
    repo.mkdir()
    package_dir = repo / "topeft"
    package_dir.mkdir()
    (package_dir / "__init__.py").write_text("__version__ = '0.0.0'\n")
    (repo / "setup.py").write_text(
        "from setuptools import setup\n\nsetup(name='topeft', version='0.0.0')\n"
    )

    subprocess.check_call(["git", "init"], cwd=repo)
    subprocess.check_call(["git", "config", "user.email", "ci@example.com"], cwd=repo)
    subprocess.check_call(["git", "config", "user.name", "CI"], cwd=repo)
    subprocess.check_call(["git", "add", "."], cwd=repo)
    subprocess.check_call(["git", "commit", "-m", "initial"], cwd=repo)

    return repo


def _create_ddr_repo(tmp_path: Path) -> Path:
    repo = tmp_path / "dynamic_data_reduction"
    (repo / "src").mkdir(parents=True)
    (repo / "pyproject.toml").write_text(
        "[project]\nname = 'dynamic_data_reduction'\nversion = '0.0.0'\n",
        encoding="utf-8",
    )
    return repo


def test_default_modules_pins():
    conda_packages = remote_environment.DEFAULT_MODULES["conda"]["packages"]
    assert "coffea=2025.7.3" in conda_packages
    assert "awkward=2.8.7" in conda_packages
    assert "fsspec-xrootd" in conda_packages
    assert "pandas>=2.2,<2.3" in conda_packages
    assert "numpy>=2.3,<2.4" in conda_packages
    assert remote_environment.DEFAULT_MODULES["pip"] == ["topcoffea"]


def test_pip_local_to_watch_includes_topeft():
    assert remote_environment.PIP_LOCAL_TO_WATCH["topeft"] == [
        "topeft",
        "setup.py",
        "pyproject.toml",
        "poetry.lock",
        "requirements.txt",
        "setup.cfg",
        "environment.yml",
    ]


def test_commits_local_pip_detects_topeft_changes(tmp_path):
    repo = _create_topeft_repo(tmp_path)

    clean_commits = remote_environment._commits_local_pip({"topeft": str(repo)})
    assert clean_commits["topeft"] != "HEAD"

    init_file = repo / "topeft" / "__init__.py"
    init_file.write_text(init_file.read_text() + "# modified\n")

    dirty_commits = remote_environment._commits_local_pip({"topeft": str(repo)})
    assert dirty_commits["topeft"] == "HEAD"


def test_get_environment_rebuilds_on_topeft_changes(tmp_path, monkeypatch):
    repo = _create_topeft_repo(tmp_path)
    init_file = repo / "topeft" / "__init__.py"
    init_file.write_text(init_file.read_text() + "# modified\n")

    monkeypatch.setattr(remote_environment, "env_dir_cache", tmp_path / "envs")

    def fake_find_local_pip():
        return {"topeft": str(repo)}

    captured = {}

    def fake_create_env(env_name, spec, force=False):
        captured["force"] = force
        captured["env_name"] = env_name
        captured["spec"] = spec
        return env_name

    monkeypatch.setattr(remote_environment, "_find_local_pip", fake_find_local_pip)
    monkeypatch.setattr(remote_environment, "_create_env", fake_create_env)
    monkeypatch.setattr(remote_environment, "_clean_cache", lambda *args, **kwargs: None)

    env_name = remote_environment.get_environment(unstaged="rebuild")

    assert env_name == captured["env_name"]
    assert captured["force"] is True


def test_get_environment_reuses_cache(tmp_path, monkeypatch):
    repo = _create_topeft_repo(tmp_path)

    monkeypatch.setattr(remote_environment, "env_dir_cache", tmp_path / "envs")

    def fake_find_local_pip():
        return {"topeft": str(repo)}

    calls = []

    def fake_create_env(env_name, spec, force=False):
        calls.append({"env_name": env_name, "force": force})
        env_path = Path(env_name)
        env_path.parent.mkdir(parents=True, exist_ok=True)
        if not env_path.exists():
            env_path.write_text("first-build")
        else:
            env_path.write_text(env_path.read_text() + "|reused")
        return env_name

    monkeypatch.setattr(remote_environment, "_find_local_pip", fake_find_local_pip)
    monkeypatch.setattr(remote_environment, "_create_env", fake_create_env)
    monkeypatch.setattr(remote_environment, "_clean_cache", lambda *args, **kwargs: None)

    env_name_first = remote_environment.get_environment()
    env_name_second = remote_environment.get_environment()

    assert env_name_first == env_name_second
    assert calls[0]["force"] is False
    assert calls[1]["force"] is False
    assert Path(env_name_second).read_text().endswith("|reused")


def test_build_environment_spec_uses_local_paths_and_no_deps(tmp_path, monkeypatch):
    topeft_repo = _create_topeft_repo(tmp_path)
    ddr_repo = _create_ddr_repo(tmp_path)

    monkeypatch.setattr(remote_environment, "_safe_check_current_env", lambda spec: spec)
    monkeypatch.setattr(
        remote_environment,
        "_ensure_installed_pip_package",
        lambda spec, _package: spec,
    )

    spec, _watch_paths = remote_environment.build_environment_spec(
        extra_pip_local={
            "topeft": ["topeft", "setup.py"],
            "dynamic_data_reduction": [
                str(ddr_repo / "src"),
                str(ddr_repo / "pyproject.toml"),
            ],
        },
        editable_paths={
            "topeft": str(topeft_repo),
            "topcoffea": str(Path(__file__).resolve().parents[1]),
        },
    )

    pip_entries = spec["pip"]
    assert pip_entries[0] == "--no-deps"
    assert any(entry.startswith("topeft @ file://") for entry in pip_entries)
    assert any(
        entry.startswith("dynamic_data_reduction @ file://")
        for entry in pip_entries
    )
    assert any(entry.startswith("topcoffea @ file://") for entry in pip_entries)
    assert not any(entry == "topeft" for entry in pip_entries)
    assert not any(entry == "dynamic_data_reduction" for entry in pip_entries)


def test_build_environment_spec_keeps_required_conda_pins_once(monkeypatch):
    monkeypatch.setattr(remote_environment, "_safe_check_current_env", lambda spec: spec)
    monkeypatch.setattr(
        remote_environment,
        "_ensure_installed_pip_package",
        lambda spec, _package: spec,
    )
    monkeypatch.setattr(
        remote_environment,
        "resolve_local_pip_installs",
        lambda *_args, **_kwargs: {},
    )

    spec, _watch_paths = remote_environment.build_environment_spec(
        extra_conda=["pandas>=2.2,<2.3"],
        editable_paths={},
    )
    conda_entries = spec["conda"]["packages"]
    pandas_entries = [dep for dep in conda_entries if dep.startswith("pandas")]
    numpy_entries = [dep for dep in conda_entries if dep.startswith("numpy")]
    fsspec_entries = [dep for dep in conda_entries if dep.startswith("fsspec-xrootd")]
    assert len(pandas_entries) == 1
    assert len(numpy_entries) == 1
    assert len(fsspec_entries) == 1
