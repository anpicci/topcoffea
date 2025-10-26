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


def test_default_modules_pip_requirements():
    assert remote_environment.DEFAULT_MODULES["pip"] == [
        "coffea==2025.7.3",
        "awkward==2.8.7",
        "topcoffea",
    ]


def test_pip_local_to_watch_includes_topeft():
    assert remote_environment.PIP_LOCAL_TO_WATCH["topeft"] == [
        "topeft",
        "setup.py",
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
