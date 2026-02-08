from __future__ import annotations

from pathlib import Path
import tomllib

import yaml


REFERENCE_SPEC = Path(__file__).resolve().parent / "data" / "ttbareft_coffea2025.yml"


def _load_yaml(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as fin:
        return yaml.safe_load(fin)


def _split_deps(spec: dict) -> tuple[list[str], list[str]]:
    conda_deps: list[str] = []
    pip_deps: list[str] = []
    for dep in spec.get("dependencies", []):
        if isinstance(dep, str):
            conda_deps.append(dep)
        elif isinstance(dep, dict) and "pip" in dep:
            pip_deps.extend(dep["pip"])
    return conda_deps, pip_deps


def _find_dep(entries: list[str], package: str) -> str | None:
    prefix = f"{package}="
    for dep in entries:
        dep_norm = dep.strip().strip("\"'")
        if dep_norm == package or dep_norm.startswith(prefix) or dep_norm.startswith(f"{package}>"):
            return dep_norm
    return None


def _pyproject_dependencies() -> list[str]:
    pyproject_path = Path(__file__).resolve().parents[1] / "pyproject.toml"
    with pyproject_path.open("rb") as fin:
        project = tomllib.load(fin)
    return list(project.get("project", {}).get("dependencies", []))


def test_environment_policy_requires_integration_intent():
    env_path = Path(__file__).resolve().parents[1] / "environment.yml"
    env_spec = _load_yaml(env_path)
    conda_deps, pip_deps = _split_deps(env_spec)

    assert _find_dep(conda_deps, "coffea") == "coffea=2025.7.3"
    assert _find_dep(conda_deps, "awkward") == "awkward=2.8.7"
    assert _find_dep(conda_deps, "numpy") == "numpy>=2.3,<2.4"

    ddr_dep = _find_dep(pip_deps, "dynamic_data_reduction")
    assert ddr_dep is not None
    assert ddr_dep.startswith("dynamic_data_reduction>=")


def test_environment_and_pyproject_dependency_alignment():
    env_path = Path(__file__).resolve().parents[1] / "environment.yml"
    env_spec = _load_yaml(env_path)
    conda_deps, pip_deps = _split_deps(env_spec)
    project_deps = _pyproject_dependencies()

    env_numpy = _find_dep(conda_deps + pip_deps, "numpy")
    pyproject_numpy = _find_dep(project_deps, "numpy")
    assert env_numpy == pyproject_numpy == "numpy>=2.3,<2.4"

    env_ddr = _find_dep(conda_deps + pip_deps, "dynamic_data_reduction")
    pyproject_ddr = _find_dep(project_deps, "dynamic_data_reduction")
    assert env_ddr is not None and pyproject_ddr is not None


def test_ttbareft_reference_is_informational_subset():
    env_path = Path(__file__).resolve().parents[1] / "environment.yml"
    env_spec = _load_yaml(env_path)
    ref_spec = _load_yaml(REFERENCE_SPEC)

    env_conda_deps, _ = _split_deps(env_spec)
    ref_conda_deps, _ = _split_deps(ref_spec)

    # Keep only core shared pins in sync; allow intentional integration drifts.
    for package in ("coffea", "awkward"):
        assert _find_dep(env_conda_deps, package) == _find_dep(ref_conda_deps, package)
