import pytest

from topcoffea.modules import executor_cli


def test_executor_config_auto_environment(monkeypatch):
    calls = {}

    def fake_get_environment(*, extra_pip_local, **_):
        calls["extra_pip_local"] = extra_pip_local
        return "/tmp/env.tar.gz"

    monkeypatch.setattr(
        executor_cli.remote_environment,
        "get_environment",
        fake_get_environment,
    )

    config = executor_cli.executor_config_from_values(
        executor="taskvine",
        environment_file=executor_cli.TASKVINE_ENVIRONMENT_AUTO,
    )

    assert config.environment_file == "/tmp/env.tar.gz"
    assert "topeft" in calls["extra_pip_local"]
    assert calls["extra_pip_local"]["topeft"] == list(
        executor_cli.TASKVINE_EXTRA_PIP_LOCAL["topeft"]
    )


def test_executor_config_merges_extra_local(monkeypatch):
    seen = {}

    def fake_get_environment(*, extra_pip_local, **_):
        seen.update(extra_pip_local)
        return "env.tar.gz"

    monkeypatch.setattr(
        executor_cli.remote_environment,
        "get_environment",
        fake_get_environment,
    )

    config = executor_cli.executor_config_from_values(
        executor="taskvine",
        environment_file=executor_cli.TASKVINE_ENVIRONMENT_AUTO,
        extra_pip_local={"package": ("pyproject.toml",)},
    )

    assert config.environment_file == "env.tar.gz"
    assert seen["package"] == ["pyproject.toml"]
    assert "topeft" in seen


def test_executor_config_skips_environment_for_futures(monkeypatch):
    def fake_get_environment(**_):
        raise AssertionError("get_environment should not be called for futures")

    monkeypatch.setattr(
        executor_cli.remote_environment,
        "get_environment",
        fake_get_environment,
    )

    config = executor_cli.executor_config_from_values(
        executor="futures",
        environment_file=executor_cli.TASKVINE_ENVIRONMENT_AUTO,
    )

    assert config.environment_file is None


def test_executor_alias_normalisation():
    config = executor_cli.executor_config_from_values(
        executor="work_queue",
    )
    assert config.executor == "taskvine"
