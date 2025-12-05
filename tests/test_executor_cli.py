import argparse

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


@pytest.mark.parametrize("executor", ("futures", "iterative", "taskvine", "TASKVINE"))
def test_executor_normalisation_accepts_supported(executor):
    config = executor_cli.executor_config_from_values(executor=executor)
    assert config.executor == executor.strip().lower()
    assert config.port == (
        executor_cli.parse_port_range(executor_cli.DEFAULT_PORT_RANGE)
        if config.executor == "taskvine"
        else None
    )


@pytest.mark.parametrize("executor", ("ddr", "work_queue", "taskvine_ddr", "unknown"))
def test_executor_normalisation_rejects_unsupported(executor):
    with pytest.raises(ValueError, match="Unsupported executor"):
        executor_cli.executor_config_from_values(executor=executor)


def test_executor_argument_help_and_validation():
    parser = argparse.ArgumentParser(prog="prog", add_help=False)
    executor_cli.register_executor_arguments(parser)

    # Help should list only the supported executors and reference TaskVine via DDR.
    help_text = parser.format_help()
    assert "futures" in help_text
    assert "iterative" in help_text
    assert "taskvine" in help_text
    assert "ddr" not in help_text
    assert "work_queue" not in help_text
    assert "TaskVine via DDR" in help_text

    args = parser.parse_args(["--executor", "taskvine"])
    assert args.executor == "taskvine"

    with pytest.raises(SystemExit):
        parser.parse_args(["--executor", "ddr"])
