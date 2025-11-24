from topcoffea.modules import executor_cli


def test_run_coffea_runner_invokes_runner_in_expected_order():
    calls = {}

    class DummyProcessor:
        pass

    class DummyRunner:
        def __call__(self, fileset, processor_instance, treename):
            if not isinstance(processor_instance, DummyProcessor):
                raise TypeError("ProcessorABC check triggered")
            calls["fileset"] = fileset
            calls["processor"] = processor_instance
            calls["treename"] = treename
            return "ok"

    runner = DummyRunner()
    processor = DummyProcessor()

    result = executor_cli.run_coffea_runner(
        runner,
        {"/tmp/dummy.root": ["tree"]},
        processor,
        "Events",
    )

    assert result == "ok"
    assert calls["fileset"] == {"/tmp/dummy.root": ["tree"]}
    assert calls["processor"] is processor
    assert calls["treename"] == "Events"
