import hist
import numpy as np

from topcoffea.modules.histEFT import HistEFT


def _build_hist():
    histogram = HistEFT(
        hist.axis.StrCategory([], name="process", growth=True),
        hist.axis.Regular(2, 0.0, 2.0, name="observable"),
        wc_names=["ctG"],
        label="Events",
    )
    histogram.fill(
        process="signal",
        observable=np.array([0.2, 1.2], dtype=float),
        eft_coeff=np.array(
            [
                [1.0, 0.3, 0.2],
                [0.8, -0.1, 0.5],
            ],
            dtype=float,
        ),
    )
    histogram.fill(
        process="background",
        observable=np.array([0.2], dtype=float),
        weight=np.array([2.0], dtype=float),
    )
    return histogram


def test_eval_dict_and_array_are_equivalent():
    histogram = _build_hist()

    eval_dict = histogram.eval({"ctG": 0.25})
    eval_array = histogram.eval(np.array([0.25], dtype=float))

    assert np.allclose(eval_dict[("signal",)], eval_array[("signal",)])
    assert np.allclose(eval_dict[("background",)], eval_array[("background",)])


def test_as_hist_exposes_expected_axes():
    histogram = _build_hist()
    evaluated = histogram.as_hist({"ctG": -0.15})

    assert isinstance(evaluated, hist.Hist)
    assert list(evaluated.axes.name) == ["process", "observable"]


def test_group_preserves_integrated_total():
    histogram = _build_hist()
    grouped = histogram.group("process", {"all": ["signal", "background"]})

    total_before = histogram.integrate("process").eval({"ctG": 0.4})[()].sum()
    total_after = grouped.integrate("process").eval({"ctG": 0.4})[()].sum()

    assert np.isclose(total_before, total_after)
