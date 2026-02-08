import numpy as np
import hist

from topcoffea.modules.histEFT import HistEFT


WC_NAMES = ["ctG"]
NEVENTS = 4
WEIGHT = 0.9

# Coefficients are ordered as: (sm, sm*ctG, ctG*ctG)
EFT_COEFFS = np.array(
    [
        [1.0, 0.2, 0.3],
        [0.8, -0.1, 0.5],
        [1.3, 0.7, -0.2],
        [0.9, 0.0, 0.4],
    ],
    dtype=float,
)


def _build_hist_pair(weighted=False):
    axis_type = hist.axis.StrCategory([], name="type", label="type", growth=True)
    axis_x = hist.axis.Regular(1, 0, 1, name="x", label="x")

    eft_hist = HistEFT(axis_type, axis_x, wc_names=WC_NAMES, label="Events")
    ref_hist = eft_hist.copy()

    xvals = np.full(NEVENTS, 0.5, dtype=float)
    if weighted:
        w = np.full(NEVENTS, WEIGHT, dtype=float)
        eft_hist.fill(type="eft", x=xvals, eft_coeff=EFT_COEFFS, weight=w)
        ref_hist.fill(type="non-eft", x=xvals, weight=w)
    else:
        eft_hist.fill(type="eft", x=xvals, eft_coeff=EFT_COEFFS)
        ref_hist.fill(type="non-eft", x=xvals)

    return eft_hist, ref_hist


def _coeff_bin(histogram, category):
    return histogram.integrate("type", category).view(as_dict=True)[()][0]


def test_add_preserves_eft_coefficients_and_non_eft_counts():
    eft_hist, ref_hist = _build_hist_pair(weighted=False)
    combined = eft_hist + ref_hist

    eft_expected = EFT_COEFFS.sum(axis=0)
    eft_observed = _coeff_bin(combined, "eft")
    assert np.allclose(eft_observed, eft_expected)

    non_eft_observed = _coeff_bin(combined, "non-eft")
    assert np.isclose(non_eft_observed[0], NEVENTS)
    assert np.allclose(non_eft_observed[1:], 0.0)


def test_weighted_add_scales_public_coefficients():
    eft_hist, ref_hist = _build_hist_pair(weighted=True)
    combined = eft_hist + ref_hist

    eft_expected = WEIGHT * EFT_COEFFS.sum(axis=0)
    eft_observed = _coeff_bin(combined, "eft")
    assert np.allclose(eft_observed, eft_expected)

    non_eft_observed = _coeff_bin(combined, "non-eft")
    assert np.isclose(non_eft_observed[0], WEIGHT * NEVENTS)
    assert np.allclose(non_eft_observed[1:], 0.0)


def test_group_uses_modern_sparsehist_signature():
    eft_hist, ref_hist = _build_hist_pair(weighted=True)
    combined = eft_hist + ref_hist

    grouped = combined.group("type", {"all": ["eft", "non-eft"]})

    total_before = combined.integrate("type").eval({"ctG": 0.3})[()].sum()
    total_after = grouped.integrate("type").eval({"ctG": 0.3})[()].sum()
    assert np.isclose(total_before, total_after)


def test_copy_reset_does_not_mutate_source_histogram():
    eft_hist, ref_hist = _build_hist_pair(weighted=True)
    source = eft_hist + ref_hist
    copied = source.copy()

    copied.reset()

    source_total = source.integrate("type").eval({"ctG": 0.2})[()].sum()
    copied_total = copied.integrate("type").eval({"ctG": 0.2})[()].sum()
    assert source_total > 0
    assert copied_total == 0


def test_quadratic_term_decomposition_matches_eval():
    eft_hist, _ = _build_hist_pair(weighted=False)
    integrated = eft_hist.integrate("type", "eft")
    coeffs = integrated.view(as_dict=True)[()][0]

    wc_value = 0.75
    sm_idx = integrated.quadratic_term_index("sm", "sm")
    lin_idx = integrated.quadratic_term_index("sm", "ctG")
    quad_idx = integrated.quadratic_term_index("ctG", "ctG")

    expected = coeffs[sm_idx] + coeffs[lin_idx] * wc_value + coeffs[quad_idx] * wc_value**2
    observed = integrated.eval({"ctG": wc_value})[()].sum()

    assert np.isclose(expected, observed)


def test_eval_mapping_and_array_inputs_are_equivalent():
    eft_hist, _ = _build_hist_pair(weighted=False)
    integrated = eft_hist.integrate("type", "eft")

    mapping_eval = integrated.eval({"ctG": 0.25})[()]
    array_eval = integrated.eval(np.array([0.25]))[()]

    assert np.allclose(mapping_eval, array_eval)
