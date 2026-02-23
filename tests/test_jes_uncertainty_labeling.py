import pytest

from topcoffea.modules.CorrectedJetsFactory import get_jec_uncertainty_label


@pytest.mark.parametrize(
    "junc_name,jec_tag,jet_algo,expected",
    [
        (
            "Summer19UL17_V5_MC_Regrouped_BBEC1_AK4PFchs",
            "Summer19UL17_V5_MC",
            "AK4PFchs",
            "BBEC1",
        ),
        (
            "Summer19UL17_V5_MC_Regrouped_Absolute_AK4PFchs",
            "Summer19UL17_V5_MC",
            "AK4PFchs",
            "Absolute",
        ),
        (
            "Summer19UL17_V5_MC_FlavorQCD_AK4PFchs",
            "Summer19UL17_V5_MC",
            "AK4PFchs",
            "FlavorQCD",
        ),
        (
            "Summer22_22Sep2023_V3_MC_Regrouped_Absolute_2022_AK4PFPuppi",
            "Summer22_22Sep2023_V3_MC",
            "AK4PFPuppi",
            "Regrouped_Absolute_2022",
        ),
        (
            "Summer22_22Sep2023_V3_MC_Regrouped_RelativeSample_2022_AK4PFPuppi",
            "Summer22_22Sep2023_V3_MC",
            "AK4PFPuppi",
            "Regrouped_RelativeSample_2022",
        ),
    ],
)
def test_get_jec_uncertainty_label(junc_name, jec_tag, jet_algo, expected):
    assert get_jec_uncertainty_label(junc_name, jec_tag, jet_algo) == expected


def test_run3_labels_are_unique_and_not_collapsed():
    jec_tag = "Summer22_22Sep2023_V3_MC"
    jet_algo = "AK4PFPuppi"
    junc_types = [
        "Regrouped_Absolute_2022",
        "Regrouped_Absolute",
        "Regrouped_BBEC1_2022",
        "Regrouped_BBEC1",
        "Regrouped_EC2_2022",
        "Regrouped_EC2",
        "Regrouped_FlavorQCD",
        "Regrouped_HF_2022",
        "Regrouped_HF",
        "Regrouped_RelativeBal",
        "Regrouped_RelativeSample_2022",
        "Regrouped_Total",
    ]
    full_names = [f"{jec_tag}_{junc}_{jet_algo}" for junc in junc_types]
    labels = [get_jec_uncertainty_label(name, jec_tag, jet_algo) for name in full_names]
    assert len(labels) == len(set(labels))
    assert "Regrouped_Absolute_2022" in labels
    assert "Regrouped_BBEC1_2022" in labels
    assert "2022" not in labels


def test_get_jec_uncertainty_label_rejects_bad_format():
    with pytest.raises(ValueError):
        get_jec_uncertainty_label(
            "Summer22_22Sep2023_V3_MC_Regrouped_Absolute_2022_AK4PFchs",
            "Summer22_22Sep2023_V3_MC",
            "AK4PFPuppi",
        )
