# Tools for object selection

import awkward as ak
import correctionlib
import numpy as np

from topcoffea.modules.paths import topcoffea_path


_RUN3_NANOV12_JET_ID_PAYLOADS = {
    "2022": "2022_Summer22",
    "2022EE": "2022_Summer22EE",
    "2023": "2023_Summer23",
    "2023BPix": "2023_Summer23BPix",
}

_RUN3_NANOV12_AK4PUPPI_JET_ID_KEYS = {
    "tight": "AK4PUPPI_Tight",
    "tight_lepton_veto": "AK4PUPPI_TightLeptonVeto",
}

_RUN3_NANOV12_JET_ID_FIELDS = (
    "eta",
    "chHEF",
    "neHEF",
    "chEmEF",
    "neEmEF",
    "muEF",
    "chMultiplicity",
    "neMultiplicity",
)


def is_tight_jet(pt, eta, jet_id, pt_cut, eta_cut, id_cut):
    mask = ((pt>pt_cut) & (abs(eta)<eta_cut) & (jet_id>id_cut))
    return mask


def _run3_nanoV12_jet_id_payload_path(year):
    try:
        payload_dir = _RUN3_NANOV12_JET_ID_PAYLOADS[year]
    except KeyError as exc:
        valid_years = ", ".join(sorted(_RUN3_NANOV12_JET_ID_PAYLOADS))
        raise ValueError(
            f"Run3 NanoV12 AK4PUPPI JetID is only defined for years: {valid_years}; "
            f"got {year!r}."
        ) from exc
    return topcoffea_path(f"data/POG/JME/{payload_dir}/jetid.json.gz")


def _run3_nanoV12_jet_id_key(working_point):
    try:
        return _RUN3_NANOV12_AK4PUPPI_JET_ID_KEYS[working_point]
    except KeyError as exc:
        valid_wps = ", ".join(sorted(_RUN3_NANOV12_AK4PUPPI_JET_ID_KEYS))
        raise ValueError(
            f"Unsupported Run3 NanoV12 AK4PUPPI JetID working point {working_point!r}; "
            f"supported values are: {valid_wps}."
        ) from exc


def _require_run3_nanoV12_jet_id_fields(jets):
    fields = set(ak.fields(jets))
    missing = [field for field in _RUN3_NANOV12_JET_ID_FIELDS if field not in fields]
    if missing:
        raise ValueError(
            "Run3 NanoV12 AK4PUPPI JetID requires jet fields: "
            f"{', '.join(_RUN3_NANOV12_JET_ID_FIELDS)}. Missing: {', '.join(missing)}."
        )


def run3_nanoV12_ak4puppi_jet_id(jets, year, working_point="tight"):
    """Return the Run3 NanoV12 AK4PUPPI JetID mask for the requested working point."""
    _require_run3_nanoV12_jet_id_fields(jets)

    correction_key = _run3_nanoV12_jet_id_key(working_point)
    payload_path = _run3_nanoV12_jet_id_payload_path(year)
    evaluator = correctionlib.CorrectionSet.from_file(payload_path)[correction_key]

    multiplicity = jets.chMultiplicity + jets.neMultiplicity
    counts = ak.num(jets.eta)
    # correctionlib does not support this jagged awkward input in the current env,
    # so evaluate flat arrays and restore the original per-event jet structure.
    inputs = [
        ak.to_numpy(ak.flatten(jets.eta)),
        ak.to_numpy(ak.flatten(jets.chHEF)),
        ak.to_numpy(ak.flatten(jets.neHEF)),
        ak.to_numpy(ak.flatten(jets.chEmEF)),
        ak.to_numpy(ak.flatten(jets.neEmEF)),
        ak.to_numpy(ak.flatten(jets.muEF)),
        ak.to_numpy(ak.flatten(jets.chMultiplicity)),
        ak.to_numpy(ak.flatten(jets.neMultiplicity)),
        ak.to_numpy(ak.flatten(multiplicity)),
    ]
    jet_id_flat = evaluator.evaluate(*inputs)
    return ak.unflatten(np.asarray(jet_id_flat, dtype=bool), counts)
