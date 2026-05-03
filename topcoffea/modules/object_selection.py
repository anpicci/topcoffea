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

_RUN3_NANOV12_JET_ID_MULTIPLICITY_FIELDS = (
    "chMultiplicity",
    "neMultiplicity",
)

_RUN3_NANOV12_JET_ID_TIGHT_RECIPE_FIELDS = (
    "eta",
    "jetId",
    "neHEF",
    "neEmEF",
)

_RUN3_NANOV12_JET_ID_TIGHT_LEPTON_VETO_RECIPE_FIELDS = (
    "eta",
    "jetId",
    "neHEF",
    "neEmEF",
    "muEF",
    "chEmEF",
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
    missing = _missing_run3_nanoV12_jet_id_fields(jets, _RUN3_NANOV12_JET_ID_FIELDS)
    if missing:
        raise ValueError(_run3_nanoV12_jet_id_missing_fields_message(jets, missing, ()))


def _missing_run3_nanoV12_jet_id_fields(jets, required_fields):
    fields = set(ak.fields(jets))
    return [field for field in required_fields if field not in fields]


def _run3_nanoV12_jet_id_missing_fields_message(jets, correctionlib_missing, recipe_missing):
    available = ak.fields(jets)
    parts = [
        "Run3 NanoV12 AK4PUPPI JetID could not be evaluated.",
        "Correctionlib path requires jet fields: "
        f"{', '.join(_RUN3_NANOV12_JET_ID_FIELDS)}.",
        f"Missing correctionlib fields: {', '.join(correctionlib_missing) or 'none'}.",
    ]
    if recipe_missing:
        parts.extend(
            [
                "Skim-compatible jetId recipe fallback was attempted because the "
                "charged/neutral multiplicity fields are unavailable, but it also "
                "requires recipe fields.",
                f"Missing recipe fields: {', '.join(recipe_missing)}.",
            ]
        )
    else:
        parts.append("Skim-compatible jetId recipe fallback was not applicable.")
    parts.append(f"Available fields: {', '.join(available) or 'none'}.")
    return " ".join(parts)


def _run3_nanoV12_ak4puppi_jet_id_correctionlib(jets, year, correction_key):
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


def _run3_nanoV12_ak4puppi_jet_id_recipe(jets, working_point):
    abs_eta = abs(jets.eta)
    tight_bit = (jets.jetId & (1 << 1)) != 0

    # NanoV12 skims can preserve the packed jetId decision while dropping the
    # charged/neutral multiplicities used by the correctionlib payload. Do not
    # substitute nConstituents: it is not equivalent to the separate charged and
    # neutral multiplicities, and payload probes show those inputs affect output.
    tight = tight_bit & (
        (abs_eta <= 2.7)
        | ((abs_eta > 2.7) & (abs_eta <= 3.0) & (jets.neHEF < 0.99))
        | ((abs_eta > 3.0) & (jets.neEmEF < 0.4))
    )

    if working_point == "tight":
        return tight

    return tight & (
        (abs_eta > 2.7)
        | ((jets.muEF < 0.8) & (jets.chEmEF < 0.8))
    )


def _run3_nanoV12_jet_id_recipe_fields(working_point):
    if working_point == "tight":
        return _RUN3_NANOV12_JET_ID_TIGHT_RECIPE_FIELDS
    return _RUN3_NANOV12_JET_ID_TIGHT_LEPTON_VETO_RECIPE_FIELDS


def run3_nanoV12_ak4puppi_jet_id(jets, year, working_point="tight"):
    """Return the Run3 NanoV12 AK4PUPPI JetID mask for the requested working point."""
    correction_key = _run3_nanoV12_jet_id_key(working_point)
    correctionlib_missing = _missing_run3_nanoV12_jet_id_fields(
        jets, _RUN3_NANOV12_JET_ID_FIELDS
    )
    if not correctionlib_missing:
        return _run3_nanoV12_ak4puppi_jet_id_correctionlib(jets, year, correction_key)

    missing_multiplicity = [
        field for field in _RUN3_NANOV12_JET_ID_MULTIPLICITY_FIELDS
        if field in correctionlib_missing
    ]
    if set(missing_multiplicity) == set(_RUN3_NANOV12_JET_ID_MULTIPLICITY_FIELDS):
        recipe_fields = _run3_nanoV12_jet_id_recipe_fields(working_point)
        recipe_missing = _missing_run3_nanoV12_jet_id_fields(jets, recipe_fields)
        if not recipe_missing:
            return _run3_nanoV12_ak4puppi_jet_id_recipe(jets, working_point)
        raise ValueError(
            _run3_nanoV12_jet_id_missing_fields_message(
                jets, correctionlib_missing, recipe_missing
            )
        )

    raise ValueError(
        _run3_nanoV12_jet_id_missing_fields_message(jets, correctionlib_missing, ())
    )
