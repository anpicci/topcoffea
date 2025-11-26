import correctionlib as clib
import correctionlib.schemav2 as schema

from topcoffea.modules.CorrectedJetsFactory import CorrectedJetsFactory
from topcoffea.modules.JECStack import JECStack


def _build_simple_cset(name: str) -> clib.CorrectionSet:
    correction = schema.Correction(
        name=name,
        description="",
        version=1,
        inputs=[schema.Variable(name="JetPt", type="real")],
        output=schema.Variable(name="weight", type="real"),
        data=schema.Formula(
            nodetype="formula",
            expression="x",
            parser="TFormula",
            variables=["JetPt"],
        ),
    )

    return clib.CorrectionSet.from_string(
        schema.CorrectionSet(schema_version=2, corrections=[correction]).json()
    )


def _build_test_stack(savecorr: bool = False) -> JECStack:
    corr_name = "TEST_L1_AK4PFchs"
    return JECStack(
        jec_tag="TEST",
        jec_levels=["L1"],
        jet_algo="AK4PFchs",
        correction_set=_build_simple_cset(corr_name),
        use_clib=True,
        savecorr=savecorr,
    )


def test_jecstack_savecorr_true_exposes_corrections():
    stack = _build_test_stack(savecorr=True)
    corr_name = "TEST_L1_AK4PFchs"

    assert corr_name in stack.corrections
    assert stack.corrections[corr_name].evaluate(5.0) == 5.0
    assert stack.correction_inputs == {"JetPt"}


def test_jecstack_default_behavior_retains_access():
    stack = _build_test_stack()
    corr_name = "TEST_L1_AK4PFchs"

    assert corr_name in stack.corrections
    assert stack.corrections[corr_name].evaluate(3.0) == 3.0


def test_corrected_jets_factory_loads_saved_clib_corrections():
    stack = _build_test_stack(savecorr=True)
    corr_name = "TEST_L1_AK4PFchs"

    name_map = {
        "JetPt": "pt",
        "JetEta": "eta",
        "JetPhi": "phi",
        "JetMass": "mass",
        "ptRaw": "pt_raw",
        "massRaw": "mass_raw",
    }

    factory = CorrectedJetsFactory(name_map, stack)
    factory.load_corrections_clib()

    assert corr_name in factory.corrections
    assert factory.corrections[corr_name].evaluate(7.0) == 7.0
