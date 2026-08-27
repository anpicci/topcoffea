# Physics interface extension guides

Use this page when changing a reusable correction, selection, EFT, or
histogram mechanism in `topcoffea`. Enabling that mechanism for a concrete
`topeft` era, sample, working point, region, or variation is a separate
analysis-policy change documented by the consumer.

## Canonical authorities

- Shared correction and weight helpers:
  [`topcoffea/modules/corrections.py`](../topcoffea/modules/corrections.py)
- Reusable object and event helpers:
  [`object_selection.py`](../topcoffea/modules/object_selection.py) and
  [`event_selection.py`](../topcoffea/modules/event_selection.py)
- JEC and corrected-object construction:
  [`JECStack.py`](../topcoffea/modules/JECStack.py),
  [`CorrectedJetsFactory.py`](../topcoffea/modules/CorrectedJetsFactory.py), and
  the current Type-1 helper in
  [`corrections.py`](../topcoffea/modules/corrections.py)
- Shared numeric/default data:
  [`params.json`](../topcoffea/params/params.json) and
  [`topcoffea/data`](../topcoffea/data)
- EFT and histogram mechanism:
  [`eft_helper.py`](../topcoffea/modules/eft_helper.py),
  [`histEFT.py`](../topcoffea/modules/histEFT.py), and
  [`sparseHist.py`](../topcoffea/modules/sparseHist.py)

Start from the current signature and a focused test. Do not create a second
default map in a consuming analysis.

## Add or update a correction or payload family

1. Identify the public evaluator, input schema, output shape, central/variation
   convention, and packaged payload selector.
2. Put shared payload data under the existing package-data family and resolve
   it with `topcoffea_path`; do not depend on a caller working directory.
3. Preserve the correctionlib NumPy boundary and jagged reconstruction owned by
   the public helper.
4. Fail closed on missing inputs, incompatible signatures, or missing
   uncertainty companions. Do not silently return nominal-only behavior.
5. Add focused schema/evaluator tests and record external payload provenance
   without re-explaining the derivation.
6. Update [correction interfaces](correction_interfaces.md).
7. In each consumer, separately select the mechanism for the intended
   era/sample and validate its downstream objects or weights.

For example, `GetPUSF(nTrueInt, year, var="nominal")` is a shared evaluator:
the caller supplies the era and variation. The evaluator does not decide which
samples receive pileup weights or which nuisance group consumes its variations.

## Extend corrected jets or MET

1. Preserve the `JECStack` evaluator-key contract and the factory `name_map`.
2. Keep central and shifted jet views aligned, including JER/JES naming and
   cache behavior.
3. Propagate applicable jet shifts into the current Type-1 MET helper and retain
   unclustered-energy variations where the interface provides them.
4. Treat the forward stochastic-JER option as a mechanism. Do not give it a
   universal analysis default in `topcoffea`.
5. Validate `tests/test_corrected_jets_factory_jer_sf.py`,
   `tests/test_corrected_met_factory.py`, and
   `tests/test_correctionlib_numpy_boundaries.py` as applicable.
6. Update every proven consumer. The legacy `CorrectedMETFactory` remains a
   maintained specialist/noncore interface and is not the current `topeft`
   Type-1 owner.

## Extend a shared object or event helper

1. Keep thresholds, working points, dataset precedence, and region windows as
   caller parameters unless this repository already owns their shared table.
2. Define input fields, returned mask shape, era/collection applicability, and
   failures.
3. Add focused helper tests—for example
   `tests/test_run3_nanov12_jet_id.py` for the maintained Run-3 jet-ID path.
4. Search consumers and update their concrete policy documentation. A generic
   overlap algorithm change and a `topeft` dataset-priority change are separate
   operations.

## Extend EFT helpers or `HistEFT`

1. Preserve coefficient ordering, term-count functions, input array shapes,
   and evaluation semantics in `eft_helper.py`.
2. For `HistEFT`, preserve ordinary axes, coefficient-bearing storage, fill,
   evaluation, conversion, scaling, variance, and serialization contracts.
3. Keep `SparseHist` classified as generic storage. Do not assign it an EFT
   sample policy.
4. Validate `tests/test_histEFT.py`, `tests/test_histEFT_add.py`,
   `tests/test_histEFT_unit.py`, and `tests/test_sparse_hist.py` as affected.
5. Update [EFT interfaces](eft_interfaces.md) and each consumer's treatment and
   compatibility tests.

A maintained minimal pattern is:

```python
import hist
from topcoffea.modules.histEFT import HistEFT

h = HistEFT(
    hist.axis.StrCategory([], name="process", growth=True),
    hist.axis.Regular(3, 0, 30, name="ht", flow=True),
    wc_names=["ctG"],
    label="Events",
)
sm_view = h.eval({})
ctg_view = h.eval({"ctG": 1.0})
```

The coefficient name and evaluation point are representative caller inputs,
not shared physics defaults. This snippet demonstrates construction and
evaluation-point semantics only; an owning consumer must fill events before
the evaluated views contain yields.

## Cross-repository closure

After a shared change:

1. validate the `topcoffea` mechanism and its public failure boundary;
2. validate each `topeft` caller's era/sample/policy choice and downstream
   category, artifact, plot, or card effect;
3. update [topeft integration](topeft_integration.md) and the consumer's
   reciprocal ownership link;
4. avoid copying algorithms, payload numbers, or coefficient equations into
   consumer documentation.
