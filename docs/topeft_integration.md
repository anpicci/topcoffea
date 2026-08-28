# topeft integration

The consuming [`topeft`](https://github.com/TopEFT/topeft) workflow owns the
exact compatible `topcoffea` ref through its installation or environment
configuration. Record both resolved commits for a campaign; do not infer a
compatible pair from similarly named branches.

## Physics ownership map

| Interface | `topcoffea` owns | `topeft` owns |
| --- | --- | --- |
| Corrections and calibrated objects | Reusable evaluators, weight helpers, JEC stack, corrected-jet and Type-1 MET factories, variation/schema contracts, and packaged shared payload interfaces | Era/tag/payload selection, working points, forward-JER policy, enabled variations, processor dispatch, and downstream category use |
| Object and event selection | Generic jet predicates, Run-3 jet-ID interpretation, dataset-overlap, SFOS, and Z-window utilities | Concrete object definitions, thresholds, trigger lists, dataset precedence, filters, mass windows, object-cleaning policy, sample-role overlap masks, and regions |
| EFT and histograms | Coefficient algebra, `HistEFT`, and the generic `SparseHist` substrate | Per-sample EFT treatment, coefficient preparation, category/observable filling, SM-point consumption, and scaling/artifact policy |
| Shared normalization data | Packaged luminosity and b-tag working-point authorities | Sample metadata, selected era/tagger/working point, sample roles, and normalized analysis yields |

Read the [correction interfaces](correction_interfaces.md) or
[EFT interfaces](eft_interfaces.md) for reusable guarantees. The corresponding
[`topeft` ownership index](https://github.com/TopEFT/topeft/blob/HEAD/docs/reference/shared_topcoffea_interfaces.md)
links the maintained analysis choices for
[corrections](https://github.com/TopEFT/topeft/blob/HEAD/docs/reference/corrections_weights_and_systematics.md),
[objects and event selection](https://github.com/TopEFT/topeft/blob/HEAD/docs/reference/objects_selections_and_triggers.md),
[sample roles](https://github.com/TopEFT/topeft/blob/HEAD/docs/reference/sample_roles_and_normalization.md),
and [EFT consumption](https://github.com/TopEFT/topeft/blob/HEAD/docs/reference/histeft.md).
The two repositories deliberately cross-link instead of copying each other's
authority.

`lo_xsec_samples`, when encountered in the consumer, is a sample-role set. It
does not contain or own numeric cross-section data.

## Change boundary

A generic evaluator, factory, payload-packaging, selection-helper, EFT-algebra,
or histogram-interface change belongs in `topcoffea`. Enabling that mechanism
for a concrete era, sample, working point, region, or systematic set belongs in
`topeft`. A cross-boundary change must validate both contracts; availability in
the shared library does not make a mechanism accepted analysis policy.

The maintained shared-extension routes are indexed in
[physics extension guides](physics_extension_guides.md). Analysis-policy
changes follow the contextual `topeft` how-to guide linked from its reference
entry.

## Quick setup checklist

From a `topeft` checkout that lives next to `topcoffea`:

1. Activate the desired analysis environment (for example `conda activate <env>`).
2. Install `topcoffea` in editable mode from the sibling checkout: `pip install -e ../topcoffea`.
3. Confirm the package is discoverable before running analysis code: `python -c "import topcoffea"`.
4. Record `git rev-parse HEAD` in both repositories with campaign evidence.

## Package data and import paths

The `topcoffea.modules.paths.topcoffea_path` helper must be used to locate packaged data and JSON payloads so the correct files are found regardless of where `topcoffea` is installed. Avoid keeping duplicate or modified copies of those resources inside a `topeft` checkout, since local overlays can mask the installed package and lead to stale data being used at runtime.

## Installation flow reference

`topeft` installs `topcoffea` through `scripts/install_topcoffea.sh`, which
clones the repository (or updates an existing checkout) and performs an
editable install. Set `TOPCOFFEA_GIT_REF` to the exact ref required by the
consuming workflow. These instructions mirror that flow so manual setups stay
in sync with the automated installation. For remote-executor archive identity
and validation, see
[Remote environment archive contract](environment_archive_contract.md).
