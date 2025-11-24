# Configuration and runtime dataclasses

This guide explains how analysis profiles become executable runs across the
`topcoffea` and `topeft` repositories.  It focuses on three ingredients:

* the `RunConfig` object that downstream analyses use to capture the shape of a
  job,
* how YAML profiles override command-line options before the job is launched,
* how dataclasses in `topcoffea` map configuration values to the behaviour of
  the executors and physics corrections.

Throughout, the examples reference
[`analysis/topeft_run2/run_analysis.py`](../../topeft/analysis/topeft_run2/run_analysis.py)
(the canonical run script in `topeft`) together with
`topcoffea`'s executor and correction helpers.

## RunConfig: normalising command-line intent

The run script starts by parsing command-line arguments and loading any YAML
profile passed via `--options`.  Those settings are consumed by the
`RunConfig` dataclass—implemented in practice by the
`ExecutorCLIConfig` definition in `topcoffea.modules.executor_cli`—so that
downstream code works with a single, immutable object.  The dataclass stores the
executor backend, worker counts, chunking information and optional TaskVine
metadata such as port ranges and environment tarballs.【F:topcoffea/modules/executor_cli.py†L13-L91】

To build the dataclass, the helper `executor_config_from_values` accepts the raw
values from the CLI and the YAML profile and normalises them.  It translates
aliases (for example, treating `work_queue` as a request for the TaskVine
executor), converts string inputs to integers, parses TaskVine port ranges and
applies the environment caching policy.【F:topcoffea/modules/executor_cli.py†L93-L189】
The resulting `RunConfig`/`ExecutorCLIConfig` instance offers convenience
properties such as `requires_port`, which is used by callers to decide whether a
TaskVine manager needs to reserve a port range.【F:topcoffea/modules/executor_cli.py†L70-L88】

## YAML profile resolution

`analysis/topeft_run2/run_analysis.py` allows command-line options to be bundled
into reusable YAML profiles.  When `--options` is provided the script loads the
YAML document, extracts the run-time keys, and overlays them onto the parser
results.  Each CLI field has a corresponding `pop` call so that the YAML profile
may define any subset of recognised options.  Values that are not provided fall
back to the CLI defaults.  This overlay happens before any validation of the
executor choice or the histogram selection so that subsequent logic can treat
the merged configuration as authoritative.【F:../topeft/analysis/topeft_run2/run_analysis.py†L215-L261】

The merged values (including paths, executor choice and chunking settings) are
then checked for consistency—verifying the executor exists, that TaskVine port
ranges are well formed, and that systematic switches are used coherently—before
being passed to the coffea processor layer.  These checks guarantee that the
`RunConfig` handed to the executors represents a self-consistent job
description.【F:../topeft/analysis/topeft_run2/run_analysis.py†L262-L324】

## Dataclasses in action inside `topcoffea`

The same dataclass approach is used beyond the executor helpers.  For example,
`topcoffea.modules.JECStack` wraps the various jet-energy correction (JEC) tools
in a single dataclass so that downstream processors can request either the
legacy coffea jetmet stack or the newer correction library workflow without
changing their call sites.【F:topcoffea/modules/JECStack.py†L1-L86】

When `use_clib` is true the `JECStack` dataclass loads a `correctionlib`
`CorrectionSet` and exposes the requested corrections as a dictionary keyed by
name; otherwise it instantiates the coffea JEC/JER classes directly.  The
`__post_init__` hook dispatches to the appropriate initialiser based on the flag
and raises informative errors when required inputs (such as the JSON payload for
correctionlib) are missing.【F:topcoffea/modules/JECStack.py†L18-L61】

The `assemble_corrections` helper groups correction objects into JEC, JER, JER
scale-factor and uncertainty buckets, mirroring the attribute layout expected by
coffea 2025.7's jet tools.  This makes the dataclass agnostic to whether the
corrections arrived from a `FactorizedJetCorrector` chain or from the
correctionlib JSON bundle while keeping the runtime API stable for processors
that consume the object.【F:topcoffea/modules/JECStack.py†L63-L110】

Factories that consume the stack (`CorrectedJetsFactory`/`CorrectedMETFactory`)
now default to a cache-free workflow: callers may omit the `lazy_cache`
argument entirely and the corrected collections will still be produced, in line
with coffea 0.7's handling of virtual arrays.  Supplying a cache remains
supported for analyses that want to materialise repeated lookups, but it is no
longer mandatory for constructing corrected jets or MET, and using a cache is
discouraged with newer coffea releases where the lazy plumbing is optional and
skipping it avoids potential compatibility problems.【F:topcoffea/modules/CorrectedJetsFactory.py†L185-L199】【F:topcoffea/modules/CorrectedMETFactory.py†L35-L59】

## Coffea 2025.7 considerations

Both the executor config and JEC helpers were updated for the Coffea 2025.7
series.  On the execution side TaskVine is treated as the preferred backend, and
its environment-handling logic ensures cached environments include editable
`topcoffea`/`topeft` installs before the workers start.【F:topcoffea/modules/executor_cli.py†L113-L186】
For jet corrections, the correctionlib pathway lets analyses adopt the
`CorrectionSet` JSONs shipped with Coffea 2025.7 while still exposing the
structure expected by existing processor code.【F:topcoffea/modules/JECStack.py†L18-L86】

Together these dataclasses provide a bridge between declarative run profiles and
runtime behaviour: CLI options and YAML overlays become a frozen `RunConfig`,
which in turn controls how executors are initialised and how physics corrections
are prepared before the coffea processor executes.
