# Coordinating `topcoffea` with `topeft`

The `ch_update_calcoffea` branch in this repository is designed to be used
alongside the `format_update_anpicci_calcoffea` branch in the
[`topeft`](https://github.com/TopEFT/topeft) repository.  Both branches contain
synchronized interface changes, so pulling only one of them can lead to
mismatched helper signatures or missing CLI flags.

The shared executor helpers on this branch expose a small `ExecutorFactory`
that wraps `coffea.processor.Runner`.  The factory validates processors against
either Coffea's classic `ProcessorABC` or the CalCoffea-compatible
`CalProcessorABC`, ensuring the `topeft` `AnalysisProcessor` (which still
derives from `ProcessorABC`) runs without modification while leaving room for
future CalCoffea migrations. Downstream users do not need to change their
processor inheritance as long as they derive from one of these ABCs.

## Pinned analysis stack and environment rebuilds

`topcoffea` and `topeft` share a single dependency stack. Keep the versions
aligned with the TaskVine-ready pins used by `topeft` to avoid ABI surprises:

* NumPy `2.0.x`
* pandas `>=2.2,<2.3`
* hist `2.9.x`

After updating these requirements, refresh the local environment and rebuild the
TaskVine tarball so workers receive the new wheels:

```bash
conda env update -f environment.yml --prune
python -m topcoffea.modules.remote_environment
```

Using the regenerated archive for both futures and TaskVine runs keeps pandas
and NumPy compiled against the same ABI and prevents hard-to-diagnose import
errors.

## Keep the namespace import available

`topeft` imports `topcoffea` via the plain namespace import

```bash
python -c "import topcoffea"
```

without modifying `PYTHONPATH`.  Any local testing of
`format_update_anpicci_calcoffea` therefore requires that the
`ch_update_calcoffea` branch be installed in the same environment used to run
`topeft`.  Use one of the following approaches from the `topcoffea` checkout:

* **Editable install (recommended while developing both repos):**
  ```bash
  python -m pip install -e .
  python -c "import topcoffea"
  ```
* **Direct install from GitHub:**
  ```bash
  python -m pip install "git+https://github.com/TopEFT/topcoffea.git@ch_update_calcoffea"
  python -c "import topcoffea"
  ```

Re-run the smoke test after pulling new commits so the cached environment picks
up the latest code.  If the command fails, reinstall the editable package inside
the `topeft` environment before continuing.

## Single-command synchronized install

If you do not already have a local `topeft` checkout, the new
`[project.optional-dependencies].topeft` extra pins the expected branch and
keeps the editable installs aligned in one shot:

```bash
python -m pip install -e "git+https://github.com/TopEFT/topcoffea.git@ch_update_calcoffea#egg=topcoffea[topeft]"
python -m topcoffea.modules.remote_environment
```

The module invocation rebuilds the TaskVine environment tarball only when the
`topcoffea` or `topeft` sources (or their dependency pins) change; otherwise it
reuses the cached `topeft-envs/*.tar.gz` payload already staged on disk.  The
`topcoffea.modules.remote_environment` helper now captures the pinned
`topeft @ ...` entry from `pip freeze`, so the cache will refresh when the
non-editable dependency from the optional extra updates.

For sibling clones that are already on `ch_update_calcoffea` and
`format_update_anpicci_calcoffea`, keep using the dual editable install to avoid
pulling a second copy from Git:

```bash
python -m pip install -e ../topcoffea -e ../topeft
python -m topcoffea.modules.remote_environment
```

## Suggested workflow

1. Clone both repositories side-by-side and check out the branches listed above.
2. Activate the conda/venv environment used to run `topeft`.
3. From the `topcoffea` checkout, run `python -m pip install -e .` to expose the
   namespace import.
4. From the `topeft` checkout, run the desired workflow (e.g., `python -m
   topeft.run --help`).  Keep the `topcoffea` checkout untouched except for
   pulling updates; the editable install automatically surfaces the latest
   changes to `topeft`.

Following these steps keeps `import topcoffea` working for downstream analyses
without requiring code edits in the `topeft` repository itself.

## Avoid loading vendored copies

Importing `topcoffea` from a directory nested under a `topeft` checkout (for
example, `/path/to/topeft/topcoffea`) now raises a startup error.  This guard
prevents stale vendored snapshots in the `topeft` tree from shadowing the real
package.  If you see the error:

```
RuntimeError: Detected topcoffea imported from a vendored copy inside a topeft checkout...
```

remove the `topeft/topcoffea` folder and reinstall the sibling repository on the
`ch_update_calcoffea` branch in editable mode:

```bash
rm -rf /path/to/topeft/topcoffea
python -m pip install -e /path/to/topcoffea
python -c "import topcoffea"
```

The import should succeed once the environment points to the standalone
`topcoffea` checkout instead of a vendored copy.
