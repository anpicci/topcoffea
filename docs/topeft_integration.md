# Coordinating `topcoffea` with `topeft`

The `ch_update_calcoffea` branch in this repository is designed to be used
alongside the `format_update_anpicci_calcoffea` branch in the
[`topeft`](https://github.com/TopEFT/topeft) repository.  Both branches contain
synchronized interface changes, so pulling only one of them can lead to
mismatched helper signatures or missing CLI flags.

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
