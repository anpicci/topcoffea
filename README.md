# topcoffea

Tools that sit on top of coffea to facilitate CMS analyses. The repository is set up as a pip installable package. To install this package into a conda environment:
```
git clone https://github.com/TopEFT/topcoffea.git
cd topcoffea
pip install -e .
```

When pairing this repository with [`topeft`](https://github.com/TopEFT/topeft), use the `run3_test_mmerged` branch of `topcoffea` alongside the `run3_test_mmerged_anpicci` branch of `topeft` to match the maintained workflow.

### Side-by-side development checklist

When working on `topcoffea` and `topeft` together from sibling checkouts:

1. Activate the analysis environment (e.g., `conda activate <env>`).
2. Run `pip install -e ../topcoffea` from the `topeft` checkout to ensure the editable install is picked up.
3. Verify the import path with a quick smoke test: `python -c "import topcoffea"`.



Examples of analysis repositories making use of `topcoffea`:
* [`topeft`](https://github.com/TopEFT/topeft): EFT analyses in the top sector.
* [`ewkcoffea`](): Multi boson analyses.

## Accessing package resources

Data files, JSON payloads, and other resources that live inside the
`topcoffea` python package should always be opened via
`topcoffea.modules.paths.topcoffea_path`.  This helper resolves files relative
to the installed package directory so the correct path is returned even when
the repository is nested inside another checkout or installed as a dependency
of a larger project.

```python
from topcoffea.modules.paths import topcoffea_path

with open(topcoffea_path("params/params.json")) as handle:
    cfg = json.load(handle)
```

Downstream projects should treat the returned path as read-only package data
and avoid constructing relative paths by hand (for example `../data/...`) so
they always inherit the correct behaviour. When integrating with
[`topeft`](https://github.com/TopEFT/topeft), the editable install performed by
[`scripts/install_topcoffea.sh`](https://github.com/TopEFT/topeft/blob/run3_test_mmerged_anpicci/scripts/install_topcoffea.sh)
should be allowed to supply the packaged resources—avoid keeping in-tree
overlays that could mask the installed `topcoffea` package.

## Streaming histogram pickle files

Deferred workflows (such as the nonprompt scripts) often read very large
`*.pkl.gz` files.  Loading the entire histogram dictionary at once can exhaust
memory, so `topcoffea` exposes helpers that read one entry at a time directly
from the gzip stream:

```python
from topcoffea.modules.utils import iterate_hist_from_pkl

for key, hist in iterate_hist_from_pkl("/path/to/output.pkl.gz", allow_empty=False):
    process_histogram(key, hist)
```

Set `materialize=True` (or `materialize="eager"`) to recover the previous eager
behavior provided by `get_hist_from_pkl` when you explicitly need the full
mapping in memory.

When downstream projects want to decouple file IO from histogram materialization
they can opt into the new lazy iterator, which yields `(key, LazyHist)` pairs
that only deserialize their payload on demand:

```python
from topcoffea.modules.utils import iterate_histograms_from_pkl

lazy_entries = list(iterate_histograms_from_pkl("/path/to/output.pkl.gz"))

for key, lazy_hist in lazy_entries:
    hist = lazy_hist.materialize()
    try:
        process_histogram(key, hist)
    finally:
        lazy_hist.release()  # drop the cached histogram when you are done
```

Using `iterate_histograms_from_pkl` keeps peak memory usage constant with
respect to the histogram payloads while still supporting the existing pickle
format.

## Compatibility helpers

The `topcoffea.modules.hist_utils` compatibility helpers expose the shims used
by `topeft` so other projects no longer need to vendor them separately:

* `ensure_histEFT_py39_compat()` loads `topcoffea.modules.histEFT` with
  Python 3.9–safe type annotations when the module contains ``|`` style union
  types.
* `ensure_hist_utils()` provides a resilient import path for
  `topcoffea.modules.hist_utils` in minimal environments where optional
  dependencies may be missing.

Call `ensure_histEFT_py39_compat()` before importing `histEFT` on Python 3.9
deployments, and use `ensure_hist_utils()` when you need to guarantee access to
the histogram streaming utilities even if `topcoffea` is partially available.
