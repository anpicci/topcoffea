# topcoffea

Tools that sit on top of coffea to facilitate CMS analyses. The repository is set up as a pip installable package. To install this package into a conda environment: 
```
git clone https://github.com/TopEFT/topcoffea.git
cd topcoffea
pip install -e .
```



Examples of analysis repositories making use of `topcoffea`:
* [`topeft`](https://github.com/TopEFT/topeft): EFT analyses in the top sector.
* [`ewkcoffea`](): Multi boson analyses.

## Streaming histogram pickle files

Deferred workflows (such as the nonprompt scripts) often read very large
`*.pkl.gz` files.  Loading the entire histogram dictionary at once can exhaust
memory, so `topcoffea` now exposes a streaming helper that reads one entry at a
time directly from the gzip stream:

```python
from topcoffea.modules.utils import iterate_hist_from_pkl

for key, hist in iterate_hist_from_pkl("/path/to/output.pkl.gz", allow_empty=False):
    process_histogram(key, hist)
```

Set `materialize=True` (or `materialize="eager"`) to recover the previous eager
behavior provided by `get_hist_from_pkl` when you explicitly need the full
mapping in memory.
