# Plotting guide

Use this page for shared plotting examples that rely on `topcoffea` histogram
utilities. This is not the main end-to-end plotting workflow for analysts.

If you want the full run-to-plot path, start in `topeft` with:

- [`topeft/docs/workflow_and_yaml_hub.md`](https://github.com/TopEFT/topeft/blob/master/docs/workflow_and_yaml_hub.md)
- [`topeft/docs/quickstart_run2.md`](https://github.com/TopEFT/topeft/blob/master/docs/quickstart_run2.md)

## Plotting surface

`topcoffea` supports modern `hist` objects only. The legacy Coffea histogram
namespace is not supported.

```python
import hist
import mplhep as hep
import matplotlib.pyplot as plt

h2 = hist.Hist(
    hist.axis.Regular(20, -5, 5, name="x"),
    hist.axis.Regular(20, -5, 5, name="y"),
)
h2.fill(x=[-1.0, 0.2, 1.7], y=[0.5, -0.8, 1.1])

hep.hist2dplot(h2, xaxis="x")
plt.tight_layout()
plt.show()
```

## Serialized histogram note

Histogram pickles and tuple-keyed outputs follow the schema documented in
[tuple_schema.md](tuple_schema.md). Use that reference when downstream plotting
utilities consume serialized histogram payloads rather than live `hist`
objects.

## Where to go next

- Need installation or shared-helper usage context: [quickstart.md](quickstart.md)
- Need smoke tests or troubleshooting: [testing.md](testing.md)
- Need end-to-end analysis plotting workflow: use
  [`topeft/docs/quickstart_run2.md`](https://github.com/TopEFT/topeft/blob/master/docs/quickstart_run2.md)
