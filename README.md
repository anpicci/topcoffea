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

## Remote environment cache

`topcoffea.modules.remote_environment.get_environment` builds and caches
Conda environments that include editable installs of `topcoffea`. The cache
key now also tracks editable `topeft` checkouts, ensuring that modifying a
local `topeft` repository forces an environment rebuild when the
`unstaged="rebuild"` policy is used.
