# topcoffea

Tools that sit on top of coffea to facilitate CMS analyses. The repository is set up as a pip installable package. To install this package into a conda environment:
```
git clone https://github.com/TopEFT/topcoffea.git
cd topcoffea
pip install -e .
```

The conda environments distributed with `topcoffea` and `topeft` pin
`coffea==2025.7.3` and `awkward==2.8.7`, alongside
`setuptools==80.9.0`, `ndcctools`, and related tooling so that the local
environment matches what the remote cache builds.



Examples of analysis repositories making use of `topcoffea`:
* [`topeft`](https://github.com/TopEFT/topeft): EFT analyses in the top sector.
* [`ewkcoffea`](): Multi boson analyses.

## Remote environment cache

`topcoffea.modules.remote_environment.get_environment` builds and caches
Conda environments that include editable installs of `topcoffea`. The
cache tarballs, named via `topcoffea.modules.env_cache`, live next to
your workflow as `topeft-envs/env_spec_<hash>_edit_<commit>.tar.gz`, and
the helper function accepts an `unstaged` policy of either `rebuild`
(default) or `fail` when it detects local changes in editable
checkouts. The cache key
tracks editable `topeft` checkouts so modifying a local `topeft`
repository forces an environment rebuild when `unstaged="rebuild"` is
used. Pair the resulting tarball with TaskVine workers submitted via
[`vine_submit_workers`](https://github.com/cooperative-computing-lab/taskvine/blob/main/doc/man/vine_submit_workers.md)
to avoid repeatedly transferring large environments.
