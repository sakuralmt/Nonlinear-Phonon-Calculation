# PyPI Release Flow

This package is published under:

- distribution name: `nonlinear-phonon-calculation`
- console command: `npc`

## 1. Prepare GitHub

Create a GitHub repository whose root is this `github_release_bundle/` directory.

Then push the current source there so GitHub Actions can build from the same tree that `install.sh` and `npc` already validate locally.

## 2. Prepare PyPI and TestPyPI

Create accounts for:

- TestPyPI
- PyPI

Enable 2FA on both.

Reserve or create the project:

- `nonlinear-phonon-calculation`

## 3. Configure Trusted Publishing

On both TestPyPI and PyPI, add a trusted publisher that points to this repository and the workflow:

- `.github/workflows/publish-python-package.yml`

Recommended first pass:

- configure TestPyPI first
- run a full TestPyPI publish
- confirm install from TestPyPI
- then add the production PyPI publisher

## 4. Local validation before publish

From the package root:

```bash
./packaging/build_python_dist.sh
./install.sh
npc
```

The build script will:

- install `build` if missing
- build wheel + sdist
- run `twine check dist/*`

## 5. GitHub Actions release flow

Use the `Publish Python Package` workflow with:

- target: `testpypi`

After the TestPyPI install path is validated, rerun with:

- target: `pypi`

## 6. Post-publish validation

Recommended checks:

```bash
python3 -m pip install --index-url https://test.pypi.org/simple/ nonlinear-phonon-calculation
npc
```

Then repeat against the production index after the final publish.
