# Release 1.0.4 stability record

Date: 2026-10-29. Baseline: `989c8ca8402e1f30e488d029a86dc3d8f71ae8a2` (1.0.3).

[Code comparison](CODE_COMPARISON_20261029.md) · [Migration](MIGRATION.md) · [Installation](INSTALL.md)

This page records maintenance-release checks, not a new materials-accuracy study. The versioned GitHub Release links the successful CI runs and checksums of the delivered artifacts.

## Checks

- Baseline stable/public comparison: 299 tracked files, identical content, clean stable checkout.
- Numerical unit tests: real-mode normalization, primitive branches, phonon gauge, actual-structure symmetry, acoustic/optical separation, momentum diagnostics, 13-column fits and factorial conversion.
- Run recovery: interrupted screen/refine/audit and matched-channel benchmarks, byte-identical checkpoint reuse, changed-input refusal and complete-grid checks.
- New rejection cases: non-Γ imported Γ mode, invalid branch indices, nonfinite or incomplete reference labels, invalid CLI arguments.
- Distribution: build wheel/sdist; inspect their actual contents; import and run the wheel outside the source checkout; execute the complete tests from an extracted sdist.
- Lightweight startup: import core modules while explicitly forbidding model-stack imports and network connections; `status` must not create a run.
- Installer: an unsupported interpreter is rejected before pip runs.
- Scientific reference stability: recompute existing published PBE/full-model/small-model comparisons using archived labels, without model inference.

## Interpretation

The checks exercise the documented interfaces, input validation, packaging and restart. Material validation remains concentrated on TMDC screening and the listed DFT benchmarks. Analytic potentials and spring models test numerical behaviour rather than real-material accuracy; see [the symmetry scope note](SYMMETRY_SCOPE.md). Base-only test environments skip the small Torch autograd diagnostic when Torch is not installed; the Torch-enabled validation covers that diagnostic separately.

The wheel deliberately excludes source-tree report scripts and scientific regression data. The full archived-data tests belong to the repository/sdist. Base installation alone does not install model stacks, provide checkpoints or start a cluster job.

Historical scientific JSON and PDF records remain unchanged. They may retain original source paths as provenance; the public analysis scripts use the bundled relative reference data. Peak process RSS, scheduler allocation and measured whole-node peak memory remain different quantities.

## Final results

| Validation surface | Result | Scope |
| --- | --- | --- |
| Original 1.0.3 source, fresh Python 3.12 | 49 passed, 1 skipped | Old tests passed despite the three independently reproduced missing checks. |
| Original source, existing Torch-capable environment | 50 passed | Includes the small energy-sum/autograd control. |
| Final candidate, fresh base Python 3.12 | **83 passed, 1 skipped** | New invalid-input, documented-geometry and real-artifact tests included; only the optional Torch diagnostic is skipped. |
| Final candidate, separate test venv reusing installed Torch | **84 passed** | No model inference or weight download; includes the small tensor/autograd control. |
| Candidate wheel | Installed and checked outside the checkout | Core imports, CLI version/help/status; no run directory created by status. |
| Extracted candidate source distribution | **83 passed, 1 skipped** | Separately installed environment; includes archived reference inputs and report/validation code. |
| Linux CI, Python 3.10 and 3.12 | Passed on the release commit | Source tests: **83 passed, 1 skipped** on each version; outside-checkout wheel checks and extracted-sdist tests also passed. [CI run](https://github.com/sakuralmt/Nonlinear-Phonon-Calculation/actions/runs/40744574073). |
| Static and source checks | Passed | Critical Ruff checks, compileall, whitespace checks, unchanged scientific-core hashes, model-free imports, documentation routes and local links. |

The clean base environment uses the currently resolved base dependencies, including NumPy 2.5.4 and Phonopy 2.38.0. Third-party deprecation warnings from ASE/spglib are retained in the logs; they are not hidden test failures. The separate Torch environment reuses an existing installation, so it is not described as a fresh all-model environment.

Of the original 299 tracked files, stable and public main were identical before this patch. Thirteen scientific core/model/sampling modules remain byte-identical in the new version; `dft_reference.py` changes only rejection checks, while CLI/version and packaging/documentation changes are listed in [the comparison](CODE_COMPARISON_20261029.md).

No new QE or MLFF material calculations were run, no numerical acceptance threshold was relaxed, and no stable checkout or running scientific job was migrated. The canonical stable baseline is retained; upgrading a frozen calculation environment is a separate operational choice.

## Reproducing the release checks

```bash
python -m pip install '.[test]'
python -m pytest -q tests
python -m build
```

The [CI workflow](../.github/workflows/tests.yml) explicitly installs each built artifact into another environment and reruns the sdist suite. Its GitHub Actions dependencies are pinned to verified official commit hashes. [Release v1.0.4](https://github.com/sakuralmt/Nonlinear-Phonon-Calculation/releases/tag/v1.0.4) provides the final wheel, source archive and SHA-256 checksums.
