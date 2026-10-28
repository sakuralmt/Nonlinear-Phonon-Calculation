# Changelog

## 1.0.4 — 2026-10-29

This maintenance release hardens invalid-input handling and repairs source-distribution reproducibility. It preserves the scientific algorithms and the v5 data contract used in 1.0.3.

### Fixed

- Reject an imported `gamma_mode` whose wavevector is not Γ, even when its eigenvector and frequency match another valid DFPT point.
- Require valid one-based integer branch indices. Branch zero must not alias the last eigenvector through Python negative indexing.
- Require finite scalar reference energies and a finite `(nat_supercell, 3)` reference force array. A single force vector or one-row array must not broadcast into a plausible benchmark error.
- Reject nonpositive/nonfinite harmonic settings and invalid shard assignments before accessing input data or starting a model run.
- Include the archived reference geometries, data, validation modules and report-analysis modules needed to run the complete tests from the source distribution.
- Make `install.sh` use the explicitly selected interpreter and reject Python <3.10 before invoking pip.

### Added

- `npc --version`, a lightweight `test` dependency extra and an explicit MIT license file.
- Ignore the documented local environment, weight, model-source and run directories in Git.
- Linux CI on Python 3.10 and 3.12: synthetic/archived-data tests, wheel installation outside the checkout, and source-distribution installation and tests.
- Regression tests for malformed DFPT identities/reference arrays, package artifacts, offline lightweight imports and installer preflight.
- Rewritten English/Chinese README, installation guide, numerical architecture, migration guide and release stability comparison.

### Unchanged

No new model weights, energy/force labels or reference results. The phonon solver, actual-structure symmetry mapping, real-mode normalization, six-point proxy, selected 13-term energy fit, sum64 MatterSim accumulation and existing numerical tolerances are unchanged. Existing valid v5 checkpoints retain their identity and remain subject to the same input-hash checks. Invalid inputs previously accepted by mistake are deliberately rejected.

Historical acceptance documents describe earlier validated snapshots. Their test counts and benchmarks are not the new release's test results; see [the release stability record](docs/STABILITY_1.0.4.md).
