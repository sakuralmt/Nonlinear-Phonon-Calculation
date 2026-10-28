# Stable code, public main and the 1.0.4 release

[中文摘要](#中文摘要) · [Stability checks](STABILITY_1.0.4.md) · [Migration](MIGRATION.md)

## The comparison baseline

On 2026-10-29, the canonical stable checkout and the freshly fetched public `main` both resolved to:

- Commit: `989c8ca8402e1f30e488d029a86dc3d8f71ae8a2`
- Git tree: `5d829a9d3d2149a6daca6c4f935213614e930ecd`
- Tracked files compared byte for byte: **299**
- Changed, missing or public-only tracked files: **0**
- Canonical stable working tree: clean.

There was no separate, unpublished stable algorithm to merge. The public feature branches are not the release baseline. In particular, `codex/stable-stage2-model-preset` has one branch-only documentation commit relative to its common ancestor, not a newer set of scientific kernels. No experimental controller, cancelled management refactor or active research-campaign deployment was promoted in this release.

The stable checkout was not edited. Work was performed in an independent release clone, with an immutable 1.0.3 source export retained for regression comparison.

## What changed and why

| Area | 1.0.3 behavior or gap | 1.0.4 behavior | Evidence |
| --- | --- | --- | --- |
| DFPT Γ identity | A selected finite-q mode could pass vector/frequency matching while labeled as Γ. | Explicitly requires Γ modulo reciprocal-lattice integers. | Reproduced with a genuine finite-q DFPT vector; regression rejects it. |
| DFPT branch identity | Branch `0` could select the last mode through `index = number - 1`. | Requires an integer branch in `1..3N`; boolean, zero, negative, out-of-range and fractional indices are rejected. | Malformed selection regressions plus unchanged valid imports. |
| Reference force metrics | A `(3,)` or `(1,3)` force array could broadcast over all atoms. | Checks finite scalar energy and the complete `(nat_supercell,3)` force array before computing errors. | Old behavior reproduced with a single zero vector and a fake calculator; new code rejects it without evaluating points. |
| CLI settings | Some invalid harmonic arguments reached model/structure setup; reference shard index could be ignored. | Validates settings before input access and provides `npc --version`. | CLI invalid-input/no-output regressions. |
| Source distribution | Omitted report/validation modules and non-JSON reference geometries; full test collection failed after extraction. | Includes the lightweight scientific regression inputs and modules. | Tests inspect real wheel/sdist contents; extracted-sdist tests run separately. |
| Wheel | Core packages only. | Still core packages only, with license and version metadata. | Installed-wheel checks run away from the checkout. |
| Installer | Always used `python3` from PATH, which may be unsupported. | `NPC_INSTALL_PYTHON` selects the interpreter; Python ≥3.10 checked before pip. | Old-interpreter preflight regression. |
| CI and documentation | No workflow; historical tests and installation statements were easy to mistake for current evidence. | Reproducible CI matrix and separated current release vs historical scientific benchmarks. | CI links and local evidence are recorded in the stability page. |

## Scientific behavior retained

The release does not alter the implementation of the following:

- Constrained two-dimensional relaxation and its convergence settings.
- Phonopy 2.38.0 integration, atom-order alignment and harmonic IFC construction.
- Actual-structure symmetry, Γ acoustic/optical separation and degenerate-subspace handling.
- Commensurate real standing waves and whole-supercell mass normalization.
- The six-point screening proxy and selected 13-column energy-only PES fit.
- Taylor derivative factorials and units.
- MatterSim's float32 network with float64 atomic-energy summation.
- Checkpoint identity, existing source/weight hashes and default selection settings.

Archived-data regression tests recompute the published fits and comparisons; successful synthetic screen → refine → audit and interrupted/resumed workflows remain tested. No QE job, model-weight download, new MLFF benchmark or production-controller restart is part of this maintenance validation. This separation is intentional: a patch to input validation should not require repeating materials calculations.

## 中文摘要

稳定库与公开 `main` 原先是同一提交，299个受控文件逐字一致。本次发布不是把另一套算法覆盖上去，而是在独立测试副本中修复三个实际可复现的数据验收问题，并完善命令行、安装、源码发行包和持续集成。原有合法数据、归一化、筛选与拟合方法保持不变；过去误接受的非法输入现在会报错。研究目录里的集群控制器与已撤销的管理架构没有进入公开包。

完整稳定性结论以[本轮验证记录](STABILITY_1.0.4.md)为准，不以“旧版测试原本通过”代替这次缺陷的回归验证。
