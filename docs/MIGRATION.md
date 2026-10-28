# Migration to 1.0.4 / 迁移到 1.0.4

Version 1.0.4 is a maintenance release of the existing two-stage workflow, strengthening input validation, installation, distribution and documentation. The default is still TECE Stage1 + pinned MatterSim Stage2; scientific sampling remains unchanged. Archived benchmark numbers retain their original version and calculation scope; this release does not claim fresh inference measurements of every model.

1.0.4 是原两阶段流程的维护版，完善输入核验、安装、发行包与文档。默认仍为 TECE＋MatterSim，科学采样方案保持不变。归档基准保留原版本和计算范围，不代表本次重新实测全部模型。

## Installation and command changes

- `npc --version` reports the package version; `npc --help` remains model independent.
- `python -m pip install '.[test]'` installs test/build tools. Run the full regression suite from a source checkout or unpacked source distribution with its public fixtures.
- The source distribution includes regression/reference data and report sources. The wheel contains the two Python packages, so an installed-wheel smoke test and a source-data regression suite are different checks.
- `install.sh` checks the selected interpreter and uses `NPC_INSTALL_PYTHON` to select it. This is separate from `NPC_PYTHON` in the optional Slurm inference script.
- Existing DFT imports receive stricter validation of Γ/mode indexing and reference force-array shape. Malformed data previously accepted should be corrected at its export source, not made valid by changing a validation threshold.

安装后可用 `npc --version`。源码/源码发行包用于完整数据回归，wheel 用于核心安装；两者不能混称。已有 DFT 导入加强 Γ／模式索引和参考力数组形状核验。被拒绝的错误导出应从来源修正，不应手工改阈值绕过。

## Before resuming an existing v5 run

Keep a copy of the original run and its inputs. Resume only if all relevant identities still agree:

1. Structure contents, atom order, constraints and source provenance.
2. Mode-pair file, primitive basis, real-mode normalization and coordinate units.
3. Exact model checkpoint and required model/source version.
4. Energy-accumulation protocol, not merely model filename or weight hash.
5. Sampling grids, selected-group count, phase and reference data where applicable.
6. For MLFF Stage1, its optimized structure and relaxation summary; for imported DFT, its original DFT source records.

Repeat the original command and let its identity checks decide whether the saved points are reusable. A maintenance version number by itself does not authorize mixing incompatible points. If a check fails, retain the old results and run corrected inputs in a **new output directory**. Do not delete an identity record, relabel a version, or copy point JSON files to make a mismatch disappear.

续算前保留旧运行。核对结构、原子顺序、模式基、归一化、单位、权重、源码、累加精度、网格、选组数及参考数据。MLFF 路线还需原优化结构和弛豫记录；DFT 路线保留原来源。完全一致时重复原命令；不一致时使用新目录，不删身份文件、不改版本号冒充兼容，也不直接拼入旧点。

## Older result contracts and floating-point protocols

Historical v3/v4 mode-pair files are diagnostic/archival references, not v5 restart inputs. Re-export from the original audited structure and harmonic data through the supported interface. There is no general automatic converter for missing provenance or a changed normalization.

The MatterSim protocol `atomic_float32_sum_float64_v2` promotes atomic energies **before** the total-energy sum, while the network stays float32. Old float32-summed results and this protocol are not interchangeable simply because they use the same weight file. A full-float64 diagnostic is another numerical control, not a silent replacement for production labels.

旧 v3/v4 只作归档参考，不是 v5 续算输入。应从可追溯的原结构和谐波数据重新导出；缺失来源或归一化变化不能靠自动改版本解决。MatterSim 的 sum64 是求和前提升原子能量精度，不是全双精度网络；相同权重不能作为混合旧数值协议的理由。

## Relocating data and benchmark manifests

Ordinary structure/JSON file content hashes survive a path change, but a manifest or saved source record can itself contain paths and have its own hash. For a benchmark on another machine:

1. Copy the published inputs without changing their scientific contents and verify the recorded file hashes.
2. Relocate paths in a new manifest to the actual input files.
3. Keep the original manifest as provenance and use a new result directory for the relocated manifest identity.

Do not copy a saved checkpoint into a new directory and assume that path relocation has been validated. Do not publish personal or cluster-specific locations as portable instructions. The documented examples use repository-relative paths and user-selected weight/run locations.

迁移机器时，保留原始科学文件和 SHA；将新清单的路径指向实际文件，保留旧清单，并使用新结果目录。文件内容没变，不等于包含路径的清单身份也没变。公开说明使用相对路径或用户自定位置，不依赖个人或集群私有路径。

## What remains unchanged

The standard phases still use the six-point screen, central `[-1,1]` 5×5 refine and expanded `[-2,2]` 9×9 audit. Audit is not a same-window density increase. A mode pair and a degenerate coupling group remain different; the 13-term fit is not a complete phonon tensor. `reference` still uses existing DFT geometry and exact reference coordinates, one checkpointed process per material.

The full-model and small-model benchmark modules remain separate optional interfaces. Their existence does not make GRACE/DPA/Eqnorm generic `npc stage2 --model` choices. No release operation requires cancelling, changing or migrating a scientific job already running with an older frozen code snapshot.

六点初筛、中央 5×5、扩窗 9×9 及既有单位约定保持不变；具体模式对、简并组和完整张量仍需区分。独立 benchmark 模块不等于默认 CLI 后端。发布新版本不要求取消或改动正在使用旧冻结快照的科学作业。
