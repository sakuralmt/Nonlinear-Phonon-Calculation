# Nonlinear phonon screening for 2D hexagonal materials

`npc` finds strong Γ–q–(−q) nonlinear phonon couplings. It relaxes a monolayer with a machine-learning force field (MLFF), calculates a 6×6×1 harmonic phonon mesh through Phonopy, and uses MatterSim to screen and fit frozen-mode potential-energy surfaces. **TECE + MatterSim** is the default route; Prophet and EquiformerV3 are optional Stage1 models. Existing **QE-DFT Stage1 geometry and phonons** can also be imported and passed to MatterSim Stage2. The public CLI does not launch QE or MD.

[中文说明](README_zh.md) · [Install and model sources](docs/INSTALL.md) · [Numerical architecture](ARCHITECTURE.md) · [Current PBE benchmark](docs/PBE_REFERENCE.md) · [Illustrated PDF](output/pdf/tmd_gga_pbe_mlff_latex_report.pdf)

## Workflow

1. **Stage1 — relax and calculate phonons.** The chosen MLFF relaxes the input structure; Phonopy calculates all 36 q points. Only Γ optical modes form coupling candidates; every finite-q branch stays eligible. Atomic symmetry is checked against the actual structure and transformed phonons. If equivalence fails, the program keeps a conservative candidate set. A verified three-atom TMD with six q orbits has 324 candidates, but other structures need not.
2. **Stage2 screen — evaluate every candidate.** MatterSim calculates six energies at `QΓ=±1` and `Qq=−1,0,+1` Å√amu. The norm of the momentum-allowed third-order Γ–q–(−q) channel ranks candidates.
3. **Stage2 refine and audit.** The best 20 complete channels by default receive a central 5×5 PES grid (`|Q|≤1`, step 0.5) for third- and fourth-order fitting. Optional `audit` extends the strongest five to 9×9 (`|Q|≤2`, step 0.5) for window checks. Six-point scores are screening proxies, not fourth-order results.

The **6×6×1 q mesh** sets phonon wavevectors. The **5×5, 9×9 and research 17×17 PES grids** sample two displacement coordinates. These measure different kinds of convergence.

The cubic comparison also includes the archived **DFT Stage1 + MatterSim Stage2** baseline for MoS₂/WSe₂, with the old LDA geometry/modes and explicit normalization conversion. It is not a PBE-mode rerun; missing WS₂ and historical quartic labels remain unfilled.

The new **PBE DFT Stage1 + MatterSim Stage2** baseline is complete for all 15 matched channels (543 configurations). It preserves the PBE DFT geometry and modes; cubic MAE for WS2/MoS2/WSe2 is 6.470/4.274/0.918. See the current report for signed quartic and same-configuration energy/force comparisons.

## DFT Stage1 + MatterSim Stage2

Use `npc stage1 --model qe-dft` to import existing audited DFPT results, without calculating or relaxing Stage1 again. Full imports feed the same `stage2 screen/refine/audit` commands. For matched detailed channels, `stage2 reference` evaluates exactly the existing QE displacement grid and compares aligned energies, forces and nonlinear derivatives. See [DFT input contract and commands](docs/DFT_STAGE1.md). DFT geometry is preserved; the model-relaxed route remains the default.

## Quick start

Use Python 3.10+ and `pip install -e .`. Phonopy is pinned to 2.38.0. Install the chosen Stage1 model and MatterSim in a compatible environment; model weights are not bundled. Tested source commits, weight checks and CPU/Slurm guidance are in [INSTALL.md](docs/INSTALL.md). Run `npc --help` for commands.

For a QE-style monolayer input with atom constraint flags:

```bash
npc stage1 --model tece \
  --structure /data/mose2/structure.scf.inp \
  --checkpoint /models/TECE-OAM-RRA-1.0.pt \
  --source-root /models/tace \
  --output-dir /runs/mose2/tece/stage1

npc stage2 screen \
  --mode-pairs-json /runs/mose2/tece/stage1/mode_pairs.selected.json \
  --structure /runs/mose2/tece/stage1/relax/optimized_structure.scf.inp \
  --checkpoint /models/mattersim-v1.0.0-5M.pth \
  --output-dir /runs/mose2/tece/stage2

npc stage2 refine \
  --mode-pairs-json /runs/mose2/tece/stage1/mode_pairs.selected.json \
  --structure /runs/mose2/tece/stage1/relax/optimized_structure.scf.inp \
  --checkpoint /models/mattersim-v1.0.0-5M.pth \
  --output-dir /runs/mose2/tece/stage2
```

Use the **same** mode-pair file, relaxed structure, MatterSim checkpoint and output directory for `screen`, `refine` and optional `audit`. Replace `--model tece` with `prophet` or `equiformer-v3` for other Stage1 routes; their source options are in [INSTALL.md](docs/INSTALL.md). Stage2 supports point-level restart and `--shard-index I --shard-count N`; after shards finish, `--finalize-only` checks completeness and writes rankings. Changed input hashes are rejected.

Stage1 writes `phonon_dataset.json` (all q points and ASR diagnostics) and `mode_pairs.selected.json` (symmetry and mode mappings). Stage2 writes `pairs/*/points.json`, `screen_ranking.json`, `selection.json`, `refine_ranking.json` and optional `audit_ranking.json`. Contract v5 rejects older mode-pair files.

## Current DFT benchmark: GGA-PBE

The [WS₂, MoS₂ and WSe₂ PBE study](docs/PBE_REFERENCE.md) uses QE 7.4.1, PBE ultrasoft pseudopotentials, PBE-relaxed cells, 120/1200 Ry, a primitive 30×30×1 k mesh and new 6×6×1 DFPT grids. **579/579** selected QE single points completed: five matched physical channels per material, central 5×5 PES fits, one 9×9 grid each and convergence checks. The [illustrated report](output/pdf/tmd_gga_pbe_mlff_latex_report.pdf), [source data](docs/reference_data/pbe_20260925/) and [portable report code](reports/pbe_three_materials/) are bundled.

| Material | TECE + MatterSim frequency MAE vs PBE (THz) | `|Φ122|` MAE | Signed `Φ1122` MAE |
| --- | ---: | ---: | ---: |
| WS₂ | 0.0928 | 6.104 | 2.594 |
| MoS₂ | 0.0880 | 4.328 | 2.939 |
| WSe₂ | 0.0654 | 1.180 | 0.628 |

Third- and fourth-order units are meV/(Å³·amu³ᐟ²) and meV/(Å⁴·amu²). Coupling errors are over the five matched channels, not the full spectrum. Changing the reference from old PZ-LDA to PBE reduces the cubic gap, while the WS₂ Γ8–M6 quartic **sign difference persists**, even on identical QE/MatterSim input structures. The old [LDA model/DFT report](docs/MODEL_DFT_COMPARISON.md) and [WS₂ supplement](docs/WS2_DFT_COMPARISON.md) are retained as historical baselines. The WS₂ Γ8–M9 fixed-window **17×17 QE/MatterSim density test is complete**: wide-window signed fourth-order fits change by +0.152% (QE) versus +4.917% (MatterSim) when 9×9 becomes 17×17. This is one-channel evidence; see the PBE benchmark for the same-geometry energy and force errors.

## Limits and repository layout

Lengths are Å, masses amu, total supercell energies eV, forces eV/Å, frequencies THz, real mode coordinates Å√amu. A Γ degeneracy is ranked by the full allowed coupling-vector norm; individual degenerate branches are basis-dependent. `Φ112` at finite q is momentum-forbidden and only a fit diagnostic.

Phonopy enforces translational ASR here, not the 2D ZA rotational sum rule. There is no non-analytic correction, SOC, external field or MD. Each model relaxes its own geometry, so cross-model comparisons require mode/subspace matching. QE PBE-USPP does not reproduce every MLFF training-label detail.

The installable package is in `nonlinear_phonon_calculation/` and `mlff_modepair_workflow/`; software tests are in `tests/`. PBE report scripts are outside the package. Model weights, raw QE output and Slurm controllers are not bundled. See [validation](docs/VALIDATION.md).

## Same-model end-to-end benchmark

TECE+TECE, Prophet+Prophet and EquiformerV3+EquiformerV3 now have 5,274 additional MLFF configurations across WS2, MoS2 and WSe2. Each material/model includes five 9x9 own-relaxed PES grids plus 181 identical-PBE-DFT-input diagnostic points. TECE full-flow cubic MAE is 1.468/1.546/0.157 and signed quartic MAE is 0.911/1.626/0.145, respectively; these errors describe the fixed five-channel sets. On identical WS2 Gamma8-M6 configurations, QE/TECE/MatterSim quartic values are -1.414/-1.425/+1.930.

See the [benchmark interface and comparison limits](docs/FULL_MODEL_BENCHMARK.md) and [complete report](output/pdf/tmd_gga_pbe_mlff_latex_report.pdf). Standard `npc stage2 screen/refine/audit` retains MatterSim; detailed same-model benchmarks have a separate checkpointed entry point.

### Small-model Stage2 diagnostics

GRACE, DPA-3.1-3M-FT and Eqnorm completed 1,629 energy/force evaluations on identical PBE DFT configurations across three materials. GRACE improves selected-channel quartic errors; DPA and Eqnorm also give the wrong WS2 M6 sign, so the issue is not unique to MatterSim. These isolate Stage2 and do not change the default backend. Local and server timings are not ranked together. See [methods, results and reproduction](docs/SMALL_MODEL_BENCHMARK.md).
