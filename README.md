# Nonlinear phonon screening for 2D hexagonal materials

`npc` finds strong Γ–q–(−q) nonlinear phonon couplings. It relaxes a monolayer with a machine-learning force field (MLFF), calculates a 6×6×1 harmonic phonon mesh through Phonopy, and uses MatterSim to screen and fit frozen-mode potential-energy surfaces. **TECE + MatterSim** is the default route; Prophet and EquiformerV3 are optional Stage1 models. The public CLI has two stages and does not run QE or MD.

[中文说明](README_zh.md) · [Install and model sources](docs/INSTALL.md) · [Numerical architecture](ARCHITECTURE.md) · [Current PBE benchmark](docs/PBE_REFERENCE.md) · [Illustrated PDF](output/pdf/tmd_gga_pbe_mlff_latex_report.pdf)

## Workflow

1. **Stage1 — relax and calculate phonons.** The chosen MLFF relaxes the input structure; Phonopy calculates all 36 q points. Only Γ optical modes form coupling candidates; every finite-q branch stays eligible. Atomic symmetry is checked against the actual structure and transformed phonons. If equivalence fails, the program keeps a conservative candidate set. A verified three-atom TMD with six q orbits has 324 candidates, but other structures need not.
2. **Stage2 screen — evaluate every candidate.** MatterSim calculates six energies at `QΓ=±1` and `Qq=−1,0,+1` Å√amu. The norm of the momentum-allowed third-order Γ–q–(−q) channel ranks candidates.
3. **Stage2 refine and audit.** The best 20 complete channels by default receive a central 5×5 PES grid (`|Q|≤1`, step 0.5) for third- and fourth-order fitting. Optional `audit` extends the strongest five to 9×9 (`|Q|≤2`, step 0.5) for window checks. Six-point scores are screening proxies, not fourth-order results.

The **6×6×1 q mesh** sets phonon wavevectors. The **5×5, 9×9 and research 17×17 PES grids** sample two displacement coordinates. These measure different kinds of convergence.

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

Third- and fourth-order units are meV/(Å³·amu³ᐟ²) and meV/(Å⁴·amu²). Coupling errors are over the five matched channels, not the full spectrum. Changing the reference from old PZ-LDA to PBE reduces the cubic gap, while the WS₂ Γ8–M6 quartic **sign difference persists**, even on identical QE/MatterSim input structures. The old [LDA model/DFT report](docs/MODEL_DFT_COMPARISON.md) and [WS₂ supplement](docs/WS2_DFT_COMPARISON.md) are retained as historical baselines. A separate WS₂ fixed-window **17×17 QE density test** is still running as of 2026-09-26; its DFT convergence is not claimed here.

## Limits and repository layout

Lengths are Å, masses amu, total supercell energies eV, forces eV/Å, frequencies THz, real mode coordinates Å√amu. A Γ degeneracy is ranked by the full allowed coupling-vector norm; individual degenerate branches are basis-dependent. `Φ112` at finite q is momentum-forbidden and only a fit diagnostic.

Phonopy enforces translational ASR here, not the 2D ZA rotational sum rule. There is no non-analytic correction, SOC, external field or MD. Each model relaxes its own geometry, so cross-model comparisons require mode/subspace matching. QE PBE-USPP does not reproduce every MLFF training-label detail.

The installable package is in `nonlinear_phonon_calculation/` and `mlff_modepair_workflow/`; software tests are in `tests/`. PBE report scripts are outside the package. Model weights, raw QE output and Slurm controllers are not bundled. See [validation](docs/VALIDATION.md).
