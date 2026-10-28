# Nonlinear Phonon Calculation (`npc`)

`npc` screens **static Γ–q–(−q) phonon couplings in hexagonal monolayers**. It builds harmonic modes with Phonopy, evaluates frozen-mode potential-energy surfaces (PES) with a pretrained machine-learning force field, and fits a defined set of third- and fourth-order terms. The default route is **TECE Stage1 + MatterSim Stage2**. An alternative route imports already completed QE–DFPT structures and modes, preserving their DFT geometry.

This is a mode-projected PES workflow. It does not launch QE, run molecular dynamics, or predict an experimental pump threshold from a coupling ranking. Model weights are separate downloads.

[中文](README_zh.md) · [Quick start](docs/QUICKSTART.md) · [Installation and weights](docs/INSTALL.md) · [Architecture](ARCHITECTURE.md) · [Migration to 1.0.4](docs/MIGRATION.md) · [Stable/public comparison](docs/CODE_COMPARISON_20261029.md) · [Release checks](docs/STABILITY_1.0.4.md)

## Install the core package

Use an installed Python **3.10 or newer** before running pip; the examples use `python3.12`. The following selects the published `v1.0.4` tag. If you already have that source checkout, skip the first three commands.

```bash
git clone https://github.com/sakuralmt/Nonlinear-Phonon-Calculation.git
cd Nonlinear-Phonon-Calculation
git checkout v1.0.4
python3.12 --version
python3.12 -m venv .venv
. .venv/bin/activate
python -m pip install .
npc --version
npc --help
```

The core declares Python 3.10+ and pins Phonopy to **2.38.0**. This installs the workflow, not every model stack. Before a calculation, install the selected model in a compatible environment and obtain the exact checkpoint listed in [INSTALL.md](docs/INSTALL.md). MatterSim Stage2 requires `mattersim==1.2.1` and the **v1.0.0-5M** weight; its upstream default 1M weight is not interchangeable.

For development and the regression suite, use a checkout or source distribution:

```bash
python -m pip install '.[test]'
python -m pytest -q tests
```

The source distribution contains the public regression/reference data and report sources. The wheel contains the two Python packages; it does not contain the full reference-data or report tree. Installing a wheel and checking `npc --help` verifies installation, not the archived scientific benchmarks or model inference.

## Choose a route

| Route | Stage1 input | Stage2 calculation | Entry |
| --- | --- | --- | --- |
| Default MLFF route | QE-format monolayer geometry; pinned TECE source and weight | MatterSim screen, refine, optional audit | `npc stage1 --model tece`; `npc stage2 …` |
| Alternative MLFF Stage1 | Pinned Prophet OAME-MBD or EquiformerV3+DeNS-OAM | The same MatterSim Stage2 | `npc stage1 --model prophet` or `equiformer-v3` |
| Existing DFT modes | Audited QE–DFPT JSON export and original DFT geometry | The same screening workflow, without MLFF relaxation | `npc stage1 --model qe-dft` |
| Matched DFT reference | Selected imported modes and an existing QE PES grid | MatterSim on those exact configurations; aligned energy/force comparison | `npc stage2 reference` |

The public CLI supports `stage1`, `stage2 screen|refine|audit|reference`, and `status`. A module fallback is `python -m nonlinear_phonon_calculation.cli`. `status` reads saved result counts; it is not a cluster scheduler or a live job monitor.

## Default calculation

The following uses a bundled, explicitly defined `ibrav=0` MoS₂ starting geometry; Stage1 still relaxes it with the selected model. Run Stage1 in the TECE environment and Stage2 in the MatterSim environment; both must contain this package. Paths are examples under the checkout, not preinstalled weights.

```bash
npc stage1 --model tece \
  --structure docs/reference_data/pbe_20260925/full_model_structures/mos2-tece.scf.inp \
  --checkpoint weights/TECE-OAM-RRA-1.0.pt \
  --source-root model-sources/tace \
  --output-dir runs/mos2/tece/stage1

npc stage2 screen \
  --mode-pairs-json runs/mos2/tece/stage1/mode_pairs.selected.json \
  --structure runs/mos2/tece/stage1/relax/optimized_structure.scf.inp \
  --checkpoint weights/mattersim-v1.0.0-5M.pth \
  --output-dir runs/mos2/tece/stage2
```

The [quick start](docs/QUICKSTART.md) continues through `refine`, optional `audit`, restart and the existing-DFT route. These commands perform real calculations; use `--help` and the model-independent import example first when checking an installation.

## What each phase computes

1. **Stage1:** perform a constrained monolayer relaxation with the selected MLFF, then obtain harmonic modes on a default 6×6×1 q mesh. The DFT-import route preserves its input geometry and modes instead. Γ translations are identified by their mass-weighted overlap and excluded from candidates; all branches at finite q remain eligible. Structure and mode covariance determine which q points can safely be grouped.
2. **Screen:** calculate six energies per candidate at `QΓ=±1`, `Qq=−1,0,+1` Å√amu. The finite-difference cubic score is a screening proxy.
3. **Refine:** evaluate a central **5×5** grid, `|Q|≤1` with step 0.5, for the selected complete coupling groups; the default is 20 groups.
4. **Audit, optional:** extend the strongest five refined groups to **9×9**, `|Q|≤2` with step 0.5. This **widens the amplitude window**; it does not densify the same window.

A **q mesh** samples wavevectors. A **PES grid** samples two mode amplitudes. Neither a denser fit nor a smaller residual makes the underlying force field more accurate by itself.

A *mode pair* specifies one Γ direction and one finite-q direction. A *coupling group* may include several Γ-degenerate directions and finite-q siblings. Therefore, 20 selected groups need not mean 20 pairs. Ranking the complete Γ component vector does not reconstruct the full finite-q degenerate tensor. The documented three-atom TMD case has 324 candidates; that is not a universal count.

## Outputs and restart

Stage1 writes `phonon_dataset.json`, `mode_pairs.selected.json`, and, for MLFF relaxation, `relax/optimized_structure.scf.inp` with `relax_summary.json`. Imported DFT references have distinct provenance and may produce `mode_pairs.reference.json`.

Standard screen/refine/audit point checkpoints save energies, not a complete per-point force dataset. Stage2 saves these checkpoints under `pairs/`, phase rankings (`screen_ranking.json`, `refine_ranking.json`, `audit_ranking.json`) and selection data. The reference phase writes `reference_result.json`.

Resume by repeating the **same command with the same inputs and output directory**. Structure, mode file, checkpoint, grids, normalization and energy-accumulation identity must agree. Do not edit a saved identity or mix old points after changing these inputs. Pair-level sharding is available for screen/refine/audit; `reference` uses one checkpointed process per material. See [restart and migration](docs/MIGRATION.md).

## Scientific scope

- The validated geometry workflow is for nonmagnetic hexagonal monolayers with explicit vacuum and periodic boundary conditions. It is not a general three-dimensional variable-cell optimizer.
- The primitive cell determines the phonon branches: `3N` at each q. A sampling supercell adds atoms for periodic compatibility; it does not create extra primitive-cell branches.
- The fit uses **13 specified monomials**, not the complete two-variable quartic Taylor expansion or a complete phonon vertex. It uses energies; available forces are not part of a joint energy–force fit.
- Units are Å, amu, eV per supercell, eV/Å, THz, and Å√amu for real mode coordinates. Taylor derivatives include the documented factorial factors.
- Γ optical modes are not automatically IR active. No Born-charge/Raman-tensor, pump, damping, phonon-lifetime or transport benchmark is supplied by the screening calculation.
- Translational ASR is distinct from the two-dimensional rotational sum rule. Non-analytic long-range corrections, SOC, external fields and full degenerate off-diagonal tensors are outside this implementation.
- Different Stage1 models relax different structures and generate different mode bases. Cross-route coefficient comparisons need reliable mode/subspace matching; pointwise energy and force errors require identical configurations.

[ARCHITECTURE.md](ARCHITECTURE.md) gives the equations, normalization and numerical limits. Rejecting an invalid input or changed identity is deliberate; lowering a validation threshold is not a recovery procedure.

## Published benchmark material

The repository retains the archived **WS₂, MoS₂ and WSe₂ GGA-PBE** study, its editable report and public data. These are historical scientific results, not new model-inference measurements for the 1.0.4 maintenance release.

| Archived comparison | What it measures | Details |
| --- | --- | --- |
| PBE DFT and TECE/Prophet/Equiformer Stage1 + MatterSim | Harmonic modes and five matched nonlinear channels per material; same-configuration diagnostics where explicitly provided | [PBE reference](docs/PBE_REFERENCE.md) |
| TECE+TECE, Prophet+Prophet, EquiformerV3+EquiformerV3 | Same-model PES and fixed-PBE configuration diagnostics via a separate module | [Full-model benchmark](docs/FULL_MODEL_BENCHMARK.md) |
| GRACE, DPA and Eqnorm on fixed PBE configurations | Optional Stage2 diagnostics, not extra default CLI backends | [Small-model benchmark](docs/SMALL_MODEL_BENCHMARK.md) |
| Earlier LDA references and focused WS₂ study | Historical baselines with their original geometry, units and limitations | [LDA comparison](docs/MODEL_DFT_COMPARISON.md), [WS₂ supplement](docs/WS2_DFT_COMPARISON.md) |

The PBE study includes 579 selected QE points and the archived 543-configuration DFT Stage1 + MatterSim baseline. Its focused WS₂ Γ8–M9 17×17 check is one-channel evidence, not full-grid convergence of every mode. Do not combine different structures, windows, functionals or timing environments into a single accuracy or speedup claim.

[Illustrated report](output/pdf/tmd_gga_pbe_mlff_latex_report.pdf) · [Public reference data](docs/reference_data/pbe_20260925/) · [Report sources](reports/pbe_three_materials/) · [Validation history](docs/VALIDATION.md)

## Code and attribution

The installable code is in `nonlinear_phonon_calculation/` and `mlff_modepair_workflow/`; tests are in `tests/`. Report generators and reference data stay outside the wheel. The optional full-model and small-model benchmark modules have separate inputs and dependencies; an isolated research QE controller is not a public CLI feature.

Project code is distributed under the [MIT license](LICENSE). Model code, checkpoints and training data retain their upstream licenses and citation requirements. `npc` supplies the projection and workflow; the pretrained network architectures and training are the work of their respective authors.
