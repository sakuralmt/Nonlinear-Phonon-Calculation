# Hexagonal 2D nonlinear phonon screening

`npc` calculates a complete 6×6×1 harmonic phonon mesh with Phonopy, then ranks Γ–q–(−q) couplings with MatterSim. TECE-OAM-RRA-1.0 is the default Stage1 model; Prophet OAME-MBD and EquiformerV3+DeNS-OAM are optional. It does not run DFT or molecular dynamics.

## What is compared

Stage1 uses all non-Γ q points and every finite-q phonon branch, but only Γ **optical** modes. It identifies the three Γ acoustic translations by mass-weighted eigenvector overlap with rigid translations, not by a frequency index. It discovers the **atomic** symmetry of the supplied structure with spglib and reduces q points only when transformed frequencies and eigenvectors agree. A hexagonal cell with a lower-symmetry atomic motif therefore has more candidates. If the phonons fail the symmetry check, the run records why and falls back to q/−q pairing. It never removes a candidate because of a little-group rule or a predicted coupling. Both Γ and finite-q covariance are checked; a failed Γ multiplet cannot authorize a q reduction. The `mode_maps` in each q orbit record atom permutation, operation, time reversal, branch mapping and overlap diagnostics. A near-degenerate branch number is not treated as a unique physical direction.

Stage2 first evaluates six energies for **every** candidate at `QΓ=±1`, `Qq=−1,0,+1` Å√amu. It ranks complete Γ-subspace channels by the norm of the third-order `Φ122` proxy. The best **20 channels by default** are expanded to all component pairs and calculated on the central 5×5 grid (`−1` to `+1` in 0.5 steps). `audit` optionally extends the strongest five refined channels to 9×9 (`−2` to `+2`). Six points are only a ranking proxy; report fourth-order results from the central fit with its fit quality and window sensitivity. The 20-channel default supersedes earlier 30-channel drafts.

Lengths are Å, masses amu, total supercell energies eV, forces eV/Å, force constants eV/Å², frequencies THz and real normal coordinates Å√amu. Third and fourth derivatives are meV/(Å³·amu³ᐟ²) and meV/(Å⁴·amu²). Outputs use contract **v5** and reject older mode-pair files, including v4 files with Γ acoustic candidates.

## Requirements

See the [completed acceptance and comparison report](docs/ACCEPTANCE.md) for version 1.0.1, including the full q-orbit checks, precision correction and historical DFT limits.

The [model-versus-DFT scientific report](docs/MODEL_DFT_COMPARISON.md) ([PDF](output/pdf/model_dft_comparison_1_0_1.pdf)) compares the rechecked five pairs, third/fourth derivatives, frequencies, mode overlaps, energy surfaces, fit residuals, window sensitivity and CPU cost. It includes the archived QE + MatterSim baseline and the three self-relaxed MLFF routes; no new DFT calculations were required.

See [installation and Slurm execution](docs/INSTALL.md) and the
[numerical architecture](ARCHITECTURE.md). The [validation report](docs/VALIDATION.md)
documents three model routes tested on two additional materials, with
[machine-readable results](docs/validation_v5.json). For N atoms and C verified q orbits,
Stage1 produces `C × (3N−3) × 3N` candidates: 324 for the validated three-atom
TMD cells with six q orbits. A one-atom primitive cell has no Γ optical pairs.

Use Python 3.10+ with `pip install -e .`. Phonopy is pinned to 2.38.0; spglib is required for atomic symmetry. Install the chosen model in its own environment: validated TECE source commit `81f65a4c188bd09cec8d1419388f7afdcc1b6fd0`, Prophet commit `c4fda8251d8a7c90c7cb7842aea4d2f57e5fc3bd`, or EquiformerV3 commit `a7300c58df683dc99cb48027d5bfd4c887486c48`. Stage2 requires MatterSim 1.2.1 and the pinned 5M checkpoint. The adapters verify source/checkpoint hashes before inference. See the corresponding `MODEL_SOURCES` and backend constants for the exact validated files.

For a QE-style input with explicit monolayer vacuum and periodic flags, run:

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

Pass the **same** mode-pair file, structure, checkpoint, output directory and `--top-channels` to every Stage2 phase. Use `npc stage2 audit` with the same arguments for 9×9 diagnostics. Replace `--model tece` with `prophet` or `equiformer-v3` for alternate Stage1; Prophet uses `NPC_PROPHET_SOURCE` or its pinned installation, whereas TECE and Equiformer require `--source-root`.

Stage1 always relaxes its input with the selected model. The optimized structure is saved at `stage1/relax/optimized_structure.scf.inp`; pass **that file** to Stage2. Both stages verify the relaxation provenance and structure hash. The constrained-relaxation writer currently requires a QE-style source with atom flags; it performs no QE calculation.

`--gamma-degeneracy-thz` sets the Γ optical grouping threshold (default 0.01 THz).
A completed Stage1 directory cannot be overwritten; use a fresh directory for
different parameters. Keep the same Stage2 settings throughout a restart.

Stage2 accepts `--shard-index I --shard-count N`. Run the same phase once per shard, then rerun with `--finalize-only` to verify all expected energies and write its ranking. Point checkpoints are reused after interruption. Never change a run's input files or top-channel count in place: its identity hash will reject the restart.

## Outputs and limits

`stage1/phonon_dataset.json` contains the 36 q-point frequencies, eigenvectors, ASR and finite-step diagnostics. `stage1/mode_pairs.selected.json` contains the structure-derived q orbits, branch mappings and complete candidates. Stage2 keeps `pairs/<pair_code>/points.json`, `screen_ranking.json`, `selection.json`, `refine_ranking.json` and optional `audit_ranking.json`. Model weights, source, structure, normalization and screening settings are recorded with each run.

Phonopy's force-constant symmetrizer enforces translational and index-exchange conditions; this release does **not** claim to enforce the 2D rotational sum rule for ZA. No non-analytic correction is applied. This workflow assumes nonmagnetic scalar MLFF energies without an external field. Its symmetry reduction is checked against the Stage1 phonons. Because each Stage1 model relaxes its own structure, cross-model differences include structural effects; compare phonon modes and couplings only after a reliable mode/subspace mapping. Historical QE results should not be called exact same-structure labels without verifying their geometry and displacement convention.

## Repository

`nonlinear_phonon_calculation/cli.py` provides the public interface. `mlff_modepair_workflow/` contains the Phonopy bridge, atomic-symmetry mapping, model adapters, frozen-mode builder and staged PES fit. `tests/` has analytic, structure-symmetry and checkpoint tests. `scripts/` contains optional CPU timing tools. All runtime inputs, checkpoints and model weights stay outside the repository.

### Precision and momentum diagnostics (1.0.1)

MatterSim retains float32 model inference and promotes per-atom energies to float64 **before** summing the supercell energy. This prevents the dominant accumulation error observed for 108-atom cells; it is not full float64 inference. The accumulation protocol is part of every checkpoint identity. Earlier v5 checkpoints and Stage1 files lacking the Γ covariance check are read-only references and cannot be silently resumed with this version.

For a Γ multiplet, each component separately couples to q and −q; the score is the norm of those `Φ122` components. It is not a two-Γ/one-q interaction. Fitted `Φ112` (`QΓ² Qq`) is momentum-forbidden at finite q and is labeled a numerical diagnostic. The 13-term fit retains such terms to expose numerical contamination; ranking never uses them. For a complete Γ multiplet and isolated finite-q mode, comparisons use the third-order vector norm and the fourth-order `Φ1122` trace.
