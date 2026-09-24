# Prophet Stage1/Stage2 v3: static phonons and nonlinear PES

This workflow evaluates **only** Γ–q–(−q) couplings with q≠Γ. It does not run MD or create new DFT PES labels. MLFF Stage1 now defaults to pinned Phonopy 2.38.0 with explicit translational ASR; the previous custom diagonalizer is available only with `--phonon-engine custom` for historical regression. The default model combination is Prophet Stage1 and MatterSim Stage2 ranking. A 6×6×1 hexagonal mesh has 35 finite q points in six point-group/time-reversal orbits. Every branch of a three-atom primitive cell is retained: 6×9×9 = **486 mode pairs**, each evaluated on **81 energy points**. The six orbit representatives are a computational reduction; the Stage1 dataset still contains modes at all 36 q points. Near-degenerate branches are identified as subspaces. Branch-specific coupling coefficients inside such a subspace depend on the chosen basis.

## Reproducible inputs

Use a Python 3.11+ environment. Install this testing worktree with `python -m pip install -e '.[prophet]'`. The optional dependency pins Prophet source commit `c4fda8251d8a7c90c7cb7842aea4d2f57e5fc3bd`. The runtime verifies the SHA-256 of five source modules and the OAME-MBD checkpoint (`28b21122f4c6c1a7c5fe9bac8a0182edf9d24a7a65a72a9ca80b8d12d4620514`); an alternate checkpoint fails before calculation. A source checkout can be supplied with `NPC_PROPHET_SOURCE=/path/to/prophet-source`. Set `NPC_PROPHET_CHECKPOINT=/path/to/prophet-oame-mbd.pt`; the model is not downloaded silently. The checkpoint covers atomic numbers 1–83 and 89–94. The parser accepts one, two, or three (and larger) atom primitive cells, subject to element coverage and a nonsingular, fully periodic cell with monolayer vacuum. The q-orbit workflow requires a hexagonal in-plane cell.

For the main comparison, pass an explicit QE-format **DFT-relaxed structure** that is shared by all models and record its provenance. For the structure-sensitivity line, use the same initial structure but `--geometry-source model_relaxed`; Prophet relaxes internal positions and one isotropic in-plane scale, retaining the original vacuum. Use different `--run-root` paths for the two lines and the two materials. The source and optimized structure hashes are recorded. `--qe-relax no` avoids silently generating a different DFT structure from a CIF.

```bash
export NPC_PROPHET_CHECKPOINT=/path/to/prophet-oame-mbd.pt
./npc --stage stage1 --stage1-backend prophet \
  --stage2-model mattersim_v1_5m --system-dir /path/to/mos2-system \
  --run-root /path/to/runs/mos2/shared_dft \
  --stage1-structure /path/to/shared-dft/mos2.scf.inp \
  --structure-provenance 'archived QE stage1, record/hash: ...' \
  --geometry-source shared_dft --qe-relax no \
  --q-grid-n 6 --fd-step 0.01 --stage1-device cpu

./npc --stage stage2 --stage2-model mattersim_v1_5m \
  --run-root /path/to/runs/mos2/shared_dft --stage2-device cpu
```

Repeat for WSe₂ and for separate `model_relaxed` run roots. Stage1 uses ±0.01 Å force differences on the commensurate supercell, then rebuilds the complete force constants at ±0.005 Å to check frequency convergence. It stores the full force constants, every q frequency/eigenvector, q-orbit membership, acoustic and Hermiticity diagnostics. Historical QE modes are an **offline validation source**, not the new mode-pair source. To compare a completed Stage1 run with archived QE files, use `python scripts/compare_prophet_stage1_qe.py --phonon-dataset ... --matdyn-input ... --qe-eig ... --output ...`.

Stage2 uses both mass-weighted mode coordinates from -2 to 2 Å√amu at 0.5 increments and fits the center 5×5 points. Its 13-column polynomial has rank 13 on that window; each pair records rank, condition number, residuals, third/fourth **derivatives** (including factorials), frequency checks, elapsed time, model and structure hashes, and the raw grid. The pinned model emits float32 atomic energies; the adapter sums these values in float64 to avoid further rounding of a large supercell total. This does not recover precision already lost inside each atomic energy, so weak couplings still require a grid-noise check. The accumulation method is included in each run signature. A point is written to an atomic checkpoint immediately after its energy is finite. Restarting the same command skips saved points. Different signatures in one output root are rejected. Diagnostic partial runs are available in the direct CLI; the normal npc Stage2 calculates all pairs.

For controlled multi-process execution, launch disjoint direct-CLI shards with a common input and output, `--shard-index 0 ... N-1 --shard-count N`; each process loads its own calculator once. Pair-level locks prevent duplicate evaluations if a shard is accidentally started twice. After all shards finish, run one process with `--finalize-only`. The finalizer verifies all expected pairs, all 81 points, matching signatures, and raw-grid checksums. Assign each GPU to at most one shard unless a measured pilot shows sharing is safe. Benchmark one representative pair and one 12×12 supercell energy/force call before setting concurrency; record GPU peak memory, CPU RSS, and wall time. A full four-line campaign comprises 4×486×81 = **157,464 energy evaluations** plus four Stage1 runs. Budget from measured throughput rather than starting all jobs at once.

On Slurm, `scripts/slurm_prophet_stage2.sh` provides a one-GPU-per-shard array template. Set the five `NPC_*` input/output variables shown in its header and submit the array size found safe by the pilot. Run the finalizer after the array completes. Use a separate output root for each material and geometry line.

## Units and comparison rules

Structure Å; mass amu; total energy eV/supercell; force eV/Å; force constants eV/Å²; frequencies THz; real mass-weighted mode coordinate Å√amu. The actual real supercell displacement is normalized to unit mass-weighted norm for self-conjugate and general q. Third derivatives are meV/(Å³·amu³ᐟ²); fourth derivatives are meV/(Å⁴·amu²). The saved polynomial coefficients are **not** derivatives. QE energies are converted from Ry only when the source unit is explicitly declared; there is no magnitude-based inference.

Old Stage1/Stage2 v2 results remain read-only. In particular, old K-point real-mode amplitudes use a different normalization, so their coefficients cannot enter a v3 ranking without recovering the old coordinate scale. Map QE and MLFF modes by complex overlap, symmetry and near-degenerate subspace overlap. Physical coupling comparison does not require the real standing-wave displacements to coincide pointwise; pointwise energy-grid subtraction does. A single direction inside a rotated degenerate subspace cannot recover the full invariant coupling tensor. Unverified geometry or incomplete mode mapping must be labelled as such.

## Promotion gate

Before a stable-repository handoff, require four independent run roots to each have 486/486 pairs and 81/81 finite grid values; no extra or duplicate summaries; explain all structure/unit/model-hash mismatches; pass analytic and 1/2/3-atom tests; verify rank 13 and real-mode unit norms; compare MoS₂ and WSe₂ full-grid frequencies, isolated modes, and degenerate subspaces against the archived QE calculation; exercise repeated runs, interruption/resume, different shard counts, 12×12 stress, and resource-limited server execution. Preserve a run manifest, physical comparison report, and performance report with exact source hashes and commands. Work in a separate Git testing worktree; only promote after the gate is satisfied.
