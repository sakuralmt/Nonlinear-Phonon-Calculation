# Prophet + MatterSim v3: controlled QE recheck

This is the test-worktree runbook for the static phonon and nonlinear PES project. The Git-managed stable repository remains the release baseline; this worktree is the validation surface. No molecular dynamics calculation is included.

## Data contract

- Use one source-identified DFT-relaxed QE structure per material for the main comparison. Keep Prophet-relaxed structures in separate runs for structure sensitivity. Never merge their rankings.
- Stage1 is Prophet finite-displacement or QE matdyn import, with a full 6×6×1 mesh. Both produce v3 unit-mass real-mode coordinates. Only Γ–q–(−q), q ≠ Γ, is retained: 35 finite q points, six symmetry orbits and 486 mode pairs for a three-atom primitive cell. Symmetry is used only to group equivalent q points.
- Formal import of an archived QE mesh requires the QE calculation's actual source structure. The importer refuses a cell or position discrepancy above 0.0001 Å. An archive without this evidence can be examined as an explicitly unverified historical reference, but cannot feed formal Stage2 or Stage3.
- Stage2 is Prophet or MatterSim on the same 9×9 `[-2,2] Å√amu` grid. The 5×5 center is the primary 13-parameter fit. Stage1 and Stage2 are matched by SHA-256 hashes of the actual structure and mode-pair file.
- Stage3 uses the exact Stage1 v3 displacement builder, with QE energy and forces at all 81 points per selected pair. `Ry` and `Ry/Bohr` are converted explicitly to `eV` and `eV/Å`.

The current scientific run chooses Prophet's top 20 by absolute Φ122 and then the 10 highest MatterSim pairs absent from that set. Run the first 20 as a pilot phase, and extend the same QE directory to 30; neither model gets a separate DFT displacement basis. The v3 Stage3 CLI also supports a single Stage2 top-N selection for other runs.

## Existing isolated server campaign

The live campaign root is `/home/lmtsakura/qiyan_shared/testing/prophet-stage12-v3`. Four Prophet CPU array tasks continue independently under Slurm. The code under `code/` is in use by those tasks. New Stage3 and advanced-model validation uses `code-next/` so live processes are not changed. The completed MatterSim shared-structure rankings live under `runs/{mos2,wse2}/shared_dft/stage2/mattersim-v1-5m/screening/`. Source structures, v3 modes and pseudopotentials live under `inputs/{mos2,wse2}/shared_dft/`.

Do not select the joint 30 until the corresponding complete Prophet `pair_ranking.json` exists. Do not use model-relaxed rankings for the shared-DFT QE recheck.

The prior WSe₂ 12×12 QE archive's source input differs from the shared DFT structure by 0.000378 Å in Se positions. Its 6×6 subset is a historical diagnostic, not an exact-structure QE Stage1 label. The archived MoS₂ eigenvector file lacks an independently verified source structure in the current handoff. Historical Prophet-versus-QE frequency/eigenvector reports are under the local `historical_qe_v3/{mos2,wse2}` validation directories and are marked `qe_geometry_verified=false`.

## Prepare a common QE batch

From `code-next/`, set the material-specific paths to the **same** structure and v3 modes used by both rankings. For MoS₂, for example:

```bash
campaign=/home/lmtsakura/qiyan_shared/testing/prophet-stage12-v3
python="$campaign/conda-cpu/bin/python"
material=mos2
input="$campaign/inputs/$material/shared_dft"
prophet="$campaign/runs/$material/shared_dft/stage2/prophet/screening/pair_ranking.json"
mattersim="$campaign/runs/$material/shared_dft/stage2/mattersim-v1-5m/screening/pair_ranking.json"
qe="$campaign/runs/$material/shared_dft/stage3/qe_v3/joint"

"$python" -m mlff_modepair_workflow.stage3_v3 select \
  --mode-pairs-json "$input/mode_pairs.selected.json" --structure "$input/structure.scf.inp" \
  --prophet-ranking "$prophet" --phase 20 --output "$qe/selection20.json"
"$python" -m mlff_modepair_workflow.stage3_v3 prepare \
  --selection "$qe/selection20.json" --mode-pairs-json "$input/mode_pairs.selected.json" \
  --structure "$input/structure.scf.inp" --pseudo-dir "$input/pseudos" \
  --output-root "$qe" \
  --env-init-line 'source /opt/intel/oneapi/setvars.sh >/dev/null 2>&1 || true'
```

Repeat preparation for WSe₂ in its own run directory. This creates inputs but submits no QE jobs. Check that each prepared point has `scf.inp`, `point_meta.json`, and a `regular` partition `submit.sh`. The entire batch is rejected on structure, mode, ranking, pseudopotential, input, or normalization mismatch.

## One shared Slurm controller

Register both material run roots in **one** global state directory. The controller uses an account-wide lock even if different state directories are supplied, counts active QE jobs across the user's queue (including historical recheck job names), and submits at most five new points per poll. Candidates alternate between registered materials, so a small batch does not starve the second material. It never exceeds the requested user-wide cap of 30 queued or running QE points. A point is retried at most once; uncertain newly submitted jobs get a two-minute visibility grace to prevent duplicate submissions.

```bash
state="$HOME/qiyan_shared/stage3_qe_global"
"$python" -m mlff_modepair_workflow.stage3_scheduler register --state-root "$state" \
  --run-root "$campaign/runs/mos2/shared_dft/stage3/qe_v3/joint"
"$python" -m mlff_modepair_workflow.stage3_scheduler register --state-root "$state" \
  --run-root "$campaign/runs/wse2/shared_dft/stage3/qe_v3/joint"

# First 81-point pair per material: low-concurrency timing and failure pilot.
"$python" -m mlff_modepair_workflow.stage3_scheduler launch --state-root "$state" \
  --max-active 5 --submit-batch 2 --pair-limit 1

"$python" -m mlff_modepair_workflow.stage3_scheduler status --state-root "$state"
```

After both pilot pairs have 81 valid energy-and-force outputs, inspect Slurm walltime, SCF convergence and memory. Then launch the same controller without `--pair-limit`, using `--max-active 30 --submit-batch 5`. Do not launch an independent controller per material. Registering an additional material while the controller runs adds it to the next polling pass. The controller itself is one `regular` Slurm job, so no long-running login-node process is required.

When the first 20 pairs are complete, generate `selection30.json` with both rankings and `--phase 30`, then run `prepare` again with the **same** `--output-root`. It preserves the first 20 and creates only the 10 missing pairs per material. Launch the shared controller again; completed points are reused.

## Recovery and comparison

The QE collector requires all 81 energy and force values for a pair. It writes `energy_grid_eV.npy`, `forces_eV_per_A.npy`, per-pair fitted coefficients and `results/qe_v3_ranking.json`:

```bash
"$python" -m mlff_modepair_workflow.stage3_v3 collect --output-root "$qe"
```

If MLFF force metrics are needed, run `stage3_force_grid` once per backend and material, using the model's original weight file; it checkpoints each point and rejects a changed model hash. Then run `stage3_compare` with both Stage2 screening directories. It reports energy errors after subtracting each grid's center energy, for both 9×9 and center 5×5; available force MAE/RMSE; Φ122, Φ112 and Φ1122 errors; 5×5/7×7/9×9 fit sensitivity; energy quantization; and rankings **only within the fixed 30 QE-selected pairs**. Near-degenerate single-branch eigenvectors remain basis dependent and require subspace comparison.

## Advanced Stage1 trial

`advanced_stage1.py` accepts three isolated ASE backends: `tece-oam-rra-1.0`, `equflashv2-45m-oam`, and `equiformer-v3-oam`. Their source commits and full source-tree hashes are pinned in the adapter. Each needs its own Python environment and weight file. Run `--preflight-only` on periodic MoS₂ and WSe₂ first; it checks energy and forces, repeated inference, and force-versus-energy finite differences without launching a full phonon mesh. Only then run the full 6×6 calculation. It produces the same v3 mode-pair file, which can be passed to **MatterSim only** for Stage2. The public Matbench discovery rank is not treated as evidence of phonon accuracy.

Each replacement model now has its **own model-relaxed structure line** for both materials. Run `advanced_stage1.py --geometry-source model_relaxed --relax-only` first, then use the same model, checkpoint, source export, initial DFT structure and output directory for the full Stage1. It reuses the verified relaxation and performs a second preflight on the optimized structure. The optimization protocol matches Prophet's constrained BFGS of allowed internal coordinates plus a one-dimensional isotropic in-plane scale search; the vacuum and QE relaxation flags remain fixed. The relaxation summary records the source and optimized geometry hashes, model identity, scale, energy, and convergence. The modular entry also accepts `--geometry-source model_relaxed` for all three advanced models and hands the optimized structure to Stage2. Use a separate run root for each `(model, material, geometry_source)` tuple; the manifest rejects an existing run root for another model or geometry.

For substitution, compare each advanced model's own-relaxed Stage1 followed by the **same fixed MatterSim Stage2** against Prophet's own-relaxed Stage1 plus MatterSim Stage2. Keep a shared-DFT-geometry line for both models as a control: at fixed structure it isolates the Stage1 model effect, while the within-model shared-versus-own comparison shows the structure effect. Do not directly compare couplings between unmatched mode bases or geometries, and do not add DFT grids on the model-relaxed structures in this campaign.

EquiformerV3 passed the strengthened real-checkpoint GPU preflight for MoS₂ and WSe₂ on the shared DFT structures (Slurm job 1019360). An optional `pyg_lib` wheel required a newer system glibc, so it was removed from the isolated environment; the installed `torch_sparse` and `torch_scatter` still import. The preflight reads raw calculator forces because QE input relaxation flags become ASE constraints, and checks force against a finite energy derivative at a deliberately displaced structure. The numerical evidence and pinned hashes are in `equiformer_v3_preflight.md`. No full EquiformerV3 Stage1 mesh has been launched. Do not include an advanced model's Stage1 results in the comparison report before its full mesh and 486-pair MatterSim Stage2 run pass.

## Release gates

For the default Prophet+MatterSim combination, MoS₂ and WSe₂ each need 486/486 Stage2 pairs and 39,366/39,366 finite energy points. The joint QE Stage3 needs 30/30 pairs and 81/81 points for each pair in both materials, with no duplicate job identity or unexplained hash mismatch. Keep software stability separate from physical accuracy. Only after these gates, the cross-model comparison, source/Slurm manifests and performance report are complete should a separate commit be handed to the Git-managed stable repository.
