# MatterSim v3 Stage2 comparison

The baseline runs **MoS2 and WSe2 shared-DFT geometries only**. It reuses each
material's Prophet v3 Stage1 `mode_pairs.selected.json` and structure. Thus
both backends evaluate the same 486 Γ–q mode pairs on identical 9×9 real-mode
displacement grids; no DFT energy calculations or MatterSim Stage1 modes are
added. The output tag is `mattersim-v1-5m`, separate from Prophet checkpoints.

The pinned weights are `mattersim-v1.0.0-5M.pth` with SHA256
`e3df9fa708725e3d453140646c7d1838324b347a3d1214cf1440522146f872b5`.
The calculator adapter requires MatterSim 1.2.1, validates the weight hash,
and records both identifiers in every result. The checkpoint can be resumed
at individual grid points. Different input structures, pair files, weights,
or backends cannot share a run directory.

On the server, place the pinned checkpoint at
`$NPC_CAMPAIGN_ROOT/model/mattersim-v1.0.0-5M.pth`, install MatterSim 1.2.1
and its runtime dependencies in `$NPC_CAMPAIGN_ROOT/venv-mattersim`, and copy
the two input directories under `$NPC_CAMPAIGN_ROOT/inputs/{mos2,wse2}/shared_dft`.
Submit the two-task Slurm array with

```bash
NPC_CAMPAIGN_ROOT=/absolute/server/campaign/root sbatch scripts/slurm_mattersim_stage2_cpu_campaign.sh
```

The script defaults to six workers with four CPU threads each per material.
Set `NPC_CPU_WORKERS` and `NPC_CPU_THREADS` only within the allocated Slurm
CPU count. A successful run writes `run_meta.json`, `pair_ranking.csv`, and
`pair_ranking.json` after verifying 486 complete 81-point grids. Compare
energy and couplings with Prophet only after checking identical input hashes
and remembering that these are Prophet-mode projections for both models.
