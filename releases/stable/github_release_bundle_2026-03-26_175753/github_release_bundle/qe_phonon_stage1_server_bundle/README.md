# QE Phonon Stage1 Server Bundle

This directory is the real `stage1` runtime used by the stable bundle.

It is responsible for:

- starting from `scf.inp`
- running `pw.x -> ph.x -> q2r.x -> matdyn.x`
- exporting `qeph.eig` and `qeph.freq`
- producing the frontend outputs that later become `selected_mode_pairs.json`

It does not perform `stage2` screening or `stage3` QE recheck.

## Entry points

```bash
python run_all.py
python assess_stage1_env.py
```

`run_all.py` is the stage1 runtime entrypoint. `assess_stage1_env.py` is the top-level environment detector that probes Slurm, MPI launchers, QE executables, and stage-specific resources.

## Default runtime assumptions

- intended host: `159.226.208.67:33223`
- system type: Slurm cluster
- default q-grid: `6x6x1`
- active phonon profile: `phonon.balanced`
- default stage resources:
  - `pw: 1 x 24`
  - `ph: 4 x 24`
  - `q2r: 1 x 1`
  - `matdyn: 1 x 24`

The active balanced profile is:

- `ecutwfc = 100`
- `ecutrho = 1000`
- `primitive_k_mesh = 12x12x1`
- `conv_thr = 1.0d-10`
- `degauss = 1.0d-10`

## Runtime behavior

`run_all.py` will:

1. assess the machine
2. resolve launcher / partition / walltime / node layout
3. prepare stage-specific QE inputs
4. submit and wait for `pw/ph/q2r/matdyn`
5. write `frontend_manifest.json` and `stage1_summary.json`

The output root is generated at runtime. This source bundle intentionally does not ship prebuilt `inputs/`, `qe_phonon_pes_run/`, or validation snapshots.

## Main outputs

- `qe_phonon_pes_run/frontend_manifest.json`
- `qe_phonon_pes_run/results/stage1_env_assessment.json`
- `qe_phonon_pes_run/results/stage1_env_assessment.md`
- `qe_phonon_pes_run/results/stage1_runtime_config.json`
- `qe_phonon_pes_run/results/stage1_summary.json`
- `qe_phonon_pes_run/matdyn/qeph.eig`
- `qe_phonon_pes_run/matdyn/qeph.freq`
