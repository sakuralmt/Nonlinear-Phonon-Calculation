# Installation and CPU execution

The base package needs Python 3.10 or later. A fresh Python 3.12 environment,
installation with `pip install .`, and the full test suite were checked for this
release. Phonopy is fixed to 2.38.0. Model inference additionally needs the
selected model's environment and checkpoint; installing the base package alone
does not install all model stacks or download weights.

```bash
python3 -m venv /envs/npc-base
/envs/npc-base/bin/python -m pip install .
/envs/npc-base/bin/npc --help
```

Use separate environments for MatterSim, Prophet and the advanced models.
The validated Linux TECE/Equiformer environment used Python 3.12.11 and Torch
2.4.0+cu121 with CPU inference; Prophet used the separate Python 3.11 environment.
An environment's CUDA-capable Torch build does not mean these jobs used a GPU.
Model dependency installation is platform dependent; the release verifies source
and weight hashes instead of silently substituting a model when an import fails.

| Model | Source / package | Checkpoint SHA256 |
| --- | --- | --- |
| TECE-OAM-RRA-1.0 | [TACE](https://github.com/xvzemin/tace), commit `81f65a4c188bd09cec8d1419388f7afdcc1b6fd0` | `9f36562582d931347c3904f763e820edcaf5f27c3f13beb6776e49fcc7de38bb` |
| Prophet OAME-MBD | [Prophet](https://github.com/kairosmaterial/prophet), commit `c4fda8251d8a7c90c7cb7842aea4d2f57e5fc3bd` | `28b21122f4c6c1a7c5fe9bac8a0182edf9d24a7a65a72a9ca80b8d12d4620514` |
| EquiformerV3+DeNS-OAM | [EquiformerV3](https://github.com/atomicarchitects/equiformer_v3), commit `a7300c58df683dc99cb48027d5bfd4c887486c48` | `429ccded98163122e7ba588d78e2441653f37f3e091e106c432807fe373c8f98` |
| MatterSim 5M | `mattersim==1.2.1` | `e3df9fa708725e3d453140646c7d1838324b347a3d1214cf1440522146f872b5` |

Prophet can be installed with this project's `prophet` extra. Its adapter also
accepts the pinned source checkout through `NPC_PROPHET_SOURCE`. TECE and
Equiformer require `--source-root`; preserve the unmodified pinned source export,
because its tracked source-tree content is checked. Follow each model's upstream
dependency instructions in its isolated environment, then install this package
there. Supply the downloaded weight path explicitly.

The input is a periodic QE-format monolayer geometry with explicit cell vectors,
atomic positions and any intended atomic constraints. The Stage1 calculator
performs its own constrained relaxation. The Stage2 structure is always
`stage1/relax/optimized_structure.scf.inp`. Preserve `relax_summary.json` with it.
The relaxation protocol and its limits are documented in [ARCHITECTURE](../ARCHITECTURE.md).

## Slurm

The example [Stage2 script](../scripts/slurm_stage2.sbatch) uses one node and
16 allocated CPUs, divided between four independent MatterSim workers. Each
worker loads one model instance. All phases share point checkpoints. It verifies
every worker's exit status before finalizing or starting the next phase.

```bash
export NPC_PYTHON=/envs/mattersim/bin/python
export NPC_RUN=/runs/mose2/tece
export NPC_CHECKPOINT=/models/mattersim-v1.0.0-5M.pth
sbatch scripts/slurm_stage2.sbatch
```

Choose allocations from `scontrol show node` and the site's Slurm configuration;
an allocated CPU is not universally the same as a physical core. The validation
node had 64 physical cores and `ThreadsPerCore=1`. Keep all related running jobs
within the account's agreed ten-node ceiling, inspecting the full account queue
before submitting another batch. The example allocates one node; it is not an
account-wide submission controller. Do not run independent, uncoordinated large
arrays from multiple terminals.

For Stage1, run the README command in the model's environment inside a CPU Slurm
job. The validation used one task, 16 CPUs and 128 GB requested memory per Stage1
job. Record the actual peak memory; requested memory is not measured usage.

## Tests and package contents

```bash
python -m pip install pytest
python -m pytest -q tests
```

The wheel includes only `nonlinear_phonon_calculation` and
`mlff_modepair_workflow`. It excludes historical QE/GPTFF/EquFlash workflows,
Stage3 recomputation code, model weights and server run directories. Historical
scientific comparisons in validation notes do not add those execution paths to
the package.
