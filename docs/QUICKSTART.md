# Quick start

[中文](QUICKSTART_zh.md) · [Install dependencies and weights](INSTALL.md) · [DFT contract](DFT_STAGE1.md)

These examples use TMDCs, the main detailed validation set. Screening of other hexagonal materials has not been tested as extensively; see [the scope note](SYMMETRY_SCOPE.md).
Run these examples from a source checkout or unpacked source distribution. The MLFF examples use explicit-cell (`ibrav=0`) MoS₂ starting geometries and still relax them with the selected model; the DFT-import examples preserve the original DFT structure. The wheel does not bundle that data directory. No checkpoint is provided by the core installation.

## 1. Check the installation without a calculation

```bash
npc --version
npc --help
npc stage1 --help
npc stage2 --help
npc status --help
```

`python -m nonlinear_phonon_calculation.cli` is an equivalent entry point. The following **DFT data import** is also model independent: it validates already computed modes; it does not run DFT or model inference.

```bash
npc stage1 --model qe-dft \
  --structure docs/reference_data/pbe_20260925/structures/mos2.scf.inp \
  --phonon-dataset docs/reference_data/pbe_20260925/qe_phonon_dataset_mos2.json \
  --output-dir runs/mos2/pbe/stage1
```

Use a new output directory for an independent experiment. Importing a geometry file alone cannot create DFT phonon eigenvectors.

## 2. Default TECE + MatterSim route

First prepare the pinned sources, weights and environments in [INSTALL.md](INSTALL.md). Set the actual interpreters for the two stages:

```bash
export NPC_STAGE1_PYTHON=.venv-tece/bin/python
export NPC_STAGE2_PYTHON=.venv-ms/bin/python
```

The following Stage1 command performs a real constrained relaxation and harmonic calculation; it is not an installation smoke test:

```bash
"$NPC_STAGE1_PYTHON" -m nonlinear_phonon_calculation.cli stage1 --model tece \
  --structure docs/reference_data/pbe_20260925/full_model_structures/mos2-tece.scf.inp \
  --checkpoint weights/TECE-OAM-RRA-1.0.pt \
  --source-root model-sources/tace \
  --output-dir runs/mos2/tece/stage1
```

Then run screen and refine in the MatterSim environment:

```bash
"$NPC_STAGE2_PYTHON" -m nonlinear_phonon_calculation.cli stage2 screen \
  --mode-pairs-json runs/mos2/tece/stage1/mode_pairs.selected.json \
  --structure runs/mos2/tece/stage1/relax/optimized_structure.scf.inp \
  --checkpoint weights/mattersim-v1.0.0-5M.pth \
  --output-dir runs/mos2/tece/stage2

"$NPC_STAGE2_PYTHON" -m nonlinear_phonon_calculation.cli stage2 refine \
  --mode-pairs-json runs/mos2/tece/stage1/mode_pairs.selected.json \
  --structure runs/mos2/tece/stage1/relax/optimized_structure.scf.inp \
  --checkpoint weights/mattersim-v1.0.0-5M.pth \
  --output-dir runs/mos2/tece/stage2
```

By default refine selects 20 complete coupling groups and samples `[-1,1]` with step 0.5. Group expansion may include more than 20 specific pairs. To use `--top-channels`, choose it before the run and keep it identical in every phase; changing it changes run identity.

Only run the following if an **expanded amplitude window** is wanted:

```bash
"$NPC_STAGE2_PYTHON" -m nonlinear_phonon_calculation.cli stage2 audit \
  --mode-pairs-json runs/mos2/tece/stage1/mode_pairs.selected.json \
  --structure runs/mos2/tece/stage1/relax/optimized_structure.scf.inp \
  --checkpoint weights/mattersim-v1.0.0-5M.pth \
  --output-dir runs/mos2/tece/stage2
```

Audit extends the strongest five refined groups to `[-2,2]` with step 0.5. It is not the same-window 5→9 density experiment and does not run a new q mesh.

To inspect saved counts:

```bash
"$NPC_STAGE2_PYTHON" -m nonlinear_phonon_calculation.cli status \
  --output-dir runs/mos2/tece/stage2
```

## 3. Other Stage1 models

For Prophet, use its environment and fixed OAME-MBD checkpoint; `--source-root` is not required for the installed package:

```bash
npc stage1 --model prophet \
  --structure docs/reference_data/pbe_20260925/full_model_structures/mos2-prophet.scf.inp \
  --checkpoint weights/prophet-oame-mbd.pt \
  --output-dir runs/mos2/prophet/stage1
```

For the pinned EquiformerV3+DeNS-OAM route:

```bash
npc stage1 --model equiformer-v3 \
  --structure docs/reference_data/pbe_20260925/full_model_structures/mos2-equiformer-v3.scf.inp \
  --checkpoint weights/omat24-mptrj-salex_gradient.pt \
  --source-root model-sources/equiformer-v3 \
  --output-dir runs/mos2/equiformer-v3/stage1
```

For either route, supply **its own** mode-pair JSON and relaxed structure to the same MatterSim Stage2 commands. Do not combine a TECE pair file with another model's geometry.

## 4. Keep existing DFT geometry and modes

The model-independent import in step 1 produces `runs/mos2/pbe/stage1/mode_pairs.selected.json`. In the MatterSim environment, it can feed the full screen/refine/audit path:

```bash
npc stage2 screen \
  --mode-pairs-json runs/mos2/pbe/stage1/mode_pairs.selected.json \
  --structure docs/reference_data/pbe_20260925/structures/mos2.scf.inp \
  --checkpoint weights/mattersim-v1.0.0-5M.pth \
  --output-dir runs/mos2/pbe/stage2
```

For a detailed comparison on the **existing selected QE configurations**, import the selection into a separate directory:

```bash
npc stage1 --model qe-dft \
  --structure docs/reference_data/pbe_20260925/structures/mos2.scf.inp \
  --phonon-dataset docs/reference_data/pbe_20260925/qe_phonon_dataset_mos2.json \
  --selection-json docs/reference_data/pbe_20260925/selection_mos2.json \
  --output-dir runs/mos2/pbe/reference-stage1

npc stage2 reference \
  --mode-pairs-json runs/mos2/pbe/reference-stage1/mode_pairs.reference.json \
  --structure docs/reference_data/pbe_20260925/structures/mos2.scf.inp \
  --checkpoint weights/mattersim-v1.0.0-5M.pth \
  --reference-results docs/reference_data/pbe_20260925/results_mos2.json \
  --output-dir runs/mos2/pbe/reference-stage2
```

Reference uses the existing coordinates and compares center-aligned energies, all atomic force components and fitted derivatives. It does not rescreen the selected pairs or submit QE. Run one checkpointed process per material; `--shard-count` other than 1 and `--finalize-only` are not supported for this phase.

## 5. Resume and optional sharding

Repeat an interrupted command with unchanged inputs and output directory. The saved identity decides whether old points can be reused; the existence of a file with a familiar name is not sufficient.

For screen/refine/audit, independent workers can divide complete pairs. The example below shows one of two screen shards; run the same command with `--shard-index 1` for the other, then wait for both to exit successfully:

```bash
npc stage2 screen \
  --mode-pairs-json runs/mos2/tece/stage1/mode_pairs.selected.json \
  --structure runs/mos2/tece/stage1/relax/optimized_structure.scf.inp \
  --checkpoint weights/mattersim-v1.0.0-5M.pth \
  --output-dir runs/mos2/tece/stage2 \
  --shard-index 0 --shard-count 2

npc stage2 screen \
  --mode-pairs-json runs/mos2/tece/stage1/mode_pairs.selected.json \
  --structure runs/mos2/tece/stage1/relax/optimized_structure.scf.inp \
  --checkpoint weights/mattersim-v1.0.0-5M.pth \
  --output-dir runs/mos2/tece/stage2 \
  --finalize-only
```

Do not start refinement before the screen phase is complete and finalized. Shards each load a model, so allocate memory and CPU threads accordingly. See [INSTALL.md](INSTALL.md) for the optional Slurm example and [MIGRATION.md](MIGRATION.md) before moving existing results.
