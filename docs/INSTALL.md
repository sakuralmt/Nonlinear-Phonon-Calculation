# Installation, model sources and execution environments

[English quick start](QUICKSTART.md) · [中文快速开始](QUICKSTART_zh.md) · [Migration](MIGRATION.md)

## 1. Install the workflow first

Python **3.10 or newer must already be installed**; the example uses `python3.12`. Acquire the published source tag below, or skip the clone/checkout commands if you already have it:

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

Use `python -m pip install -e .` when developing. The optional installer selects an interpreter explicitly:

```bash
NPC_INSTALL_PYTHON=.venv/bin/python ./install.sh
```

`install.sh` defaults to editable installation; `./install.sh --wheel` selects a non-editable `pip install .`. Despite the option name, it does not take a prebuilt wheel path. `NPC_INSTALL_PYTHON` is the installer variable. The separate Slurm example uses `NPC_PYTHON` for its inference interpreter.

The base requirements are NumPy, SciPy, ASE, Matplotlib, **Phonopy 2.38.0**, and spglib 2.x. Python 3.10+ is the package declaration, not a promise that every optional model supports every Python version. The earlier Linux TECE/Equiformer run used Python 3.12.11 and Torch 2.4.0+cu121 for **CPU** inference; Prophet used a separate Python 3.11 environment. These identify historical environments, not a fresh all-model installation or speed benchmark for this maintenance release.

The base install does not download weights, install every model, install QE, or make a GPU necessary. `npc --help` and `npc --version` do not load a model. The module entry point is `python -m nonlinear_phonon_calculation.cli`; the package root has no `__main__` entry.

## 2. Use a separate environment for each model stack

Install this project into each environment from which you run it. Stage1 and Stage2 exchange JSON and structure files, so their model dependencies need not coexist. The examples below are installation recipes; platform-specific PyTorch/PyG dependencies still follow the pinned upstream instructions. They are not a claim that all optional inference stacks were rerun for 1.0.4.

### MatterSim Stage2

In the intended MatterSim environment, from the project root:

```bash
python -m pip install .
python -m pip install 'mattersim==1.2.1'
```

The adapter accepts only the pinned **MatterSim-v1.0.0-5M** checkpoint. The package version `1.2.1` and the weight version `v1.0.0` describe different things. Upstream's default 1M model, a newer package or another 5M file is not silently substituted. Use `--device cpu` for CPU inference; the public device choices are `cpu` and `cuda`, not Apple MPS. A CUDA-enabled Torch build alone does not mean a job used a GPU.

Upstream: [MatterSim 1.2.1 installation](https://github.com/microsoft/mattersim/tree/v1.2.1) and [its pretrained files](https://github.com/microsoft/mattersim/tree/v1.2.1/pretrained_models).

### TECE Stage1

Keep the inference source checkout clean and at the exact commit below. Install the package through a separate build location rather than generating build metadata inside that checked source tree:

```bash
python -m pip install .
python -m pip install 'TACE @ git+https://github.com/xvzemin/tace.git@81f65a4c188bd09cec8d1419388f7afdcc1b6fd0'
git clone https://github.com/xvzemin/tace.git model-sources/tace
git -C model-sources/tace checkout 81f65a4c188bd09cec8d1419388f7afdcc1b6fd0
```

Pass `--source-root model-sources/tace` and the TECE weight explicitly. TECE is the Edge model implemented in the TACE repository; it is not an interchangeable TACE checkpoint. The pinned upstream instructions are in [README.rst](https://github.com/xvzemin/tace/blob/81f65a4c188bd09cec8d1419388f7afdcc1b6fd0/README.rst) and [pyproject.toml](https://github.com/xvzemin/tace/blob/81f65a4c188bd09cec8d1419388f7afdcc1b6fd0/pyproject.toml).

### Prophet Stage1

Use Python 3.11 or a compatible environment required by the pinned upstream package. From this repository:

```bash
python -m pip install '.[prophet]'
```

The extra pins `prophet-mlip` to commit `c4fda8251d8a7c90c7cb7842aea4d2f57e5fc3bd`. Supply `prophet-oame-mbd.pt` with `--checkpoint`; the magnetic Prophet-Spin weights are not the supported route. The public CLI still requires `--checkpoint`, even though the lower-level adapter has an environment-variable fallback. No `--source-root` is required for an installed Prophet package. An explicit source checkout can instead be selected with `NPC_PROPHET_SOURCE`; its checked source files must match the pinned commit.

Upstream: [pinned installation and model description](https://github.com/kairosmaterial/prophet/tree/c4fda8251d8a7c90c7cb7842aea4d2f57e5fc3bd) and [weights](https://huggingface.co/kairosmaterial/prophet).

### EquiformerV3 Stage1

Use the pinned EquiformerV3 source and the **OAM gradient** checkpoint, not an arbitrary file from its checkpoint directory:

```bash
git clone https://github.com/atomicarchitects/equiformer_v3.git model-sources/equiformer-v3
git -C model-sources/equiformer-v3 checkout a7300c58df683dc99cb48027d5bfd4c887486c48
```

Follow the [environment setup at that commit](https://github.com/atomicarchitects/equiformer_v3/blob/a7300c58df683dc99cb48027d5bfd4c887486c48/experimental/docs/env_setup.md), including its matching fairchem/PyG dependencies. If upstream installation needs editable sources, use a **second working copy** for installation and preserve `model-sources/equiformer-v3` as the unmodified source export passed to `--source-root`. Install `npc` into the resulting environment as well. The upstream CUDA training recipe is not evidence that this workflow requires training or that every CPU/GPU combination has been validated.

### What “pinned source” checks

TECE and Equiformer verify the expected Git commit, when available, and the source-tree hash. The current hash includes **all files** except `.git`, `__pycache__` and `.pyc`; it is not limited to files tracked by Git. A weight, virtual environment, `.egg-info`, log or build product added under the checked source root can therefore cause rejection. Keep those files elsewhere. Do not edit the expected hash to accept a modified upstream tree.

Prophet checks the specific source modules listed in its adapter. Dependency versions are not made fully reproducible by a source hash alone: preserve the environment/package inventory with results.

## 3. Download and check the exact weights

Checkpoints are not bundled in the source distribution or wheel. The filenames below are identifiers; the adapter checks the file contents by SHA-256.

| Route | Required file | Official source | SHA-256 |
| --- | --- | --- | --- |
| TECE Stage1 | `TECE-OAM-RRA-1.0.pt` | [TACE foundations](https://huggingface.co/xvzemin/tace-foundations/blob/main/TECE-OAM-RRA-1.0.pt) | `9f36562582d931347c3904f763e820edcaf5f27c3f13beb6776e49fcc7de38bb` |
| Prophet Stage1 | `prophet-oame-mbd.pt` | [Prophet](https://huggingface.co/kairosmaterial/prophet/blob/main/prophet-oame-mbd.pt) | `28b21122f4c6c1a7c5fe9bac8a0182edf9d24a7a65a72a9ca80b8d12d4620514` |
| EquiformerV3 Stage1 | `omat24-mptrj-salex_gradient.pt` | [EquiformerV3](https://huggingface.co/mirror-physics/equiformer_v3/blob/main/checkpoint/omat24-mptrj-salex_gradient.pt) | `429ccded98163122e7ba588d78e2441653f37f3e091e106c432807fe373c8f98` |
| MatterSim Stage2 | `mattersim-v1.0.0-5M.pth` | [MatterSim v1.2.1](https://github.com/microsoft/mattersim/blob/v1.2.1/pretrained_models/mattersim-v1.0.0-5M.pth) | `e3df9fa708725e3d453140646c7d1838324b347a3d1214cf1440522146f872b5` |

For the default route, these explicit downloads are sufficient; they are large model files, not installation smoke tests:

```bash
mkdir -p weights
curl --fail --location \
  https://huggingface.co/xvzemin/tace-foundations/resolve/main/TECE-OAM-RRA-1.0.pt \
  --output weights/TECE-OAM-RRA-1.0.pt
curl --fail --location \
  https://raw.githubusercontent.com/microsoft/mattersim/v1.2.1/pretrained_models/mattersim-v1.0.0-5M.pth \
  --output weights/mattersim-v1.0.0-5M.pth
```

For the alternatives:

```bash
curl --fail --location \
  https://huggingface.co/kairosmaterial/prophet/resolve/main/prophet-oame-mbd.pt \
  --output weights/prophet-oame-mbd.pt
curl --fail --location \
  https://huggingface.co/mirror-physics/equiformer_v3/resolve/main/checkpoint/omat24-mptrj-salex_gradient.pt \
  --output weights/omat24-mptrj-salex_gradient.pt
```

To inspect a downloaded file without loading its model:

```bash
python -c "import hashlib,pathlib; p=pathlib.Path('weights/TECE-OAM-RRA-1.0.pt'); print(hashlib.file_digest(p.open('rb'),'sha256').hexdigest())"
```

`hashlib.file_digest` requires Python 3.11+. On Python 3.10 use `sha256sum FILE` (Linux) or `shasum -a 256 FILE` (macOS). Compare with the table before running. If an upstream `main` weight changes, the fixed hash must still match; do not rename another model to bypass the mismatch. Model licenses and attribution remain those of the upstream authors.

## 4. Input structures and existing DFT data

For the documented QE input path, MLFF Stage1 expects `ibrav=0`, an explicit `CELL_PARAMETERS` cell, atomic positions and intended atomic constraint flags. The general Stage1 ASE reader does not support the bundled DFT reference's `ibrav=4` shorthand. Use the explicit-cell files under `docs/reference_data/pbe_20260925/full_model_structures/` as MLFF starting examples; Stage1 still relaxes them. It preserves the vacuum length and hexagonal cell shape during its constrained relaxation. It is not a general QE input executor.

For an **MLFF** route, pass `stage1/relax/optimized_structure.scf.inp` and preserve its `relax_summary.json` when starting Stage2. For a **qe-dft import**, use the original DFT structure instead; there is no MLFF relaxation or Stage1 checkpoint. The DFT-specific reader also accepts the supported `ibrav=4` A/C or celldm representation used by the bundled reference. The import requires the audited JSON DFPT contract, not an arbitrary `matdyn.modes` file. See [the input contract](DFT_STAGE1.md) and [both runnable routes](QUICKSTART.md).

## 5. Tests, source distributions and wheels

From a source checkout or an unpacked source distribution:

```bash
python -m pip install '.[test]'
python -m pytest -q tests
python -m build
```

The source distribution includes the public fixtures needed by the regression suite and the reference/report sources. A wheel installs only `nonlinear_phonon_calculation` and `mlff_modepair_workflow`, not the full test/data/report tree. Use a fresh environment and a directory outside the checkout when checking an installed wheel, so the source tree cannot mask an installation error:

```bash
python -I -m nonlinear_phonon_calculation.cli --version
python -I -m nonlinear_phonon_calculation.cli --help
```

These tests require no downloaded model weights unless a test explicitly says otherwise. They do not reproduce the archived DFT calculations or establish fresh inference accuracy for every optional model. Historical acceptance records keep their original version and scope; current release checks belong in the release notes.

## 6. CPU execution and optional Slurm example

Set thread variables before inference, with values appropriate to the actual allocation. Each shard loads its own model, so adding workers increases memory usage. Process RSS is not a measured whole-node peak.

`scripts/slurm_stage2.sbatch` is a site-editable example for the **MLFF Stage1 directory layout**. It starts four MatterSim workers within a 16-CPU allocation, checks their exits, and finalizes each phase. It runs `audit` as well as screen/refine; edit that phase list if an expanded-window calculation is not wanted. It is not an account-wide controller and does not enforce a universal node or node-hour budget.

```bash
export NPC_PYTHON="$PWD/.venv-ms/bin/python"
export NPC_RUN="$PWD/runs/mos2/tece"
export NPC_CHECKPOINT="$PWD/weights/mattersim-v1.0.0-5M.pth"
sbatch scripts/slurm_stage2.sbatch
```

Change the partition, CPUs, memory and time limit for the local site before submitting. The example cannot be used unchanged for a qe-dft import, whose structure path is different. Do not overlap duplicate runs into the same output directory. No Slurm, QE or GPU is required for the public CLI help or DFT-data import.

## 7. Common failures

| Symptom | Check |
| --- | --- |
| `npc` not found | Activate the environment where the package was installed, or use its Python with `-m nonlinear_phonon_calculation.cli`. |
| Model import/version error | Install that model in its own environment; check the pinned package/source rather than substituting the newest release. |
| Checkpoint SHA mismatch | Check the exact file, failed/partial downloads and upstream version. A filename match is insufficient. |
| TECE/Equiformer source mismatch | Check the commit and any added build/cache/weight files under the source root. |
| Changed run identity | Preserve the old results and use new inputs in a new output directory; see [MIGRATION.md](MIGRATION.md). |
| Relaxation reaches a scale boundary or fails | Review the structure, constraints and the model's domain; this is not evidence of a converged relaxed structure. |
| Γ translation, covariance or fit-rank failure | Review primitive-cell/mode provenance and actual data. Do not change thresholds or fill unknown coefficients just to force completion. |

Optional same-model and small-model diagnostics have separate dependencies and manifests: [full-model](FULL_MODEL_BENCHMARK.md), [small-model](SMALL_MODEL_BENCHMARK.md). Installing them does not turn their models into a generic `npc stage2 --model` backend.
