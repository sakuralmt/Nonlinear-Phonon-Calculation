#!/bin/bash
#SBATCH --job-name=prophet-phonopy-relaxed
#SBATCH --partition=regular
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=24
#SBATCH --mem=64G
#SBATCH --time=02:00:00
#SBATCH --output=/home/lmtsakura/qiyan_shared/testing/prophet-stage12-v3/logs/prophet-phonopy-relaxed-%j.out

set -euo pipefail
campaign=/home/lmtsakura/qiyan_shared/testing/prophet-stage12-v3
cd "$campaign/code-next"
export OMP_NUM_THREADS=24
export MKL_NUM_THREADS=24
export OPENBLAS_NUM_THREADS=1
export PYTHONPATH="$campaign/env-phonopy238-py311:$campaign/code-next"
export NPC_PROPHET_SOURCE="$campaign/source"
"$campaign/conda-cpu/bin/python" - <<'PY'
from pathlib import Path
from mlff_modepair_workflow.prophet_stage1 import run_prophet_stage1

campaign = Path('/home/lmtsakura/qiyan_shared/testing/prophet-stage12-v3')
for material in ('mos2', 'wse2'):
    result = run_prophet_stage1(
        campaign / 'inputs' / material / 'model_relaxed' / 'structure.scf.inp',
        campaign / 'model' / 'prophet-oame-mbd.pt',
        campaign / 'validation' / 'prophet-phonopy' / material / 'model_relaxed' / 'stage1',
        mesh_n=6, step=0.01, convergence_step=0.005, device='cpu',
        geometry_source='model_relaxed', phonon_engine='phonopy', phonopy_asr=True,
    )
    print(material, result, flush=True)
PY
