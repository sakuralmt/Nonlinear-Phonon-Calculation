#!/bin/bash
#SBATCH --job-name=tece-phonopy-stage1
#SBATCH --partition=regular
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=128G
#SBATCH --time=02:00:00
#SBATCH --output=/home/lmtsakura/qiyan_shared/testing/prophet-stage12-v3/logs/tece-phonopy-stage1-%j.out

set -euo pipefail
campaign=/home/lmtsakura/qiyan_shared/testing/prophet-stage12-v3
cd "$campaign/code-next"
export OMP_NUM_THREADS=16
export MKL_NUM_THREADS=16
export OPENBLAS_NUM_THREADS=1
export PYTHONPATH="$campaign/env-tece-overlay:$campaign/env-phonopy238-py312:$campaign/code-next"
for material in mos2 wse2; do
  "$campaign/env-equiformer/bin/python" -m mlff_modepair_workflow.advanced_stage1 \
    --model tece-oam-rra-1.0 \
    --checkpoint "$campaign/weights/TECE-OAM-RRA-1.0.pt" \
    --source-root "$campaign/advanced-models-source/tace" \
    --structure "$campaign/inputs/$material/shared_dft/structure.scf.inp" \
    --output-dir "$campaign/validation/tece/$material/shared_dft/phonopy_stage1_asr" \
    --device cpu --geometry-source shared_dft \
    --mesh-n 6 --step 0.01 --convergence-step 0.005 \
    --phonon-engine phonopy
done
