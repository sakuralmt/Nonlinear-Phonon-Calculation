#!/bin/bash
#SBATCH --job-name=tece-cpu-preflight
#SBATCH --partition=regular
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=128G
#SBATCH --time=02:00:00
#SBATCH --output=/home/lmtsakura/qiyan_shared/testing/prophet-stage12-v3/logs/tece-cpu-preflight-%j.out

set -euo pipefail
campaign=/home/lmtsakura/qiyan_shared/testing/prophet-stage12-v3
cd "$campaign/code-next"
export OMP_NUM_THREADS=16
export MKL_NUM_THREADS=16
export OPENBLAS_NUM_THREADS=1
export PYTHONPATH="$campaign/env-tece-overlay:$campaign/code-next"
for material in mos2 wse2; do
  "$campaign/env-equiformer/bin/python" -m mlff_modepair_workflow.advanced_stage1 \
    --model tece-oam-rra-1.0 \
    --checkpoint "$campaign/weights/TECE-OAM-RRA-1.0.pt" \
    --source-root "$campaign/advanced-models-source/tace" \
    --structure "$campaign/inputs/$material/shared_dft/structure.scf.inp" \
    --output-dir "$campaign/validation/tece/$material/shared_dft/preflight_cpu_compat" \
    --device cpu --geometry-source shared_dft --preflight-only
done
