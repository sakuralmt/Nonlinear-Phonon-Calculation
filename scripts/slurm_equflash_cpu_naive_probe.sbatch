#!/bin/bash
#SBATCH --job-name=equflash-cpu-naive
#SBATCH --partition=regular
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=64G
#SBATCH --time=00:30:00
#SBATCH --output=/home/lmtsakura/qiyan_shared/testing/prophet-stage12-v3/logs/equflash-cpu-naive-%j.out

set -euo pipefail
campaign=/home/lmtsakura/qiyan_shared/testing/prophet-stage12-v3
cd "$campaign/code-next"
export OMP_NUM_THREADS=8
export MKL_NUM_THREADS=8
export OPENBLAS_NUM_THREADS=1
export PYTHONPATH="$campaign/code-next:$campaign/env-equflash-overlay"
export EQUFLASH_SOURCE_ROOT="$campaign/advanced-models-source/GGNN"
for material in mos2 wse2; do
  "$campaign/env-equiformer/bin/python" scripts/equflash_cpu_naive_probe.py \
    --model equflashv2-45m-oam \
    --checkpoint "$campaign/weights/equflashv2_oam.pt" \
    --source-root "$EQUFLASH_SOURCE_ROOT" \
    --structure "$campaign/inputs/$material/shared_dft/structure.scf.inp" \
    --output-dir "$campaign/validation/equflashv2/$material/shared_dft/preflight_cpu_naive" \
    --device cpu --geometry-source shared_dft --preflight-only
done
