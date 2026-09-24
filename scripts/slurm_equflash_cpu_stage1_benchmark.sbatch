#!/bin/bash
#SBATCH --job-name=equflash-cpu-fc-bench
#SBATCH --partition=regular
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=128G
#SBATCH --time=02:00:00
#SBATCH --output=/home/lmtsakura/qiyan_shared/testing/prophet-stage12-v3/logs/equflash-cpu-fc-bench-%j.out

set -euo pipefail
campaign=/home/lmtsakura/qiyan_shared/testing/prophet-stage12-v3
cd "$campaign/code-next"
export OMP_NUM_THREADS=16
export MKL_NUM_THREADS=16
export OPENBLAS_NUM_THREADS=1
export PYTHONPATH="$campaign/code-next:$campaign/env-equflash-overlay"
export EQUFLASH_SOURCE_ROOT="$campaign/advanced-models-source/GGNN"
"$campaign/env-equiformer/bin/python" -m scripts.equflash_cpu_benchmark \
  --model equflashv2-45m-oam \
  --checkpoint "$campaign/weights/equflashv2_oam.pt" \
  --source-root "$EQUFLASH_SOURCE_ROOT" \
  --structure "$campaign/inputs/mos2/shared_dft/structure.scf.inp" \
  --output "$campaign/validation/equflashv2/cpu_stage1_benchmark.json" \
  --mesh-n 6 --threads 4 8 16
