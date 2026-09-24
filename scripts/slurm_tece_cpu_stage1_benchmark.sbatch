#!/bin/bash
#SBATCH --job-name=tece-cpu-fc-bench
#SBATCH --partition=regular
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=128G
#SBATCH --time=02:00:00
#SBATCH --output=/home/lmtsakura/qiyan_shared/testing/prophet-stage12-v3/logs/tece-cpu-fc-bench-%j.out

set -euo pipefail
campaign=/home/lmtsakura/qiyan_shared/testing/prophet-stage12-v3
cd "$campaign/code-next"
export OMP_NUM_THREADS=16
export MKL_NUM_THREADS=16
export OPENBLAS_NUM_THREADS=1
export PYTHONPATH="$campaign/env-tece-overlay:$campaign/code-next"
"$campaign/env-equiformer/bin/python" -m scripts.benchmark_advanced_stage1_cpu \
  --model tece-oam-rra-1.0 \
  --checkpoint "$campaign/weights/TECE-OAM-RRA-1.0.pt" \
  --source-root "$campaign/advanced-models-source/tace" \
  --structure "$campaign/inputs/mos2/shared_dft/structure.scf.inp" \
  --output "$campaign/validation/tece/cpu_stage1_benchmark.json" \
  --mesh-n 6 --threads 4 8 16
