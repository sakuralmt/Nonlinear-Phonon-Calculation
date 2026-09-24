#!/bin/bash
#SBATCH --job-name=e3v3-fc-bench
#SBATCH --partition=regular
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=64G
#SBATCH --time=01:00:00
#SBATCH --output=/home/lmtsakura/qiyan_shared/testing/prophet-stage12-v3/logs/e3v3-fc-bench-%j.out

set -euo pipefail
campaign=/home/lmtsakura/qiyan_shared/testing/prophet-stage12-v3
cd "$campaign/code-next"
export OMP_NUM_THREADS=16
export MKL_NUM_THREADS=16
export OPENBLAS_NUM_THREADS=1
"$campaign/env-equiformer/bin/python" -m scripts.benchmark_advanced_stage1_cpu \
  --model equiformer-v3-oam \
  --checkpoint "$campaign/weights/omat24-mptrj-salex_gradient.pt" \
  --source-root "$campaign/advanced-models-source/equiformer_v3" \
  --structure "$campaign/inputs/mos2/shared_dft/structure.scf.inp" \
  --output "$campaign/validation/equiformer-v3/cpu_stage1_benchmark.json" \
  --mesh-n 6 --threads 4 8 16
