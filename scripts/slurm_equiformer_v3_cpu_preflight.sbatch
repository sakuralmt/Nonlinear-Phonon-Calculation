#!/bin/bash
#SBATCH --job-name=e3v3-cpu-check
#SBATCH --partition=regular
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=64G
#SBATCH --time=00:30:00
#SBATCH --output=/home/lmtsakura/qiyan_shared/testing/prophet-stage12-v3/logs/e3v3-cpu-check-%j.out

set -euo pipefail
campaign=/home/lmtsakura/qiyan_shared/testing/prophet-stage12-v3
cd "$campaign/code-next"
export OMP_NUM_THREADS=8
export MKL_NUM_THREADS=8
export OPENBLAS_NUM_THREADS=1
"$campaign/env-equiformer/bin/python" -m mlff_modepair_workflow.advanced_stage1 \
  --model equiformer-v3-oam \
  --checkpoint "$campaign/weights/omat24-mptrj-salex_gradient.pt" \
  --source-root "$campaign/advanced-models-source/equiformer_v3" \
  --structure "$campaign/inputs/mos2/shared_dft/structure.scf.inp" \
  --output-dir "$campaign/validation/equiformer-v3/mos2/cpu_preflight" \
  --device cpu --preflight-only
