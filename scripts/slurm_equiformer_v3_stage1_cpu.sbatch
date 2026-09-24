#!/bin/bash
#SBATCH --job-name=e3v3-stage1-cpu
#SBATCH --partition=regular
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=64G
#SBATCH --time=02:00:00
#SBATCH --output=/home/lmtsakura/qiyan_shared/testing/prophet-stage12-v3/logs/e3v3-stage1-cpu-%j.out

set -euo pipefail
campaign=/home/lmtsakura/qiyan_shared/testing/prophet-stage12-v3
cd "$campaign/code-next"
export OMP_NUM_THREADS=16
export MKL_NUM_THREADS=16
export OPENBLAS_NUM_THREADS=1
python="$campaign/env-equiformer/bin/python"

for material in mos2 wse2; do
  for geometry in model_relaxed shared_dft; do
    echo "Stage1 $material $geometry started at $(date -Is)"
    "$python" -m mlff_modepair_workflow.advanced_stage1 \
      --model equiformer-v3-oam \
      --checkpoint "$campaign/weights/omat24-mptrj-salex_gradient.pt" \
      --source-root "$campaign/advanced-models-source/equiformer_v3" \
      --structure "$campaign/inputs/$material/shared_dft/structure.scf.inp" \
      --output-dir "$campaign/validation/equiformer-v3/$material/$geometry/stage1" \
      --device cpu --geometry-source "$geometry" \
      --mesh-n 6 --step 0.01 --convergence-step 0.005
    echo "Stage1 $material $geometry finished at $(date -Is)"
  done
done
