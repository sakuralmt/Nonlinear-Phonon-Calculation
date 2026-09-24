#!/bin/bash
#SBATCH --job-name=e3v3-own-relax
#SBATCH --partition=gpu
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --gres=gpu:1
#SBATCH --mem=64G
#SBATCH --time=04:00:00
#SBATCH --output=/home/lmtsakura/qiyan_shared/testing/prophet-stage12-v3/logs/e3v3-own-relax-%j.out

set -euo pipefail
campaign=/home/lmtsakura/qiyan_shared/testing/prophet-stage12-v3
cd "$campaign/code-next"
export OMP_NUM_THREADS=8
export MKL_NUM_THREADS=8
export OPENBLAS_NUM_THREADS=1
python="$campaign/env-equiformer/bin/python"
"$python" -c 'import torch; assert torch.cuda.is_available(), "GPU not assigned"; print(torch.__version__, torch.cuda.get_device_name(0), flush=True)'

for material in mos2 wse2; do
  "$python" -m mlff_modepair_workflow.advanced_stage1 \
    --model equiformer-v3-oam \
    --checkpoint "$campaign/weights/omat24-mptrj-salex_gradient.pt" \
    --source-root "$campaign/advanced-models-source/equiformer_v3" \
    --structure "$campaign/inputs/$material/shared_dft/structure.scf.inp" \
    --output-dir "$campaign/validation/equiformer-v3/$material/model_relaxed/stage1" \
    --device cuda --geometry-source model_relaxed --relax-only
done
