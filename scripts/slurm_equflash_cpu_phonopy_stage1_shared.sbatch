#!/bin/bash
#SBATCH --job-name=equflash-phonopy-s1
#SBATCH --partition=regular
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=128G
#SBATCH --time=02:00:00
#SBATCH --array=0-1%2
#SBATCH --output=/home/lmtsakura/qiyan_shared/testing/prophet-stage12-v3/logs/equflash-phonopy-s1-%A_%a.out

set -euo pipefail
campaign=/home/lmtsakura/qiyan_shared/testing/prophet-stage12-v3
case "${SLURM_ARRAY_TASK_ID:?}" in
  0) material=mos2 ;;
  1) material=wse2 ;;
  *) exit 2 ;;
esac
cd "$campaign/code-next"
export OMP_NUM_THREADS=16
export MKL_NUM_THREADS=16
export OPENBLAS_NUM_THREADS=1
export PYTHONPATH="$campaign/code-next:$campaign/env-equflash-overlay:$campaign/env-phonopy238-py312"
export EQUFLASH_SOURCE_ROOT="$campaign/advanced-models-source/GGNN"
"$campaign/env-equiformer/bin/python" -m scripts.equflash_cpu_naive_probe \
  --model equflashv2-45m-oam \
  --checkpoint "$campaign/weights/equflashv2_oam.pt" \
  --source-root "$EQUFLASH_SOURCE_ROOT" \
  --structure "$campaign/inputs/$material/shared_dft/structure.scf.inp" \
  --output-dir "$campaign/validation/equflashv2/$material/shared_dft/phonopy_stage1_asr_cpu" \
  --device cpu --geometry-source shared_dft \
  --mesh-n 6 --step 0.01 --convergence-step 0.005 \
  --phonon-engine phonopy
