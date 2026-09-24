#!/bin/bash
#SBATCH --job-name=compare-prophet-phonopy
#SBATCH --partition=regular
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=32G
#SBATCH --time=00:30:00
#SBATCH --output=/home/lmtsakura/qiyan_shared/testing/prophet-stage12-v3/logs/compare-prophet-phonopy-%j.out

set -euo pipefail
campaign=/home/lmtsakura/qiyan_shared/testing/prophet-stage12-v3
cd "$campaign/code-next"
export OMP_NUM_THREADS=1
export MKL_NUM_THREADS=1
export OPENBLAS_NUM_THREADS=1
for material in mos2 wse2; do
  old="$campaign/runs/$material/shared_dft/stage2/mattersim-v1-5m/screening"
  new="$campaign/validation/mattersim-phonopy-stage2/prophet/$material/shared_dft/mattersim-v1-5m/screening"
  "$campaign/venv-mattersim/bin/python" -m scripts.compare_prophet_phonopy_stage2 \
    --old-screening "$old" --new-screening "$new" \
    --structure "$campaign/inputs/$material/shared_dft/structure.scf.inp" \
    --output "$campaign/validation/prophet-phonopy/$material/shared_dft/custom_vs_phonopy_stage2.json"
done
