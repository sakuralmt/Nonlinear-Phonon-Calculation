#!/bin/bash
#SBATCH --job-name=ms-prophet-phonopy-pilot
#SBATCH --partition=regular
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=64G
#SBATCH --time=01:00:00
#SBATCH --output=/home/lmtsakura/qiyan_shared/testing/prophet-stage12-v3/logs/ms-prophet-phonopy-pilot-%j.out

set -euo pipefail
campaign=/home/lmtsakura/qiyan_shared/testing/prophet-stage12-v3
cd "$campaign/code-next"
export OMP_NUM_THREADS=8
export MKL_NUM_THREADS=8
export OPENBLAS_NUM_THREADS=1
export NUMEXPR_NUM_THREADS=8
pids=()
for material in mos2 wse2; do
  if [[ "$material" == mos2 ]]; then
    code=Gamma_p0_m8__M_q_0.500_0.000_0.000_m9
  else
    code=Gamma_p0_m8__M_q_0.500_0.000_0.000_m8
  fi
  "$campaign/venv-mattersim/bin/python" -m mlff_modepair_workflow.prophet_stage2 \
    --backend mattersim \
    --mode-pairs-json "$campaign/validation/prophet-phonopy/$material/shared_dft/stage1/mode_pairs.selected.json" \
    --structure "$campaign/inputs/$material/shared_dft/structure.scf.inp" \
    --model "$campaign/model/mattersim-v1.0.0-5M.pth" --device cpu \
    --output-root "$campaign/validation/mattersim-phonopy-stage2/prophet/$material/shared_dft" \
    --run-tag mattersim-v1-5m --pair-code "$code" \
    >"$campaign/logs/ms-prophet-phonopy-pilot-$material-$SLURM_JOB_ID.out" 2>&1 &
  pids+=("$!")
done
failed=0
for pid in "${pids[@]}"; do
  if ! wait "$pid"; then failed=1; fi
done
if (( failed )); then
  echo "A Prophet-Phonopy MatterSim Stage2 pilot failed" >&2
  exit 1
fi
