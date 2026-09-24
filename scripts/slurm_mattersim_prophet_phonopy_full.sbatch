#!/bin/bash
#SBATCH --job-name=ms-prophet-phonopy-full
#SBATCH --partition=regular
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=64G
#SBATCH --time=04:00:00
#SBATCH --array=0-1%2
#SBATCH --output=/home/lmtsakura/qiyan_shared/testing/prophet-stage12-v3/logs/ms-prophet-phonopy-full-%A_%a.out

set -euo pipefail
campaign=/home/lmtsakura/qiyan_shared/testing/prophet-stage12-v3
case "${SLURM_ARRAY_TASK_ID:?}" in
  0) material=mos2 ;;
  1) material=wse2 ;;
  *) exit 2 ;;
esac
cd "$campaign/code-next"
export OMP_NUM_THREADS=4
export MKL_NUM_THREADS=4
export OPENBLAS_NUM_THREADS=1
export NUMEXPR_NUM_THREADS=4
input="$campaign/validation/prophet-phonopy/$material/shared_dft/stage1"
output="$campaign/validation/mattersim-phonopy-stage2/prophet/$material/shared_dft"
args=(
  --backend mattersim --mode-pairs-json "$input/mode_pairs.selected.json"
  --structure "$campaign/inputs/$material/shared_dft/structure.scf.inp"
  --model "$campaign/model/mattersim-v1.0.0-5M.pth"
  --device cpu --output-root "$output" --run-tag mattersim-v1-5m
)
pids=()
for index in 0 1 2 3; do
  "$campaign/venv-mattersim/bin/python" -m mlff_modepair_workflow.prophet_stage2 \
    "${args[@]}" --shard-index "$index" --shard-count 4 \
    >"$campaign/logs/ms-prophet-phonopy-$material-${SLURM_ARRAY_JOB_ID}_${SLURM_ARRAY_TASK_ID}-worker-$index.out" 2>&1 &
  pids+=("$!")
done
failed=0
for pid in "${pids[@]}"; do
  if ! wait "$pid"; then failed=1; fi
done
if (( failed )); then
  echo "A MatterSim Stage2 shard failed; keep point checkpoints for retry" >&2
  exit 1
fi
"$campaign/venv-mattersim/bin/python" -m mlff_modepair_workflow.prophet_stage2 \
  "${args[@]}" --finalize-only
