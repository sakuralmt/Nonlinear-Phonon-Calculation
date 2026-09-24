#!/bin/bash
#SBATCH --job-name=ms-phonopy-tece
#SBATCH --partition=regular
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=64G
#SBATCH --time=04:00:00
#SBATCH --array=0-3%2
#SBATCH --output=/home/lmtsakura/qiyan_shared/testing/prophet-stage12-v3/logs/ms-phonopy-tece-%A_%a.out

set -euo pipefail
campaign=/home/lmtsakura/qiyan_shared/testing/prophet-stage12-v3
case "${SLURM_ARRAY_TASK_ID:?}" in
  0) material=mos2; geometry=shared_dft ;;
  1) material=wse2; geometry=shared_dft ;;
  2) material=mos2; geometry=model_relaxed ;;
  3) material=wse2; geometry=model_relaxed ;;
  *) exit 2 ;;
esac
cd "$campaign/code-next"
export OMP_NUM_THREADS=4
export MKL_NUM_THREADS=4
export OPENBLAS_NUM_THREADS=1
export NUMEXPR_NUM_THREADS=4
input="$campaign/validation/tece/$material/$geometry/phonopy_stage1_asr"
if [[ "$geometry" == shared_dft ]]; then
  structure="$campaign/inputs/$material/shared_dft/structure.scf.inp"
else
  structure="$input/relax/optimized_structure.scf.inp"
fi
test -s "$input/mode_pairs.selected.json"
test -s "$structure"
output="$campaign/validation/mattersim-phonopy-stage2/tece/$material/$geometry"
args=(
  --backend mattersim --mode-pairs-json "$input/mode_pairs.selected.json"
  --structure "$structure" --model "$campaign/model/mattersim-v1.0.0-5M.pth"
  --device cpu --output-root "$output" --run-tag mattersim-v1-5m
)
pids=()
for index in 0 1 2 3; do
  "$campaign/venv-mattersim/bin/python" -m mlff_modepair_workflow.prophet_stage2 \
    "${args[@]}" --shard-index "$index" --shard-count 4 \
    >"$campaign/logs/ms-phonopy-tece-$material-$geometry-${SLURM_ARRAY_JOB_ID}_${SLURM_ARRAY_TASK_ID}-worker-$index.out" 2>&1 &
  pids+=("$!")
done
failed=0
for pid in "${pids[@]}"; do
  if ! wait "$pid"; then failed=1; fi
done
if (( failed )); then
  echo "MatterSim Stage2 shard failed; retain point checkpoints for retry" >&2
  exit 1
fi
"$campaign/venv-mattersim/bin/python" -m mlff_modepair_workflow.prophet_stage2 \
  "${args[@]}" --finalize-only
