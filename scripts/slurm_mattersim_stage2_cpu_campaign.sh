#!/usr/bin/env bash
#SBATCH --job-name=mattersim-v3-cpu
#SBATCH --partition=long
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=48
#SBATCH --exclusive
#SBATCH --mem=0
#SBATCH --time=3-00:00:00
#SBATCH --array=0-1%2
#SBATCH --output=mattersim-v3-cpu-%A_%a.out

set -euo pipefail
: "${NPC_CAMPAIGN_ROOT:?Set NPC_CAMPAIGN_ROOT to the isolated server campaign directory}"

root="$(realpath "$NPC_CAMPAIGN_ROOT")"
python="$root/venv-mattersim/bin/python"
workers="${NPC_CPU_WORKERS:-12}"
threads="${NPC_CPU_THREADS:-4}"
if ! [[ "$workers" =~ ^[1-9][0-9]*$ && "$threads" =~ ^[1-9][0-9]*$ ]]; then
    echo "NPC_CPU_WORKERS and NPC_CPU_THREADS must be positive integers" >&2
    exit 2
fi
if (( workers * threads > ${SLURM_CPUS_PER_TASK:-48} )); then
    echo "Worker threads exceed the allocated Slurm CPUs" >&2
    exit 2
fi

case "${SLURM_ARRAY_TASK_ID:?Submit as a two-task Slurm array}" in
    0) material=mos2 ;;
    1) material=wse2 ;;
    *) echo "Unexpected array task id: $SLURM_ARRAY_TASK_ID" >&2; exit 2 ;;
esac

export OMP_NUM_THREADS="$threads"
export MKL_NUM_THREADS="$threads"
export OPENBLAS_NUM_THREADS="$threads"
export NUMEXPR_NUM_THREADS="$threads"
export PYTHONUNBUFFERED=1

input="$root/inputs/$material/shared_dft"
output="$root/runs/$material/shared_dft/stage2"
logs="$root/logs"
mkdir -p "$logs"
cd "$root/code"

args=(
    --backend mattersim
    --mode-pairs-json "$input/mode_pairs.selected.json"
    --structure "$input/structure.scf.inp"
    --model "$root/model/mattersim-v1.0.0-5M.pth"
    --device cpu
    --output-root "$output"
    --run-tag mattersim-v1-5m
)

pids=()
stop_workers() {
    trap - TERM INT
    for pid in "${pids[@]}"; do kill "$pid" 2>/dev/null || true; done
    for pid in "${pids[@]}"; do wait "$pid" 2>/dev/null || true; done
    exit 143
}
trap stop_workers TERM INT

for (( index=0; index<workers; index++ )); do
    log="$logs/mattersim-stage2-${SLURM_ARRAY_JOB_ID}_${SLURM_ARRAY_TASK_ID}-worker-${index}.out"
    "$python" -m mlff_modepair_workflow.prophet_stage2 "${args[@]}" \
        --shard-index "$index" --shard-count "$workers" >"$log" 2>&1 &
    pids+=("$!")
done

failed=0
for pid in "${pids[@]}"; do
    if ! wait "$pid"; then failed=1; fi
done
trap - TERM INT
if (( failed )); then
    echo "At least one worker failed; ranking was not finalized. Inspect $logs." >&2
    exit 1
fi

"$python" -m mlff_modepair_workflow.prophet_stage2 "${args[@]}" --finalize-only
