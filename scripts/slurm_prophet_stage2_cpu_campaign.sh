#!/usr/bin/env bash
#SBATCH --job-name=prophet-v3-cpu
#SBATCH --partition=long
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=48
#SBATCH --exclusive
#SBATCH --mem=0
#SBATCH --time=6-23:00:00
#SBATCH --array=0-3%4
#SBATCH --output=prophet-v3-cpu-%A_%a.out

set -euo pipefail
: "${NPC_CAMPAIGN_ROOT:?Set NPC_CAMPAIGN_ROOT to the isolated server campaign directory}"

root="$(realpath "$NPC_CAMPAIGN_ROOT")"
python="$root/conda-cpu/bin/python"
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

case "${SLURM_ARRAY_TASK_ID:?Submit as a four-task Slurm array}" in
    0) material=mos2; geometry=shared_dft ;;
    1) material=wse2; geometry=shared_dft ;;
    2) material=mos2; geometry=model_relaxed ;;
    3) material=wse2; geometry=model_relaxed ;;
    *) echo "Unexpected array task id: $SLURM_ARRAY_TASK_ID" >&2; exit 2 ;;
esac

export NPC_PROPHET_SOURCE="$root/source"
export NPC_PROPHET_CHECKPOINT="$root/model/prophet-oame-mbd.pt"
export OMP_NUM_THREADS="$threads"
export MKL_NUM_THREADS="$threads"
export OPENBLAS_NUM_THREADS="$threads"
export NUMEXPR_NUM_THREADS="$threads"
export PYTHONUNBUFFERED=1

input="$root/inputs/$material/$geometry"
output="$root/runs/$material/$geometry/stage2"
logs="$root/logs"
mkdir -p "$logs"
cd "$root/code"

args=(
    --mode-pairs-json "$input/mode_pairs.selected.json"
    --structure "$input/structure.scf.inp"
    --model prophet_oame_mbd
    --device cpu
    --output-root "$output"
    --run-tag prophet
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
    log="$logs/stage2-${SLURM_ARRAY_JOB_ID}_${SLURM_ARRAY_TASK_ID}-worker-${index}.out"
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
