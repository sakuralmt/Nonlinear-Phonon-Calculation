#!/usr/bin/env bash
#SBATCH --job-name=npc-stage2
#SBATCH --partition=regular
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=32G
#SBATCH --time=04:00:00
set -euo pipefail

: "${NPC_PYTHON:?Absolute Python path of the MatterSim environment}"
: "${NPC_RUN:?Run directory containing stage1 and stage2}"
: "${NPC_CHECKPOINT:?Pinned MatterSim checkpoint}"
workers=${NPC_WORKERS:-4}
cores=${SLURM_CPUS_PER_TASK:-16}
if (( workers < 1 || workers > cores )); then
  echo "NPC_WORKERS must be between 1 and allocated CPUs" >&2
  exit 2
fi
export OMP_NUM_THREADS=$(( cores / workers ))
export MKL_NUM_THREADS=$OMP_NUM_THREADS
export OPENBLAS_NUM_THREADS=1
top=${NPC_TOP_CHANNELS:-20}
common=(--mode-pairs-json "$NPC_RUN/stage1/mode_pairs.selected.json"
  --structure "$NPC_RUN/stage1/relax/optimized_structure.scf.inp"
  --checkpoint "$NPC_CHECKPOINT" --output-dir "$NPC_RUN/stage2"
  --device cpu --top-channels "$top")
mkdir -p "$NPC_RUN/logs"
for phase in screen refine audit; do
  pids=()
  for (( shard=0; shard<workers; shard++ )); do
    "$NPC_PYTHON" -m nonlinear_phonon_calculation.cli stage2 "$phase" \
      "${common[@]}" --shard-index "$shard" --shard-count "$workers" \
      >"$NPC_RUN/logs/$phase-${SLURM_JOB_ID:-local}-$shard.log" 2>&1 &
    pids+=("$!")
  done
  failed=0
  for pid in "${pids[@]}"; do if ! wait "$pid"; then failed=1; fi; done
  if (( failed )); then echo "Failed Stage2 phase: $phase" >&2; exit 1; fi
  "$NPC_PYTHON" -m nonlinear_phonon_calculation.cli stage2 "$phase" \
    "${common[@]}" --finalize-only
done
