#!/usr/bin/env bash
#SBATCH --job-name=prophet-v3-stage2
#SBATCH --partition=gpu
#SBATCH --gres=gpu:1
#SBATCH --cpus-per-task=6
#SBATCH --mem=32G
#SBATCH --time=24:00:00
#SBATCH --output=prophet-stage2-%A_%a.out

set -euo pipefail
: "${NPC_MODE_PAIRS_JSON:?Set NPC_MODE_PAIRS_JSON to the v3 pair file}"
: "${NPC_STRUCTURE:?Set NPC_STRUCTURE to the matching QE structure}"
: "${NPC_PROPHET_CHECKPOINT:?Set NPC_PROPHET_CHECKPOINT to the pinned OAME-MBD checkpoint}"
: "${NPC_STAGE2_OUTPUT_ROOT:?Set NPC_STAGE2_OUTPUT_ROOT to an isolated material/geometry output root}"
: "${NPC_SHARD_COUNT:?Set NPC_SHARD_COUNT to the Slurm array task count}"

export OMP_NUM_THREADS="${SLURM_CPUS_PER_TASK:-6}"
export MKL_NUM_THREADS="$OMP_NUM_THREADS"
export OPENBLAS_NUM_THREADS="$OMP_NUM_THREADS"

python -m mlff_modepair_workflow.prophet_stage2 \
  --mode-pairs-json "$NPC_MODE_PAIRS_JSON" \
  --structure "$NPC_STRUCTURE" \
  --model "$NPC_PROPHET_CHECKPOINT" \
  --device cuda \
  --output-root "$NPC_STAGE2_OUTPUT_ROOT" \
  --run-tag prophet \
  --shard-index "${SLURM_ARRAY_TASK_ID:?Submit as a Slurm array}" \
  --shard-count "$NPC_SHARD_COUNT"
