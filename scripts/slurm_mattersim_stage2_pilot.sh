#!/usr/bin/env bash
#SBATCH --job-name=mattersim-v3-pilot
#SBATCH --partition=regular
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=32G
#SBATCH --time=00:30:00
#SBATCH --output=mattersim-v3-pilot-%j.out

set -euo pipefail
: "${NPC_CAMPAIGN_ROOT:?Set NPC_CAMPAIGN_ROOT to the isolated server campaign directory}"
root="$(realpath "$NPC_CAMPAIGN_ROOT")"
export OMP_NUM_THREADS=4 MKL_NUM_THREADS=4 OPENBLAS_NUM_THREADS=4 NUMEXPR_NUM_THREADS=4
cd "$root/code"
for material in mos2 wse2; do
    input="$root/inputs/$material/shared_dft"
    for pair in \
        Gamma_p0_m1__M_q_0.500_0.000_0.000_m1 \
        Gamma_p0_m1__line_q_0.000_0.167_0.000_m1; do
        "$root/venv-mattersim/bin/python" -m mlff_modepair_workflow.prophet_stage2 \
            --backend mattersim --mode-pairs-json "$input/mode_pairs.selected.json" \
            --structure "$input/structure.scf.inp" \
            --model "$root/model/mattersim-v1.0.0-5M.pth" --device cpu \
            --output-root "$root/runs/$material/shared_dft/stage2" \
            --run-tag mattersim-v1-5m-pilot --pair-code "$pair"
    done
done
