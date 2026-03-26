#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
MLFF_RUNS = Path(__file__).resolve().parent / "runs"
GPTFF_BENCHMARK = ROOT / "gptff_modepair_workflow" / "benchmark" / "ranking.csv"
GPTFF_SCREENING = ROOT / "gptff_modepair_workflow" / "screening" / "pair_ranking.csv"
OUT_DIR = Path(__file__).resolve().parent / "runs" / "compare_gptff"

GOLDEN_PAIR = "Gamma_p0_m8__M_q_0.500_0.000_0.000_m3"

MLFF_TAGS = [
    "chgnet_qe",
    "chgnet_r2scan_relaxed",
    "chgnet_0_2_relaxed",
    "mace_20231210_l0_relaxed",
]


def load_csv(path: Path):
    return list(csv.DictReader(path.open()))


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    golden_rows = []
    for tag in MLFF_TAGS:
        bench = json.loads((MLFF_RUNS / tag / "benchmark" / "summary.json").read_text())
        screening = load_csv(MLFF_RUNS / tag / "screening" / "pair_ranking.csv")
        row = next(r for r in screening if r["pair_code"] == GOLDEN_PAIR)
        golden_rows.append(
            {
                "tag": tag,
                "kind": "mlff",
                "gamma_axis_thz": bench["analysis"]["axis_checks"]["mode1_axis_fit"]["freq"].get("thz"),
                "q_axis_thz": bench["analysis"]["axis_checks"]["mode2_axis_fit"]["freq"].get("thz"),
                "phi122_mev": bench["analysis"]["physics"]["phi_122_mev_per_A3amu32"],
                "golden_rank": int(row["rank"]),
            }
        )

    gptff_bench = load_csv(GPTFF_BENCHMARK)
    gptff_best = gptff_bench[0]
    gptff_screen = load_csv(GPTFF_SCREENING)
    gptff_golden = next(r for r in gptff_screen if r["pair_code"] == GOLDEN_PAIR)
    golden_rows.append(
        {
            "tag": "gptff_best",
            "kind": "gptff",
            "gamma_axis_thz": float(gptff_best["axis_mode1_thz"]),
            "q_axis_thz": float(gptff_best["axis_mode2_thz"]),
            "phi122_mev": float(gptff_best["phi122_mev"]),
            "golden_rank": int(gptff_golden["rank"]),
        }
    )

    top_overlap_rows = []
    gptff_rows = load_csv(GPTFF_SCREENING)
    for n in (10, 20):
        gptff_top = set(r["pair_code"] for r in gptff_rows[:n])
        for tag in MLFF_TAGS:
            mlff_rows = load_csv(MLFF_RUNS / tag / "screening" / "pair_ranking.csv")
            mlff_top = set(r["pair_code"] for r in mlff_rows[:n])
            overlap = sorted(gptff_top & mlff_top)
            top_overlap_rows.append(
                {
                    "top_n": n,
                    "tag": tag,
                    "overlap_count": len(overlap),
                    "overlap_pairs": overlap,
                }
            )

    summary = {
        "golden_pair": GOLDEN_PAIR,
        "golden_pair_comparison": golden_rows,
        "top_overlap_vs_gptff": top_overlap_rows,
        "gptff_top10": [r["pair_code"] for r in gptff_rows[:10]],
        "consensus_top10": [r["pair_code"] for r in load_csv(MLFF_RUNS / "consensus_chgnet_0_2_relaxed_chgnet_r2scan_relaxed_chgnet_qe_mace_20231210_l0_relaxed" / "consensus_pair_ranking.csv")[:10]],
    }

    (OUT_DIR / "gptff_vs_mlff_summary.json").write_text(json.dumps(summary, indent=2))

    with (OUT_DIR / "golden_pair_comparison.csv").open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["tag", "kind", "gamma_axis_thz", "q_axis_thz", "phi122_mev", "golden_rank"])
        for row in golden_rows:
            writer.writerow(
                [
                    row["tag"],
                    row["kind"],
                    f"{row['gamma_axis_thz']:.6f}",
                    f"{row['q_axis_thz']:.6f}",
                    f"{row['phi122_mev']:.6f}",
                    row["golden_rank"],
                ]
            )

    with (OUT_DIR / "top_overlap_vs_gptff.csv").open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["top_n", "tag", "overlap_count", "overlap_pairs"])
        for row in top_overlap_rows:
            writer.writerow([row["top_n"], row["tag"], row["overlap_count"], ";".join(row["overlap_pairs"])])

    print(f"saved: {OUT_DIR / 'gptff_vs_mlff_summary.json'}")
    print(f"saved: {OUT_DIR / 'golden_pair_comparison.csv'}")
    print(f"saved: {OUT_DIR / 'top_overlap_vs_gptff.csv'}")
    print("golden pair comparison:")
    for row in golden_rows:
        print(
            f"{row['tag']}: gamma={row['gamma_axis_thz']:.3f} THz, "
            f"q={row['q_axis_thz']:.3f} THz, phi122={row['phi122_mev']:.3f} meV, "
            f"rank={row['golden_rank']}"
        )


if __name__ == "__main__":
    main()
