"""Check the published three-material PBE reference without external run folders."""

from __future__ import annotations

import json
import math
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "docs/reference_data/pbe_20260925"
MATERIALS = ("ws2", "mos2", "wse2")
MODELS = ("tece", "prophet", "equiformer-v3")


def read(name: str) -> dict:
    return json.loads((DATA / name).read_text())


def mae(values: list[float]) -> float:
    return sum(abs(value) for value in values) / len(values)


def check() -> list[tuple[str, str, float, float, float]]:
    audit = read("campaign_audit.json")
    if audit["total"]["n"] != 579:
        raise ValueError("PBE campaign does not contain 579 audited QE points")
    dense_model = read("ws2_17x17_mattersim.json")
    if len(dense_model["grid_ev"]) != 17 or any(
        len(row) != 17 for row in dense_model["grid_ev"]
    ):
        raise ValueError("Incomplete WS2 MatterSim 17x17 grid")
    if dense_model["fits"]["wide_dense_17x17_step0p25"]["points"] != 289:
        raise ValueError("WS2 MatterSim dense-grid fit is incomplete")
    comparison = read("coupling_comparison.json")
    summary = []
    for material in MATERIALS:
        result = read(f"results_{material}.json")
        phonons = read(f"{material}_phonon_comparison.json")
        qe_phonons = read(f"qe_phonon_dataset_{material}.json")
        if len(qe_phonons["q_points"]) != 36:
            raise ValueError(f"{material}: incomplete QE phonon mesh")
        if result["central_fits_complete"] != 5:
            raise ValueError(f"{material}: incomplete central fits")
        if result["total_completed_base_points"] != 181:
            raise ValueError(f"{material}: incomplete PES base grid")
        rows = comparison["materials"][material]["rows"]
        if len(rows) != 5 or len(result["rows"]) != 5:
            raise ValueError(f"{material}: expected five matched channels")
        if any(row["center_fit"]["fit_design_rank"] != 13 for row in result["rows"]):
            raise ValueError(f"{material}: rank-deficient PES fit")
        if result["rows"][0]["wide_fit_2"]["fit_design_rank"] != 13:
            raise ValueError(f"{material}: rank-deficient wide fit")
        for model in MODELS:
            mlff_dataset = read(f"mlff_phonon_dataset_{material}_{model}.json")
            if len(mlff_dataset["q_points"]) != 36:
                raise ValueError(f"{material}/{model}: incomplete MLFF phonon mesh")
            cubic = mae([
                row["mlff"][model]["phi122_abs"] - row["pbe"]["phi122_abs"]
                for row in rows
            ])
            quartic = mae([
                row["mlff"][model]["phi1122"] - row["pbe"]["phi1122"]
                for row in rows
            ])
            metrics = comparison["materials"][material]["metrics"]["pbe"][model]
            if not math.isclose(cubic, metrics["phi122_abs"]["mae"], abs_tol=1e-9):
                raise ValueError(f"{material}/{model}: cubic MAE mismatch")
            if not math.isclose(quartic, metrics["phi1122"]["mae"], abs_tol=1e-9):
                raise ValueError(f"{material}/{model}: quartic MAE mismatch")
            frequency = phonons["mlff_vs_pbe"][model]["frequency_mae_thz"]
            summary.append((material, model, frequency, cubic, quartic))
    return summary


if __name__ == "__main__":
    for material, model, frequency, cubic, quartic in check():
        print(f"{material:4s} {model:15s} frequency={frequency:.4f} THz "
              f"cubic={cubic:.3f} quartic={quartic:.3f}")
