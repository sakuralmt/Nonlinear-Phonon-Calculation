"""Re-fit archived QE rank-1 grids at fixed displacement extent and two spacings.

This checks PES sampling density, distinct from the 6x6 reciprocal-space mesh
and from changing the fitted Q window. It does not create new DFT data.
"""

from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

import numpy as np

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
ROOT = REPO / "docs/reference_data/pbe_20260925"
STABLE = REPO
sys.path.insert(0, str(STABLE))

from mlff_modepair_workflow.core import analyze_pair_grid  # noqa: E402


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> None:
    rows = []
    key = "phi_1122_mev_per_A4amu2"
    for material in ("ws2", "mos2", "wse2"):
        result_path = ROOT / f"results_{material}.json"
        selection_path = ROOT / f"selection_{material}.json"
        result = json.loads(result_path.read_text())["rows"][0]
        pair = json.loads(selection_path.read_text())["selected"][0]["pair"]
        if result["completed_base_points"] != 81:
            raise ValueError(f"Incomplete rank-1 grid: {material}")
        grid = np.asarray(result["wide_grid_relative_ev"], dtype=float)
        if grid.shape != (9, 9):
            raise ValueError(f"Unexpected PES shape: {material}")
        axis = np.linspace(-2, 2, 9)
        sparse = analyze_pair_grid(pair, grid[::2, ::2], axis[::2], axis[::2], fit_window=2)
        dense = analyze_pair_grid(pair, grid, axis, axis, fit_window=2)
        central = analyze_pair_grid(pair, grid[2:7, 2:7], axis[2:7], axis[2:7], fit_window=1)
        if any(fit["fit_design_rank"] != 13 for fit in (sparse, dense, central)):
            raise ValueError(f"Rank-deficient fit: {material}")
        archived = result["wide_fit_2"]["physics"][key]
        if abs(dense["physics"][key] - archived) > 1e-8:
            raise ValueError(f"Re-fit does not reproduce archived value: {material}")
        values = {
            "central_5x5_step_0p5_extent_1": central["physics"][key],
            "wide_5x5_step_1_extent_2": sparse["physics"][key],
            "wide_9x9_step_0p5_extent_2": dense["physics"][key],
        }
        rows.append({
            "material": material,
            "channel": result["lda_channel"],
            "values_mev_per_A4amu2": values,
            "density_change_percent": 100 * (values["wide_9x9_step_0p5_extent_2"] /
                                             values["wide_5x5_step_1_extent_2"] - 1),
            "window_change_percent": 100 * (values["wide_9x9_step_0p5_extent_2"] /
                                            values["central_5x5_step_0p5_extent_1"] - 1),
            "design_rank": {"sparse": 13, "dense": 13},
            "source_sha256": {
                str(result_path.relative_to(REPO)): sha(result_path),
                str(selection_path.relative_to(REPO)): sha(selection_path),
            },
        })
    report = {
        "description": "Fixed |Q|<=2: sparse 5x5 delta Q=1 versus full 9x9 delta Q=0.5, both from identical archived 81 QE energies; central 5x5 |Q|<=1 is a separate window comparison.",
        "fitter_sha256": sha(STABLE / "mlff_modepair_workflow/core.py"),
        "limitations": "Only rank-1 channels; no denser 9x9 grid within |Q|<=1 and no other-channel density test.",
        "rows": rows,
    }
    (HERE / "density_audit.json").write_text(json.dumps(report, indent=2) + "\n")
    for row in rows:
        print(row["material"], row["values_mev_per_A4amu2"],
              f"density change={row['density_change_percent']:+.3f}%")


if __name__ == "__main__":
    main()
