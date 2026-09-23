"""Recompute overlap assignments from saved QE/MLFF vectors without new forces."""

import argparse
import json
from pathlib import Path

import numpy as np

from mlff_modepair_workflow.phonon_eigenvectors import compare_modes


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("result", nargs="+", type=Path)
    args = parser.parse_args()
    for path in args.result:
        data = json.loads(path.read_text())
        saved = np.load(path.with_suffix(".npz"))
        isolated = []
        degenerate = []
        for iq, row in enumerate(data["q_records"]):
            assignment, overlaps, groups = compare_modes(
                saved["qe_freq"][iq], saved["qe_vectors"][iq],
                saved["ml_freq"][iq], saved["ml_vectors"][iq],
                gamma_acoustic=bool(np.allclose(saved["q"][iq], 0)),
            )
            row["qe_to_ml_mode_one_based"] = (assignment + 1).tolist()
            row["individual_overlap_squared"] = overlaps.tolist()
            row["matched_frequency_error_thz"] = (
                saved["ml_freq"][iq][assignment] - saved["qe_freq"][iq]
            ).tolist()
            row["degeneracy_groups"] = groups
            isolated.extend(group["subspace_overlap"] for group in groups if len(group["qe_modes"]) == 1)
            degenerate.extend(group["subspace_overlap"] for group in groups if len(group["qe_modes"]) > 1)
        data["isolated_mode_count"] = len(isolated)
        data["isolated_mode_median_overlap_squared"] = float(np.median(isolated))
        data["degenerate_group_count"] = len(degenerate)
        data["degenerate_group_median_subspace_overlap"] = float(np.median(degenerate)) if degenerate else None
        path.write_text(json.dumps(data, indent=2))
        print(path.name, len(isolated), data["isolated_mode_median_overlap_squared"])


if __name__ == "__main__":
    main()
