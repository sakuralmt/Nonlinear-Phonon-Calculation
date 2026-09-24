"""Import a complete QE matdyn mesh into the shared v3 real-mode contract."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np

from .core import load_atoms_from_qe
from .phonon_eigenvectors import read_matdyn_q_mesh, read_qe_eigenvectors
from .prophet_backend import sha256_file
from .prophet_stage1 import _degenerate_groups, _encode_mode, _phase_fix, equivalent_pair_channels, finite_q_orbits, mode_pairs_from_phonons
from .units import CONTRACT_VERSION, NORMALIZATION_VERSION, UNITS
from qe_phonon_stage1_server_bundle.qpair_tools.common import is_hexagonal_2d


def import_qe_mesh(structure: Path, matdyn_input: Path, qe_eig: Path, output_dir: Path,
                   mesh_n: int = 6, geometry_source: str = "shared_dft",
                   qe_source_structure: Path | None = None):
    structure, matdyn_input, qe_eig = (Path(p).resolve() for p in (structure, matdyn_input, qe_eig))
    primitive = load_atoms_from_qe(structure)
    geometry_verified = False
    source_structure_hash = None
    max_geometry_difference = None
    if qe_source_structure is not None:
        qe_source_structure = Path(qe_source_structure).resolve()
        reference = load_atoms_from_qe(qe_source_structure)
        if len(reference) != len(primitive) or reference.get_chemical_symbols() != primitive.get_chemical_symbols():
            raise ValueError("QE source structure has different atom count or element ordering")
        max_geometry_difference = float(max(np.max(np.abs(reference.cell.array - primitive.cell.array)),
                                            np.max(np.abs(reference.positions - primitive.positions))))
        if max_geometry_difference > 1e-4:
            raise ValueError(f"QE source structure differs by {max_geometry_difference:.6g} Å; formal same-structure import refused")
        geometry_verified = True
        source_structure_hash = sha256_file(qe_source_structure)
    if not bool(np.all(primitive.pbc)):
        raise ValueError("QE v3 Stage1 requires a fully periodic cell with monolayer vacuum")
    hexagonal, details = is_hexagonal_2d(primitive.cell.array, 0.05, 3.0)
    if not hexagonal:
        raise ValueError(f"QE v3 Stage1 requires an in-plane hexagonal cell: {details}")
    q = read_matdyn_q_mesh(matdyn_input)
    indices = np.rint(q[:, :2] * mesh_n).astype(int) % mesh_n
    if (len(q) != mesh_n * mesh_n or not np.allclose(q[:, :2] * mesh_n, np.rint(q[:, :2] * mesh_n), atol=1e-7)
            or not np.allclose(q[:, 2], 0) or len({tuple(index) for index in indices}) != mesh_n * mesh_n):
        raise ValueError("QE matdyn input is not a complete, unique in-plane q mesh")
    frequencies, vectors = read_qe_eigenvectors(qe_eig, len(primitive), q)
    records = []
    for iq, index in enumerate(indices):
        modes = [_encode_mode(_phase_fix(vectors[iq, :, mode]), len(primitive))
                 for mode in range(vectors.shape[2])]
        records.append({
            "q_index": index.tolist(),
            "q_frac": [float(index[0] / mesh_n), float(index[1] / mesh_n), 0.0],
            "freqs_thz": frequencies[iq].tolist(),
            "eigenvectors": modes,
            "degenerate_groups_one_based": _degenerate_groups(frequencies[iq]),
        })
    records.sort(key=lambda row: tuple(row["q_index"]))
    orbits = finite_q_orbits(mesh_n)
    pairs = mode_pairs_from_phonons(records, orbits, len(primitive))
    source = {
        "backend": "qe", "structure": str(structure), "structure_sha256": sha256_file(structure),
        "matdyn_input_sha256": sha256_file(matdyn_input), "qe_eig_sha256": sha256_file(qe_eig),
        "natoms_primitive": len(primitive), "symbols": primitive.get_chemical_symbols(),
        "masses_amu": primitive.get_masses().tolist(), "q_grid": [mesh_n, mesh_n, 1],
        "geometry_source": geometry_source, "normalization_version": NORMALIZATION_VERSION,
        "qe_geometry_verified": geometry_verified,
        "qe_source_structure_sha256": source_structure_hash,
        "qe_max_cell_or_position_difference_A": max_geometry_difference,
        "units": UNITS,
    }
    output_dir = Path(output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    phonon = output_dir / "phonon_dataset.json"
    mode_pairs = output_dir / "mode_pairs.selected.json"
    phonon.write_text(json.dumps({"kind": "qe_phonon_mesh", "version": CONTRACT_VERSION,
                                  "source": source, "q_points": records, "q_orbits": orbits}, indent=2) + "\n")
    mode_pairs.write_text(json.dumps({"kind": "mode_pairs_qgamma_qpair", "version": CONTRACT_VERSION,
                                      "source": source, "selection": "momentum_conservation_only",
                                      "finite_q_orbits": orbits,
                                      "equivalent_pair_channels": equivalent_pair_channels(records, orbits, pairs, len(primitive)),
                                      "pairs": pairs}, indent=2) + "\n")
    return mode_pairs, phonon


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--structure", type=Path, required=True)
    parser.add_argument("--matdyn-input", type=Path, required=True)
    parser.add_argument("--qe-eig", type=Path, required=True)
    parser.add_argument("--qe-source-structure", type=Path,
                        help="QE calculation's actual input structure; required for formal same-structure Stage2")
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--mesh-n", type=int, default=6)
    args = parser.parse_args(argv)
    print(import_qe_mesh(args.structure, args.matdyn_input, args.qe_eig,
                         args.output_dir, args.mesh_n,
                         qe_source_structure=args.qe_source_structure))


if __name__ == "__main__":
    main()
