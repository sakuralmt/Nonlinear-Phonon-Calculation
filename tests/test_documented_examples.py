"""The bundled example geometries must be readable by their documented route."""

from pathlib import Path

import numpy as np
import pytest

from mlff_modepair_workflow.core import load_atoms_from_qe
from mlff_modepair_workflow.dft_reference import read_dft_structure


DATA = Path(__file__).resolve().parents[1] / "docs/reference_data/pbe_20260925"


@pytest.mark.parametrize("model", ["tece", "prophet", "equiformer-v3"])
def test_mlff_quick_start_uses_readable_explicit_cell(model):
    atoms = load_atoms_from_qe(DATA / f"full_model_structures/mos2-{model}.scf.inp")
    assert atoms.get_chemical_formula() == "MoS2"
    assert atoms.pbc.all()
    assert np.isfinite(atoms.cell.array).all()
    assert atoms.cell.volume > 0


def test_dft_quick_start_preserves_the_imported_hexagonal_cell():
    atoms = read_dft_structure(DATA / "structures/mos2.scf.inp")
    assert atoms.get_chemical_formula() == "MoS2"
    np.testing.assert_allclose(atoms.cell.lengths(), [3.183252423] * 2 + [18.104741573])
