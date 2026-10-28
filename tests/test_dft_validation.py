"""Invalid imported mode identities and QE labels must not produce benchmarks."""

import copy
import json
from pathlib import Path

import numpy as np
import pytest
from ase.calculators.calculator import Calculator, all_changes

from mlff_modepair_workflow.dft_reference import import_dft_stage1, run_reference
from mlff_modepair_workflow.prophet_backend import sha256_file


DATA = Path(__file__).resolve().parents[1] / "docs/reference_data/pbe_20260925"


def _selection():
    selection = json.loads((DATA / "selection_mos2.json").read_text())
    selection["selected"] = selection["selected"][:1]
    return selection


def _import(selection, tmp_path):
    path = tmp_path / "selection.json"
    path.write_text(json.dumps(selection))
    return import_dft_stage1(
        DATA / "structures/mos2.scf.inp",
        DATA / "qe_phonon_dataset_mos2.json",
        path,
        tmp_path / "stage1",
    )


def test_finite_q_mode_cannot_be_imported_as_gamma(tmp_path):
    selection = _selection()
    pair = selection["selected"][0]["pair"]
    dataset = json.loads((DATA / "qe_phonon_dataset_mos2.json").read_text())
    q = pair["target_mode"]["q_frac"]
    point = next(p for p in dataset["q_points"] if np.allclose(p["q_frac"], q))
    index = pair["gamma_mode"]["mode_number_one_based"] - 1
    # This vector and frequency really do match the DFPT source, but at the
    # wrong q. The frozen-phonon Gamma direction has no Bloch phase factor.
    pair["gamma_mode"].update(
        q_frac=q,
        eigenvector=point["eigenvectors"][index],
        freq_thz=point["freqs_thz"][index],
    )
    with pytest.raises(ValueError, match="Gamma mode must be at Gamma"):
        _import(selection, tmp_path)


@pytest.mark.parametrize("branch", [0, -1, 10, True, 1.5])
def test_target_branch_must_be_one_based_integer(branch, tmp_path):
    selection = _selection()
    # The original first target is branch9. With branch0, Python[-1] used to
    # accept its unchanged last-branch vector and frequency as branch zero.
    selection["selected"][0]["pair"]["target_mode"][
        "mode_number_one_based"
    ] = branch
    with pytest.raises(ValueError, match="valid one-based branch"):
        _import(selection, tmp_path)


@pytest.mark.parametrize("qbar", [[0.5, 0], [float("nan"), 0, 0]])
def test_qbar_must_be_finite_three_vector(qbar, tmp_path):
    selection = _selection()
    selection["selected"][0]["pair"]["target_mode"]["qbar_frac"] = qbar
    with pytest.raises(ValueError, match="Gamma-q-minus-q"):
        _import(selection, tmp_path)


class FiniteCalculator(Calculator):
    implemented_properties = ["energy", "forces"]

    def __init__(self):
        super().__init__()
        self.calls = 0

    def calculate(self, atoms=None, properties=("energy",), system_changes=all_changes):
        super().calculate(atoms, properties, system_changes)
        self.calls += 1
        self.results = {
            "energy": float(np.sum(atoms.positions**2)),
            "forces": np.zeros((len(atoms), 3)),
        }


def _reference_inputs(tmp_path):
    selection = json.loads((DATA / "selection_mos2.json").read_text())
    selection["selected"] = selection["selected"][1:2]
    selection_path = tmp_path / "selection.json"
    selection_path.write_text(json.dumps(selection))
    pairs = import_dft_stage1(
        DATA / "structures/mos2.scf.inp",
        DATA / "qe_phonon_dataset_mos2.json",
        selection_path,
        tmp_path / "stage1",
    )
    reference = json.loads((DATA / "results_mos2.json").read_text())
    reference["rows"] = reference["rows"][1:2]
    reference["mode_selection_sha256"] = sha256_file(selection_path)
    checkpoint = tmp_path / "test-model.pth"
    checkpoint.write_bytes(b"test fixture: no actual model inference")
    return pairs, reference, checkpoint


@pytest.mark.parametrize(
    "kind", ["vector", "one_row", "empty", "nan_force", "nan_energy", "array_energy"]
)
def test_qe_reference_requires_scalar_energy_and_every_atomic_force(kind, tmp_path):
    pairs, reference, checkpoint = _reference_inputs(tmp_path)
    point = reference["rows"][0]["source_points"][0]
    if kind == "vector":
        point["forces_ev_per_A"] = [0.0, 0.0, 0.0]
    elif kind == "one_row":
        point["forces_ev_per_A"] = [[0.0, 0.0, 0.0]]
    elif kind == "empty":
        point["forces_ev_per_A"] = []
    elif kind == "nan_force":
        point["forces_ev_per_A"][0][0] = float("nan")
    elif kind == "nan_energy":
        point["energy_ev"] = float("nan")
    else:
        point["energy_ev"] = [point["energy_ev"]]
    path = tmp_path / "reference.json"
    path.write_text(json.dumps(reference))
    calc = FiniteCalculator()
    with pytest.raises(ValueError, match="finite energy and all atomic forces"):
        run_reference(
            pairs,
            DATA / "structures/mos2.scf.inp",
            checkpoint,
            path,
            tmp_path / "stage2",
            calculator=calc,
        )
    assert calc.calls == 0
    assert not (tmp_path / "stage2/reference_result.json").exists()


def test_unmodified_qe_reference_still_completes(tmp_path):
    pairs, reference, checkpoint = _reference_inputs(tmp_path)
    original = copy.deepcopy(reference)
    path = tmp_path / "reference.json"
    path.write_text(json.dumps(reference))
    calc = FiniteCalculator()
    result = run_reference(
        pairs,
        DATA / "structures/mos2.scf.inp",
        checkpoint,
        path,
        tmp_path / "stage2",
        calculator=calc,
    )
    assert json.loads(result.read_text())["complete_points"] == 25
    assert calc.calls == 25
    assert reference == original
