"""DFT geometry/basis import, point recovery and shared Stage2 handoff."""

import json
from pathlib import Path

import numpy as np
import pytest
from ase.calculators.calculator import Calculator, all_changes

from mlff_modepair_workflow.dft_reference import (
    import_dft_stage1,
    read_dft_structure,
    run_reference,
)
from mlff_modepair_workflow.screening_stage2 import validate_relaxed_stage1
from mlff_modepair_workflow.prophet_backend import sha256_file


DATA = Path(__file__).resolve().parents[1] / "docs/reference_data/pbe_20260925"


@pytest.mark.parametrize("material", ["ws2", "mos2", "wse2"])
def test_existing_dft_mesh_hands_off_without_relaxation(material, tmp_path):
    structure = DATA / "structures" / f"{material}.scf.inp"
    original_hash = sha256_file(structure)
    full = import_dft_stage1(
        structure, DATA / f"qe_phonon_dataset_{material}.json", None, tmp_path / "full"
    )
    payload = json.loads(full.read_text())
    validate_relaxed_stage1(payload, structure)
    assert len(payload["pairs"]) == 324
    assert payload["source"]["geometry_source"] == "dft_relaxed"
    assert sha256_file(structure) == original_hash
    selected = import_dft_stage1(
        structure,
        DATA / f"qe_phonon_dataset_{material}.json",
        DATA / f"selection_{material}.json",
        tmp_path / "selected",
    )
    assert len(json.loads(selected.read_text())["pairs"]) == 5
    assert not read_dft_structure(structure).constraints
    structure_copy = tmp_path / "changed.inp"
    structure_copy.write_text(structure.read_text() + "\n! changed provenance\n")
    with pytest.raises(ValueError, match="hash"):
        import_dft_stage1(
            structure_copy,
            DATA / f"qe_phonon_dataset_{material}.json",
            DATA / f"selection_{material}.json",
            tmp_path / "changed",
        )


def test_import_rejects_unmatched_mode_and_unit_contract(tmp_path):
    selection = json.loads((DATA / "selection_mos2.json").read_text())
    selection["selected"][0]["pair"]["gamma_mode"]["freq_thz"] += 0.1
    path = tmp_path / "selection.json"
    path.write_text(json.dumps(selection))
    with pytest.raises(ValueError, match="differs"):
        import_dft_stage1(
            DATA / "structures/mos2.scf.inp",
            DATA / "qe_phonon_dataset_mos2.json",
            path,
            tmp_path / "output",
        )
    selection["normalization"] = "legacy"
    path.write_text(json.dumps(selection))
    with pytest.raises(ValueError, match="normalization"):
        import_dft_stage1(
            DATA / "structures/mos2.scf.inp",
            DATA / "qe_phonon_dataset_mos2.json",
            path,
            tmp_path / "output",
        )


def test_shared_stage2_screen_accepts_dft_geometry(monkeypatch, tmp_path):
    from nonlinear_phonon_calculation.cli import main
    from mlff_modepair_workflow import screening_stage2

    structure = DATA / "structures/mos2.scf.inp"
    path = import_dft_stage1(
        structure, DATA / "qe_phonon_dataset_mos2.json", None, tmp_path / "stage1"
    )
    checkpoint = tmp_path / "model.pth"
    checkpoint.write_bytes(b"test model")
    monkeypatch.setattr(
        screening_stage2,
        "make_mattersim_calculator",
        lambda *args: (FailingEnergy(), {}),
    )
    assert (
        main(
            [
                "stage2",
                "screen",
                "--mode-pairs-json",
                str(path),
                "--structure",
                str(structure),
                "--checkpoint",
                str(checkpoint),
                "--output-dir",
                str(tmp_path / "stage2"),
                "--shard-count",
                "324",
                "--shard-index",
                "0",
            ]
        )
        == 0
    )
    points = list((tmp_path / "stage2/pairs").glob("*/points.json"))
    assert len(points) == 1
    assert len(json.loads(points[0].read_text())["energies_ev"]) == 6


class FailingEnergy(Calculator):
    implemented_properties = ["energy", "forces"]

    def __init__(self, fail_after=None):
        super().__init__()
        self.calls = 0
        self.fail_after = fail_after

    def calculate(self, atoms=None, properties=("energy",), system_changes=all_changes):
        super().calculate(atoms, properties, system_changes)
        if self.fail_after is not None and self.calls == self.fail_after:
            raise RuntimeError("injected failure")
        self.calls += 1
        self.results = {
            "energy": float(np.sum(atoms.positions**2)),
            "forces": -2 * atoms.positions,
        }


def test_reference_resumes_forces_and_rejects_changed_inputs(tmp_path):
    selection = json.loads((DATA / "selection_mos2.json").read_text())
    selection["selected"] = selection["selected"][1:2]
    selected_path = tmp_path / "selection.json"
    selected_path.write_text(json.dumps(selection))
    structure = DATA / "structures/mos2.scf.inp"
    pairs = import_dft_stage1(
        structure,
        DATA / "qe_phonon_dataset_mos2.json",
        selected_path,
        tmp_path / "stage1",
    )
    reference = json.loads((DATA / "results_mos2.json").read_text())
    reference["rows"] = reference["rows"][1:2]
    reference["mode_selection_sha256"] = sha256_file(selected_path)
    reference_path = tmp_path / "qe.json"
    reference_path.write_text(json.dumps(reference))
    checkpoint = tmp_path / "model.pth"
    checkpoint.write_bytes(b"test model")
    root = tmp_path / "stage2"
    with pytest.raises(RuntimeError, match="injected"):
        run_reference(
            pairs,
            structure,
            checkpoint,
            reference_path,
            root,
            calculator=FailingEnergy(3),
        )
    calc = FailingEnergy()
    result = run_reference(
        pairs, structure, checkpoint, reference_path, root, calculator=calc
    )
    assert calc.calls == 22
    payload = json.loads(result.read_text())
    assert payload["complete_points"] == 25
    assert payload["rows"][0]["center_fit"]["fit_design_rank"] == 13
    repeat = FailingEnergy()
    run_reference(pairs, structure, checkpoint, reference_path, root, calculator=repeat)
    assert repeat.calls == 0
    checkpoint.write_bytes(b"changed model")
    with pytest.raises(ValueError, match="different inputs"):
        run_reference(
            pairs, structure, checkpoint, reference_path, root, calculator=repeat
        )
