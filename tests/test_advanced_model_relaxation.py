"""Model-specific geometry must survive the advanced Stage1/Stage2 handoff."""

import json
import sys
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest
from ase.calculators.calculator import Calculator, all_changes

from mlff_modepair_workflow import advanced_stage1
from mlff_modepair_workflow.core import load_atoms_from_qe
from mlff_modepair_workflow.model_relaxation import relax_structure_with_calculator
from mlff_modepair_workflow.prophet_backend import sha256_file
from server_highthroughput_workflow import run_modular_pipeline


class ToyPeriodicPotential(Calculator):
    implemented_properties = ["energy", "forces"]

    def calculate(self, atoms=None, properties=("energy", "forces"), system_changes=all_changes):
        super().calculate(atoms, properties, system_changes)
        positions = atoms.get_positions()
        target = np.zeros_like(positions)
        target[:, 2] = 1.8
        delta = positions - target
        inplane_length = np.linalg.norm(atoms.cell.array[0])
        self.results["energy"] = float(4 * np.sum(delta**2) + 20 * (inplane_length - 3.06) ** 2)
        self.results["forces"] = -8 * delta


def _qe_input(path: Path) -> Path:
    path.write_text("""&CONTROL
  calculation = 'scf'
  pseudo_dir = './absent-pseudos'
/
&SYSTEM
  ibrav = 0, nat = 1, ntyp = 1
  ecutwfc = 30
/
&ELECTRONS
  conv_thr = 1.0d-8
/
ATOMIC_SPECIES
H 1.008 H.UPF
ATOMIC_POSITIONS crystal
H 0.0 0.0 0.1  0 0 1
K_POINTS automatic
1 1 1 0 0 0
CELL_PARAMETERS angstrom
3.0 0.0 0.0
-1.5 2.598076211353316 0.0
0.0 0.0 15.0
""")
    return path


def test_model_own_relaxation_records_geometry_and_refuses_mismatched_restart(tmp_path):
    source = _qe_input(tmp_path / "initial.inp")
    model = {"checkpoint_sha256": "first", "source_commit": "toy"}
    output = tmp_path / "own_geometry"
    structure, summary = relax_structure_with_calculator(
        source, output, ToyPeriodicPotential(), model, "equiformer-v3-oam"
    )
    atoms = load_atoms_from_qe(structure)
    assert summary["best_inplane_scale"] == pytest.approx(1.02, abs=0.002)
    assert atoms.positions[0, 2] == pytest.approx(1.8, abs=0.01)
    assert atoms.cell.array[2, 2] == pytest.approx(15.0)
    assert summary["optimized_structure_sha256"] == sha256_file(structure)
    assert summary["source_structure_sha256"] == sha256_file(source)
    assert summary["backend"] == "equiformer-v3-oam"
    assert relax_structure_with_calculator(
        source, output, ToyPeriodicPotential(), model, "equiformer-v3-oam"
    )[0] == structure
    with pytest.raises(ValueError, match="differs"):
        relax_structure_with_calculator(
            source, output, ToyPeriodicPotential(), {"checkpoint_sha256": "other"}, "equiformer-v3-oam"
        )


def test_advanced_stage1_uses_its_own_relaxed_structure_and_hash(tmp_path, monkeypatch):
    source = _qe_input(tmp_path / "initial.inp")
    checkpoint = tmp_path / "model.pt"
    checkpoint.write_bytes(b"toy model")
    model = {"backend": "equiformer-v3-oam", "checkpoint_sha256": sha256_file(checkpoint)}
    monkeypatch.setattr(advanced_stage1, "make_advanced_calculator",
                        lambda *args: (ToyPeriodicPotential(), model))
    output = tmp_path / "stage1"
    summary_path = advanced_stage1.run_advanced_stage1(
        source, checkpoint, tmp_path, "equiformer-v3-oam", output,
        device="cpu", geometry_source="model_relaxed", relax_only=True,
    )
    summary = json.loads(summary_path.read_text())
    assert summary["optimized_structure_sha256"] != sha256_file(source)
    pairs_path, phonon_path, force_path = advanced_stage1.run_advanced_stage1(
        source, checkpoint, tmp_path, "equiformer-v3-oam", output,
        device="cpu", geometry_source="model_relaxed", convergence_step=None,
    )
    phonon = json.loads(phonon_path.read_text())
    pairs = json.loads(pairs_path.read_text())
    assert force_path.is_file()
    assert phonon["source"]["geometry_source"] == "model_relaxed"
    assert phonon["source"]["structure_sha256"] == summary["optimized_structure_sha256"]
    assert pairs["source"]["relaxation"]["source_structure_sha256"] == sha256_file(source)
    assert len(pairs["pairs"]) == 54
    with pytest.raises(ValueError, match="another model, geometry or source structure"):
        advanced_stage1.run_advanced_stage1(
            source, checkpoint, tmp_path, "equiformer-v3-oam", output,
            device="cpu", geometry_source="shared_dft", preflight_only=True,
        )


def test_advanced_phonopy_route_records_asr_and_keeps_raw_constants(tmp_path, monkeypatch):
    pytest.importorskip("phonopy")
    source = _qe_input(tmp_path / "initial.inp")
    checkpoint = tmp_path / "model.pt"
    checkpoint.write_bytes(b"toy model")
    model = {"backend": "equiformer-v3-oam", "checkpoint_sha256": sha256_file(checkpoint)}
    monkeypatch.setattr(advanced_stage1, "make_advanced_calculator",
                        lambda *args: (ToyPeriodicPotential(), model))
    output = tmp_path / "phonopy_stage1"
    pairs, phonon, force = advanced_stage1.run_advanced_stage1(
        source, checkpoint, tmp_path, "equiformer-v3-oam", output,
        device="cpu", convergence_step=None, phonon_engine="phonopy",
    )
    dataset = json.loads(phonon.read_text())
    arrays = np.load(force)
    assert len(json.loads(pairs.read_text())["pairs"]) == 54
    assert dataset["source"]["phonon_engine"]["acoustic_sum_rule"] is True
    assert dataset["diagnostics"]["phonopy_asr"]["corrected_max_translational_drift_ev_per_A2"] < 1e-10
    assert "force_constants_raw_ev_per_A2" in arrays
    assert (output / "phonopy_params.yaml").is_file()
    with pytest.raises(ValueError, match="another model, geometry or source structure"):
        advanced_stage1.run_advanced_stage1(
            source, checkpoint, tmp_path, "equiformer-v3-oam", output,
            device="cpu", preflight_only=True, phonon_engine="custom",
        )


def test_modular_advanced_stage1_manifest_hands_off_own_structure(tmp_path, monkeypatch):
    source = _qe_input(tmp_path / "initial.inp")
    checkpoint = tmp_path / "model.pt"
    checkpoint.write_bytes(b"toy model")
    model = {"backend": "equiformer-v3-oam", "checkpoint_sha256": sha256_file(checkpoint)}
    monkeypatch.setattr(advanced_stage1, "make_advanced_calculator",
                        lambda *args: (ToyPeriodicPotential(), model))
    monkeypatch.setattr(run_modular_pipeline, "_prepare_system_runtime",
                        lambda spec, run_root: {"mock": True})
    args = SimpleNamespace(
        stage1_backend="equiformer-v3-oam", geometry_source="model_relaxed",
        stage1_structure=str(source), stage1_checkpoint=str(checkpoint),
        stage1_source_root=str(tmp_path), stage1_device="cpu", q_grid_n=6,
        fd_step=0.01, qe_relax="no", scheduler="local",
        structure_provenance=None, input_root=str(tmp_path),
    )
    spec = SimpleNamespace(
        already_relaxed=True, system_id="toy", system_dir=tmp_path,
        structure_cif=None, metadata_path=None, workflow_family="test",
    )
    run_root = tmp_path / "run"
    manifest = run_modular_pipeline.run_stage1(args, run_root, spec)
    payload = json.loads(manifest.read_text())
    handed_off = run_root / payload["files"]["structure"]
    assert payload["backend"] == "equiformer-v3-oam"
    assert payload["geometry_source"] == "model_relaxed"
    assert payload["structure_sha256"] == sha256_file(handed_off)
    assert payload["structure_sha256"] != sha256_file(source)
    assert payload["model"]["checkpoint_sha256"] == sha256_file(checkpoint)
    with pytest.raises(ValueError, match="another Stage1 model or geometry source"):
        run_modular_pipeline.run_stage1(
            SimpleNamespace(**{**vars(args), "geometry_source": "shared_dft"}), run_root, spec
        )


def test_modular_mlff_defaults_to_phonopy_with_asr(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["run_modular_pipeline.py"])
    args = run_modular_pipeline.parse_args()
    assert args.phonon_engine == "phonopy"
    assert args.phonopy_asr is True
