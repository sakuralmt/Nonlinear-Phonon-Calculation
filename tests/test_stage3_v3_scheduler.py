import json
import fcntl
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest
from ase import Atoms
from ase.calculators.calculator import Calculator, all_changes

from mlff_modepair_workflow import stage3_scheduler, stage3_v3
from mlff_modepair_workflow.stage3_compare import compare
from mlff_modepair_workflow.stage3_force_grid import evaluate_force_pair
from mlff_modepair_workflow.advanced_stage1 import preflight_calculator
from mlff_modepair_workflow.compare_stage1_qe_v3 import compare as compare_stage1
from mlff_modepair_workflow.prophet_stage1 import finite_q_orbits, mode_pairs_from_phonons, phonons_from_force_constants
from mlff_modepair_workflow.prophet_backend import sha256_file
from mlff_modepair_workflow.units import NORMALIZATION_VERSION
from mlff_modepair_workflow.units import RY_TO_EV


def _prepared_run(tmp_path, name="mos2"):
    run_root = tmp_path / name
    pair_root = run_root / "pairs" / "pair_one"
    pair_root.mkdir(parents=True)
    (run_root / "run_manifest.json").write_text(json.dumps({
        "kind": "qe_v3_stage3_run", "selected_pair_codes": ["pair_one"]}))
    (pair_root / "pair_meta.json").write_text(json.dumps({"builder": {"nat_super": 1}}))
    for i in range(9):
        for j in range(9):
            point = pair_root / f"grid_{i:02d}_{j:02d}"
            point.mkdir()
            (point / "scf.inp").write_text("input")
            (point / "submit.sh").write_text(
                f"#!/bin/bash\n#SBATCH --job-name=qv3_{name}_{i:02d}{j:02d}\n")
    return run_root


def test_stage3_controller_enforces_userwide_cap_and_small_batch(tmp_path, monkeypatch):
    state_root = tmp_path / "global"
    run_root = _prepared_run(tmp_path)
    stage3_scheduler.register(state_root, run_root)
    submitted = []

    def fake_run(cmd, **kwargs):
        assert cmd[0] == "sbatch"
        submitted.append(cmd[1])
        return SimpleNamespace(stdout=f"Submitted batch job {1000 + len(submitted)}\n")

    monkeypatch.setattr(stage3_scheduler.subprocess, "run", fake_run)
    external = {str(i): (f"qe_external_{i}", "RUNNING") for i in range(29)}
    monkeypatch.setattr(stage3_scheduler, "_queue", lambda: external)
    stage3_scheduler.run(state_root, max_active=30, submit_batch=5, once=True)
    assert len(submitted) == 1
    status = json.loads((run_root / "pairs/pair_one/grid_00_00/job_status.json").read_text())
    assert status["state"] == "submitted" and status["job_id"] == "1001" and status["attempts"] == 1
    assert status["submitted_at"] > 0
    assert stage3_scheduler._count_stage3_active(external) == 29
    assert stage3_scheduler._count_stage3_active({"old": ("prophet_r03_r01_0000", "RUNNING")}) == 1


def test_stage3_controller_recovers_unrecorded_active_job_without_duplicate(tmp_path, monkeypatch):
    state_root = tmp_path / "global"
    run_root = _prepared_run(tmp_path)
    stage3_scheduler.register(state_root, run_root)
    queue = {"321": ("qv3_mos2_0000", "RUNNING")}
    monkeypatch.setattr(stage3_scheduler, "_queue", lambda: queue)
    submissions = []

    def fake_run(cmd, **kwargs):
        submissions.append(cmd)
        return SimpleNamespace(stdout="Submitted batch job 999\n")

    monkeypatch.setattr(stage3_scheduler.subprocess, "run", fake_run)
    stage3_scheduler.run(state_root, max_active=1, submit_batch=1, once=True)
    assert submissions == []
    status = json.loads((run_root / "pairs/pair_one/grid_00_00/job_status.json").read_text())
    assert status["job_id"] == "321"


def test_stage3_controller_waits_for_scheduler_visibility_before_retry(tmp_path):
    state_root = tmp_path / "global"
    run_root = _prepared_run(tmp_path)
    stage3_scheduler.register(state_root, run_root)
    point = run_root / "pairs/pair_one/grid_00_00/job_status.json"
    point.write_text(json.dumps({"state": "submitted", "job_id": "321",
                                 "attempts": 1, "submitted_at": stage3_scheduler.time.time()}))
    snapshot = stage3_scheduler._snapshot(state_root, queue={})
    assert snapshot["active_stage3_jobs_userwide"] == 1
    assert point.parent not in snapshot["pending_job_dirs"]


def test_stage3_pilot_visits_only_first_pair(tmp_path):
    state_root = tmp_path / "global"
    run_root = _prepared_run(tmp_path)
    stage3_scheduler.register(state_root, run_root)
    assert len(stage3_scheduler._snapshot(state_root, queue={}, pair_limit=1)["pending_job_dirs"]) == 81
    with pytest.raises(ValueError, match="positive"):
        stage3_scheduler.run(state_root, pair_limit=0, once=True)


def test_stage3_small_batch_alternates_registered_materials(tmp_path, monkeypatch):
    state_root = tmp_path / "global"
    mos2 = _prepared_run(tmp_path, "mos2")
    wse2 = _prepared_run(tmp_path, "wse2")
    stage3_scheduler.register(state_root, mos2)
    stage3_scheduler.register(state_root, wse2)
    submitted = []

    def fake_run(cmd, **kwargs):
        submitted.append(Path(cmd[1]))
        return SimpleNamespace(stdout=f"Submitted batch job {1000 + len(submitted)}\n")

    monkeypatch.setattr(stage3_scheduler.subprocess, "run", fake_run)
    monkeypatch.setattr(stage3_scheduler, "_queue", lambda: {})
    stage3_scheduler.run(state_root, max_active=4, submit_batch=4, once=True)
    assert [path.parents[3].name for path in submitted] == ["mos2", "wse2", "mos2", "wse2"]


def test_stage3_controller_lock_is_shared_across_state_roots(tmp_path, monkeypatch):
    monkeypatch.setattr(stage3_scheduler.Path, "home", lambda: tmp_path)
    first = tmp_path / "first"
    second = tmp_path / "second"
    stage3_scheduler.register(first, _prepared_run(tmp_path, "mos2"))
    stage3_scheduler.register(second, _prepared_run(tmp_path, "wse2"))
    lock_path = tmp_path / ".cache/mlff_modepair_workflow/stage3_controller.lock"
    lock_path.parent.mkdir(parents=True)
    with lock_path.open("a+") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(BlockingIOError):
            stage3_scheduler.run(second, once=True)


def test_qe_parser_converts_energy_and_forces_and_requires_completion(tmp_path):
    output = tmp_path / "scf.out"
    output.write_text("!    total energy = -10.0000 Ry\n"
                      "atom    1 type  1   force = 0.1 -0.2 0.3\nJOB DONE.\n")
    energy, forces = stage3_v3.parse_qe_output(output, 1)
    assert energy == pytest.approx(-10 * RY_TO_EV)
    assert forces.shape == (1, 3)
    assert forces[0, 0] == pytest.approx(0.1 * RY_TO_EV / stage3_v3.BOHR_TO_ANGSTROM)
    assert stage3_v3.parse_qe_output(output, 2) is None
    output.write_text(output.read_text().replace("JOB DONE.", ""))
    assert stage3_v3.parse_qe_output(output, 1) is None


def test_same_displacement_comparison_removes_absolute_energy_offset(tmp_path):
    structure = tmp_path / "structure.inp"
    structure.write_text("same structure")
    pair_file = tmp_path / "pairs.json"
    pair = {"pair_code": "one", "gamma_mode": {"freq_thz": 2.0},
            "target_mode": {"freq_thz": 3.0}}
    pair_file.write_text(json.dumps({"pairs": [pair]}))
    qe_root = tmp_path / "qe"
    qe_pair = qe_root / "pairs/one"
    qe_pair.mkdir(parents=True)
    (qe_root / "run_manifest.json").write_text(json.dumps({
        "selected_pair_codes": ["one"], "structure": str(structure),
        "mode_pairs_json": str(pair_file)}))
    (qe_root / "selection.json").write_text(json.dumps({
        "structure_sha256": sha256_file(structure),
        "mode_pairs_sha256": sha256_file(pair_file)}))
    (qe_pair / "summary.json").write_text(json.dumps({"complete": True, "completed_points": 81}))
    q1, q2 = np.meshgrid(stage3_v3.AXES, stage3_v3.AXES)
    qe_grid = -100 + .2 * q1**2 + .4 * q2**2 + .01 * q1 * q2**2 + .005 * q1**2 * q2**2
    np.save(qe_pair / "energy_grid_eV.npy", qe_grid)
    stage2 = tmp_path / "mattersim"
    model_pair = stage2 / "pairs/one"
    model_pair.mkdir(parents=True)
    (stage2 / "pair_ranking.json").write_text(json.dumps({
        "backend": {"backend": "mattersim"}, "signature": {
            "structure_sha256": sha256_file(structure),
            "mode_pairs_sha256": sha256_file(pair_file),
            "normalization_version": NORMALIZATION_VERSION}}))
    (model_pair / "summary.json").write_text(json.dumps({"pair_code": "one", "completed_points": 81}))
    np.save(model_pair / "energy_grid_eV.npy", qe_grid + 500)
    result = compare(qe_root, {"mattersim": stage2})
    metrics = result["pairs"][0]["models"]["mattersim"]
    assert metrics["energy_error_full9_eV"]["mae"] < 1e-12
    assert metrics["physics_error"]["phi122"] == pytest.approx(0, abs=1e-8)
    assert metrics["physics_error"]["phi1122"] == pytest.approx(0, abs=1e-8)


def test_force_grid_resumes_without_recomputing_saved_points(tmp_path):
    phi = np.zeros((2, 2, 1, 3, 1, 3))
    phi[0, 0, 0, :, 0, :] = np.eye(3)
    records = phonons_from_force_constants(phi, np.ones(1), 2)
    pair = mode_pairs_from_phonons(records, finite_q_orbits(2), 1)[0]
    primitive = Atoms("H", positions=[[0, 0, 0]], cell=np.diag([3.0, 3.0, 15.0]), pbc=True)

    class Spring(Calculator):
        implemented_properties = ["forces"]
        calls = 0

        def calculate(self, atoms=None, properties=("forces",), system_changes=all_changes):
            super().calculate(atoms, properties, system_changes)
            self.calls += 1
            self.results["forces"] = -2 * atoms.positions

    calc = Spring()
    root = tmp_path / "forces"
    assert evaluate_force_pair(pair, primitive, calc, root, {"same": True}) == 81
    assert calc.calls == 81
    assert evaluate_force_pair(pair, primitive, calc, root, {"same": True}) == 0
    assert calc.calls == 81
    grid = np.load(root / "forces_eV_per_A.npy")
    assert grid.shape[:2] == (9, 9)
    assert np.isfinite(grid).all()
    with pytest.raises(ValueError, match="signature changed"):
        evaluate_force_pair(pair, primitive, calc, root, {"different": True})


def test_advanced_stage1_preflight_checks_conservative_energy_force_units():
    atoms = Atoms("H", positions=[[0.1, 0, 0]], cell=np.diag([3.0, 3.0, 15.0]), pbc=True)

    class Harmonic(Calculator):
        implemented_properties = ["energy", "forces"]

        def calculate(self, atoms=None, properties=("energy", "forces"), system_changes=all_changes):
            super().calculate(atoms, properties, system_changes)
            self.results["energy"] = float(np.sum(atoms.positions**2))
            self.results["forces"] = -2 * atoms.positions

    result = preflight_calculator(atoms, Harmonic(), "cpu")
    assert result["repeat_energy_difference_eV"] == pytest.approx(0)
    assert result["force_energy_difference_eV_per_A"] == pytest.approx(0, abs=1e-10)


def test_full_q_mesh_comparison_checks_same_structure_and_modes():
    phi = np.zeros((2, 2, 1, 3, 1, 3))
    phi[0, 0, 0, :, 0, :] = np.diag([1, 2, 3])
    q_points = phonons_from_force_constants(phi, np.ones(1), 2)
    source = {"normalization_version": NORMALIZATION_VERSION,
              "structure_sha256": "same", "q_grid": [2, 2, 1], "natoms_primitive": 1}
    qe = {"version": 3, "source": {**source, "backend": "qe", "qe_geometry_verified": True}, "q_points": q_points}
    model = {"version": 3, "source": {**source, "backend": "toy"}, "q_points": q_points}
    result = compare_stage1(model, qe)
    assert result["q_count"] == 4
    assert result["matched_frequency_mae_thz"] == pytest.approx(0)
    assert result["isolated_mode_median_overlap_squared"] == pytest.approx(1)
    model["source"]["structure_sha256"] = "different"
    with pytest.raises(ValueError, match="one structure"):
        compare_stage1(model, qe)
