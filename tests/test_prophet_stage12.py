import json
import csv

import numpy as np
import pytest
from ase import Atoms
from ase.build import make_supercell
from ase.calculators.calculator import Calculator, all_changes

from mlff_modepair_workflow.core import analyze_pair_grid
from mlff_modepair_workflow.prophet_backend import validate_atoms
from mlff_modepair_workflow.prophet_stage1 import (
    equivalent_pair_channels, finite_q_orbits, mode_pairs_from_phonons, phonons_from_force_constants,
)
from mlff_modepair_workflow.phonon_eigenvectors import real_space_force_constants
from mlff_modepair_workflow.prophet_stage2 import _ensure_run_signature, _signature, evaluate_pair, finalize
from mlff_modepair_workflow.units import NORMALIZATION_VERSION, energies_to_ev, projected_derivatives
from server_highthroughput_workflow.stage_contracts import create_stage1_manifest, load_json
from server_highthroughput_workflow.real_stage1_prophet import write_relaxed_structure_from_qe_template
from server_highthroughput_workflow import real_stage1_prophet
from scripts.refit_legacy_dft_grid import read_grid


def _analytic_grid():
    axes = np.linspace(-2, 2, 9)
    q1, q2 = np.meshgrid(axes, axes)
    return axes, 0.2 * q1**2 + 0.4 * q2**2 + 0.01 * q1 * q2**2 + 0.005 * q1**2 * q2**2


def test_units_are_explicit_and_third_fourth_derivatives_include_factorials():
    coeff = {"c12": .01, "c21": .02, "c30": .03, "c03": .04,
             "c22": .005, "c40": .006, "c04": .007}
    result = projected_derivatives(coeff)
    assert result["phi_122_mev_per_A3amu32"] == pytest.approx(20)
    assert result["phi_1122_mev_per_A4amu2"] == pytest.approx(20)
    assert result["phi_1111_mev_per_A4amu2"] == pytest.approx(144)
    assert energies_to_ev([-1], "Ry")[0] == pytest.approx(-13.605693009)
    with pytest.raises(ValueError):
        energies_to_ev([1], "unknown")


def test_nine_by_nine_center_fit_is_overdetermined_and_recovers_couplings():
    axes, grid = _analytic_grid()
    pair = {"gamma_mode": {"freq_thz": 2.0}, "target_mode": {"freq_thz": 3.0}}
    result = analyze_pair_grid(pair, grid, axes, axes, fit_window=1.0)
    assert result["fit_points"] == 25
    assert result["fit_design_rank"] == 13
    assert result["fit_condition_number"] < 30
    assert result["physics"]["phi_122_mev_per_A3amu32"] == pytest.approx(20)
    assert result["physics"]["phi_1122_mev_per_A4amu2"] == pytest.approx(20)


def test_legacy_grid_layout_is_explicit_and_preserves_phi122(tmp_path):
    axes, grid = _analytic_grid()
    energy = tmp_path / "energy_grid_eV.dat"
    np.savetxt(energy, grid)
    table = tmp_path / "amplitude_grid.csv"
    with table.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["a1", "a2", "a1_index", "a2_index"])
        writer.writeheader()
        for i, x in enumerate(axes):
            for j, y in enumerate(axes):
                writer.writerow({"a1": x, "a2": y, "a1_index": i, "a2_index": j})
    x, y, recovered = read_grid(table, energy, "eV")
    assert np.allclose(x, axes) and np.allclose(y, axes)
    result = analyze_pair_grid({"gamma_mode": {"freq_thz": 2}, "target_mode": {"freq_thz": 3}},
                               recovered, x, y, fit_window=1.0)
    assert result["physics"]["phi_122_mev_per_A3amu32"] == pytest.approx(20)


def _toy_records(n: int, nat: int):
    phi = np.zeros((n, n, nat, 3, nat, 3))
    for atom in range(nat):
        for axis in range(3):
            phi[0, 0, atom, axis, atom, axis] = 1 + axis + atom / 10
    return phonons_from_force_constants(phi, np.ones(nat), n)


@pytest.mark.parametrize("nat", [1, 2, 3])
def test_only_finite_q_momentum_pairs_with_full_branch_coverage(nat):
    orbits = finite_q_orbits(6)
    assert len(orbits) == 6
    assert sum(orbit["size"] for orbit in orbits) == 35
    records = _toy_records(6, nat)
    pairs = mode_pairs_from_phonons(records, orbits, nat)
    assert len(pairs) == 6 * (3 * nat) ** 2
    assert len({pair["pair_code"] for pair in pairs}) == len(pairs)
    for pair in pairs:
        q = np.asarray(pair["target_mode"]["q_frac"])
        qbar = np.asarray(pair["target_mode"]["qbar_frac"])
        assert not np.allclose(q, 0)
        assert np.allclose((q + qbar) % 1, 0)
    assert {pair["gamma_mode"]["mode_number_one_based"] for pair in pairs} == set(range(1, 3 * nat + 1))


@pytest.mark.parametrize("nat", [1, 2, 3])
def test_stage1_physical_channels_cover_each_pair_once(nat):
    orbits = finite_q_orbits(6)
    records = _toy_records(6, nat)
    pairs = mode_pairs_from_phonons(records, orbits, nat)
    plan = equivalent_pair_channels(records, orbits, pairs, nat)
    assert plan["finite_q_point_count"] == 35
    assert plan["q_orbit_count"] == 6
    assert plan["pair_count"] == 6 * (3 * nat) ** 2
    assert plan["gamma_groups_one_based"][0] == [1, 2, 3]
    assert plan["channel_count"] == 6 * 3 * nat * len(plan["gamma_groups_one_based"])
    listed = [code for channel in plan["channels"] for code in channel["pair_codes"]]
    assert len(listed) == len(set(listed)) == len(pairs)
    assert set(listed) == {pair["pair_code"] for pair in pairs}
    assert all(channel["q_orbit_member_indices"] for channel in plan["channels"])
    with pytest.raises(ValueError, match="complete mode pairs"):
        equivalent_pair_channels(records, orbits, pairs[:-1], nat)


def test_gamma_multiplet_is_not_replaced_by_one_arbitrary_component():
    orbits = finite_q_orbits(6)
    records = _toy_records(6, 3)
    gamma = next(row for row in records if row["q_index"] == [0, 0])
    gamma["freqs_thz"] = [0, 0, 0, 5, 5.001, 8, 9, 10, 11]
    pairs = mode_pairs_from_phonons(records, orbits, 3)
    plan = equivalent_pair_channels(records, orbits, pairs, 3)
    assert plan["gamma_groups_one_based"][:2] == [[1, 2, 3], [4, 5]]
    assert plan["channel_count"] == 6 * 9 * 6
    multiplet = next(row for row in plan["channels"] if row["gamma_modes_one_based"] == [4, 5])
    assert len(multiplet["pair_codes"]) == 2
    assert multiplet["requires_all_gamma_components"]


def test_default_gamma_threshold_does_not_merge_accidentally_close_optical_mode():
    orbits = finite_q_orbits(6)
    records = _toy_records(6, 3)
    gamma = next(row for row in records if row["q_index"] == [0, 0])
    gamma["freqs_thz"] = [0, 0, 0, 4.8773, 4.8774, 7.0839, 7.0842, 7.1769, 8.9327]
    pairs = mode_pairs_from_phonons(records, orbits, 3)
    plan = equivalent_pair_channels(records, orbits, pairs, 3)
    assert plan["gamma_groups_one_based"] == [[1, 2, 3], [4, 5], [6, 7], [8], [9]]
    assert plan["channel_count"] == 270
    assert equivalent_pair_channels(records, orbits, pairs, 3, 0.1)["channel_count"] == 216


def test_checkpoint_covers_each_point_and_recovers_from_interruption(tmp_path):
    records = _toy_records(2, 1)
    pair = mode_pairs_from_phonons(records, finite_q_orbits(2), 1)[0]
    primitive = Atoms("H", positions=[[0, 0, 0]], cell=np.diag([3.0, 3.0, 15.0]), pbc=True)

    class ToyEnergy(Calculator):
        implemented_properties = ["energy"]

        def calculate(self, atoms=None, properties=("energy",), system_changes=all_changes):
            super().calculate(atoms, properties, system_changes)
            self.results["energy"] = float(np.sum((atoms.positions - primitive.positions)**2))

    signature = {"test": "fixed", "normalization_version": NORMALIZATION_VERSION}
    pair_dir = tmp_path / "pair"
    with pytest.raises(RuntimeError, match="Intentional"):
        evaluate_pair(pair, primitive, ToyEnergy(), {"backend": "toy"}, signature, pair_dir,
                      stop_after=7, global_count=[0])
    checkpoint = json.loads((pair_dir / "checkpoint.json").read_text())
    assert sum(value is not None for row in checkpoint["energies_ev_supercell"] for value in row) == 7
    summary = evaluate_pair(pair, primitive, ToyEnergy(), {"backend": "toy"}, signature, pair_dir)
    assert summary["completed_points"] == 81
    assert summary["analysis"]["fit_design_rank"] == 13
    assert np.isfinite(np.loadtxt(pair_dir / "energy_grid_eV.dat")).all()
    with pytest.raises(ValueError, match="signature mismatch"):
        evaluate_pair(pair, primitive, ToyEnergy(), {"backend": "toy"}, {"test": "changed"}, pair_dir)
    screening = tmp_path / "screening"
    proper_pair_dir = screening / "pairs" / pair["pair_code"]
    proper_pair_dir.parent.mkdir(parents=True)
    pair_dir.rename(proper_pair_dir)
    first_csv = finalize(screening, [pair], signature)
    first_bytes = first_csv.read_bytes()
    assert first_bytes == finalize(screening, [pair], signature).read_bytes()
    assert len(json.loads((screening / "pair_ranking.json").read_text())["pairs"]) == 1
    grid_path = proper_pair_dir / "energy_grid_eV.npy"
    original_grid = np.load(grid_path)
    damaged_grid = original_grid.copy()
    damaged_grid[0, 0] += 0.01
    np.save(grid_path, damaged_grid)
    with pytest.raises(ValueError, match="checksum mismatch"):
        finalize(screening, [pair], signature)
    np.save(grid_path, original_grid)
    (screening / "pairs" / "unexpected").mkdir()
    (screening / "pairs" / "unexpected" / "summary.json").write_text("{}")
    with pytest.raises(ValueError, match="unexpected pairs"):
        finalize(screening, [pair], signature)


def test_run_signature_lock_is_idempotent_and_rejects_mixed_inputs(tmp_path):
    root = tmp_path / "screening"
    _ensure_run_signature(root, {"structure": "one"}, {"checkpoint_sha256": "fixed"})
    _ensure_run_signature(root, {"structure": "one"}, {"checkpoint_sha256": "fixed"})
    with pytest.raises(ValueError, match="different inputs"):
        _ensure_run_signature(root, {"structure": "two"}, {"checkpoint_sha256": "fixed"})


def test_stage2_backend_signatures_keep_prophet_resume_and_separate_mattersim(tmp_path):
    structure = tmp_path / "structure.scf.inp"
    structure.write_text("same structure for both backends")
    source = {"mode_pairs_sha256": "same-pairs", "geometry_source": "shared_dft"}
    common = {"checkpoint_sha256": "pinned-weight", "source_commit": "pinned-code",
              "energy_accumulation": "pinned-energy"}
    prophet = _signature(source, structure, {**common, "backend": "prophet"})
    mattersim = _signature(source, structure, {**common, "backend": "mattersim"})
    assert "backend" not in prophet  # active Prophet checkpoint signatures stay unchanged
    assert mattersim["backend"] == "mattersim"
    assert prophet != mattersim


def test_checkpoint_element_and_atom_count_preflight():
    allowed = set(range(1, 84)) | set(range(89, 95))
    for symbols in ("H", "BN", "MoS2", "WSe2"):
        atoms = Atoms(symbols, positions=np.zeros((len(Atoms(symbols)), 3)),
                      cell=np.diag([3.0, 3.0, 15.0]), pbc=True)
        validate_atoms(atoms, allowed)
    unsupported = Atoms("Po", positions=[[0, 0, 0]], cell=np.diag([3.0, 3.0, 15.0]), pbc=True)
    with pytest.raises(ValueError, match="does not support"):
        validate_atoms(unsupported, allowed)


@pytest.mark.parametrize("symbols", ["H", "BN", "MoS2"])
def test_finite_difference_stage1_accepts_one_two_three_atom_cells(symbols):
    primitive = Atoms(symbols, positions=[[i * 0.3, 0.0, 0.0] for i in range(len(Atoms(symbols)))],
                      cell=[[3, 0, 0], [-1.5, 2.598076211, 0], [0, 0, 15]], pbc=True)
    reference = make_supercell(primitive, np.diag([2, 2, 1])).positions.copy()

    class Springs(Calculator):
        implemented_properties = ["forces"]

        def calculate(self, atoms=None, properties=("forces",), system_changes=all_changes):
            super().calculate(atoms, properties, system_changes)
            self.results["forces"] = -3.0 * (atoms.positions - reference)

    phi = real_space_force_constants(primitive, Springs(), 2, 0.01)
    records = phonons_from_force_constants(phi, primitive.get_masses(), 2)
    assert len(records) == 4
    assert all(len(record["freqs_thz"]) == 3 * len(primitive) for record in records)
    assert np.allclose(phi[0, 0, np.arange(len(primitive)), :, np.arange(len(primitive)), :],
                       np.eye(3) * 3.0)


def test_stage1_v3_manifest_keeps_handoff_fields_and_units(tmp_path):
    source = tmp_path / "source.scf.inp"
    source.write_text("sample structure\n")
    pairs = tmp_path / "pairs.json"
    pairs.write_text('{"pairs": []}\n')
    run_root = tmp_path / "run"
    manifest = create_stage1_manifest(run_root, pairs, source, backend="prophet", geometry_source="shared_dft")
    payload = load_json(manifest)
    assert payload["version"] == 3
    assert payload["backend"] == "prophet"
    assert payload["files"]["mode_pairs_json"]
    assert payload["files"]["structure"]
    assert payload["units"]["normal_coordinate"] == "Angstrom*sqrt(amu)"


def test_legacy_qe_manifest_retains_v2_without_false_normalization_claim(tmp_path):
    structure = tmp_path / "source.scf.inp"
    structure.write_text("old QE structure\n")
    pairs = tmp_path / "pairs.json"
    pairs.write_text('{"pairs": []}\n')
    payload = load_json(create_stage1_manifest(tmp_path / "legacy", pairs, structure))
    assert payload["version"] == 2
    assert "normalization_version" not in payload
    assert "units" not in payload


def test_stage1_reuse_rejects_changed_model_or_mesh(tmp_path, monkeypatch):
    structure = tmp_path / "source.scf.inp"
    structure.write_text("fixed structure\n")
    pairs = tmp_path / "pairs.json"
    pairs.write_text('{"pairs": []}\n')
    run_root = tmp_path / "run"
    phonon = run_root / "phonon.json"
    phonon.parent.mkdir(parents=True)
    phonon.write_text('{"source": {"q_grid": [6, 6, 1], "finite_difference_step_angstrom": 0.01}}\n')
    checkpoint = tmp_path / "model.pt"
    checkpoint.write_bytes(b"test checkpoint")
    monkeypatch.setattr(real_stage1_prophet, "resolve_checkpoint", lambda _: checkpoint)
    manifest = create_stage1_manifest(run_root, pairs, structure, backend="prophet",
                                      geometry_source="shared_dft", phonon_dataset=phonon,
                                      model={"checkpoint_sha256": "different"})
    kwargs = dict(run_root=run_root, structure=structure, pseudo_dir=tmp_path,
                  checkpoint="dummy", device="cpu", mesh_n=6, step=0.01,
                  geometry_source="shared_dft")
    with pytest.raises(ValueError, match="different Prophet checkpoint"):
        real_stage1_prophet.run_real_prophet_stage1(**kwargs)
    payload = load_json(manifest)
    payload["model"]["checkpoint_sha256"] = real_stage1_prophet.sha256_file(checkpoint)
    manifest.write_text(json.dumps(payload))
    with pytest.raises(ValueError, match="different q grid"):
        real_stage1_prophet.run_real_prophet_stage1(**{**kwargs, "mesh_n": 4})


def test_phonopy_stage1_restart_reuses_verified_manifest(tmp_path, monkeypatch):
    structure = tmp_path / "structure.scf.inp"
    structure.write_text("fixed structure\n")
    source_hash = real_stage1_prophet.sha256_file(structure)
    pairs = tmp_path / "pairs.json"
    pairs.write_text(json.dumps({"source": {"structure_sha256": source_hash}, "pairs": []}))
    run_root = tmp_path / "run"
    phonon = run_root / "phonon.json"
    phonon.parent.mkdir(parents=True)
    phonon.write_text(json.dumps({"source": {
        "q_grid": [6, 6, 1], "finite_difference_step_angstrom": 0.01,
        "phonon_engine": {"name": "phonopy", "acoustic_sum_rule": True},
    }}))
    constants = run_root / "force_constants.npz"
    constants.write_bytes(b"already calculated")
    checkpoint = tmp_path / "model.pt"
    checkpoint.write_bytes(b"pinned checkpoint")
    monkeypatch.setattr(real_stage1_prophet, "resolve_checkpoint", lambda _: checkpoint)
    monkeypatch.setattr(real_stage1_prophet, "run_prophet_stage1",
                        lambda *args, **kwargs: pytest.fail("Stage1 should be reused"))
    manifest = create_stage1_manifest(
        run_root, pairs, structure, backend="prophet", geometry_source="shared_dft",
        phonon_dataset=phonon, force_constants=constants,
        model={"checkpoint_sha256": real_stage1_prophet.sha256_file(checkpoint)},
    )
    assert real_stage1_prophet.run_real_prophet_stage1(
        run_root=run_root, structure=structure, pseudo_dir=tmp_path,
        checkpoint="dummy", device="cpu", mesh_n=6, step=0.01,
        geometry_source="shared_dft",
    ) == manifest


def test_model_relaxed_geometry_round_trips_without_pseudopotentials(tmp_path):
    source = tmp_path / "source.inp"
    source.write_text("""&CONTROL
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
H 0.0 0.0 0.1
K_POINTS automatic
1 1 1 0 0 0
CELL_PARAMETERS angstrom
3.0 0.0 0.0
0.0 3.0 0.0
0.0 0.0 15.0
""")
    atoms = Atoms("H", scaled_positions=[[0.1, 0.2, 0.3]], cell=np.diag([3.1, 3.1, 15.0]), pbc=True)
    target = tmp_path / "relaxed.inp"
    write_relaxed_structure_from_qe_template(source, target, atoms)
    assert "H  0.100000000000000" in target.read_text()
