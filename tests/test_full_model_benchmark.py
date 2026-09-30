"""Validate benchmark resume, grid orientation and immutable provenance."""

import json
from pathlib import Path

import numpy as np
import pytest
from ase.calculators.calculator import Calculator, all_changes

from mlff_modepair_workflow.full_model_benchmark import run
from mlff_modepair_workflow.prophet_backend import sha256_file
from mlff_modepair_workflow.units import UNITS, NORMALIZATION_VERSION

DATA = Path(__file__).resolve().parents[1] / "docs/reference_data/pbe_20260925"


class Probe(Calculator):
    implemented_properties = ["energy", "forces"]

    def __init__(self, fail=None):
        super().__init__()
        self.calls = 0
        self.fail = fail

    def calculate(self, atoms=None, properties=("energy",), system_changes=all_changes):
        super().calculate(atoms, properties, system_changes)
        if self.calls == self.fail:
            raise RuntimeError("interrupt")
        self.calls += 1
        self.results = {
            "energy": float(np.sum(atoms.positions**2)),
            "forces": -2 * atoms.positions,
        }


def test_resume_and_model_identity(tmp_path):
    structure = DATA / "structures/ws2.scf.inp"
    pair = json.loads((DATA / "selection_ws2.json").read_text())["selected"][0]["pair"]
    manifest = tmp_path / "manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "units": UNITS,
                "normalization_version": NORMALIZATION_VERSION,
                "model": "tece",
                "material": "ws2",
                "tasks": [
                    {
                        "task_id": "dft-01",
                        "scope": "fixed_pbe_dft_diagnostic",
                        "structure": str(structure),
                        "structure_sha256": sha256_file(structure),
                        "pair": pair,
                        "axis": [-1, -0.5, 0, 0.5, 1],
                    }
                ],
            }
        )
    )
    checkpoint = tmp_path / "model.pt"
    checkpoint.write_bytes(b"checkpoint")
    with pytest.raises(RuntimeError, match="interrupt"):
        run(manifest, checkpoint, tmp_path / "run", "tece", calculator=Probe(fail=3))
    calc = Probe()
    path = run(manifest, checkpoint, tmp_path / "run", "tece", calculator=calc)
    assert calc.calls == 22
    result = json.loads(path.read_text())
    assert result["complete_points"] == 25
    assert result["rows"][0]["central"]["fit_design_rank"] == 13
    repeat = Probe()
    run(manifest, checkpoint, tmp_path / "run", "tece", calculator=repeat)
    assert repeat.calls == 0
    fresh = json.loads(
        run(
            manifest, checkpoint, tmp_path / "fresh", "tece", calculator=Probe()
        ).read_text()
    )
    np.testing.assert_allclose(
        result["rows"][0]["energy_grid_ev"], fresh["rows"][0]["energy_grid_ev"], atol=0
    )
    checkpoint.write_bytes(b"different model")
    with pytest.raises(ValueError, match="different inputs"):
        run(manifest, checkpoint, tmp_path / "run", "tece", calculator=Probe())
