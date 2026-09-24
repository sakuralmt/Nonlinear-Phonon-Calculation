"""Pilot checks for v3 real-mode normalization and energy/force consistency."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from mlff_modepair_workflow.core import ModePairFrozenPhononBuilder, load_atoms_from_qe
from mlff_modepair_workflow.prophet_backend import make_prophet_calculator, sha256_file
from mlff_modepair_workflow.units import CONTRACT_VERSION, NORMALIZATION_VERSION, UNITS


def check_pair(pair: dict, primitive, calculator, step: float = 0.1) -> dict:
    if step <= 0:
        raise ValueError("step must be positive")
    builder = ModePairFrozenPhononBuilder(pair, primitive)
    mass = builder.supercell.get_masses()[:, None]
    gamma = builder.displacement_cart(1, 0)
    target = builder.displacement_cart(0, 1)
    unit_norms = [float(np.sum(mass * displacement**2)) for displacement in (gamma, target)]
    cross = float(np.sum(mass * gamma * target))
    atoms = builder.build_atoms(0, 0)
    atoms.calc = calculator
    forces = np.asarray(atoms.get_forces(), dtype=float)
    results = []
    for index, tangent in enumerate((gamma, target)):
        q_plus = (step, 0) if index == 0 else (0, step)
        q_minus = (-step, 0) if index == 0 else (0, -step)
        plus = builder.build_atoms(*q_plus)
        plus.calc = calculator
        minus = builder.build_atoms(*q_minus)
        minus.calc = calculator
        central_energy_slope = (float(plus.get_potential_energy()) - float(minus.get_potential_energy())) / (2 * step)
        force_slope = -float(np.sum(forces * tangent))
        results.append({
            "mode": "gamma" if index == 0 else "finite_q",
            "energy_slope_ev_per_Q": central_energy_slope,
            "force_projection_ev_per_Q": force_slope,
            "absolute_difference_ev_per_Q": abs(central_energy_slope - force_slope),
        })
    return {
        "pair_code": pair["pair_code"], "n_super": builder.n_super,
        "mass_weighted_norms": unit_norms, "mass_weighted_cross_overlap": cross,
        "slope_step_Q": step, "slope_comparisons": results,
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode-pairs-json", type=Path, required=True)
    parser.add_argument("--structure", type=Path, required=True)
    parser.add_argument("--model", default="prophet_oame_mbd")
    parser.add_argument("--device", choices=["cpu", "cuda"], default="cpu")
    parser.add_argument("--pair-code")
    parser.add_argument("--step", type=float, default=0.1)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    payload = json.loads(args.mode_pairs_json.read_text())
    if payload.get("version") != CONTRACT_VERSION or payload["source"].get("normalization_version") != NORMALIZATION_VERSION:
        raise ValueError("Expected a v3 normalized mode-pair file")
    if sha256_file(args.structure) != payload["source"].get("structure_sha256"):
        raise ValueError("Structure hash does not match Stage1")
    pair = next((row for row in payload["pairs"] if args.pair_code is None or row["pair_code"] == args.pair_code), None)
    if pair is None:
        raise ValueError(f"Unknown pair code: {args.pair_code}")
    primitive = load_atoms_from_qe(args.structure)
    calculator, model = make_prophet_calculator(args.model, args.device, primitive)
    result = {
        "kind": "prophet_real_mode_pilot", "version": CONTRACT_VERSION,
        "source": payload["source"], "model": model, "units": UNITS,
        "check": check_pair(pair, primitive, calculator, args.step),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
