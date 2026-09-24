"""Versioned numerical and unit conventions for frozen-phonon results."""

from __future__ import annotations

import numpy as np


CONTRACT_VERSION = 3
NORMALIZATION_VERSION = "real_mass_weighted_unit_v2"
RY_TO_EV = 13.605693009
CONV_TO_THZ = 15.63330423985619
CONV_TO_CM1 = 521.4708983725064

UNITS = {
    "length": "Angstrom",
    "mass": "amu",
    "energy": "eV/supercell",
    "force": "eV/Angstrom",
    "force_constant": "eV/Angstrom^2",
    "frequency": "THz",
    "normal_coordinate": "Angstrom*sqrt(amu)",
    "third_order": "meV/(Angstrom^3*amu^(3/2))",
    "fourth_order": "meV/(Angstrom^4*amu^2)",
    "eigenvector": "dimensionless mass-weighted Cartesian",
}


def energies_to_ev(values, source_unit: str):
    """Convert an explicitly declared total-energy unit to eV."""
    unit = source_unit.strip().lower()
    if unit in {"ev", "ev/supercell"}:
        factor = 1.0
    elif unit in {"ry", "rydberg", "ry/supercell"}:
        factor = RY_TO_EV
    else:
        raise ValueError(f"Unsupported or undeclared energy unit: {source_unit!r}")
    return np.asarray(values, dtype=float) * factor


def projected_derivatives(coefficients: dict[str, float]) -> dict[str, float]:
    """E(Q1,Q2) Taylor derivatives, with factorials included explicitly."""
    return {
        "phi_122_mev_per_A3amu32": 2_000.0 * coefficients["c12"],
        "phi_112_mev_per_A3amu32": 2_000.0 * coefficients["c21"],
        "phi_111_mev_per_A3amu32": 6_000.0 * coefficients["c30"],
        "phi_222_mev_per_A3amu32": 6_000.0 * coefficients["c03"],
        "phi_1122_mev_per_A4amu2": 4_000.0 * coefficients["c22"],
        "phi_1111_mev_per_A4amu2": 24_000.0 * coefficients["c40"],
        "phi_2222_mev_per_A4amu2": 24_000.0 * coefficients["c04"],
    }
