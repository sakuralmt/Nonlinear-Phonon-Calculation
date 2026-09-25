"""Unit-aware frozen-mode displacements and small PES fits.

Only the two-stage MLFF workflow depends on this module.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
from ase.build import make_supercell
from ase.io import read
from ase.io.espresso import read_espresso_in

from .units import (
    CONV_TO_CM1,
    CONV_TO_THZ,
    NORMALIZATION_VERSION,
    UNITS,
    projected_derivatives,
)


def decode_complex_mode(mode):
    if not mode:
        return np.zeros((0, 3), dtype=np.complex128)

    first = mode[0]
    if isinstance(first, dict):
        rows = []
        for vec in mode:
            rows.append(
                [
                    float(vec["x"]["re"]) + 1j * float(vec["x"]["im"]),
                    float(vec["y"]["re"]) + 1j * float(vec["y"]["im"]),
                    float(vec["z"]["re"]) + 1j * float(vec["z"]["im"]),
                ]
            )
        return np.array(rows, dtype=np.complex128)

    return np.array(
        [[c[0] + 1j * c[1] for c in vec] for vec in mode], dtype=np.complex128
    )


def canonicalize_q(q, tol: float = 1.0e-8):
    q = np.array(q, dtype=float)
    q = q - np.floor(q)
    q[np.abs(q) < tol] = 0.0
    q[np.abs(q - 1.0) < tol] = 0.0
    return q


def infer_commensurate_supercell_n(q_frac, n_max: int = 12, tol: float = 1.0e-8):
    q = canonicalize_q(q_frac)
    for n in range(1, n_max + 1):
        if abs(q[2]) > tol:
            continue
        cond_x = abs(q[0] * n - round(q[0] * n)) < tol
        cond_y = abs(q[1] * n - round(q[1] * n)) < tol
        if cond_x and cond_y:
            return n
    raise ValueError(
        f"Could not find commensurate nxnx1 supercell for q={q_frac} up to n={n_max}"
    )


def load_atoms_from_qe(scf_file: Path):
    suffix = scf_file.suffix.lower()
    if suffix in {".xyz", ".extxyz", ".traj", ".cif", ".vasp", ".poscar"}:
        return read(scf_file)
    try:
        with scf_file.open("r") as f:
            return read_espresso_in(f)
    except Exception:
        return read(scf_file)


def fit_func(xy, c20, c02, c12, c21, c30, c03, c11, c40, c04, c22, c10, c01, c00):
    x, y = xy
    return (
        c20 * x**2
        + c02 * y**2
        + c12 * x * y**2
        + c21 * x**2 * y
        + c30 * x**3
        + c03 * y**3
        + c11 * x * y
        + c40 * x**4
        + c04 * y**4
        + c22 * x**2 * y**2
        + c10 * x
        + c01 * y
        + c00
    )


def freq_from_c2(c2: float):
    if c2 > 0:
        root = np.sqrt(2.0 * c2)
        return {
            "stable": True,
            "thz": float(root * CONV_TO_THZ),
            "cm1": float(root * CONV_TO_CM1),
        }
    return {
        "stable": False,
        "imag_thz": float(np.sqrt(2.0 * abs(c2)) * CONV_TO_THZ),
        "imag_cm1": float(np.sqrt(2.0 * abs(c2)) * CONV_TO_CM1),
    }


def fit_1d_axis_quartic(a: np.ndarray, e: np.ndarray):
    design = np.column_stack([a**2, a**3, a**4, a, np.ones_like(a)])
    coeff, _, _, _ = np.linalg.lstsq(design, e, rcond=None)
    c2, c3, c4, c1, c0 = coeff
    pred = design @ coeff
    rmse = float(np.sqrt(np.mean((pred - e) ** 2)))
    return {
        "c2": float(c2),
        "c3": float(c3),
        "c4": float(c4),
        "c1": float(c1),
        "c0": float(c0),
        "rmse": rmse,
        "freq": freq_from_c2(float(c2)),
    }


def polynomial_design(
    a1_vals: np.ndarray, a2_vals: np.ndarray, fit_window: float | None = None
):
    x = np.repeat(a1_vals, len(a2_vals))
    y = np.tile(a2_vals, len(a1_vals))

    if fit_window is not None:
        mask = (np.abs(x) <= fit_window) & (np.abs(y) <= fit_window)
        x_fit = x[mask]
        y_fit = y[mask]
    else:
        mask = np.ones(len(x), dtype=bool)
        x_fit, y_fit = x, y

    design = np.column_stack(
        [
            x_fit**2,
            y_fit**2,
            x_fit * y_fit**2,
            x_fit**2 * y_fit,
            x_fit**3,
            y_fit**3,
            x_fit * y_fit,
            x_fit**4,
            y_fit**4,
            x_fit**2 * y_fit**2,
            x_fit,
            y_fit,
            np.ones_like(x_fit),
        ]
    )

    return design, mask


def fit_polynomial(
    a1_vals: np.ndarray,
    a2_vals: np.ndarray,
    energies: np.ndarray,
    fit_window: float | None = None,
):
    x = np.repeat(a1_vals, len(a2_vals))
    y = np.tile(a2_vals, len(a1_vals))
    design, mask = polynomial_design(a1_vals, a2_vals, fit_window)
    params, _, _, _ = np.linalg.lstsq(design, energies[mask], rcond=None)
    e_model_all = fit_func(np.vstack([x, y]), *params)
    residuals = e_model_all - energies
    sse = np.sum(residuals**2)
    sst = np.sum((energies - np.mean(energies)) ** 2)
    r2 = float(1.0 - sse / sst) if sst > 1e-24 else (1.0 if sse <= 1e-24 else 0.0)
    rmse = float(np.sqrt(np.mean(residuals**2)))
    return params, residuals, r2, rmse


def extract_physics(params: np.ndarray):
    c20, c02, c12, c21, c30, c03, c11, c40, c04, c22, c10, c01, c00 = params
    coefficients = {
        "c20": float(c20),
        "c02": float(c02),
        "c12": float(c12),
        "c21": float(c21),
        "c30": float(c30),
        "c03": float(c03),
        "c11": float(c11),
        "c40": float(c40),
        "c04": float(c04),
        "c22": float(c22),
        "c10": float(c10),
        "c01": float(c01),
        "c00": float(c00),
    }
    return {
        "freq_mode1": freq_from_c2(float(c20)),
        "freq_mode2": freq_from_c2(float(c02)),
        **projected_derivatives(coefficients),
        "coefficients_ev": coefficients,
        "coefficient_units": "eV/(Angstrom*sqrt(amu))^degree; c00 is eV/supercell",
        "normalization_version": NORMALIZATION_VERSION,
    }


def axis_frequency_checks(a1_vals: np.ndarray, a2_vals: np.ndarray, e_grid: np.ndarray):
    idx_a2_0 = int(np.argmin(np.abs(a2_vals)))
    idx_a1_0 = int(np.argmin(np.abs(a1_vals)))
    e_a1 = e_grid[idx_a2_0, :]
    e_a2 = e_grid[:, idx_a1_0]
    return {
        "mode1_axis_fit": fit_1d_axis_quartic(a1_vals, e_a1),
        "mode2_axis_fit": fit_1d_axis_quartic(a2_vals, e_a2),
    }


class ModePairFrozenPhononBuilder:
    def __init__(self, pair_record: dict, prim_atoms):
        self.pair_record = pair_record
        self.prim_atoms = prim_atoms
        self.gamma_mode = decode_complex_mode(pair_record["gamma_mode"]["eigenvector"])
        self.q_mode = decode_complex_mode(pair_record["target_mode"]["eigenvector_q"])
        self.q_frac = np.array(pair_record["target_mode"]["q_frac"], dtype=float)
        self.n_super = infer_commensurate_supercell_n(self.q_frac)
        self.nat_prim = len(self.prim_atoms)

        self.supercell = make_supercell(
            self.prim_atoms, [[self.n_super, 0, 0], [0, self.n_super, 0], [0, 0, 1]]
        )
        # QE relaxation flags are constraints for geometry optimization, not
        # for frozen-phonon displacements or force-grid validation.
        self.supercell.set_constraint()
        self.n_cells = self.n_super * self.n_super
        self.base_cell = self.supercell.get_cell().array.copy()
        self.base_frac = self.supercell.get_scaled_positions().copy()
        self.cell_inv = np.linalg.inv(self.base_cell)
        self.prim_indices = np.arange(len(self.supercell), dtype=int) % self.nat_prim
        self.replica_r = np.array(
            [
                [i, j, 0]
                for i in range(self.n_super)
                for j in range(self.n_super)
                for _ in range(self.nat_prim)
            ],
            dtype=float,
        )
        self.phase_q = np.exp(2j * np.pi * np.dot(self.replica_r, self.q_frac))
        self.gamma_super = self.gamma_mode[self.prim_indices]
        self.q_super = self.q_mode[self.prim_indices]

        # For Gamma and other self-conjugate points an arbitrary global QE
        # phase can make the real displacement vanish. Rotate only when the
        # supercell wave has a well-defined real quadrature. At generic q the
        # quadratures have equal norm and we preserve the supplied QE gauge.
        def real_quadrature_phase(wave):
            pseudo_norm = np.sum(wave**2)
            total_norm = np.sum(np.abs(wave) ** 2)
            if abs(pseudo_norm) < 1e-8 * total_norm:
                return 1.0 + 0.0j
            return np.exp(-0.5j * np.angle(pseudo_norm))

        self.gamma_phase_factor = real_quadrature_phase(self.gamma_super)
        self.q_phase_factor = real_quadrature_phase(
            self.q_super * self.phase_q[:, None]
        )
        self.gamma_super *= self.gamma_phase_factor
        self.q_super *= self.q_phase_factor
        # QE eigenvectors are normalized in the primitive cell.  Taking the real
        # part of a Bloch wave changes its norm at a non-self-conjugate q point.
        # Normalize the actual real supercell displacement, not the complex wave.
        gamma_norm = np.linalg.norm(np.real(self.gamma_super)) / np.sqrt(self.n_cells)
        q_norm = np.linalg.norm(
            np.real(self.q_super * self.phase_q[:, None])
        ) / np.sqrt(self.n_cells)
        if gamma_norm < 1e-12 or q_norm < 1e-12:
            raise ValueError(
                "Real frozen-phonon displacement has zero norm; choose another mode phase"
            )
        self.gamma_amplitude_factor = 1.0 / gamma_norm
        self.q_amplitude_factor = 1.0 / q_norm
        masses = np.asarray(self.supercell.get_masses(), dtype=float)
        self.mass_sqrt = np.sqrt(masses)[:, None]

    @property
    def nat_super(self):
        return len(self.supercell)

    def displacement_cart(self, a1: float, a2: float):
        u_complex = (
            a1 * self.gamma_amplitude_factor * self.gamma_super
            + a2 * self.q_amplitude_factor * self.q_super * self.phase_q[:, None]
        )
        u_complex = u_complex / np.sqrt(self.n_cells)
        return np.real(u_complex) / self.mass_sqrt

    def fractional_positions(self, a1: float, a2: float):
        frac = self.base_frac + self.displacement_cart(a1, a2) @ self.cell_inv
        frac[:, :2] %= 1.0
        return frac

    def displacement_scale(self, a1: float, a2: float):
        norms = np.linalg.norm(self.displacement_cart(a1, a2), axis=1)
        return float(np.max(norms)), float(np.mean(norms))

    def build_atoms(self, a1: float, a2: float):
        atoms = self.supercell.copy()
        atoms.set_scaled_positions(self.fractional_positions(a1, a2))
        return atoms

    def build_atoms_list(self, a1_vals: np.ndarray, a2_vals: np.ndarray):
        index_map = []
        atoms_list = []
        for i_a2, a2 in enumerate(a2_vals):
            for j_a1, a1 in enumerate(a1_vals):
                index_map.append((i_a2, j_a1, float(a1), float(a2)))
                atoms_list.append(self.build_atoms(float(a1), float(a2)))
        return index_map, atoms_list

    def evaluate_grid(
        self,
        calc,
        a1_vals: np.ndarray,
        a2_vals: np.ndarray,
        row_callback=None,
        batch_size: int = 1,
    ):
        grid = np.zeros((len(a2_vals), len(a1_vals)), dtype=float)
        if batch_size > 1 and hasattr(calc, "predict_energies"):
            index_map, atoms_list = self.build_atoms_list(a1_vals, a2_vals)
            energies = calc.predict_energies(atoms_list, batch_size=batch_size)
            for (i_a2, j_a1, _a1, _a2), energy in zip(index_map, energies):
                grid[i_a2, j_a1] = float(energy)
            if row_callback is not None:
                for i_a2, a2 in enumerate(a2_vals):
                    row_callback(i_a2, float(a2))
            return grid

        for i_a2, a2 in enumerate(a2_vals):
            for j_a1, a1 in enumerate(a1_vals):
                atoms = self.build_atoms(float(a1), float(a2))
                atoms.calc = calc
                grid[i_a2, j_a1] = float(atoms.get_potential_energy())
            if row_callback is not None:
                row_callback(i_a2, float(a2))
        return grid

    def metadata(self):
        return {
            "pair_code": self.pair_record["pair_code"],
            "n_super": self.n_super,
            "n_cells": self.n_cells,
            "nat_prim": self.nat_prim,
            "nat_super": self.nat_super,
            "q_frac": self.q_frac.tolist(),
            "normalization": "each real mass-weighted supercell mode has unit norm; u = Re[(A1 c_Gamma e_Gamma + A2 c_q e_q exp(i qR))/sqrt(N_cells)]/sqrt(M)",
            "gamma_amplitude_factor": self.gamma_amplitude_factor,
            "q_amplitude_factor": self.q_amplitude_factor,
            "gamma_phase_factor": [
                self.gamma_phase_factor.real,
                self.gamma_phase_factor.imag,
            ],
            "q_phase_factor": [self.q_phase_factor.real, self.q_phase_factor.imag],
        }


def evaluate_pair_grid(
    pair_record: dict,
    structure_path: Path | None,
    calc,
    a1_vals: np.ndarray,
    a2_vals: np.ndarray,
    row_callback=None,
    prim_atoms=None,
    batch_size: int = 1,
):
    if prim_atoms is None:
        if structure_path is None:
            raise ValueError(
                "structure_path is required when prim_atoms is not provided."
            )
        prim_atoms = load_atoms_from_qe(structure_path)
    builder = ModePairFrozenPhononBuilder(pair_record, prim_atoms)
    grid = builder.evaluate_grid(
        calc, a1_vals, a2_vals, row_callback=row_callback, batch_size=batch_size
    )
    return grid, builder


def analyze_pair_grid(
    pair_record: dict,
    e_grid_ev_supercell: np.ndarray,
    a1_vals: np.ndarray,
    a2_vals: np.ndarray,
    fit_window: float | None = 1.0,
):
    e_shift = e_grid_ev_supercell - np.min(e_grid_ev_supercell)
    design, center_mask = polynomial_design(a1_vals, a2_vals, fit_window)
    params, residuals, r2, rmse = fit_polynomial(
        a1_vals, a2_vals, e_shift.T.reshape(-1), fit_window=fit_window
    )
    fit_energies = e_shift.T.reshape(-1)[center_mask]
    center_sse = float(np.sum(residuals[center_mask] ** 2))
    center_sst = float(np.sum((fit_energies - np.mean(fit_energies)) ** 2))
    center_r2 = (
        1.0 - center_sse / center_sst
        if center_sst > 1e-24
        else (1.0 if center_sse <= 1e-24 else 0.0)
    )
    physics = extract_physics(params)
    axis = axis_frequency_checks(a1_vals, a2_vals, e_shift)
    mode_pair_reference = {
        "reference_kind": "mode_pair_frequency",
        "reference_label": "selected_mode_pair_frequency",
        "gamma_freq_thz": float(pair_record["gamma_mode"]["freq_thz"]),
        "target_freq_thz": float(pair_record["target_mode"]["freq_thz"]),
    }
    result = {
        "fit_window": fit_window,
        "fit_points": int(len(design)),
        "fit_design_rank": int(np.linalg.matrix_rank(design)),
        "fit_condition_number": float(np.linalg.cond(design)),
        "r2": r2,
        "center_fit_r2": float(center_r2),
        "rmse_ev_supercell": rmse,
        "center_fit_rmse_ev_supercell": float(
            np.sqrt(np.mean(residuals[center_mask] ** 2))
        ),
        "max_abs_residual_ev_supercell": float(np.max(np.abs(residuals))),
        "physics": physics,
        "axis_checks": axis,
        "mode_pair_reference": mode_pair_reference,
        "reference": mode_pair_reference,
        "units": UNITS,
        "normalization_version": NORMALIZATION_VERSION,
    }
    if "q_frac" in pair_record["target_mode"]:
        result["momentum_diagnostics"] = momentum_diagnostics(pair_record, physics)
    return result


def momentum_diagnostics(pair_record: dict, physics: dict) -> dict:
    """Label polynomial terms by primitive-lattice momentum conservation.

    A real finite-q coordinate contains both q and -q. A y**n monomial is
    translation-allowed if one of its (2*k-n)*q harmonics is reciprocal. This
    is a necessary condition only; point-group selection rules are separate.
    Forbidden fitted coefficients remain visible as numerical diagnostics.
    """
    q = np.asarray(pair_record["target_mode"]["q_frac"], dtype=float)
    gamma = np.asarray(pair_record["gamma_mode"].get("q_frac", [0, 0, 0]))
    if not np.allclose(gamma, np.rint(gamma), atol=1e-9, rtol=0):
        raise ValueError("This analysis requires a Gamma first coordinate")
    if np.allclose(q, np.rint(q), atol=1e-9, rtol=0):
        raise ValueError("This analysis requires a non-Gamma second coordinate")
    terms = {}
    for name, value in physics["coefficients_ev"].items():
        q_power = int(name[2])
        allowed = any(
            np.allclose(
                (2 * k - q_power) * q, np.rint((2 * k - q_power) * q), atol=1e-8, rtol=0
            )
            for k in range(q_power + 1)
        )
        terms[name] = {
            "translation_allowed": allowed,
            "role": "projected_polynomial" if allowed else "numerical_diagnostic_only",
            "coefficient_ev": value,
        }
    return {
        "q_frac": q.tolist(),
        "terms": terms,
        "phi122_role": "Gamma_q_minus_q_coupling",
        "phi112_role": "momentum_forbidden_numerical_diagnostic_only",
        "phi1122_role": "Gamma_Gamma_q_minus_q_coupling",
        "forbidden_phi112_mev_per_A3amu32": physics["phi_112_mev_per_A3amu32"],
        "note": "Momentum allowance does not imply point-group allowance; no forbidden term is ranked.",
    }
