from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class FamilyConvergenceProfile:
    workflow_family: str
    common_base_overrides: dict
    common_axes: dict
    phonon_balanced_thresholds: dict
    phonon_balanced_relaxed_scale: float
    pes_balanced_thresholds: dict
    pes_balanced_relaxed_scale: float
    pes_fast_thresholds: dict
    pes_fast_relaxed_scale: float


TMDS_MONOLAYER_HEX = FamilyConvergenceProfile(
    workflow_family="tmd_monolayer_hex",
    common_base_overrides={
        "calculation": "vc-relax",
        "include_ions": True,
        "include_cell": True,
        "tprnfor": True,
        "tstress": True,
        "ion_dynamics": "bfgs",
        "cell_dynamics": "bfgs",
        "press_conv_thr": "0.1",
        "forc_conv_thr": "1.0d-5",
        "ecutwfc": 80,
        "ecutrho": 800,
        "conv_thr": "1.0d-10",
        "degauss": "1.0d-10",
        "primitive_k_mesh": [12, 12, 1],
        "electron_maxstep": 10000,
        "mixing_beta": "0.3d0",
    },
    common_axes={
        "ecut": [
            {"label": "ecut60", "overrides": {"ecutwfc": 60, "ecutrho": 600}},
            {"label": "ecut80", "overrides": {"ecutwfc": 80, "ecutrho": 800}},
            {"label": "ecut100", "overrides": {"ecutwfc": 100, "ecutrho": 1000}},
            {"label": "ecut120", "overrides": {"ecutwfc": 120, "ecutrho": 1200}},
        ],
        "primitive_k_mesh": [
            {"label": "k6", "primitive_k_mesh": [6, 6, 1]},
            {"label": "k8", "primitive_k_mesh": [8, 8, 1]},
            {"label": "k10", "primitive_k_mesh": [10, 10, 1]},
            {"label": "k12", "primitive_k_mesh": [12, 12, 1]},
        ],
        "conv_thr": [
            {"label": "conv1e8", "overrides": {"conv_thr": "1.0d-8"}},
            {"label": "conv1e10", "overrides": {"conv_thr": "1.0d-10"}},
            {"label": "conv1e12", "overrides": {"conv_thr": "1.0d-12"}},
        ],
        "degauss": [
            {"label": "degauss1e8", "overrides": {"degauss": "1.0d-8"}},
            {"label": "degauss1e9", "overrides": {"degauss": "1.0d-9"}},
            {"label": "degauss1e10", "overrides": {"degauss": "1.0d-10"}},
        ],
    },
    phonon_balanced_thresholds={
        "energy_abs_diff_mev": 1.0,
        "max_position_delta_A": 0.003,
        "max_cell_delta_A": 0.003,
        "max_atomic_force_ry_bohr": 5.0e-4,
    },
    phonon_balanced_relaxed_scale=1.5,
    pes_balanced_thresholds={
        "energy_abs_diff_mev": 3.0,
        "max_position_delta_A": 0.010,
        "max_cell_delta_A": 0.010,
    },
    pes_balanced_relaxed_scale=1.5,
    pes_fast_thresholds={
        "energy_abs_diff_mev": 8.0,
        "max_position_delta_A": 0.020,
        "max_cell_delta_A": 0.020,
    },
    pes_fast_relaxed_scale=1.25,
)


_PROFILES = {
    TMDS_MONOLAYER_HEX.workflow_family: TMDS_MONOLAYER_HEX,
}


def resolve_family_convergence_profile(workflow_family: str) -> FamilyConvergenceProfile:
    key = str(workflow_family).strip()
    if key not in _PROFILES:
        raise KeyError(f"Unsupported workflow family for convergence tuning: {key}")
    return _PROFILES[key]
