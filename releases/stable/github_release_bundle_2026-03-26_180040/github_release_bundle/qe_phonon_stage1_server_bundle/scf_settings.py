#!/usr/bin/env python3
from __future__ import annotations

from copy import deepcopy


DEFAULT_PRESET_NAME = "phonon_balanced"


SCF_PRESETS = {
    "relax_strict": {
        "description": "High-precision vc-relax reference for the user input structure.",
        "settings": {
            "disk_io": "low",
            "verbosity": "low",
            "tprnfor": True,
            "tstress": True,
            "calculation": "vc-relax",
            "include_ions": True,
            "include_cell": True,
            "ion_dynamics": "bfgs",
            "cell_dynamics": "bfgs",
            "press_conv_thr": "0.1",
            "forc_conv_thr": "1.0d-5",
            "etot_conv_thr": "1.0d-10",
            "occupations": "smearing",
            "smearing": "gauss",
            "degauss": "1.0d-10",
            "ecutwfc": 120,
            "ecutrho": 1200,
            "electron_maxstep": 10000,
            "conv_thr": "1.0d-12",
            "mixing_mode": "plain",
            "mixing_beta": "0.3d0",
            "diagonalization": "david",
            "primitive_k_mesh": [12, 12, 1],
        },
    },
    "template80": {
        "description": "Close to the current QE template settings.",
        "settings": {
            "disk_io": "low",
            "verbosity": "low",
            "tprnfor": False,
            "tstress": False,
            "calculation": "scf",
            "include_ions": False,
            "include_cell": False,
            "forc_conv_thr": "1.0d-8",
            "etot_conv_thr": "1.0d-10",
            "occupations": "smearing",
            "smearing": "gauss",
            "degauss": "1.0d-10",
            "ecutwfc": 80,
            "ecutrho": 800,
            "electron_maxstep": 10000,
            "conv_thr": "1.0d-10",
            "mixing_mode": "plain",
            "mixing_beta": "0.3d0",
            "diagonalization": "david",
            "k_scale": 1.0,
            "primitive_k_mesh": [8, 8, 1],
        },
    },
    "ht_balanced": {
        "description": "Balanced high-throughput preset for the top-pair QE recheck.",
        "settings": {
            "disk_io": "low",
            "verbosity": "low",
            "tprnfor": False,
            "tstress": False,
            "calculation": "scf",
            "include_ions": False,
            "include_cell": False,
            "forc_conv_thr": "1.0d-7",
            "occupations": "smearing",
            "smearing": "gauss",
            "degauss": "1.0d-3",
            "ecutwfc": 80,
            "ecutrho": 800,
            "electron_maxstep": 400,
            "conv_thr": "1.0d-8",
            "mixing_mode": "plain",
            "mixing_beta": "0.4d0",
            "diagonalization": "david",
            "k_scale": 1.0,
            "primitive_k_mesh": [8, 8, 1],
        },
    },
    "phonon_strict": {
        "description": "Strict phonon-side SCF profile after vc-relax.",
        "settings": {
            "disk_io": "low",
            "verbosity": "low",
            "tprnfor": False,
            "tstress": False,
            "calculation": "scf",
            "include_ions": False,
            "include_cell": False,
            "forc_conv_thr": "1.0d-8",
            "etot_conv_thr": "1.0d-10",
            "occupations": "smearing",
            "smearing": "gauss",
            "degauss": "1.0d-10",
            "ecutwfc": 120,
            "ecutrho": 1200,
            "electron_maxstep": 10000,
            "conv_thr": "1.0d-12",
            "mixing_mode": "plain",
            "mixing_beta": "0.3d0",
            "diagonalization": "david",
            "primitive_k_mesh": [12, 12, 1],
        },
    },
    "pes_strict": {
        "description": "Strict PES-side SCF profile after vc-relax.",
        "settings": {
            "disk_io": "low",
            "verbosity": "low",
            "tprnfor": False,
            "tstress": False,
            "calculation": "scf",
            "include_ions": False,
            "include_cell": False,
            "forc_conv_thr": "1.0d-8",
            "etot_conv_thr": "1.0d-10",
            "occupations": "smearing",
            "smearing": "gauss",
            "degauss": "1.0d-10",
            "ecutwfc": 100,
            "ecutrho": 1000,
            "electron_maxstep": 10000,
            "conv_thr": "1.0d-11",
            "mixing_mode": "plain",
            "mixing_beta": "0.3d0",
            "diagonalization": "david",
            "primitive_k_mesh": [12, 12, 1],
        },
    },
    "ht_k4": {
        "description": "More aggressive preset with reduced effective k-mesh density.",
        "settings": {
            "disk_io": "low",
            "verbosity": "low",
            "tprnfor": False,
            "tstress": False,
            "calculation": "scf",
            "include_ions": False,
            "include_cell": False,
            "forc_conv_thr": "1.0d-7",
            "occupations": "smearing",
            "smearing": "gauss",
            "degauss": "3.0d-3",
            "ecutwfc": 65,
            "ecutrho": 650,
            "electron_maxstep": 300,
            "conv_thr": "1.0d-8",
            "mixing_mode": "plain",
            "mixing_beta": "0.45d0",
            "diagonalization": "david",
            "k_scale": 2.0 / 3.0,
            "primitive_k_mesh": [6, 6, 1],
        },
    },
}


def preset_names():
    return tuple(SCF_PRESETS.keys())


def resolve_scf_settings(preset: str | None = None, overrides: dict | None = None):
    preset_name = DEFAULT_PRESET_NAME if preset is None else str(preset)
    if preset_name not in SCF_PRESETS:
        raise KeyError(f"Unknown SCF preset: {preset_name}")

    resolved = deepcopy(SCF_PRESETS[preset_name]["settings"])
    if overrides:
        resolved.update(overrides)
    return resolved


def compact_settings_summary(settings: dict):
    parts = [
        f"ecut={settings['ecutwfc']}/{settings['ecutrho']}",
        f"conv={settings['conv_thr']}",
        f"beta={settings['mixing_beta']}",
        f"k={settings.get('primitive_k_mesh', 'template')}",
        f"k_scale={settings.get('k_scale', 1.0)}",
        f"occ={settings['occupations']}",
    ]
    if settings.get("occupations") == "smearing":
        parts.append(f"degauss={settings['degauss']}")
    if settings.get("calculation"):
        parts.append(f"calc={settings['calculation']}")
    return ", ".join(parts)


def scale_k_mesh(k_super: list[int], k_scale: float | None):
    if k_scale is None:
        return [int(v) for v in k_super]
    scaled = []
    for value in k_super:
        scaled_value = max(1, int(round(float(value) * float(k_scale))))
        scaled.append(scaled_value)
    return scaled
