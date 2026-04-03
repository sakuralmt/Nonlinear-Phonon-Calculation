#!/usr/bin/env python3
from __future__ import annotations

from copy import deepcopy


DEFAULT_PRESET_NAME = "static_balanced"


SCF_PRESETS = {
    "baseline_strict": {
        "description": "Strict energy-only reference: high cutoff and tight SCF, but no force/stress printing.",
        "settings": {
            "disk_io": "low",
            "verbosity": "low",
            "tprnfor": False,
            "tstress": False,
            "include_ions": False,
            "include_cell": False,
            "forc_conv_thr": "1.0d-8",
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
            "k_scale": 1.0,
        },
    },
    "template80": {
        "description": "Energy-only variant close to the current template settings.",
        "settings": {
            "disk_io": "low",
            "verbosity": "low",
            "tprnfor": False,
            "tstress": False,
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
        },
    },
    "static_balanced": {
        "description": "Convergence-tested balanced PES preset.",
        "settings": {
            "disk_io": "low",
            "verbosity": "low",
            "tprnfor": False,
            "tstress": False,
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
            "conv_thr": "1.0d-10",
            "mixing_mode": "plain",
            "mixing_beta": "0.3d0",
            "diagonalization": "david",
            "k_scale": 1.0,
        },
    },
    "static_fast": {
        "description": "Static lighter PES preset for high-throughput recheck after supercell reduction.",
        "settings": {
            "disk_io": "low",
            "verbosity": "low",
            "tprnfor": False,
            "tstress": False,
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
            "k_scale": 0.5,
        },
    },
    "ht_balanced": {
        "description": "High-throughput balanced preset for energy grids: original k-mesh density, looser SCF.",
        "settings": {
            "disk_io": "low",
            "verbosity": "low",
            "tprnfor": False,
            "tstress": False,
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
        },
    },
    "ht_cut65": {
        "description": "Lower-cutoff energy-grid test while keeping the original k-mesh density.",
        "settings": {
            "disk_io": "low",
            "verbosity": "low",
            "tprnfor": False,
            "tstress": False,
            "include_ions": False,
            "include_cell": False,
            "forc_conv_thr": "1.0d-7",
            "occupations": "smearing",
            "smearing": "gauss",
            "degauss": "1.0d-3",
            "ecutwfc": 65,
            "ecutrho": 650,
            "electron_maxstep": 400,
            "conv_thr": "1.0d-8",
            "mixing_mode": "plain",
            "mixing_beta": "0.4d0",
            "diagonalization": "david",
            "k_scale": 1.0,
        },
    },
    "ht_k4": {
        "description": "Aggressive energy-grid preset: lower cutoff and reduced k-mesh.",
        "settings": {
            "disk_io": "low",
            "verbosity": "low",
            "tprnfor": False,
            "tstress": False,
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
        },
    },
    "semicon_fixed": {
        "description": "Optional semiconductor-only test without smearing.",
        "settings": {
            "disk_io": "low",
            "verbosity": "low",
            "tprnfor": False,
            "tstress": False,
            "include_ions": False,
            "include_cell": False,
            "forc_conv_thr": "1.0d-7",
            "occupations": "fixed",
            "ecutwfc": 80,
            "ecutrho": 800,
            "electron_maxstep": 400,
            "conv_thr": "1.0d-8",
            "mixing_mode": "plain",
            "mixing_beta": "0.4d0",
            "diagonalization": "david",
            "k_scale": 1.0,
        },
    },
}


BENCHMARK_PRESET_NAMES = [
    "baseline_strict",
    "template80",
    "ht_balanced",
    "ht_cut65",
    "ht_k4",
]

LEGACY_STATIC_PRESET_ALIASES = {
    "pes_balanced": "static_balanced",
    "pes_fast": "static_fast",
}


def preset_names():
    return tuple(list(SCF_PRESETS.keys()) + list(LEGACY_STATIC_PRESET_ALIASES.keys()))


def normalize_static_preset_name(preset: str | None):
    preset_name = DEFAULT_PRESET_NAME if preset is None else str(preset)
    canonical = LEGACY_STATIC_PRESET_ALIASES.get(preset_name, preset_name)
    return canonical, canonical != preset_name


def resolve_scf_settings(preset: str | None = None, overrides: dict | None = None):
    preset_name, _resolved_from_alias = normalize_static_preset_name(preset)
    if preset_name not in SCF_PRESETS:
        raise KeyError(f"Unknown SCF preset: {preset_name}")

    resolved = deepcopy(SCF_PRESETS[preset_name]["settings"])
    if overrides:
        resolved.update(overrides)
    return resolved


def preset_description(preset: str):
    if preset not in SCF_PRESETS:
        raise KeyError(f"Unknown SCF preset: {preset}")
    return str(SCF_PRESETS[preset]["description"])


def scale_k_mesh(k_super: list[int], k_scale: float | None):
    if k_scale is None:
        return [int(v) for v in k_super]
    scaled = []
    for value in k_super:
        scaled_value = max(1, int(round(float(value) * float(k_scale))))
        scaled.append(scaled_value)
    return scaled


def compact_settings_summary(settings: dict):
    parts = [
        f"ecut={settings['ecutwfc']}/{settings['ecutrho']}",
        f"conv={settings['conv_thr']}",
        f"beta={settings['mixing_beta']}",
        f"extra_k_scale={settings.get('k_scale', 1.0)}",
        f"occ={settings['occupations']}",
        f"f/stress={'on' if settings.get('tprnfor', True) or settings.get('tstress', True) else 'off'}",
    ]
    if settings.get("occupations") == "smearing":
        parts.append(f"degauss={settings['degauss']}")
    return ", ".join(parts)
