#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path

try:
    from .scf_settings import (
        DEFAULT_PRESET_NAME,
        compact_settings_summary,
        normalize_static_preset_name,
        resolve_scf_settings,
    )
except ImportError:
    from scf_settings import (
        DEFAULT_PRESET_NAME,
        compact_settings_summary,
        normalize_static_preset_name,
        resolve_scf_settings,
    )


def load_stage1_profile_inputs(
    convergence_summary_path: str | Path | None = None,
    selected_profiles_path: str | Path | None = None,
) -> dict:
    convergence_summary = None
    resolved_selected_profiles_path = None if selected_profiles_path is None else Path(selected_profiles_path).expanduser().resolve()
    if convergence_summary_path is not None:
        summary_path = Path(convergence_summary_path).expanduser().resolve()
        if summary_path.exists():
            convergence_summary = json.loads(summary_path.read_text())
            if resolved_selected_profiles_path is None:
                candidate = convergence_summary.get("selected_profiles_json")
                if candidate:
                    resolved_selected_profiles_path = Path(candidate).expanduser().resolve()
    selected_profiles = None
    if resolved_selected_profiles_path is not None and resolved_selected_profiles_path.exists():
        selected_profiles = json.loads(resolved_selected_profiles_path.read_text())
    return {
        "convergence_summary": convergence_summary,
        "selected_profiles_path": resolved_selected_profiles_path,
        "selected_profiles": selected_profiles,
    }


def resolve_stage3_scf_profile(
    *,
    qe_scf_profile_level: str = "balanced",
    qe_static_preset: str | None = None,
    legacy_scf_preset: str | None = None,
    convergence_summary_path: str | Path | None = None,
    selected_profiles_path: str | Path | None = None,
) -> dict:
    profile_inputs = load_stage1_profile_inputs(
        convergence_summary_path=convergence_summary_path,
        selected_profiles_path=selected_profiles_path,
    )
    selected_profiles = profile_inputs["selected_profiles"]
    selected_profiles_json = profile_inputs["selected_profiles_path"]
    requested_static = DEFAULT_PRESET_NAME if qe_static_preset is None else str(qe_static_preset)

    if legacy_scf_preset:
        resolved_static, resolved_from_legacy_alias = normalize_static_preset_name(str(legacy_scf_preset))
        scf_settings = resolve_scf_settings(resolved_static)
        return {
            "scf_settings": scf_settings,
            "scf_settings_summary": compact_settings_summary(scf_settings),
            "scf_profile_source": "legacy_static",
            "scf_profile_branch": None,
            "scf_profile_level": None,
            "scf_static_preset": resolved_static,
            "selected_profiles_json": None if selected_profiles_json is None else str(selected_profiles_json),
            "resolved_from_legacy_alias": resolved_from_legacy_alias,
            "extra_k_mesh_scale_after_supercell_reduction": scf_settings.get("k_scale", 1.0),
        }

    branch = None if selected_profiles is None else selected_profiles.get("pes")
    if isinstance(branch, dict):
        selected = branch.get(qe_scf_profile_level)
        settings = None if not isinstance(selected, dict) else selected.get("settings")
        if isinstance(settings, dict):
            scf_settings = dict(settings)
            return {
                "scf_settings": scf_settings,
                "scf_settings_summary": compact_settings_summary(scf_settings),
                "scf_profile_source": "stage1_autotune",
                "scf_profile_branch": "pes",
                "scf_profile_level": qe_scf_profile_level,
                "scf_static_preset": None,
                "selected_profiles_json": None if selected_profiles_json is None else str(selected_profiles_json),
                "resolved_from_legacy_alias": False,
                "extra_k_mesh_scale_after_supercell_reduction": scf_settings.get("k_scale", 1.0),
            }

    resolved_static, resolved_from_legacy_alias = normalize_static_preset_name(requested_static)
    scf_settings = resolve_scf_settings(resolved_static)
    return {
        "scf_settings": scf_settings,
        "scf_settings_summary": compact_settings_summary(scf_settings),
        "scf_profile_source": "static_fallback",
        "scf_profile_branch": "pes",
        "scf_profile_level": qe_scf_profile_level,
        "scf_static_preset": resolved_static,
        "selected_profiles_json": None if selected_profiles_json is None else str(selected_profiles_json),
        "resolved_from_legacy_alias": resolved_from_legacy_alias,
        "extra_k_mesh_scale_after_supercell_reduction": scf_settings.get("k_scale", 1.0),
    }
