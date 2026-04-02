#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import shutil
from pathlib import Path

import numpy as np

from common import build_pair_structure_generator, dump_json, write_scf_input

try:
    from .scf_profile_resolver import resolve_stage3_scf_profile
    from .scf_settings import DEFAULT_PRESET_NAME, preset_names, scale_k_mesh
except ImportError:
    from scf_profile_resolver import resolve_stage3_scf_profile
    from scf_settings import DEFAULT_PRESET_NAME, preset_names, scale_k_mesh


ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUT_DIR = Path(__file__).resolve().parent / "runs" / "consensus_top5_qe"


A1_VALS = np.linspace(-2.0, 2.0, 9)
A2_VALS = np.linspace(-2.0, 2.0, 9)


def _job_name(i1: int, i2: int):
    return f"grid_{i1:02d}_{i2:02d}"


def _slurm_job_label(job_prefix: str, rank: int, i1: int, i2: int):
    return f"{job_prefix}_r{rank:02d}_{i1:02d}{i2:02d}"


def _write_submit_script(job_dir: Path, args, rank: int, i1: int, i2: int):
    lines = [
        "#!/bin/bash",
        f"#SBATCH --job-name={_slurm_job_label(args.slurm_job_prefix, rank, i1, i2)}",
        "#SBATCH --nodes=1",
        f"#SBATCH --ntasks={args.ntasks}",
        f"#SBATCH --time={args.walltime}",
        f"#SBATCH --partition={args.partition}",
        f"#SBATCH --chdir={job_dir}",
        "#SBATCH --output=slurm-%j.out",
        "#SBATCH --error=slurm-%j.err",
    ]
    if args.qos:
        lines.append(f"#SBATCH --qos={args.qos}")

    lines.extend(
        [
            "",
            f"cd \"{job_dir}\"",
            f"export OMP_NUM_THREADS={args.omp_num_threads}",
            "mkdir -p tmp",
        ]
    )
    lines.extend(args.env_init_line)
    lines.extend(["", args.launcher_command.format(ntasks=args.ntasks), ""])

    submit_path = job_dir / "submit.sh"
    submit_path.write_text("\n".join(lines))
    submit_path.chmod(0o755)


def parse_args():
    p = argparse.ArgumentParser(description="Prepare QE two-mode inputs for top ranked consensus pairs")
    p.add_argument("--consensus-json", type=str, required=True)
    p.add_argument("--mode-pairs-json", type=str, required=True)
    p.add_argument("--scf-template", type=str, required=True)
    p.add_argument("--pseudo-dir", type=str, required=True)
    p.add_argument("--output-dir", type=str, default=str(DEFAULT_OUT_DIR))
    p.add_argument("--top-n", type=int, default=5)
    p.add_argument("--ntasks", type=int, default=24)
    p.add_argument("--partition", type=str, default="debug")
    p.add_argument("--qos", type=str, default=None)
    p.add_argument("--walltime", type=str, default="72:00:00")
    p.add_argument("--convergence-summary", type=str, default=None)
    p.add_argument("--selected-profiles-json", type=str, default=None)
    p.add_argument("--qe-scf-profile-level", type=str, default="balanced", choices=["balanced", "fast"])
    p.add_argument("--qe-static-preset", type=str, default=DEFAULT_PRESET_NAME, choices=preset_names())
    p.add_argument("--scf-preset", type=str, default=None, choices=preset_names())
    p.add_argument("--slurm-job-prefix", type=str, default="qe")
    p.add_argument("--omp-num-threads", type=int, default=1)
    p.add_argument("--launcher-command", type=str, default="mpirun -np {ntasks} pw.x < scf.inp > scf.out")
    p.add_argument("--env-init-line", action="append", default=[])
    return p.parse_args()


def main():
    args = parse_args()
    consensus_json = Path(args.consensus_json).expanduser().resolve()
    mode_pairs_json = Path(args.mode_pairs_json).expanduser().resolve()
    scf_template = Path(args.scf_template).expanduser().resolve()
    pseudo_dir = Path(args.pseudo_dir).expanduser().resolve()
    output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    scf_profile = resolve_stage3_scf_profile(
        qe_scf_profile_level=args.qe_scf_profile_level,
        qe_static_preset=args.qe_static_preset,
        legacy_scf_preset=args.scf_preset,
        convergence_summary_path=args.convergence_summary,
        selected_profiles_path=args.selected_profiles_json,
    )
    scf_settings = scf_profile["scf_settings"]
    compatibility_scf_preset = (
        scf_profile["scf_static_preset"]
        if scf_profile["scf_static_preset"] is not None
        else f"pes.{scf_profile['scf_profile_level']}"
    )

    consensus_rows = json.loads(consensus_json.read_text())["rows"][: args.top_n]
    pair_db = {item["pair_code"]: item for item in json.loads(mode_pairs_json.read_text())["pairs"]}

    selected_pairs = []
    for row in consensus_rows:
        pair_code = row["pair_code"]
        pair_record = pair_db[pair_code]
        selected_pairs.append(
            {
                "rank": len(selected_pairs) + 1,
                "consensus": row,
                "pair": pair_record,
            }
        )

    with (output_dir / "selected_top_pairs.json").open("w") as f:
        json.dump(selected_pairs, f, indent=2)

    with (output_dir / "selected_top_pairs.csv").open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["rank", "pair_code", "coupling_type", "point_label", "gamma_mode_code", "target_mode_code", "phi122_mean_mev"])
        for item in selected_pairs:
            row = item["consensus"]
            writer.writerow(
                [
                    item["rank"],
                    row["pair_code"],
                    row["coupling_type"],
                    row["point_label"],
                    row["gamma_mode_code"],
                    row["target_mode_code"],
                    row["phi122_mean_mev"],
                ]
            )

    all_job_dirs = []
    for item in selected_pairs:
        pair = item["pair"]
        pair_code = pair["pair_code"]
        pair_dir = output_dir / pair_code
        pair_dir.mkdir(parents=True, exist_ok=True)

        builder = build_pair_structure_generator(pair, scf_template)
        rows = []

        for i1, a1 in enumerate(A1_VALS):
            for i2, a2 in enumerate(A2_VALS):
                frac_pos = builder["fractional_positions"](float(a1), float(a2))
                job_name = _job_name(i1, i2)
                job_dir = pair_dir / job_name
                job_dir.mkdir(parents=True, exist_ok=True)

                write_scf_input(
                    out_file=job_dir / "scf.inp",
                    base_cell=builder["base_cell"],
                    symbols=builder["symbols"],
                    frac_positions=frac_pos,
                    constraints=builder["constraints_prim"],
                    k_super=scale_k_mesh(builder["k_super"], scf_settings.get("k_scale")),
                    atomic_species_entries=builder["atomic_species_entries"],
                    scf_settings=scf_settings,
                )

                for entry in builder["atomic_species_entries"]:
                    pseudo = entry["pseudo"]
                    src = pseudo_dir / pseudo
                    dst = job_dir / pseudo
                    if not src.exists():
                        raise FileNotFoundError(f"Missing pseudopotential: {src}")
                    shutil.copy2(src, dst)

                _write_submit_script(job_dir, args, item["rank"], i1, i2)
                rows.append([f"{a1:.10f}", f"{a2:.10f}", str(i1), str(i2), job_name])
                all_job_dirs.append(str(job_dir))

        dump_json(
            pair_dir / "pair_meta.json",
            {
                "pair_code": pair_code,
                "rank": item["rank"],
                "consensus": item["consensus"],
                "n_super": builder["n_super"],
                "n_cells": builder["n_cells"],
                "a1_vals": A1_VALS.tolist(),
                "a2_vals": A2_VALS.tolist(),
                "scf_preset": compatibility_scf_preset,
                "legacy_scf_preset": args.scf_preset,
                "qe_scf_profile_level": args.qe_scf_profile_level,
                "qe_static_preset": args.qe_static_preset,
                "scf_profile_source": scf_profile["scf_profile_source"],
                "scf_profile_branch": scf_profile["scf_profile_branch"],
                "scf_profile_level": scf_profile["scf_profile_level"],
                "scf_static_preset": scf_profile["scf_static_preset"],
                "selected_profiles_json": scf_profile["selected_profiles_json"],
                "resolved_from_legacy_alias": scf_profile["resolved_from_legacy_alias"],
                "extra_k_mesh_scale_after_supercell_reduction": scf_profile["extra_k_mesh_scale_after_supercell_reduction"],
                "scf_settings": scf_settings,
                "scf_settings_summary": scf_profile["scf_settings_summary"],
                "source_scf_template": str(scf_template),
                "source_mode_pairs_json": str(mode_pairs_json),
                "launcher_command": args.launcher_command,
                "env_init_line": args.env_init_line,
            },
        )

        with (pair_dir / "amplitude_grid.csv").open("w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["a1", "a2", "a1_index", "a2_index", "job_name"])
            writer.writerows(rows)

    dump_json(
        output_dir / "run_manifest.json",
        {
            "kind": "qe_top_pairs_run",
            "top_n": args.top_n,
            "consensus_json": str(consensus_json),
            "mode_pairs_json": str(mode_pairs_json),
            "scf_template": str(scf_template),
            "scf_preset": compatibility_scf_preset,
            "legacy_scf_preset": args.scf_preset,
            "qe_scf_profile_level": args.qe_scf_profile_level,
            "qe_static_preset": args.qe_static_preset,
            "scf_profile_source": scf_profile["scf_profile_source"],
            "scf_profile_branch": scf_profile["scf_profile_branch"],
            "scf_profile_level": scf_profile["scf_profile_level"],
            "scf_static_preset": scf_profile["scf_static_preset"],
            "selected_profiles_json": scf_profile["selected_profiles_json"],
            "resolved_from_legacy_alias": scf_profile["resolved_from_legacy_alias"],
            "scf_settings": scf_settings,
            "scf_settings_summary": scf_profile["scf_settings_summary"],
            "extra_k_mesh_scale_after_supercell_reduction": scf_profile["extra_k_mesh_scale_after_supercell_reduction"],
            "pseudo_dir": str(pseudo_dir),
            "pair_dirs": [str((output_dir / item["pair"]["pair_code"]).resolve()) for item in selected_pairs],
            "job_count": len(all_job_dirs),
        },
    )

    print(f"prepared top pairs: {len(selected_pairs)}")
    print(f"job count: {len(all_job_dirs)}")
    print(
        "scf selection: "
        + f"source={scf_profile['scf_profile_source']}, "
        + f"profile_level={scf_profile['scf_profile_level']}, "
        + f"static_preset={scf_profile['scf_static_preset']} "
        + f"({scf_profile['scf_settings_summary']})"
    )
    print(f"saved: {output_dir / 'run_manifest.json'}")


if __name__ == "__main__":
    main()
