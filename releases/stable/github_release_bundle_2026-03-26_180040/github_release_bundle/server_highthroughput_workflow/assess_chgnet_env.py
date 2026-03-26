#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SCRIPT_DIR = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from mlff_modepair_workflow.core import (
    ModePairFrozenPhononBuilder,
    cpu_topology_summary,
    configure_torch_runtime,
    load_atoms_from_qe,
    load_pairs,
    make_calculator,
    resolve_chgnet_runtime_config,
    select_runtime_config_path,
)
from server_highthroughput_workflow.scheduler import (
    SLURM_CLUSTER_REPORT_NAME,
    SLURM_EXPORT_SCRIPT_NAME,
    SLURM_RUNTIME_CONFIG_NAME,
    probe_slurm_cluster,
    render_slurm_export_script,
    resolve_slurm_job_settings,
    slurm_available,
)

DEFAULT_STRUCTURE = ROOT / "nonlocal phonon" / "scf.inp"
DEFAULT_MODE_PAIRS = ROOT / "hex_qgamma_qpair_workflow" / "hex_qgamma_qpair_run" / "mode_pairs" / "selected_mode_pairs.json"
DEFAULT_OUTPUT_DIR = SCRIPT_DIR / "env_reports"


def parse_args():
    p = argparse.ArgumentParser(description="Assess CHGNet CPU inference environment on the current server.")
    p.add_argument("--structure", type=str, default=str(DEFAULT_STRUCTURE))
    p.add_argument("--mode-pairs-json", type=str, default=str(DEFAULT_MODE_PAIRS))
    p.add_argument("--output-dir", type=str, default=str(DEFAULT_OUTPUT_DIR))
    p.add_argument("--model", type=str, default="r2scan")
    p.add_argument("--device", type=str, default="cpu")
    p.add_argument("--grid-size", type=int, default=9)
    p.add_argument("--sample-pair-index", type=int, default=0)
    p.add_argument("--thread-candidates", nargs="*", type=int, default=[1, 4, 16, 38, 76])
    p.add_argument("--batch-size-candidates", nargs="*", type=int, default=[4, 8, 16, 32])
    return p.parse_args()


def run_cmd(cmd: list[str]):
    try:
        out = subprocess.run(cmd, capture_output=True, text=True, check=False, timeout=20)
        return {
            "cmd": cmd,
            "returncode": out.returncode,
            "stdout": out.stdout.strip(),
            "stderr": out.stderr.strip(),
        }
    except Exception as exc:
        return {"cmd": cmd, "error": repr(exc)}


def read_node_state():
    try:
        out = subprocess.run(["scontrol", "show", "node", "server01"], capture_output=True, text=True, check=True)
    except Exception:
        return None
    text = out.stdout
    state = {}
    for key in ["CPUAlloc", "CPUTot", "CPULoad", "RealMemory", "AllocMem", "FreeMem"]:
        marker = f"{key}="
        if marker in text:
            state[key] = text.split(marker, 1)[1].split()[0]
    return state


def build_atoms_list(structure: Path, mode_pairs_json: Path, pair_index: int, grid_size: int):
    pairs = load_pairs(mode_pairs_json)
    pair = pairs[pair_index]
    prim_atoms = load_atoms_from_qe(structure)
    builder = ModePairFrozenPhononBuilder(pair, prim_atoms)
    values = [float(x) for x in __import__("numpy").linspace(-2.0, 2.0, int(grid_size))]
    _, atoms_list = builder.build_atoms_list(__import__("numpy").array(values), __import__("numpy").array(values))
    return pair["pair_code"], builder.n_super, builder.nat_super, atoms_list


def benchmark_current_loop(calc, atoms_list):
    t0 = time.time()
    energies = []
    for atoms in atoms_list:
        atoms.calc = calc
        energies.append(float(atoms.get_potential_energy()))
    return time.time() - t0, energies


def benchmark_batch(calc, atoms_list, batch_size: int):
    if not hasattr(calc, "predict_energies"):
        return None, None
    t0 = time.time()
    energies = calc.predict_energies(atoms_list, batch_size=batch_size)
    return time.time() - t0, [float(x) for x in energies]


def recommend_config(thread_results: list[dict], node_state: dict | None):
    portable_runtime, portable_meta = resolve_chgnet_runtime_config(
        config_path=None,
        cpu_summary=cpu_topology_summary(),
    )
    best_batch = None
    for row in thread_results:
        if row.get("best_batch_total_sec") is None:
            continue
        if best_batch is None or row["best_batch_total_sec"] < best_batch["best_batch_total_sec"]:
            best_batch = row

    if best_batch is not None and best_batch["threads"] <= 16:
        portable_runtime["torch_threads"] = int(best_batch["threads"])
    if best_batch is not None and best_batch.get("best_batch_size") is not None:
        portable_runtime["batch_size"] = int(best_batch["best_batch_size"])
    return portable_runtime, portable_meta


def portable_profile_candidates(repo_root: Path):
    cpu_summary = cpu_topology_summary()
    candidates = {}
    for profile_name in ["default", "small", "medium", "large"]:
        path = select_runtime_config_path(repo_root, profile_name=profile_name)
        runtime, meta = resolve_chgnet_runtime_config(path, cpu_summary=cpu_summary)
        candidates[profile_name] = {
            "config_path": None if path is None else str(path),
            "runtime": runtime,
            "meta": meta,
        }
    return candidates


def recommend_portable_profile(recommended_runtime: dict, profile_candidates: dict):
    best_name = None
    best_payload = None
    best_score = None
    for name, payload in profile_candidates.items():
        runtime = payload["runtime"]
        score = (
            abs(int(runtime["num_workers"]) - int(recommended_runtime["num_workers"])) * 10
            + abs(int(runtime["torch_threads"]) - int(recommended_runtime["torch_threads"])) * 4
            + abs(int(runtime["batch_size"]) - int(recommended_runtime["batch_size"])) * 1
        )
        if best_score is None or score < best_score:
            best_name = name
            best_payload = payload
            best_score = score
    return {
        "profile_name": best_name,
        "score": best_score,
        "config_path": None if best_payload is None else best_payload["config_path"],
        "runtime": None if best_payload is None else best_payload["runtime"],
    }


def build_markdown(summary: dict):
    lines = []
    lines.append("# CHGNet Server Assessment")
    lines.append("")
    lines.append(f"- pair: `{summary['sample_pair_code']}`")
    lines.append(f"- supercell n: `{summary['n_super']}`")
    lines.append(f"- supercell atoms: `{summary['nat_super']}`")
    lines.append(f"- grid structures: `{summary['n_structures']}`")
    if summary["node_state"] is not None:
        lines.append(f"- Slurm CPUs: `{summary['node_state'].get('CPUAlloc', '?')}/{summary['node_state'].get('CPUTot', '?')}` allocated")
    lines.append("")
    lines.append("## Thread Scan")
    for row in summary["thread_results"]:
        line = f"- threads `{row['threads']}`: current loop `{row['current_loop_sec']:.3f}s`"
        if row["best_batch_total_sec"] is not None:
            line += f", best batch `{row['best_batch_size']}` => `{row['best_batch_total_sec']:.3f}s`, speedup `{row['speedup_vs_current']:.2f}x`"
        lines.append(line)
    lines.append("")
    lines.append("## Recommended Config")
    for key, value in summary["recommended_config"].items():
        lines.append(f"- `{key}`: `{value}`")
    lines.append("")
    lines.append("## Recommended Portable Profile")
    lines.append(f"- profile: `{summary['recommended_portable_profile']['profile_name']}`")
    lines.append(f"- config: `{summary['recommended_portable_profile']['config_path']}`")
    lines.append("")
    if summary.get("slurm_cluster"):
        lines.append("## Slurm Cluster")
        cluster = summary["slurm_cluster"]
        default_partition = cluster.get("default_partition") or "(none)"
        lines.append(f"- default partition: `{default_partition}`")
        for part in cluster.get("partitions", []):
            rows = ",".join(part.get("row_states", [])) or "(none)"
            lines.append(
                f"- `{part['name']}`: usable=`{part['usable']}`, max=`{part.get('max_time_raw')}`, default=`{part.get('default_time_raw')}`, states=`{rows}`"
            )
        lines.append("")
        lines.append("## Recommended Slurm Defaults")
        for job_kind, payload in summary.get("slurm_job_settings", {}).items():
            lines.append(f"- `{job_kind}`: partition=`{payload['partition']}`, walltime=`{payload['walltime']}`, qos=`{payload.get('qos')}`")
            for note in payload.get("notes", []):
                lines.append(f"  - {note}")
        lines.append("")
    return "\n".join(lines)


def collect_slurm_settings():
    if not slurm_available():
        return None, None, None

    from server_highthroughput_workflow.qe_relax_preflight import RELAX_PARTITION, RELAX_QOS, RELAX_TIME
    from server_highthroughput_workflow.run_server_pipeline import (
        MLFF_PARTITION,
        MLFF_WALLTIME,
        QE_PARTITION,
        QE_QOS,
        QE_WALLTIME,
    )

    cluster = probe_slurm_cluster()
    job_settings = {
        "qe_relax": resolve_slurm_job_settings(
            "qe_relax",
            requested_partition=RELAX_PARTITION,
            requested_walltime=RELAX_TIME,
            requested_qos=RELAX_QOS,
        ),
        "mlff_screening": resolve_slurm_job_settings(
            "mlff_screening",
            requested_partition=MLFF_PARTITION,
            requested_walltime=MLFF_WALLTIME,
            requested_qos=None,
        ),
        "stage3_continuation": resolve_slurm_job_settings(
            "stage3_continuation",
            requested_partition=MLFF_PARTITION,
            requested_walltime=QE_WALLTIME,
            requested_qos=None,
        ),
        "qe_recheck": resolve_slurm_job_settings(
            "qe_recheck",
            requested_partition=QE_PARTITION,
            requested_walltime=QE_WALLTIME,
            requested_qos=QE_QOS,
        ),
    }
    runtime_config = {
        "kind": "slurm_runtime_config",
        "generated_by": "assess_chgnet_env.py",
        "cluster_default_partition": cluster.get("default_partition"),
        "jobs": {
            job_kind: {
                "partition": payload["partition"],
                "walltime": payload["walltime"],
                "qos": payload.get("qos"),
                "notes": payload.get("notes", []),
                "requested_partition": payload.get("requested_partition"),
                "requested_walltime": payload.get("requested_walltime"),
                "requested_qos": payload.get("requested_qos"),
            }
            for job_kind, payload in job_settings.items()
        },
    }
    return cluster, job_settings, runtime_config


def main():
    args = parse_args()
    out_dir = Path(args.output_dir).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    structure = Path(args.structure).expanduser().resolve()
    mode_pairs_json = Path(args.mode_pairs_json).expanduser().resolve()
    pair_code, n_super, nat_super, atoms_list = build_atoms_list(structure, mode_pairs_json, int(args.sample_pair_index), int(args.grid_size))

    thread_results = []
    for threads in [int(x) for x in args.thread_candidates]:
        configure_torch_runtime(torch_threads=threads, interop_threads=1)
        calc, _ = make_calculator("chgnet", device=args.device, model=args.model)
        current_loop_sec, current_energies = benchmark_current_loop(calc, [atoms.copy() for atoms in atoms_list])

        best_batch_total_sec = None
        best_batch_size = None
        for batch_size in [int(x) for x in args.batch_size_candidates]:
            batch_total_sec, batch_energies = benchmark_batch(calc, [atoms.copy() for atoms in atoms_list], batch_size=batch_size)
            if batch_total_sec is None:
                continue
            max_diff = max(abs(a - b) for a, b in zip(current_energies, batch_energies))
            if best_batch_total_sec is None or batch_total_sec < best_batch_total_sec:
                best_batch_total_sec = batch_total_sec
                best_batch_size = batch_size
                best_max_diff = max_diff

        thread_results.append(
            {
                "threads": threads,
                "current_loop_sec": current_loop_sec,
                "best_batch_total_sec": best_batch_total_sec,
                "best_batch_size": best_batch_size,
                "speedup_vs_current": None if best_batch_total_sec is None else current_loop_sec / best_batch_total_sec,
                "max_energy_diff_ev": None if best_batch_total_sec is None else best_max_diff,
            }
        )

    summary = {
        "structure": str(structure),
        "mode_pairs_json": str(mode_pairs_json),
        "sample_pair_code": pair_code,
        "n_super": n_super,
        "nat_super": nat_super,
        "n_structures": len(atoms_list),
        "commands": {
            "uname": run_cmd(["uname", "-a"]),
            "lscpu": run_cmd(["lscpu"]),
            "free": run_cmd(["free", "-h"]),
            "python": run_cmd(["python3", "--version"]),
        },
        "node_state": read_node_state(),
        "thread_results": thread_results,
    }
    recommended_config, portable_meta = recommend_config(thread_results, summary["node_state"])
    summary["recommended_config"] = recommended_config
    summary["portable_runtime_meta"] = portable_meta
    summary["portable_profile_candidates"] = portable_profile_candidates(ROOT)
    summary["recommended_portable_profile"] = recommend_portable_profile(
        recommended_config,
        summary["portable_profile_candidates"],
    )
    slurm_cluster, slurm_job_settings, slurm_runtime_config = collect_slurm_settings()
    if slurm_cluster is not None:
        summary["slurm_cluster"] = slurm_cluster
        summary["slurm_job_settings"] = slurm_job_settings

    runtime_config = {
        "kind": "chgnet_runtime_config",
        "mode": "fixed",
        "profile_name": "assessed_cpu_runtime",
        "generated_by": "assess_chgnet_env.py",
        "cpu_summary": cpu_topology_summary(),
        **recommended_config,
    }

    json_path = out_dir / "chgnet_env_assessment.json"
    md_path = out_dir / "chgnet_env_assessment.md"
    runtime_path = out_dir / "chgnet_runtime_config.json"
    portable_profile_path = out_dir / "recommended_portable_profile.json"
    slurm_cluster_path = out_dir / SLURM_CLUSTER_REPORT_NAME
    slurm_runtime_path = out_dir / SLURM_RUNTIME_CONFIG_NAME
    slurm_script_path = out_dir / SLURM_EXPORT_SCRIPT_NAME
    json_path.write_text(json.dumps(summary, indent=2))
    md_path.write_text(build_markdown(summary))
    runtime_path.write_text(json.dumps(runtime_config, indent=2))
    portable_profile_path.write_text(json.dumps(summary["recommended_portable_profile"], indent=2))
    print(f"saved: {json_path}")
    print(f"saved: {md_path}")
    print(f"saved: {runtime_path}")
    print(f"saved: {portable_profile_path}")
    if slurm_cluster is not None:
        slurm_cluster_path.write_text(json.dumps(slurm_cluster, indent=2))
        slurm_runtime_path.write_text(json.dumps(slurm_runtime_config, indent=2))
        slurm_script_path.write_text(render_slurm_export_script(slurm_job_settings))
        print(f"saved: {slurm_cluster_path}")
        print(f"saved: {slurm_runtime_path}")
        print(f"saved: {slurm_script_path}")


if __name__ == "__main__":
    main()
