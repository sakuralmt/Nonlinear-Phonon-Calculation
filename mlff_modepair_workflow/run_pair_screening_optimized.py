#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import multiprocessing as mp
import re
import time
from pathlib import Path

import numpy as np

from core import (
    analyze_pair_grid,
    choose_device,
    compare_golden_metrics,
    compare_mode_frequency_metrics,
    compare_with_reference_grid,
    configure_torch_runtime,
    dump_json,
    evaluate_pair_grid,
    find_golden_pair,
    load_atoms_from_qe,
    load_golden_reference,
    load_mode_pair_reference,
    load_pairs,
    make_calculator,
    resolve_chgnet_runtime_config,
    save_pair_plot,
    select_runtime_config_path,
    set_process_cpu_affinity,
    suggest_worker_cpu_sets,
)


ROOT = Path(__file__).resolve().parent.parent
SCRIPT_DIR = Path(__file__).resolve().parent

DEFAULT_OUT_ROOT = SCRIPT_DIR / "runs"

A_MIN = -2.0
A_MAX = 2.0

_WORKER_CALC = None
_WORKER_PRIM_ATOMS = None


def parse_args():
    p = argparse.ArgumentParser(description="Optimized CHGNet screening over selected Gamma-q mode pairs.")
    p.add_argument("--backend", type=str, default="chgnet")
    p.add_argument("--device", type=str, default="auto")
    p.add_argument("--model", type=str, default="r2scan")
    p.add_argument("--run-tag", type=str, default=None, help="Optional output subdirectory tag")
    p.add_argument("--mode-pairs-json", type=str, required=True)
    p.add_argument("--structure", type=str, required=True)
    p.add_argument("--golden-fit-json", type=str, default=None)
    p.add_argument("--golden-ref-grid", type=str, default=None)
    p.add_argument("--output-root", type=str, default=str(DEFAULT_OUT_ROOT))
    p.add_argument("--limit", type=int, default=None)
    p.add_argument("--fit-window", type=float, default=1.0)
    p.add_argument("--runtime-config", type=str, default=None, help="Optional runtime config JSON. If omitted, auto-detect or use the default portable CPU config.")
    p.add_argument("--runtime-profile", type=str, default=None, choices=["default", "small", "medium", "large"], help="Optional portable CPU profile name.")
    p.add_argument("--batch-size", type=int, default=None)
    p.add_argument("--num-workers", type=int, default=None)
    p.add_argument("--torch-threads", type=int, default=None)
    p.add_argument("--interop-threads", type=int, default=None)
    p.add_argument("--chunksize", type=int, default=None)
    p.add_argument("--maxtasksperchild", type=int, default=None)
    p.add_argument("--mp-start", type=str, default="spawn", choices=["spawn", "fork", "forkserver"])
    p.add_argument("--worker-affinity", type=str, default=None, choices=["off", "auto"])
    p.add_argument("--strategy", type=str, default=None, choices=["full", "coarse_to_fine"])
    p.add_argument("--coarse-grid-size", type=int, default=None)
    p.add_argument("--full-grid-size", type=int, default=None)
    p.add_argument("--refine-top-k", type=int, default=None)
    return p.parse_args()


def grid_values(size: int):
    size = max(3, int(size))
    return np.linspace(A_MIN, A_MAX, size)


def pair_output_dir(root: Path, stage_name: str, pair_code: str):
    return root / stage_name / pair_code


def worker_slot():
    match = re.search(r"(\d+)$", mp.current_process().name)
    return 0 if match is None else max(0, int(match.group(1)) - 1)


def init_worker(
    backend: str,
    device: str,
    model: str | None,
    structure_path: str,
    torch_threads: int,
    interop_threads: int,
    cpu_sets: list[list[int]] | None,
):
    global _WORKER_CALC, _WORKER_PRIM_ATOMS
    slot = worker_slot()
    if cpu_sets:
        set_process_cpu_affinity(cpu_sets[slot % len(cpu_sets)])
    configure_torch_runtime(torch_threads=torch_threads, interop_threads=interop_threads)
    _WORKER_CALC, _ = make_calculator(backend=backend, device=device, model=model)
    _WORKER_PRIM_ATOMS = load_atoms_from_qe(Path(structure_path))


def evaluate_task(payload: tuple[int, dict, list[float], list[float], int, float]):
    if _WORKER_CALC is None or _WORKER_PRIM_ATOMS is None:
        raise RuntimeError("Worker is not initialized.")

    idx, pair, a1_list, a2_list, batch_size, fit_window = payload
    a1_vals = np.array(a1_list, dtype=float)
    a2_vals = np.array(a2_list, dtype=float)
    t0 = time.time()
    e_grid, builder = evaluate_pair_grid(
        pair,
        structure_path=None,
        calc=_WORKER_CALC,
        a1_vals=a1_vals,
        a2_vals=a2_vals,
        prim_atoms=_WORKER_PRIM_ATOMS,
        batch_size=batch_size,
    )
    analysis = analyze_pair_grid(pair, e_grid, a1_vals, a2_vals, fit_window=fit_window)
    return {
        "idx": idx,
        "pair": pair,
        "e_grid": e_grid,
        "analysis": analysis,
        "builder_meta": builder.metadata(),
        "elapsed_sec": time.time() - t0,
    }


def build_backend_meta(args):
    return {
        "backend": args.backend.lower(),
        "device": choose_device(args.device),
        "model": args.model,
    }


def build_summary(result, structure: Path, backend_meta: dict, golden_pair_code: str | None, mode_pair_reference: dict | None, golden_reference: dict | None, golden_ref_grid: Path | None, stage_name: str):
    pair = result["pair"]
    e_grid = result["e_grid"]
    analysis = result["analysis"]
    mode_pair_compare = None
    golden_compare = None
    ref_compare = None
    if golden_pair_code is not None and golden_reference is not None and mode_pair_reference is not None and golden_ref_grid is not None and pair["pair_code"] == golden_pair_code:
        mode_pair_compare = compare_mode_frequency_metrics(analysis, mode_pair_reference)
        golden_compare = compare_golden_metrics(analysis, golden_reference)
        ref_compare = compare_with_reference_grid(golden_ref_grid, e_grid)

    return {
        "pair_code": pair["pair_code"],
        "structure": str(structure),
        "backend": backend_meta,
        "stage": stage_name,
        "elapsed_sec": result["elapsed_sec"],
        "builder": result["builder_meta"],
        "analysis": analysis,
        "mode_pair_reference": mode_pair_reference if golden_pair_code is not None and pair["pair_code"] == golden_pair_code else None,
        "mode_pair_frequency_compare": mode_pair_compare,
        "golden_pes_reference": golden_reference if golden_pair_code is not None and pair["pair_code"] == golden_pair_code else None,
        "golden_pes_compare": golden_compare,
        "golden_compare": golden_compare,
        "reference_grid_compare": ref_compare,
    }


def ranking_row_from_result(result, stage_name: str):
    pair = result["pair"]
    analysis = result["analysis"]
    gamma_axis = analysis["axis_checks"]["mode1_axis_fit"]["freq"]
    target_axis = analysis["axis_checks"]["mode2_axis_fit"]["freq"]
    return {
        "pair_code": pair["pair_code"],
        "coupling_type": pair["coupling_type"],
        "point_label": pair["target_mode"]["point_label"],
        "q_frac": pair["target_mode"]["q_frac"],
        "n_super": result["builder_meta"]["n_super"],
        "gamma_mode_code": pair["gamma_mode"]["mode_code"],
        "gamma_mode_number": pair["gamma_mode"]["mode_number_one_based"],
        "gamma_freq_ref_thz": pair["gamma_mode"]["freq_thz"],
        "gamma_freq_fit_thz": gamma_axis.get("thz"),
        "target_mode_code": pair["target_mode"]["mode_code"],
        "target_mode_number": pair["target_mode"]["mode_number_one_based"],
        "target_freq_ref_thz": pair["target_mode"]["freq_thz"],
        "target_freq_fit_thz": target_axis.get("thz"),
        "phi122_mev": analysis["physics"]["phi_122_mev_per_A3amu32"],
        "phi112_mev": analysis["physics"]["phi_112_mev_per_A3amu32"],
        "r2": analysis["r2"],
        "rmse_ev_supercell": analysis["rmse_ev_supercell"],
        "elapsed_sec": result["elapsed_sec"],
        "source_stage": stage_name,
    }


def write_pair_outputs(stage_root: Path, result, summary, a1_vals: np.ndarray, a2_vals: np.ndarray):
    pair_dir = stage_root / result["pair"]["pair_code"]
    pair_dir.mkdir(parents=True, exist_ok=True)
    np.savetxt(pair_dir / "energy_grid_eV.dat", result["e_grid"], fmt="%.10f")
    dump_json(pair_dir / "summary.json", summary)
    save_pair_plot(pair_dir / "pes_map.png", result["e_grid"], a1_vals, a2_vals, title=result["pair"]["pair_code"])


def run_stage(stage_name: str, pair_records: list[dict], a1_vals: np.ndarray, a2_vals: np.ndarray, args, output_root: Path, golden_pair_code: str, mode_pair_reference: dict, golden_reference: dict, golden_ref_grid: Path):
    stage_root = output_root / stage_name
    stage_root.mkdir(parents=True, exist_ok=True)
    backend_meta = build_backend_meta(args)
    results = {}
    total = len(pair_records)

    if int(args.num_workers) <= 1:
        if args.worker_affinity == "auto":
            cpu_sets = suggest_worker_cpu_sets(1, int(args.torch_threads))
            if cpu_sets:
                set_process_cpu_affinity(cpu_sets[0])
        configure_torch_runtime(torch_threads=int(args.torch_threads), interop_threads=int(args.interop_threads))
        calc, _ = make_calculator(backend=args.backend, device=args.device, model=args.model)
        prim_atoms = load_atoms_from_qe(Path(args.structure))
        for idx, pair in enumerate(pair_records, start=1):
            t0 = time.time()
            e_grid, builder = evaluate_pair_grid(
                pair,
                structure_path=None,
                calc=calc,
                a1_vals=a1_vals,
                a2_vals=a2_vals,
                prim_atoms=prim_atoms,
                batch_size=int(args.batch_size),
            )
            result = {
                "idx": idx,
                "pair": pair,
                "e_grid": e_grid,
                "analysis": analyze_pair_grid(pair, e_grid, a1_vals, a2_vals, fit_window=float(args.fit_window)),
                "builder_meta": builder.metadata(),
                "elapsed_sec": time.time() - t0,
            }
            summary = build_summary(result, Path(args.structure), backend_meta, golden_pair_code, mode_pair_reference, golden_reference, golden_ref_grid, stage_name)
            write_pair_outputs(stage_root, result, summary, a1_vals, a2_vals)
            row = ranking_row_from_result(result, stage_name)
            results[pair["pair_code"]] = {
                "pair": pair,
                "summary": summary,
                "ranking_row": row,
            }
            print(f"[{stage_name}] [{idx}/{total}] {pair['pair_code']} done in {result['elapsed_sec']:.3f}s")
        return results, backend_meta

    cpu_sets = None
    if args.worker_affinity == "auto":
        cpu_sets = suggest_worker_cpu_sets(int(args.num_workers), int(args.torch_threads))

    ctx = mp.get_context(args.mp_start)
    tasks = [
        (idx, pair, a1_vals.tolist(), a2_vals.tolist(), int(args.batch_size), float(args.fit_window))
        for idx, pair in enumerate(pair_records, start=1)
    ]
    with ctx.Pool(
        processes=int(args.num_workers),
        initializer=init_worker,
        initargs=(
            args.backend,
            args.device,
            args.model,
            args.structure,
            int(args.torch_threads),
            int(args.interop_threads),
            cpu_sets,
        ),
        maxtasksperchild=int(args.maxtasksperchild),
    ) as pool:
        for result in pool.imap_unordered(evaluate_task, tasks, chunksize=int(args.chunksize)):
            summary = build_summary(result, Path(args.structure), backend_meta, golden_pair_code, mode_pair_reference, golden_reference, golden_ref_grid, stage_name)
            write_pair_outputs(stage_root, result, summary, a1_vals, a2_vals)
            row = ranking_row_from_result(result, stage_name)
            results[result["pair"]["pair_code"]] = {
                "pair": result["pair"],
                "summary": summary,
                "ranking_row": row,
            }
            print(f"[{stage_name}] [{result['idx']}/{total}] {result['pair']['pair_code']} done in {result['elapsed_sec']:.3f}s")
    return results, backend_meta


def write_final_ranking(output_dir: Path, ranking: list[dict]):
    ranking_csv = output_dir / "pair_ranking.csv"
    with ranking_csv.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "rank",
                "pair_code",
                "coupling_type",
                "point_label",
                "qx",
                "qy",
                "qz",
                "n_super",
                "gamma_mode_code",
                "gamma_mode_number",
                "gamma_freq_ref_thz",
                "gamma_freq_fit_thz",
                "target_mode_code",
                "target_mode_number",
                "target_freq_ref_thz",
                "target_freq_fit_thz",
                "phi122_mev",
                "phi112_mev",
                "r2",
                "rmse_ev_supercell",
                "elapsed_sec",
                "source_stage",
            ]
        )
        for rank, item in enumerate(ranking, start=1):
            q = item["q_frac"]
            writer.writerow(
                [
                    rank,
                    item["pair_code"],
                    item["coupling_type"],
                    item["point_label"],
                    f"{q[0]:.6f}",
                    f"{q[1]:.6f}",
                    f"{q[2]:.6f}",
                    item["n_super"],
                    item["gamma_mode_code"],
                    item["gamma_mode_number"],
                    f"{item['gamma_freq_ref_thz']:.6f}",
                    f"{item['gamma_freq_fit_thz']:.6f}" if item["gamma_freq_fit_thz"] is not None else "",
                    item["target_mode_code"],
                    item["target_mode_number"],
                    f"{item['target_freq_ref_thz']:.6f}",
                    f"{item['target_freq_fit_thz']:.6f}" if item["target_freq_fit_thz"] is not None else "",
                    f"{item['phi122_mev']:.6f}",
                    f"{item['phi112_mev']:.6f}",
                    f"{item['r2']:.6f}",
                    f"{item['rmse_ev_supercell']:.6f}",
                    f"{item['elapsed_sec']:.6f}",
                    item["source_stage"],
                ]
            )
    return ranking_csv


def write_ranking_plots(output_dir: Path, ranking: list[dict], title: str):
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    x = np.array([item["gamma_freq_ref_thz"] for item in ranking], dtype=float)
    y = np.array([item["target_freq_ref_thz"] for item in ranking], dtype=float)
    z = np.array([item["phi122_mev"] for item in ranking], dtype=float)

    fig = plt.figure(figsize=(8.0, 6.0))
    ax = fig.add_subplot(111, projection="3d")
    sc = ax.scatter(x, y, z, c=np.abs(z), cmap="viridis", s=36)
    fig.colorbar(sc, ax=ax, shrink=0.75, label="|phi122| (meV)")
    ax.set_xlabel("Gamma mode freq (THz)")
    ax.set_ylabel("q-mode freq (THz)")
    ax.set_zlabel("phi122 (meV)")
    ax.set_title(title)
    fig.tight_layout()
    fig.savefig(output_dir / "pair_screening_3d.png", dpi=220, bbox_inches="tight")
    plt.close(fig)

    top_n = min(15, len(ranking))
    fig, ax = plt.subplots(figsize=(9.0, 5.5))
    labels = [item["pair_code"] for item in ranking[:top_n]]
    vals = [abs(item["phi122_mev"]) for item in ranking[:top_n]]
    ax.barh(range(top_n), vals, color="#2E86AB")
    ax.set_yticks(range(top_n))
    ax.set_yticklabels(labels, fontsize=8)
    ax.invert_yaxis()
    ax.set_xlabel("|phi122| (meV)")
    ax.set_title("Top Coupling Pairs")
    fig.tight_layout()
    fig.savefig(output_dir / "pair_ranking_top15.png", dpi=220, bbox_inches="tight")
    plt.close(fig)


def main():
    args = parse_args()
    runtime_overrides = {
        "strategy": args.strategy,
        "coarse_grid_size": args.coarse_grid_size,
        "full_grid_size": args.full_grid_size,
        "refine_top_k": args.refine_top_k,
        "batch_size": args.batch_size,
        "num_workers": args.num_workers,
        "torch_threads": args.torch_threads,
        "interop_threads": args.interop_threads,
        "worker_affinity": args.worker_affinity,
        "chunksize": args.chunksize,
        "maxtasksperchild": args.maxtasksperchild,
    }
    runtime_config_path = None
    if args.runtime_config:
        runtime_config_path = Path(args.runtime_config).expanduser().resolve()
    else:
        runtime_config_path = select_runtime_config_path(ROOT, profile_name=args.runtime_profile)
    runtime_config, runtime_meta = resolve_chgnet_runtime_config(runtime_config_path, overrides=runtime_overrides)
    args.strategy = runtime_config["strategy"]
    args.coarse_grid_size = runtime_config["coarse_grid_size"]
    args.full_grid_size = runtime_config["full_grid_size"]
    args.refine_top_k = runtime_config["refine_top_k"]
    args.batch_size = runtime_config["batch_size"]
    args.num_workers = runtime_config["num_workers"]
    args.torch_threads = runtime_config["torch_threads"]
    args.interop_threads = runtime_config["interop_threads"]
    args.worker_affinity = runtime_config["worker_affinity"]
    args.chunksize = runtime_config["chunksize"]
    args.maxtasksperchild = runtime_config["maxtasksperchild"]

    mode_pairs_json = Path(args.mode_pairs_json).expanduser().resolve()
    structure = Path(args.structure).expanduser().resolve()
    output_root = Path(args.output_root).expanduser().resolve()
    run_tag = args.run_tag or args.backend
    output_dir = output_root / run_tag / "screening"
    output_dir.mkdir(parents=True, exist_ok=True)

    pair_records = load_pairs(mode_pairs_json)
    if args.limit is not None:
        pair_records = pair_records[: args.limit]

    golden_pair_code = None
    mode_pair_reference = None
    golden_reference = None
    golden_ref_grid = None
    if args.golden_fit_json and args.golden_ref_grid:
        golden_fit_json = Path(args.golden_fit_json).expanduser().resolve()
        golden_ref_grid = Path(args.golden_ref_grid).expanduser().resolve()
        golden_pair = find_golden_pair(load_pairs(mode_pairs_json))
        golden_pair_code = golden_pair["pair_code"]
        mode_pair_reference = load_mode_pair_reference(golden_pair)
        golden_reference = load_golden_reference(golden_fit_json)

    if args.strategy == "coarse_to_fine":
        coarse_results, backend_meta = run_stage(
            "screening_coarse",
            pair_records,
            grid_values(args.coarse_grid_size),
            grid_values(args.coarse_grid_size),
            args,
            output_root / run_tag,
            golden_pair_code,
            mode_pair_reference,
            golden_reference,
            golden_ref_grid,
        )
        coarse_ranking = sorted(
            [item["ranking_row"] for item in coarse_results.values()],
            key=lambda item: abs(item["phi122_mev"]),
            reverse=True,
        )
        refine_codes = {item["pair_code"] for item in coarse_ranking[: max(1, int(args.refine_top_k))]}
        refine_pairs = [pair for pair in pair_records if pair["pair_code"] in refine_codes]
        refined_results, _ = run_stage(
            "screening_refined",
            refine_pairs,
            grid_values(args.full_grid_size),
            grid_values(args.full_grid_size),
            args,
            output_root / run_tag,
            golden_pair_code,
            mode_pair_reference,
            golden_reference,
            golden_ref_grid,
        )
        final_results = dict(coarse_results)
        final_results.update(refined_results)
    else:
        final_results, backend_meta = run_stage(
            "screening",
            pair_records,
            grid_values(args.full_grid_size),
            grid_values(args.full_grid_size),
            args,
            output_root / run_tag,
            golden_pair_code,
            mode_pair_reference,
            golden_reference,
            golden_ref_grid,
        )
        refine_codes = set()

    ranking = sorted(
        [item["ranking_row"] for item in final_results.values()],
        key=lambda item: abs(item["phi122_mev"]),
        reverse=True,
    )
    ranking_csv = write_final_ranking(output_dir, ranking)
    dump_json(
        output_dir / "runtime_config_used.json",
        {
            "runtime": runtime_config,
            "meta": runtime_meta,
        },
    )
    dump_json(
        output_dir / "pair_ranking.json",
        {
            "backend": backend_meta,
            "runtime": runtime_config,
            "runtime_meta": runtime_meta,
            "refined_pair_codes": sorted(refine_codes),
            "pairs": ranking,
        },
    )
    write_ranking_plots(output_dir, ranking, title=f"{args.backend} Pair Screening ({args.strategy})")
    dump_json(
        output_dir / "run_meta.json",
        {
            "run_tag": run_tag,
            "backend": backend_meta,
            "structure": str(structure),
            "n_pairs": len(ranking),
            "runtime": runtime_config,
            "runtime_meta": runtime_meta,
        },
    )

    print(f"backend used: {args.backend}")
    print(f"screened pairs: {len(ranking)}")
    print(f"saved: {ranking_csv}")
    print(f"saved: {output_dir / 'pair_screening_3d.png'}")
    print(f"saved: {output_dir / 'pair_ranking_top15.png'}")


if __name__ == "__main__":
    main()
