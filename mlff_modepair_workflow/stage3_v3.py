"""Select and prepare QE rechecks on the exact v3 MLFF displacement grid."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import shutil
import tempfile
from pathlib import Path

import numpy as np

from .core import ModePairFrozenPhononBuilder, analyze_pair_grid, load_atoms_from_qe
from .prophet_backend import sha256_file
from .prophet_stage2 import AXES
from .units import CONTRACT_VERSION, NORMALIZATION_VERSION, RY_TO_EV, UNITS
from qe_phonon_stage1_server_bundle.common import load_qe_template, write_qe_input
from qe_modepair_handoff_workflow.scf_profile_resolver import resolve_stage3_scf_profile

BOHR_TO_ANGSTROM = 0.529177210903
FORCE_RE = re.compile(r"atom\s+\d+\s+type\s+\d+\s+force\s*=\s*([-+\d.EeDd]+)\s+([-+\d.EeDd]+)\s+([-+\d.EeDd]+)")
ENERGY_RE = re.compile(r"^\s*!\s+total energy\s*=\s*([-+\d.EeDd]+)\s+Ry", re.M)


def _dump(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(mode="w", dir=path.parent, prefix=f".{path.name}.",
                                     delete=False) as handle:
        temp = Path(handle.name)
        json.dump(payload, handle, indent=2, allow_nan=False)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temp, path)


def _load_pairs(path: Path, structure: Path) -> dict:
    data = json.loads(path.read_text())
    if data.get("version") != CONTRACT_VERSION or data.get("source", {}).get("normalization_version") != NORMALIZATION_VERSION:
        raise ValueError("Stage3 requires v3 real-mass-weighted mode pairs")
    if data["source"].get("backend") == "qe" and not data["source"].get("qe_geometry_verified"):
        raise ValueError("QE source geometry is unverified; formal Stage3 requires an exact-structure mode basis")
    if data["source"].get("structure_sha256") != sha256_file(structure):
        raise ValueError("Stage3 structure differs from its Stage1 mode source")
    codes = [row["pair_code"] for row in data["pairs"]]
    if len(codes) != len(set(codes)):
        raise ValueError("Duplicate mode-pair codes")
    return data


def _load_ranking(path: Path, pair_hash: str, structure_hash: str) -> list[dict]:
    data = json.loads(path.read_text())
    signature = data["signature"]
    if signature["mode_pairs_sha256"] != pair_hash or signature["structure_sha256"] != structure_hash:
        raise ValueError(f"Stage2 ranking does not use the requested v3 structure and modes: {path}")
    rows = data["pairs"]
    if len(rows) != 486 or len({row["pair_code"] for row in rows}) != 486:
        raise ValueError(f"Stage2 ranking is not a complete 486-pair result: {path}")
    return sorted(rows, key=lambda row: (-abs(float(row["phi122_mev_per_A3amu32"])), row["pair_code"]))


def select_pairs(mode_pairs: Path, structure: Path, prophet_ranking: Path | None,
                 mattersim_ranking: Path | None, phase: int) -> dict:
    mode_pairs, structure = Path(mode_pairs).resolve(), Path(structure).resolve()
    pairs = _load_pairs(mode_pairs, structure)
    if phase not in {20, 30}:
        raise ValueError("Joint DFT selection phase must be 20 or 30")
    if prophet_ranking is None:
        raise ValueError("Joint selection requires the complete Prophet ranking")
    source_hash = sha256_file(mode_pairs)
    structure_hash = sha256_file(structure)
    prophet = _load_ranking(Path(prophet_ranking), source_hash, structure_hash)
    mattersim = None if mattersim_ranking is None else _load_ranking(Path(mattersim_ranking), source_hash, structure_hash)
    if phase == 30 and mattersim is None:
        raise ValueError("The 30-pair phase also requires the complete MatterSim ranking")
    selected = [{"pair_code": row["pair_code"], "selection_source": "prophet_top20",
                 "prophet_rank": i + 1} for i, row in enumerate(prophet[:20])]
    seen = {row["pair_code"] for row in selected}
    if phase == 30:
        for rank, row in enumerate(mattersim, 1):
            if row["pair_code"] in seen:
                continue
            selected.append({"pair_code": row["pair_code"], "selection_source": "mattersim_unique_top10",
                             "mattersim_rank": rank})
            seen.add(row["pair_code"])
            if len(selected) == 30:
                break
    valid = {row["pair_code"] for row in pairs["pairs"]}
    if len(selected) != phase or any(row["pair_code"] not in valid for row in selected):
        raise ValueError("Selected pairs are incomplete or not present in Stage1")
    return {
        "kind": "joint_v3_dft_selection", "version": CONTRACT_VERSION, "phase": phase,
        "structure_sha256": structure_hash, "mode_pairs_sha256": source_hash,
        "prophet_ranking_sha256": sha256_file(Path(prophet_ranking)),
        "mattersim_ranking_sha256": None if mattersim_ranking is None else sha256_file(Path(mattersim_ranking)),
        "selected_pairs": selected,
    }


def select_single_top_n(mode_pairs: Path, structure: Path, ranking: Path, top_n: int = 30) -> dict:
    mode_pairs, structure, ranking = (Path(path).resolve() for path in (mode_pairs, structure, ranking))
    pairs = _load_pairs(mode_pairs, structure)
    if not 1 <= top_n <= len(pairs["pairs"]):
        raise ValueError("top_n is outside the v3 mode-pair count")
    ranked = _load_ranking(ranking, sha256_file(mode_pairs), sha256_file(structure))
    backend = json.loads(ranking.read_text())["backend"]["backend"]
    return {
        "kind": "single_v3_dft_selection", "version": CONTRACT_VERSION, "phase": top_n,
        "structure_sha256": sha256_file(structure), "mode_pairs_sha256": sha256_file(mode_pairs),
        "ranking_sha256": sha256_file(ranking), "backend": backend,
        "selected_pairs": [{"pair_code": row["pair_code"], "selection_source": f"{backend}_top{top_n}",
                            "rank": i + 1} for i, row in enumerate(ranked[:top_n])],
    }


def _check_selection_extension(existing: dict, updated: dict) -> None:
    old = [row["pair_code"] for row in existing["selected_pairs"]]
    new = [row["pair_code"] for row in updated["selected_pairs"]]
    for key in ("structure_sha256", "mode_pairs_sha256", "prophet_ranking_sha256", "ranking_sha256"):
        if existing.get(key) != updated.get(key):
            raise ValueError(f"Prepared Stage3 input changed: {key}")
    if (existing.get("mattersim_ranking_sha256") is not None
            and existing["mattersim_ranking_sha256"] != updated.get("mattersim_ranking_sha256")):
        raise ValueError("Prepared Stage3 input changed: mattersim_ranking_sha256")
    if existing.get("kind") != updated.get("kind"):
        raise ValueError("Cannot extend Stage3 using a different selection policy")
    if old != new[:len(old)]:
        raise ValueError("A 30-pair extension must preserve the prepared first 20 pairs")


def _job_script(job_dir: Path, pair_index: int, i: int, j: int, ntasks: int,
                partition: str, walltime: str, launcher: str, env_init: list[str]) -> str:
    if partition != "regular":
        raise ValueError("v3 QE Stage3 uses the regular Slurm partition")
    run_tag = hashlib.sha256(str(job_dir.parents[2].resolve()).encode()).hexdigest()[:8]
    job_name = f"qv3_{run_tag}_{pair_index:02d}_{i:02d}{j:02d}"
    lines = ["#!/bin/bash", f"#SBATCH --job-name={job_name}", "#SBATCH --nodes=1",
             f"#SBATCH --ntasks={ntasks}", f"#SBATCH --partition={partition}",
             f"#SBATCH --time={walltime}", f"#SBATCH --chdir={job_dir}",
             "#SBATCH --output=slurm-%j.out", "#SBATCH --error=slurm-%j.err", "",
             "export OMP_NUM_THREADS=1", "export MKL_NUM_THREADS=1", "export OPENBLAS_NUM_THREADS=1",
             "mkdir -p tmp", *env_init, launcher.format(ntasks=ntasks)]
    return "\n".join(lines) + "\n"


def prepare(selection: dict, mode_pairs: Path, structure: Path, pseudo_dir: Path,
            output_root: Path, *, ntasks: int = 24, walltime: str = "72:00:00",
            launcher: str = "mpirun -np {ntasks} pw.x < scf.inp > scf.out",
            env_init: list[str] | None = None, selected_profiles: Path | None = None) -> dict:
    mode_pairs, structure, pseudo_dir, output_root = (Path(p).resolve() for p in
        (mode_pairs, structure, pseudo_dir, output_root))
    data = _load_pairs(mode_pairs, structure)
    if (selection["mode_pairs_sha256"] != sha256_file(mode_pairs)
            or selection["structure_sha256"] != sha256_file(structure)):
        raise ValueError("Stage3 selection signature does not match its inputs")
    existing_path = output_root / "selection.json"
    if existing_path.exists():
        _check_selection_extension(json.loads(existing_path.read_text()), selection)
    primitive = load_atoms_from_qe(structure)
    template = load_qe_template(structure)
    by_code = {row["pair_code"]: row for row in data["pairs"]}
    profile = resolve_stage3_scf_profile(selected_profiles_path=selected_profiles)
    settings = dict(profile["scf_settings"])
    settings.update({"tprnfor": True, "tstress": False, "include_ions": False,
                     "include_cell": False, "calculation": "scf"})
    pseudo_store = output_root / "pseudos"
    pseudo_store.mkdir(parents=True, exist_ok=True)
    pseudo_hashes = {}
    for entry in template["atomic_species_entries"]:
        source = pseudo_dir / entry["pseudo"]
        target = pseudo_store / entry["pseudo"]
        if not source.is_file():
            raise FileNotFoundError(source)
        digest = sha256_file(source)
        if target.exists() and sha256_file(target) != digest:
            raise ValueError(f"Prepared pseudopotential changed: {target}")
        if not target.exists():
            shutil.copy2(source, target)
        pseudo_hashes[entry["pseudo"]] = digest
    for ordinal, selected in enumerate(selection["selected_pairs"], 1):
        code = selected["pair_code"]
        pair = by_code[code]
        builder = ModePairFrozenPhononBuilder(pair, primitive)
        pair_root = output_root / "pairs" / code
        pair_root.mkdir(parents=True, exist_ok=True)
        pair_meta = {
            "pair_code": code, "ordinal": ordinal, "builder": builder.metadata(),
            "structure_sha256": selection["structure_sha256"],
            "mode_pairs_sha256": selection["mode_pairs_sha256"],
            "a1_values": AXES.tolist(), "a2_values": AXES.tolist(),
            "pseudo_sha256": pseudo_hashes, "scf_settings": settings,
            "units": UNITS, "normalization_version": NORMALIZATION_VERSION,
        }
        meta_path = pair_root / "pair_meta.json"
        if meta_path.exists() and json.loads(meta_path.read_text()) != pair_meta:
            raise ValueError(f"Prepared pair metadata changed: {code}")
        if not meta_path.exists():
            _dump(meta_path, pair_meta)
        for i, a2 in enumerate(AXES):
            for j, a1 in enumerate(AXES):
                job_dir = pair_root / f"grid_{i:02d}_{j:02d}"
                job_dir.mkdir(parents=True, exist_ok=True)
                atoms = builder.build_atoms(float(a1), float(a2))
                scale = float(settings.get("k_scale", 1.0))
                k_mesh = [max(1, math.ceil(k / builder.n_super * scale)) for k in template["k_points"][:2]]
                k_mesh.append(max(1, math.ceil(template["k_points"][2])))
                constraints = template["constraints"] * builder.n_cells
                inp = job_dir / "scf.inp"
                with tempfile.NamedTemporaryFile(dir=job_dir, prefix=".scf.", delete=False) as handle:
                    expected_input = Path(handle.name)
                try:
                    write_qe_input(
                        out_file=expected_input, cell=atoms.cell.array.tolist(), symbols=atoms.get_chemical_symbols(),
                        frac_positions=atoms.get_scaled_positions().tolist(), constraints=constraints,
                        k_mesh=k_mesh, pseudo_dir_rel=os.path.relpath(pseudo_store, job_dir),
                        scf_settings=settings, atomic_species_entries=template["atomic_species_entries"],
                    )
                    if inp.exists():
                        if inp.read_bytes() != expected_input.read_bytes():
                            raise ValueError(f"Prepared QE point differs from requested displacement or settings: {inp}")
                    else:
                        os.replace(expected_input, inp)
                finally:
                    expected_input.unlink(missing_ok=True)
                point_meta = {"pair_code": code, "a1": float(a1), "a2": float(a2),
                              "qe_input_sha256": sha256_file(inp),
                              "structure_sha256": selection["structure_sha256"],
                              "mode_pairs_sha256": selection["mode_pairs_sha256"]}
                point_meta_path = job_dir / "point_meta.json"
                if point_meta_path.exists() and json.loads(point_meta_path.read_text()) != point_meta:
                    raise ValueError(f"Prepared QE point input changed: {inp}")
                if not point_meta_path.exists():
                    _dump(point_meta_path, point_meta)
                script = job_dir / "submit.sh"
                content = _job_script(job_dir, ordinal, i, j, ntasks, "regular", walltime,
                                      launcher, env_init or [])
                if script.exists() and script.read_text() != content:
                    raise ValueError(f"Prepared Slurm script changed: {script}")
                if not script.exists():
                    script.write_text(content)
                    script.chmod(0o755)
    _dump(existing_path, selection)
    manifest = {
        "kind": "qe_v3_stage3_run", "version": CONTRACT_VERSION, "phase": selection["phase"],
        "selected_pair_codes": [row["pair_code"] for row in selection["selected_pairs"]],
        "job_count": 81 * selection["phase"], "partition": "regular", "ntasks_per_job": ntasks,
        "pseudo_sha256": pseudo_hashes, "scf_profile": profile,
        "mode_pairs_json": str(mode_pairs), "structure": str(structure),
    }
    _dump(output_root / "run_manifest.json", manifest)
    return manifest


def parse_qe_output(path: Path, natoms: int):
    if not path.is_file():
        return None
    content = path.read_text(errors="replace")
    if "JOB DONE" not in content:
        return None
    energy = ENERGY_RE.findall(content)
    if not energy:
        return None
    forces = FORCE_RE.findall(content)
    if len(forces) < natoms:
        return None
    array = np.asarray([[float(value.replace("D", "E").replace("d", "e")) for value in row]
                        for row in forces[-natoms:]], dtype=float)
    if not np.isfinite(array).all():
        return None
    return float(energy[-1].replace("D", "E").replace("d", "e")) * RY_TO_EV, array * (RY_TO_EV / BOHR_TO_ANGSTROM)


def collect(output_root: Path) -> dict:
    output_root = Path(output_root).resolve()
    manifest = json.loads((output_root / "run_manifest.json").read_text())
    pair_data = json.loads(Path(manifest["mode_pairs_json"]).read_text())
    by_code = {row["pair_code"]: row for row in pair_data["pairs"]}
    rows = []
    for code in manifest["selected_pair_codes"]:
        root = output_root / "pairs" / code
        builder = ModePairFrozenPhononBuilder(by_code[code], load_atoms_from_qe(Path(manifest["structure"])))
        grid = np.full((9, 9), np.nan)
        forces = np.full((9, 9, builder.nat_super, 3), np.nan)
        for i in range(9):
            for j in range(9):
                result = parse_qe_output(root / f"grid_{i:02d}_{j:02d}" / "scf.out", builder.nat_super)
                if result is not None:
                    grid[i, j], forces[i, j] = result
        count = int(np.isfinite(grid).sum())
        if count != 81:
            rows.append({"pair_code": code, "complete": False, "completed_points": count})
            continue
        analysis = analyze_pair_grid(by_code[code], grid, AXES, AXES, fit_window=1.0)
        if analysis["fit_design_rank"] != 13:
            raise ValueError(f"Rank-deficient DFT fit: {code}")
        np.save(root / "energy_grid_eV.npy", grid)
        np.save(root / "forces_eV_per_A.npy", forces)
        summary = {"pair_code": code, "complete": True, "completed_points": 81,
                   "analysis": analysis, "units": UNITS,
                   "normalization_version": NORMALIZATION_VERSION}
        _dump(root / "summary.json", summary)
        rows.append({"pair_code": code, "complete": True,
                     "phi122_mev_per_A3amu32": analysis["physics"]["phi_122_mev_per_A3amu32"],
                     "phi1122_mev_per_A4amu2": analysis["physics"]["phi_1122_mev_per_A4amu2"]})
    complete = [row for row in rows if row["complete"]]
    complete.sort(key=lambda row: (-abs(row["phi122_mev_per_A3amu32"]), row["pair_code"]))
    result = {"kind": "qe_v3_stage3_collection", "version": CONTRACT_VERSION,
              "complete_pairs": len(complete), "expected_pairs": len(rows),
              "dft_ranking_within_selected": complete,
              "incomplete_pairs": [row for row in rows if not row["complete"]]}
    _dump(output_root / "results" / "qe_v3_ranking.json", result)
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    select = sub.add_parser("select")
    select.add_argument("--mode-pairs-json", type=Path, required=True)
    select.add_argument("--structure", type=Path, required=True)
    select.add_argument("--prophet-ranking", type=Path, required=True)
    select.add_argument("--mattersim-ranking", type=Path)
    select.add_argument("--phase", type=int, choices=[20, 30], required=True)
    select.add_argument("--output", type=Path, required=True)
    single = sub.add_parser("select-single")
    single.add_argument("--mode-pairs-json", type=Path, required=True)
    single.add_argument("--structure", type=Path, required=True)
    single.add_argument("--ranking", type=Path, required=True)
    single.add_argument("--top-n", type=int, default=30)
    single.add_argument("--output", type=Path, required=True)
    prepare_parser = sub.add_parser("prepare")
    prepare_parser.add_argument("--selection", type=Path, required=True)
    prepare_parser.add_argument("--mode-pairs-json", type=Path, required=True)
    prepare_parser.add_argument("--structure", type=Path, required=True)
    prepare_parser.add_argument("--pseudo-dir", type=Path, required=True)
    prepare_parser.add_argument("--output-root", type=Path, required=True)
    prepare_parser.add_argument("--selected-profiles", type=Path)
    prepare_parser.add_argument("--env-init-line", action="append", default=[])
    collect_parser = sub.add_parser("collect")
    collect_parser.add_argument("--output-root", type=Path, required=True)
    args = parser.parse_args(argv)
    if args.command == "select":
        _dump(args.output, select_pairs(args.mode_pairs_json, args.structure,
                                        args.prophet_ranking, args.mattersim_ranking, args.phase))
        print(args.output)
    elif args.command == "select-single":
        _dump(args.output, select_single_top_n(args.mode_pairs_json, args.structure,
                                               args.ranking, args.top_n))
        print(args.output)
    elif args.command == "prepare":
        selected = json.loads(args.selection.read_text())
        print(prepare(selected, args.mode_pairs_json, args.structure, args.pseudo_dir,
                      args.output_root, env_init=args.env_init_line,
                      selected_profiles=args.selected_profiles))
    else:
        print(collect(args.output_root))


if __name__ == "__main__":
    main()
