#!/usr/bin/env python3
from __future__ import annotations

import json
import shutil
from datetime import datetime
from pathlib import Path


MANIFEST_VERSION = 2
STAGE1_KIND = "stage1_manifest"
STAGE2_KIND = "stage2_manifest"
STAGE3_KIND = "stage3_manifest"


def timestamp_now():
    return datetime.now().astimezone().isoformat(timespec="seconds")


def dump_json(path: Path, payload: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n")


def load_json(path: Path):
    return json.loads(Path(path).read_text())


def manifest_path(run_root: Path, stage_kind: str):
    name = {
        STAGE1_KIND: "stage1.manifest.json",
        STAGE2_KIND: "stage2.manifest.json",
        STAGE3_KIND: "stage3.manifest.json",
    }[stage_kind]
    return Path(run_root) / "contracts" / name


def _copy_file(src: Path, dst: Path):
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)


def _rel(path: Path, run_root: Path):
    return str(Path(path).resolve().relative_to(Path(run_root).resolve()))


def resolve_relative_file(run_root: Path, relative_path: str):
    return (Path(run_root) / relative_path).resolve()


def stage1_defaults(run_root: Path):
    stage_root = Path(run_root) / "stage1"
    return {
        "mode_pairs_json": stage_root / "outputs" / "mode_pairs.selected.json",
        "structure": stage_root / "inputs" / "system.scf.inp",
        "pseudo_dir": stage_root / "inputs" / "pseudos",
        "system_meta": stage_root / "inputs" / "system.json",
        "source_cif": stage_root / "inputs" / "structure.cif",
    }


def create_stage1_manifest(
    run_root: Path,
    mode_pairs_json: Path,
    structure: Path,
    pseudo_dir: Path | None = None,
    *,
    system_id: str | None = None,
    system_dir: Path | None = None,
    source_cif: Path | None = None,
    system_meta: Path | None = None,
):
    run_root = Path(run_root).expanduser().resolve()
    dsts = stage1_defaults(run_root)
    source_structure = Path(structure).expanduser().resolve()
    pseudo_source = source_structure.parent if pseudo_dir is None else Path(pseudo_dir).expanduser().resolve()
    _copy_file(Path(mode_pairs_json).expanduser().resolve(), dsts["mode_pairs_json"])
    _copy_file(source_structure, dsts["structure"])
    if source_cif is not None and Path(source_cif).exists():
        _copy_file(Path(source_cif).expanduser().resolve(), dsts["source_cif"])
    if system_meta is not None and Path(system_meta).exists():
        _copy_file(Path(system_meta).expanduser().resolve(), dsts["system_meta"])
    dsts["pseudo_dir"].mkdir(parents=True, exist_ok=True)
    copied_pseudos = []
    for pseudo in sorted(pseudo_source.glob("*.UPF")):
        dst = dsts["pseudo_dir"] / pseudo.name
        _copy_file(pseudo, dst)
        copied_pseudos.append(dst)

    payload = {
        "kind": STAGE1_KIND,
        "version": MANIFEST_VERSION,
        "created_at": timestamp_now(),
        "run_root": str(run_root),
        "system_id": system_id,
        "system_dir": None if system_dir is None else str(Path(system_dir).expanduser().resolve()),
        "files": {
            "mode_pairs_json": _rel(dsts["mode_pairs_json"], run_root),
            "structure": _rel(dsts["structure"], run_root),
        },
        "source_files": {},
        "pseudo_dir": _rel(dsts["pseudo_dir"], run_root),
        "pseudo_files": [_rel(path, run_root) for path in copied_pseudos],
        "next_stage": STAGE2_KIND,
    }
    if source_cif is not None and dsts["source_cif"].exists():
        payload["source_files"]["structure_cif"] = _rel(dsts["source_cif"], run_root)
    if system_meta is not None and dsts["system_meta"].exists():
        payload["source_files"]["system_meta"] = _rel(dsts["system_meta"], run_root)
    out = manifest_path(run_root, STAGE1_KIND)
    dump_json(out, payload)
    return out


def create_stage2_manifest(
    run_root: Path,
    stage1_manifest: Path,
    ranking_csv: Path,
    ranking_json: Path,
    runtime_config_used: Path | None,
    run_meta: Path | None,
    pair_ranking_json: Path | None = None,
):
    run_root = Path(run_root).expanduser().resolve()
    stage1 = load_json(stage1_manifest)
    payload = {
        "kind": STAGE2_KIND,
        "version": MANIFEST_VERSION,
        "created_at": timestamp_now(),
        "run_root": str(run_root),
        "system_id": stage1.get("system_id"),
        "stage1_manifest": _rel(Path(stage1_manifest).resolve(), run_root),
        "input_files": dict(stage1["files"]),
        "pseudo_dir": stage1.get("pseudo_dir"),
        "pseudo_files": list(stage1.get("pseudo_files", [])),
        "output_files": {
            "ranking_csv": _rel(Path(ranking_csv).resolve(), run_root),
            "ranking_json": _rel(Path(ranking_json).resolve(), run_root),
        },
        "runtime_files": {},
        "next_stage": STAGE3_KIND,
    }
    if runtime_config_used is not None and Path(runtime_config_used).exists():
        payload["runtime_files"]["runtime_config_used"] = _rel(Path(runtime_config_used).resolve(), run_root)
    if run_meta is not None and Path(run_meta).exists():
        payload["runtime_files"]["run_meta"] = _rel(Path(run_meta).resolve(), run_root)
    if pair_ranking_json is not None and Path(pair_ranking_json).exists():
        payload["runtime_files"]["pair_ranking_json"] = _rel(Path(pair_ranking_json).resolve(), run_root)
    out = manifest_path(run_root, STAGE2_KIND)
    dump_json(out, payload)
    return out


def create_stage3_manifest(run_root: Path, stage2_manifest: Path, qe_run_root: Path, qe_ranking_json: Path | None = None):
    run_root = Path(run_root).expanduser().resolve()
    stage2 = load_json(stage2_manifest)
    payload = {
        "kind": STAGE3_KIND,
        "version": MANIFEST_VERSION,
        "created_at": timestamp_now(),
        "run_root": str(run_root),
        "system_id": stage2.get("system_id"),
        "stage2_manifest": _rel(Path(stage2_manifest).resolve(), run_root),
        "input_files": dict(stage2["input_files"]),
        "screening_files": dict(stage2["output_files"]),
        "qe_files": {
            "qe_run_root": _rel(Path(qe_run_root).resolve(), run_root),
        },
    }
    if qe_ranking_json is not None and Path(qe_ranking_json).exists():
        payload["qe_files"]["qe_ranking_json"] = _rel(Path(qe_ranking_json).resolve(), run_root)
    out = manifest_path(run_root, STAGE3_KIND)
    dump_json(out, payload)
    return out
