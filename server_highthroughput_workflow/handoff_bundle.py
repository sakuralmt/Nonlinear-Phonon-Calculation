#!/usr/bin/env python3
from __future__ import annotations

import json
import tarfile
from datetime import datetime
from pathlib import Path

from server_highthroughput_workflow.stage_contracts import (
    STAGE1_KIND,
    STAGE2_KIND,
    load_json,
    manifest_path,
    resolve_relative_file,
)


HANDOFF_METADATA = "handoff_bundle.json"


def timestamp_now() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def _assert_within_run_root(path: Path, run_root: Path) -> None:
    try:
        path.resolve().relative_to(run_root.resolve())
    except ValueError as exc:
        raise RuntimeError(f"Manifest path escapes run root: {path}") from exc


def _unique_paths(paths: list[Path]) -> list[Path]:
    ordered: list[Path] = []
    seen: set[str] = set()
    for path in paths:
        key = str(path.resolve())
        if key in seen:
            continue
        seen.add(key)
        ordered.append(path)
    return ordered


def _resolve_manifest_paths(run_root: Path, mapping: dict[str, str]) -> list[Path]:
    resolved: list[Path] = []
    for relative_path in mapping.values():
        path = resolve_relative_file(run_root, relative_path)
        _assert_within_run_root(path, run_root)
        resolved.append(path)
    return resolved


def _pseudo_paths(run_root: Path, manifest: dict) -> list[Path]:
    files: list[Path] = []
    for relative_path in manifest.get("pseudo_files", []):
        path = resolve_relative_file(run_root, relative_path)
        _assert_within_run_root(path, run_root)
        files.append(path)
    return files


def _stage1_export_paths(run_root: Path, stage1_manifest: dict) -> list[Path]:
    paths: list[Path] = [manifest_path(run_root, STAGE1_KIND)]
    paths.extend(_resolve_manifest_paths(run_root, stage1_manifest.get("files", {})))
    paths.extend(_resolve_manifest_paths(run_root, stage1_manifest.get("source_files", {})))
    paths.extend(_pseudo_paths(run_root, stage1_manifest))
    return _unique_paths(paths)


def _stage2_export_paths(run_root: Path, stage2_manifest: dict) -> list[Path]:
    stage1_manifest_path = resolve_relative_file(run_root, stage2_manifest["stage1_manifest"])
    stage1_manifest = load_json(stage1_manifest_path)
    paths = _stage1_export_paths(run_root, stage1_manifest)
    paths.append(manifest_path(run_root, STAGE2_KIND))
    paths.extend(_resolve_manifest_paths(run_root, stage2_manifest.get("output_files", {})))
    paths.extend(_resolve_manifest_paths(run_root, stage2_manifest.get("runtime_files", {})))
    return _unique_paths(paths)


def _export_paths(run_root: Path, stage: str) -> list[Path]:
    if stage == "stage1":
        return _stage1_export_paths(run_root, load_json(manifest_path(run_root, STAGE1_KIND)))
    if stage == "stage2":
        return _stage2_export_paths(run_root, load_json(manifest_path(run_root, STAGE2_KIND)))
    raise RuntimeError(f"Unsupported handoff export stage: {stage}")


def _metadata_for_export(run_root: Path, stage: str) -> dict:
    manifest_name = STAGE1_KIND if stage == "stage1" else STAGE2_KIND
    payload = load_json(manifest_path(run_root, manifest_name))
    backend_tag = None
    if stage == "stage2":
        ranking_json = payload.get("output_files", {}).get("ranking_json", "")
        parts = Path(ranking_json).parts
        if len(parts) >= 3:
            backend_tag = parts[-3]
    return {
        "kind": "npc_handoff_bundle",
        "created_at": timestamp_now(),
        "exported_stage": stage,
        "source_run_root": str(run_root.resolve()),
        "system_id": payload.get("system_id"),
        "backend_tag": backend_tag,
    }


def export_handoff_bundle(run_root: Path, stage: str, output_path: Path) -> Path:
    run_root = Path(run_root).expanduser().resolve()
    output_path = Path(output_path).expanduser().resolve()
    paths = _export_paths(run_root, stage)
    metadata = _metadata_for_export(run_root, stage)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(output_path, "w:gz") as archive:
        for path in paths:
            if not path.exists():
                raise RuntimeError(f"Missing required handoff file: {path}")
            archive.add(path, arcname=str(path.resolve().relative_to(run_root.resolve())))
        metadata_blob = json.dumps(metadata, indent=2, ensure_ascii=False) + "\n"
        info = tarfile.TarInfo(HANDOFF_METADATA)
        encoded = metadata_blob.encode("utf-8")
        info.size = len(encoded)
        archive.addfile(info, fileobj=__import__("io").BytesIO(encoded))
    return output_path


def _safe_members(archive: tarfile.TarFile) -> list[tarfile.TarInfo]:
    members = archive.getmembers()
    for member in members:
        member_path = Path(member.name)
        if member_path.is_absolute():
            raise RuntimeError(f"Refusing to import absolute archive path: {member.name}")
        if ".." in member_path.parts:
            raise RuntimeError(f"Refusing to import unsafe archive path: {member.name}")
    return members


def _validate_manifest_refs(run_root: Path, manifest: dict, *, mapping_keys: list[str]) -> None:
    for key in mapping_keys:
        mapping = manifest.get(key, {})
        if not isinstance(mapping, dict):
            continue
        for relative_path in mapping.values():
            path = resolve_relative_file(run_root, relative_path)
            _assert_within_run_root(path, run_root)
            if not path.exists():
                raise RuntimeError(f"Imported manifest references a missing file: {path}")
    for relative_path in manifest.get("pseudo_files", []):
        path = resolve_relative_file(run_root, relative_path)
        _assert_within_run_root(path, run_root)
        if not path.exists():
            raise RuntimeError(f"Imported manifest references a missing pseudopotential: {path}")


def validate_imported_run_root(run_root: Path) -> dict:
    run_root = Path(run_root).expanduser().resolve()
    results: dict[str, str] = {}

    stage1_path = manifest_path(run_root, STAGE1_KIND)
    if stage1_path.exists():
        stage1 = load_json(stage1_path)
        _validate_manifest_refs(run_root, stage1, mapping_keys=["files", "source_files"])
        results["stage1"] = str(stage1_path)

    stage2_path = manifest_path(run_root, STAGE2_KIND)
    if stage2_path.exists():
        stage2 = load_json(stage2_path)
        _validate_manifest_refs(run_root, stage2, mapping_keys=["input_files", "output_files", "runtime_files"])
        results["stage2"] = str(stage2_path)

    if not results:
        raise RuntimeError(f"No imported stage1/stage2 manifest was found under {run_root}")
    return results


def import_handoff_bundle(bundle_path: Path, run_root: Path) -> dict:
    bundle_path = Path(bundle_path).expanduser().resolve()
    run_root = Path(run_root).expanduser().resolve()
    run_root.mkdir(parents=True, exist_ok=True)
    with tarfile.open(bundle_path, "r:gz") as archive:
        archive.extractall(path=run_root, members=_safe_members(archive))
    return validate_imported_run_root(run_root)
