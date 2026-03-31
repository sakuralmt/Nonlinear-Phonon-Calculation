from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from nonlinear_phonon_calculation.workflow_families import resolve_workflow_family, supported_workflow_families


DEFAULT_INPUT_ROOT = Path.cwd() / "inputs"
DEFAULT_RUNS_ROOT_NAME = "Nonlinear-Phonon-Calculation-runs"


@dataclass(frozen=True)
class SystemSpec:
    system_id: str
    system_dir: Path
    structure_cif: Path
    pseudo_dir: Path
    metadata_path: Path
    workflow_family: str
    formula: str | None
    already_relaxed: bool
    preferred_pseudos: dict[str, str]
    notes: str | None


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text())


def discover_system_dirs(input_root: Path) -> list[Path]:
    input_root = Path(input_root).expanduser().resolve()
    if not input_root.exists():
        return []
    out: list[Path] = []
    for child in sorted(input_root.iterdir()):
        if not child.is_dir():
            continue
        if (child / "structure.cif").exists() and (child / "system.json").exists() and (child / "pseudos").is_dir():
            out.append(child)
    return out


def list_system_ids(input_root: Path) -> list[str]:
    return [path.name for path in discover_system_dirs(input_root)]


def resolve_system_dir(input_root: Path, system_id: str) -> Path:
    system_dir = Path(input_root).expanduser().resolve() / system_id
    if not system_dir.exists():
        raise FileNotFoundError(f"Missing system directory: {system_dir}")
    return system_dir


def load_system_spec(system_dir: Path) -> SystemSpec:
    system_dir = Path(system_dir).expanduser().resolve()
    metadata_path = system_dir / "system.json"
    structure_cif = system_dir / "structure.cif"
    pseudo_dir = system_dir / "pseudos"

    if not metadata_path.exists():
        raise FileNotFoundError(f"Missing system.json: {metadata_path}")
    if not structure_cif.exists():
        raise FileNotFoundError(f"Missing structure.cif: {structure_cif}")
    if not pseudo_dir.is_dir():
        raise FileNotFoundError(f"Missing pseudos directory: {pseudo_dir}")

    payload = _load_json(metadata_path)
    workflow_family = str(payload.get("workflow_family", "tmd_monolayer_hex"))
    try:
        resolve_workflow_family(workflow_family)
    except KeyError:
        raise ValueError(
            f"Unsupported workflow_family '{workflow_family}' in {metadata_path}. "
            f"Supported values: {list(supported_workflow_families())}"
        )

    preferred_pseudos = payload.get("preferred_pseudos") or {}
    if not isinstance(preferred_pseudos, dict):
        raise ValueError(f"preferred_pseudos must be an object in {metadata_path}")

    return SystemSpec(
        system_id=str(payload.get("system_id") or system_dir.name),
        system_dir=system_dir,
        structure_cif=structure_cif,
        pseudo_dir=pseudo_dir,
        metadata_path=metadata_path,
        workflow_family=workflow_family,
        formula=payload.get("formula"),
        already_relaxed=bool(payload.get("already_relaxed", False)),
        preferred_pseudos={str(k): str(v) for k, v in preferred_pseudos.items()},
        notes=payload.get("notes"),
    )


def default_runs_root(input_root: Path) -> Path:
    input_root = Path(input_root).expanduser().resolve()
    return input_root.parent / DEFAULT_RUNS_ROOT_NAME


def build_run_tag(system_id: str) -> str:
    stamp = datetime.now().astimezone().strftime("%Y%m%d_%H%M%S")
    return f"{system_id}_{stamp}"


def latest_run_root(runs_root: Path, system_id: str) -> Path | None:
    system_runs = Path(runs_root).expanduser().resolve() / system_id
    if not system_runs.exists():
        return None
    candidates = [path for path in system_runs.iterdir() if path.is_dir()]
    if not candidates:
        return None
    candidates.sort(key=lambda path: path.stat().st_mtime, reverse=True)
    return candidates[0]
