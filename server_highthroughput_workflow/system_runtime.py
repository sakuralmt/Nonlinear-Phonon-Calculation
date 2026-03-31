#!/usr/bin/env python3
from __future__ import annotations

import json
import shutil
from pathlib import Path

from qe_phonon_stage1_server_bundle.scf_settings import resolve_scf_settings
from server_highthroughput_workflow.qe_input_utils import (
    DEFAULT_STAGE1_K_MESH,
    cif_to_structure_payload,
    resolve_pseudopotential,
    write_qe_input,
)


def dump_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n")


def prepare_runtime_system(system_dir: Path, runtime_root: Path, preferred_pseudos: dict[str, str] | None = None) -> dict:
    system_dir = Path(system_dir).expanduser().resolve()
    runtime_root = Path(runtime_root).expanduser().resolve()
    preferred_pseudos = {} if preferred_pseudos is None else dict(preferred_pseudos)

    source_cif = system_dir / "structure.cif"
    source_meta = system_dir / "system.json"
    source_pseudos = system_dir / "pseudos"

    runtime_inputs = runtime_root / "inputs"
    runtime_pseudos = runtime_inputs / "pseudos"
    runtime_pseudos.mkdir(parents=True, exist_ok=True)

    shutil.copy2(source_cif, runtime_inputs / "structure.cif")
    shutil.copy2(source_meta, runtime_inputs / "system.json")

    pseudo_map: dict[str, str] = {}
    for pseudo in sorted(source_pseudos.glob("*.UPF")):
        shutil.copy2(pseudo, runtime_pseudos / pseudo.name)

    structure = cif_to_structure_payload(source_cif, source_pseudos, k_mesh=DEFAULT_STAGE1_K_MESH)
    for symbol in sorted(set(structure["symbols"])):
        requested = preferred_pseudos.get(symbol)
        if requested:
            requested_path = source_pseudos / requested
            if not requested_path.exists():
                raise FileNotFoundError(f"preferred pseudopotential for {symbol} not found: {requested_path}")
            pseudo_map[symbol] = requested
        else:
            pseudo_map[symbol] = resolve_pseudopotential(symbol, source_pseudos).name

    raw_scf = runtime_inputs / "system.scf.inp"
    scf_settings = resolve_scf_settings("template80")
    write_qe_input(
        out_file=raw_scf,
        cell=structure["cell"],
        symbols=structure["symbols"],
        frac_positions=structure["frac_positions"],
        constraints=structure["constraints"],
        k_mesh=structure["k_mesh"],
        pseudo_dir=source_pseudos,
        pseudo_dir_rel="./pseudos",
        scf_settings=scf_settings,
    )

    system_summary = {
        "kind": "runtime_system_preparation",
        "source_system_dir": str(system_dir),
        "runtime_root": str(runtime_root),
        "structure_cif": str(runtime_inputs / "structure.cif"),
        "system_meta": str(runtime_inputs / "system.json"),
        "raw_scf": str(raw_scf),
        "pseudo_dir": str(runtime_pseudos),
        "pseudo_map": pseudo_map,
        "symbols": structure["symbols"],
        "k_mesh": structure["k_mesh"],
    }
    dump_json(runtime_root / "system_runtime.json", system_summary)
    return system_summary
