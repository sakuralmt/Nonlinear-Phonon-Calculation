#!/usr/bin/env python3
from __future__ import annotations

import re
from pathlib import Path

from ase.data import atomic_masses, atomic_numbers
from ase.io import read


SUPPORTED_TMD_METALS = {"Mo", "W"}
SUPPORTED_TMD_CHALCOGENS = {"S", "Se"}
DEFAULT_STAGE1_K_MESH = [12, 12, 1]
PREFERRED_PSEUDO_FILENAMES = {
    "Mo": "Mo.pz-spn-rrkjus_psl.0.2.UPF",
    "S": "S.pz-n-rrkjus_psl.0.1.UPF",
    "Se": "Se.pz-n-rrkjus_psl.0.2.UPF",
    "W": "W.pz-spn-rrkjus_psl.1.0.0.UPF",
}


def atomic_mass(symbol: str) -> float:
    try:
        return float(atomic_masses[atomic_numbers[symbol]])
    except Exception as exc:
        raise KeyError(f"Unsupported element for atomic mass lookup: {symbol}") from exc


def ordered_unique(items: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def resolve_pseudopotential(symbol: str, pseudo_dir: Path) -> Path:
    pseudo_dir = Path(pseudo_dir).expanduser().resolve()
    candidates = sorted(path for path in pseudo_dir.glob(f"{symbol}*.UPF") if path.is_file())
    if not candidates:
        raise FileNotFoundError(f"Missing pseudopotential for {symbol} under {pseudo_dir}")

    preferred_name = PREFERRED_PSEUDO_FILENAMES.get(symbol)
    if preferred_name is not None:
        preferred_path = pseudo_dir / preferred_name
        if preferred_path.exists():
            return preferred_path

    if len(candidates) == 1:
        return candidates[0]

    pz_rrkjus = [path for path in candidates if ".pz-" in path.name and "rrkjus" in path.name]
    if len(pz_rrkjus) == 1:
        return pz_rrkjus[0]

    names = ", ".join(path.name for path in candidates)
    raise RuntimeError(f"Multiple pseudopotentials found for {symbol} under {pseudo_dir}: {names}")


def atomic_species_records(symbols: list[str], pseudo_dir: Path) -> list[dict]:
    records: list[dict] = []
    for symbol in ordered_unique(symbols):
        pseudo_path = resolve_pseudopotential(symbol, pseudo_dir)
        records.append(
            {
                "symbol": symbol,
                "mass": atomic_mass(symbol),
                "pseudo": pseudo_path.name,
                "path": pseudo_path,
            }
        )
    return records


def atomic_species_lines(symbols: list[str], pseudo_dir: Path) -> list[str]:
    return [
        f"{record['symbol']}  {record['mass']:.6f} {record['pseudo']}"
        for record in atomic_species_records(symbols, pseudo_dir)
    ]


def replace_control_value(text: str, key: str, value_literal: str) -> str:
    lines = text.splitlines()
    in_control = False
    inserted = False
    output: list[str] = []
    key_pattern = re.compile(rf"^\s*{re.escape(key)}\s*=", re.I)
    for line in lines:
        stripped = line.strip()
        if stripped.upper().startswith("&CONTROL"):
            in_control = True
            output.append(line)
            continue
        if in_control and stripped == "/":
            if not inserted:
                output.append(f"  {key} = {value_literal}")
                inserted = True
            in_control = False
            output.append(line)
            continue
        if in_control and key_pattern.match(stripped):
            output.append(f"  {key} = {value_literal}")
            inserted = True
            continue
        output.append(line)
    if not inserted:
        raise ValueError(f"Could not update {key} in &CONTROL")
    return "\n".join(output) + ("\n" if text.endswith("\n") else "")


def rewrite_qe_control_paths(path: Path, pseudo_dir_literal: str, outdir_literal: str = "'./tmp'") -> None:
    text = Path(path).read_text()
    text = replace_control_value(text, "pseudo_dir", pseudo_dir_literal)
    text = replace_control_value(text, "outdir", outdir_literal)
    Path(path).write_text(text)


def validate_supported_tmd_cif(cif_path: Path):
    atoms = read(cif_path)
    symbols = atoms.get_chemical_symbols()
    unique_symbols = ordered_unique(symbols)
    if len(symbols) != 3:
        raise ValueError(f"Only 3-atom primitive monolayer TMD CIFs are supported, got nat={len(symbols)} from {cif_path}")
    if len(unique_symbols) != 2:
        raise ValueError(f"Expected exactly two species in {cif_path}, got {unique_symbols}")

    metals = [symbol for symbol in unique_symbols if symbol in SUPPORTED_TMD_METALS]
    chalcogens = [symbol for symbol in unique_symbols if symbol in SUPPORTED_TMD_CHALCOGENS]
    if len(metals) != 1 or len(chalcogens) != 1:
        raise ValueError(
            f"Only 2H monolayer Mo/W + S/Se TMDs are supported, got species {unique_symbols} from {cif_path}"
        )

    metal = metals[0]
    chalcogen = chalcogens[0]
    if symbols.count(metal) != 1 or symbols.count(chalcogen) != 2:
        raise ValueError(f"Expected 1 metal + 2 chalcogens in {cif_path}, got symbols {symbols}")

    a_len, b_len, c_len = atoms.cell.lengths()
    _, _, gamma = atoms.cell.angles()
    if abs(a_len - b_len) > 1.0e-3:
        raise ValueError(f"Expected hexagonal in-plane lattice with a=b, got a={a_len:.6f}, b={b_len:.6f} for {cif_path}")
    if abs(gamma - 120.0) > 1.0e-3:
        raise ValueError(f"Expected gamma=120 deg for {cif_path}, got gamma={gamma:.6f}")
    if c_len < 10.0:
        raise ValueError(f"Expected monolayer vacuum padding along c for {cif_path}, got c={c_len:.6f}")

    return atoms


def default_tmd_constraints(symbols: list[str]) -> list[str]:
    constraints: list[str] = []
    for symbol in symbols:
        if symbol in SUPPORTED_TMD_METALS:
            constraints.append("0   0   0")
        elif symbol in SUPPORTED_TMD_CHALCOGENS:
            constraints.append("0   0   1")
        else:
            raise ValueError(f"Unsupported TMD element in constraint builder: {symbol}")
    return constraints


def cif_to_structure_payload(cif_path: Path, pseudo_dir: Path, k_mesh: list[int] | None = None) -> dict:
    cif_path = Path(cif_path).expanduser().resolve()
    pseudo_dir = Path(pseudo_dir).expanduser().resolve()
    atoms = validate_supported_tmd_cif(cif_path)
    symbols = atoms.get_chemical_symbols()
    payload = {
        "cif_path": str(cif_path),
        "symbols": symbols,
        "cell": atoms.cell.array.tolist(),
        "frac_positions": atoms.get_scaled_positions().tolist(),
        "constraints": default_tmd_constraints(symbols),
        "k_mesh": list(DEFAULT_STAGE1_K_MESH if k_mesh is None else k_mesh),
        "atomic_species_lines": atomic_species_lines(symbols, pseudo_dir),
    }
    return payload


def write_qe_input(
    out_file: Path,
    cell: list[list[float]],
    symbols: list[str],
    frac_positions: list[list[float]],
    constraints: list[str],
    k_mesh: list[int],
    pseudo_dir: Path,
    pseudo_dir_rel: str,
    scf_settings: dict,
) -> None:
    species_lines = atomic_species_lines(symbols, pseudo_dir)
    nat = len(symbols)
    ntyp = len(ordered_unique(symbols))
    calculation = scf_settings.get("calculation", "scf")
    disk_io = scf_settings.get("disk_io", "low")
    verbosity = scf_settings.get("verbosity", "high")
    tprnfor = ".true." if scf_settings.get("tprnfor", True) else ".false."
    tstress = ".true." if scf_settings.get("tstress", True) else ".false."
    include_ions = bool(scf_settings.get("include_ions", False))
    include_cell = bool(scf_settings.get("include_cell", False))
    ion_dynamics = scf_settings.get("ion_dynamics", "bfgs")
    cell_dynamics = scf_settings.get("cell_dynamics", "bfgs")
    press_conv_thr = scf_settings.get("press_conv_thr", "0.1")

    out_file.parent.mkdir(parents=True, exist_ok=True)
    with out_file.open("w") as handle:
        handle.write("&CONTROL\n")
        handle.write(f"  calculation = '{calculation}'\n")
        handle.write(f"  disk_io = '{disk_io}'\n")
        handle.write("  prefix = 'pwscf'\n")
        handle.write(f"  pseudo_dir = '{pseudo_dir_rel}'\n")
        handle.write("  outdir = './tmp'\n")
        handle.write(f"  verbosity = '{verbosity}'\n")
        handle.write(f"  tprnfor = {tprnfor}\n")
        handle.write(f"  tstress = {tstress}\n")
        handle.write(f"  forc_conv_thr = {scf_settings['forc_conv_thr']}\n")
        if scf_settings.get("etot_conv_thr"):
            handle.write(f"  etot_conv_thr = {scf_settings['etot_conv_thr']}\n")
        handle.write("/\n\n")

        handle.write("&SYSTEM\n")
        handle.write("  ibrav = 0\n")
        handle.write(f"  nat = {nat}, ntyp = {ntyp}\n")
        if scf_settings.get("occupations") == "smearing":
            handle.write(
                "  occupations = 'smearing', "
                f"smearing = '{scf_settings['smearing']}', degauss = {scf_settings['degauss']}\n"
            )
        else:
            handle.write(f"  occupations = '{scf_settings['occupations']}'\n")
        handle.write(f"  ecutwfc = {scf_settings['ecutwfc']}, ecutrho = {scf_settings['ecutrho']}\n")
        handle.write("/\n\n")

        handle.write("&ELECTRONS\n")
        handle.write(f"  electron_maxstep = {scf_settings['electron_maxstep']}\n")
        handle.write(f"  conv_thr = {scf_settings['conv_thr']}\n")
        handle.write(f"  mixing_mode = '{scf_settings['mixing_mode']}'\n")
        handle.write(f"  mixing_beta = {scf_settings['mixing_beta']}\n")
        handle.write(f"  diagonalization = '{scf_settings['diagonalization']}'\n")
        handle.write("/\n\n")

        if include_ions:
            handle.write("&IONS\n")
            handle.write(f"  ion_dynamics = '{ion_dynamics}'\n")
            handle.write("/\n\n")

        if include_cell:
            handle.write("&CELL\n")
            handle.write(f"  cell_dynamics = '{cell_dynamics}'\n")
            handle.write(f"  press_conv_thr = {press_conv_thr}\n")
            handle.write("/\n\n")

        handle.write("ATOMIC_SPECIES\n")
        for line in species_lines:
            handle.write(f"{line}\n")
        handle.write("\n")

        handle.write("CELL_PARAMETERS (angstrom)\n")
        for row in cell:
            handle.write(f"   {row[0]:.9f}   {row[1]:.9f}   {row[2]:.9f}\n")

        handle.write("\nATOMIC_POSITIONS (crystal)\n")
        for index, symbol in enumerate(symbols):
            position = frac_positions[index]
            constraint = constraints[index] if constraints else "0   0   0"
            handle.write(f"{symbol:<4}   {position[0]:.10f}   {position[1]:.10f}   {position[2]:.10f}   {constraint}\n")

        handle.write("\nK_POINTS {automatic}\n")
        handle.write(f"{k_mesh[0]} {k_mesh[1]} {k_mesh[2]} 0 0 0\n")
