#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path


def parse_args():
    p = argparse.ArgumentParser(description="Prepare matdyn input for screened hexagonal q-points")
    p.add_argument("--screening-json", type=str, required=True)
    p.add_argument("--job-dir", type=str, required=True)
    p.add_argument("--flfrc", type=str, default="MM.fc")
    p.add_argument("--flfrq", type=str, default="screened_hex_6x6.freq")
    p.add_argument("--fleig", type=str, default="screened_hex_6x6.eig")
    p.add_argument("--job-name", type=str, default="ph_hex")
    p.add_argument("--nodes", type=int, default=1)
    p.add_argument("--ntasks-per-node", type=int, default=50)
    p.add_argument("--partition", type=str, default="long")
    p.add_argument("--walltime", type=str, default="3-00:00:00")
    p.add_argument("--load-modules", action="store_true")
    p.add_argument("--launcher-command", type=str, default="mpirun -np {ntasks} matdyn.x < matdyn.inp > dynmat.out")
    p.add_argument("--env-init-line", action="append", default=[])
    return p.parse_args()


def main():
    args = parse_args()

    screening_json = Path(args.screening_json).expanduser().resolve()
    job_dir = Path(args.job_dir).expanduser().resolve()
    job_dir.mkdir(parents=True, exist_ok=True)

    summary = json.loads(screening_json.read_text())
    selected_points = summary["selected_points"]
    if not selected_points:
        raise RuntimeError("No selected q-points were found in screening summary")

    matdyn_inp = job_dir / "matdyn.inp"
    with matdyn_inp.open("w") as f:
        f.write("&input\n")
        f.write("  asr = 'simple'\n")
        f.write(f"  flfrc='{args.flfrc}'\n")
        f.write(f"  flfrq = '{args.flfrq}'\n")
        f.write(f"  fleig = '{args.fleig}'\n")
        f.write("  q_in_cryst_coord=.true.\n")
        f.write("/\n")
        f.write(f"{len(selected_points)}\n")
        for item in selected_points:
            q = item["rep_q_frac"]
            f.write(f"{q[0]:.10f}  {q[1]:.10f}  {q[2]:.10f}\n")

    run_sh = job_dir / "run.sh"
    run_lines = [
        "#!/bin/bash",
        f"#SBATCH --job-name={args.job_name}",
        f"#SBATCH -N {args.nodes}",
        f"#SBATCH --ntasks-per-node={args.ntasks_per_node}",
        f"#SBATCH --time={args.walltime}",
        f"#SBATCH -p {args.partition}",
        "",
        "ulimit -s unlimited",
        "ulimit -c unlimited",
    ]
    if args.load_modules:
        run_lines.extend(
            [
                "module load parallel_studio/2019.0.045",
                "module load intelmpi/2019.0.045",
            ]
        )
    run_lines.extend(args.env_init_line)
    run_lines.extend(
        [
            "",
            "rm -f TDPWSTOP",
            args.launcher_command.format(ntasks=args.ntasks_per_node),
            "",
            "exit",
        ]
    )
    run_sh.write_text("\n".join(run_lines) + "\n")
    run_sh.chmod(0o755)

    selected_json = job_dir / "selected_qpoints.json"
    selected_json.write_text(json.dumps(selected_points, indent=2))

    selected_csv = job_dir / "selected_qpoints.csv"
    with selected_csv.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["qx", "qy", "qz", "label", "star_size", "little_group_order_inplane"])
        for item in selected_points:
            q = item["rep_q_frac"]
            writer.writerow(
                [
                    f"{q[0]:.6f}",
                    f"{q[1]:.6f}",
                    f"{q[2]:.6f}",
                    item["label"],
                    item["star_size"],
                    item["little_group_order_inplane"],
                ]
            )

    manifest = {
        "kind": "matdyn_job_qpair",
        "screening_json": str(screening_json),
        "job_dir": str(job_dir),
        "matdyn_input": str(matdyn_inp),
        "run_script": str(run_sh),
        "flfrc": args.flfrc,
        "flfrq": args.flfrq,
        "fleig": args.fleig,
        "selected_points": selected_points,
    }
    (job_dir / "job_manifest.json").write_text(json.dumps(manifest, indent=2))

    print(f"selected q-points: {len(selected_points)}")
    print(f"saved: {matdyn_inp}")
    print(f"saved: {run_sh}")
    print(f"saved: {selected_json}")


if __name__ == "__main__":
    main()
