"""Build the WS2 fixed-window QE/MatterSim density comparison assets."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
DATA = REPO / "docs/reference_data/pbe_20260925"
QE = DATA / "ws2_17x17_qe.json"
MODEL = DATA / "ws2_17x17_mattersim.json"
RESOURCE = DATA / "ws2_17x17_resource_audit.json"


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> None:
    qe = json.loads(QE.read_text())
    model = json.loads(MODEL.read_text())
    resource = json.loads(RESOURCE.read_text())
    if (qe["complete_new"], qe["complete_all"], qe["total_all"]) != (208, 289, 289):
        raise ValueError("Incomplete QE 17x17 grid")
    if qe["manifest_sha256"] != model["manifest_sha256"]:
        raise ValueError("QE/MatterSim configuration manifest mismatch")
    if (resource["manifest_sha256"] != qe["manifest_sha256"]
            or resource["completed_jobs"] != 208):
        raise ValueError("Slurm cost audit does not match the QE grid")
    if qe["pair_code"] != model["pair_code"] or not np.allclose(qe["axis"], model["axis"]):
        raise ValueError("QE/MatterSim mode or coordinate axis mismatch")

    specifications = (
        ("wide_sparse_5x5_step1", r"$|Q|\leq2$, 5$\times$5"),
        ("wide_old_9x9_step0p5", r"$|Q|\leq2$, 9$\times$9"),
        ("wide_dense_17x17_step0p25", r"$|Q|\leq2$, 17$\times$17"),
        ("center_old_5x5_step0p5", r"$|Q|\leq1$, 5$\times$5"),
        ("center_dense_9x9_step0p25", r"$|Q|\leq1$, 9$\times$9"),
        ("tight_5x5_step0p25", r"$|Q|\leq0.5$, 5$\times$5"),
    )
    rows = []
    for key, label in specifications:
        q = qe["fits"][key]
        m = model["fits"].get(key)
        error = qe["matched_mattersim_errors"][key]
        mvalue = (q["phi1122"] - error["phi1122_qe_minus_mattersim"]
                  if m is None else m["phi1122"])
        rows.append(
            f"{label} & {q['points']} & {q['step']:.2f} & "
            f"{q['phi122_abs']:.3f} & {q['phi1122']:.4f} & {mvalue:.4f} & "
            f"{error['energy_mae_mev_supercell']:.3f} & "
            f"{error['force_mae_ev_per_A']:.5f} " + r"\\"
        )
    table = "\n".join([
        r"\begin{tabular}{lrrrrrrr}",
        r"\toprule",
        r"固定窗口/网格 & 点数 & $\Delta Q$ & QE $|\Phi_{122}|$ & QE $\Phi_{1122}$ & MS $\Phi_{1122}$ & $E$ MAE & $F$ MAE \\",
        r"\midrule",
        *rows,
        r"\bottomrule",
        r"\end{tabular}",
        "",
    ])
    (HERE / "tables/dense_ws2.tex").write_text(table)

    fig, axes = plt.subplots(1, 2, figsize=(10.5, 3.8), layout="constrained")
    for ax, names, title in (
        (axes[0], [x[0] for x in specifications[:3]], r"$|Q|\leq2$"),
        (axes[1], [x[0] for x in specifications[3:5]], r"$|Q|\leq1$"),
    ):
        x = np.arange(len(names))
        qvals = [qe["fits"][name]["phi1122"] for name in names]
        mvals = [
            (model["fits"][name]["phi1122"] if name in model["fits"] else
             qe["fits"][name]["phi1122"] -
             qe["matched_mattersim_errors"][name]["phi1122_qe_minus_mattersim"])
            for name in names
        ]
        ax.plot(x, qvals, "o-", color="#1e618a", label="QE GGA-PBE")
        ax.plot(x, mvals, "s-", color="#dc8b31", label="MatterSim same geometry")
        ax.set_xticks(x, [f"{qe['fits'][name]['points']} points" for name in names])
        ax.set_title(title)
        ax.set_ylabel(r"Signed $\Phi_{1122}$ (meV / $\AA^4$ amu$^2$)")
        ax.grid(alpha=0.2)
    axes[0].legend(frameon=False, fontsize=8)
    fig.savefig(HERE / "figures/dense_ws2.pdf", bbox_inches="tight")
    fig.savefig(HERE / "figures/dense_ws2.png", dpi=180, bbox_inches="tight")
    plt.close(fig)

    amplitudes = [x for x in ("0.25", "0.50", "0.75", "1.00", "1.25", "1.50", "1.75", "2.00")
                  if x in qe["even_mixed"] and x in model["even_mixed"]]
    fig, ax = plt.subplots(figsize=(6.0, 3.5), layout="constrained")
    for payload, label, color in (
        (qe, "QE GGA-PBE", "#1e618a"),
        (model, "MatterSim same geometry", "#dc8b31"),
    ):
        ax.plot(
            [float(x) for x in amplitudes],
            [payload["even_mixed"][x]["effective_phi1122_mev_per_A4amu2"]
             for x in amplitudes],
            "o-", label=label, color=color,
        )
    ax.axhline(0, color="black", linewidth=0.7)
    ax.set_xlabel(r"Equal amplitude $|Q_\Gamma|=|Q_q|$ ($\AA\sqrt{amu}$)")
    ax.set_ylabel(r"Raw even-mixed $\Phi_{1122}$ (meV / $\AA^4$ amu$^2$)")
    ax.grid(alpha=0.2)
    ax.legend(frameon=False, fontsize=8)
    fig.savefig(HERE / "figures/dense_ws2_even_mixed.pdf", bbox_inches="tight")
    fig.savefig(HERE / "figures/dense_ws2_even_mixed.png", dpi=180, bbox_inches="tight")
    plt.close(fig)
    (HERE / "dense_ws2_sources.json").write_text(json.dumps({
        "qe_result_sha256": sha(QE),
        "mattersim_result_sha256": sha(MODEL),
        "resource_audit_sha256": sha(RESOURCE),
        "manifest_sha256": qe["manifest_sha256"],
        "note": "Point-level QE output hashes and Slurm identities are in the published QE and resource JSON files.",
    }, indent=2) + "\n")
    print("WS2 density assets complete",
          "wide QE change", qe["wide_density_change_percent"],
          "center QE change", qe["center_density_change_percent"])


if __name__ == "__main__":
    main()
