"""Render read-only, audited WS2 model/DFT comparison evidence.

No QE input generation, submission or inference is included in this helper.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

MODELS = ("tece", "prophet", "equiformer-v3")
LABELS = {
    "tece": "TECE + MS",
    "prophet": "Prophet + MS",
    "equiformer-v3": "EquiformerV3 + MS",
    "qe": "QE-DFT",
}
COLORS = {
    "qe": "#333333",
    "tece": "#26778e",
    "prophet": "#d27330",
    "equiformer-v3": "#7556a2",
}
NAMES = ["M9", "L9", "M6", "M5", "K9"]
U3 = r"meV / ($\AA^3$ amu$^{3/2}$)"
U4 = r"meV / ($\AA^4$ amu$^2$)"
KEY3 = "phi_122_mev_per_A3amu32"
KEY4 = "phi_1122_mev_per_A4amu2"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--allow-partial", action="store_true")
    args = parser.parse_args()
    d = json.loads(args.evidence.read_text())
    if not d["complete"] and not args.allow_partial:
        raise ValueError("Final report requires all 193 QE points")
    out = args.output
    figs = out / "figures"
    figs.mkdir(parents=True, exist_ok=True)
    data = out / "acceptance_data"
    data.mkdir(exist_ok=True)
    (data / "ws2_dft_top5.json").write_text(args.evidence.read_text())
    if args.evidence.with_suffix(".csv").exists():
        (data / "ws2_dft_top5.csv").write_text(
            args.evidence.with_suffix(".csv").read_text()
        )
    plt.rcParams.update(
        {
            "font.size": 10,
            "axes.spines.top": False,
            "axes.spines.right": False,
            "figure.dpi": 150,
            "savefig.dpi": 180,
        }
    )
    pairs = [p for p in d["pairs"] if p["qe"] is not None]
    x = np.arange(len(pairs))
    tick = [NAMES[p["rank"] - 1] for p in pairs]

    def save(fig, name):
        fig.savefig(figs / name, bbox_inches="tight")
        plt.close(fig)

    fig, axes = plt.subplots(1, 2, figsize=(11, 4.2), layout="constrained")
    for k, (key, unit, label) in enumerate(
        [(KEY3, U3, r"$|\Phi_{122}|$"), (KEY4, U4, r"$\Phi_{1122}$")]
    ):
        for i, model in enumerate(("qe", *MODELS)):
            vals = [
                p["qe"]["center"]["physics"][key]
                if model == "qe"
                else p["models"][model]["central"]["physics"][key]
                for p in pairs
            ]
            if k == 0:
                vals = np.abs(vals)
            axes[k].bar(
                x + (i - 1.5) * 0.2, vals, 0.2, label=LABELS[model], color=COLORS[model]
            )
        axes[k].axhline(0, color="#666", lw=0.6)
        axes[k].set(
            xticks=x,
            xticklabels=tick,
            xlabel="Matched finite-q branch (Gamma mode 8)",
            ylabel=unit,
            title=label,
        )
    axes[0].legend(fontsize=8, ncol=2)
    save(fig, "ws2_dft_couplings.png")
    fig, axes = plt.subplots(1, 2, figsize=(11, 4), layout="constrained")
    for k, metric in enumerate(("cubic", "quartic")):
        for i, m in enumerate(MODELS):
            v = d["metrics"][m][metric]
            axes[k].bar(
                np.arange(3) + (i - 1) * 0.24,
                [v["mae"], v["rmse"], v["maxae"]],
                0.24,
                label=LABELS[m],
                color=COLORS[m],
            )
        axes[k].set(
            xticks=np.arange(3),
            xticklabels=["MAE", "RMSE", "MaxAE"],
            ylabel=U3 if k == 0 else U4,
            title="Third derivative" if k == 0 else "Fourth derivative",
        )
    axes[0].legend(fontsize=8)
    save(fig, "ws2_dft_errors.png")
    fig, axes = plt.subplots(1, 2, figsize=(11, 4), layout="constrained")
    for k, (field, truth) in enumerate(
        [("ml_stage1_q_thz", "qe_stage1_q_thz"), ("ml_pes_q_thz", "qe_pes_q_thz")]
    ):
        rows = [r for r in d["comparisons"] if r["model"] == "tece"]
        axes[k].plot(x, [r[truth] for r in rows], "o-", color=COLORS["qe"], label="QE")
        for m in MODELS:
            rows = [r for r in d["comparisons"] if r["model"] == m]
            axes[k].plot(
                x, [r[field] for r in rows], "o--", color=COLORS[m], label=LABELS[m]
            )
        axes[k].set(
            xticks=x,
            xticklabels=tick,
            ylabel="Frequency (THz)",
            title="Stage1 phonon frequency"
            if k == 0
            else "PES axis curvature (central window)",
        )
    axes[0].legend(fontsize=8)
    save(fig, "ws2_dft_frequencies.png")
    fig, axes = plt.subplots(1, 2, figsize=(11, 4.2), layout="constrained")
    for model in MODELS:
        rr = [
            r for r in d["mode_rows"] if r["model"] == model and not r["gamma_acoustic"]
        ]
        axes[0].scatter(
            [r["qe_frequency_thz"] for r in rr],
            [r["ml_frequency_thz"] for r in rr],
            s=10,
            alpha=0.35,
            label=LABELS[model],
            color=COLORS[model],
        )
        iso = sorted(r["overlap_squared"] for r in rr if r["subspace_dimension"] == 1)
        axes[1].plot(
            iso,
            np.arange(1, len(iso) + 1) / len(iso),
            label=LABELS[model],
            color=COLORS[model],
        )
    axes[0].plot([0, 14], [0, 14], "k:", lw=1)
    axes[0].set(
        xlabel="QE frequency (THz)",
        ylabel="MLFF frequency (THz)",
        title="36 q points; translational Gamma modes excluded",
    )
    axes[1].set(
        xlabel="Squared eigenvector overlap",
        ylabel="Cumulative fraction",
        title="Isolated modes only",
    )
    axes[1].legend(fontsize=8)
    save(fig, "ws2_dft_full_grid.png")
    fig, axes = plt.subplots(1, 2, figsize=(11, 4), layout="constrained")
    for m in ("qe", *MODELS):
        residual = [
            p["qe"]["center"]["center_fit_rmse_ev_supercell"] * 1000
            if m == "qe"
            else p["models"][m]["central"]["center_fit_rmse_ev_supercell"] * 1000
            for p in pairs
        ]
        axes[0].plot(x, residual, "o-", color=COLORS[m], label=LABELS[m])
    axes[0].set(
        xticks=x,
        xticklabels=tick,
        yscale="log",
        ylabel="Fit RMSE (meV/supercell)",
        title="Fit residual is not model-vs-DFT error",
    )
    axes[0].legend(fontsize=8)
    for m in MODELS:
        axes[1].plot(
            x,
            [p["maps"][m]["q_overlap_squared"] for p in pairs],
            "o-",
            label=LABELS[m],
            color=COLORS[m],
        )
    axes[1].set(
        xticks=x,
        xticklabels=tick,
        ylim=(0.97, 1.002),
        ylabel="Squared overlap",
        title="Selected-pair mode matching",
    )
    save(fig, "ws2_dft_fit_and_mapping.png")
    # A raw-energy even contrast separates mixed even terms from the large
    # harmonic background; no polynomial fit or phase convention is required.
    m6 = next(p for p in pairs if p["rank"] == 3)
    fig, ax = plt.subplots(figsize=(8, 3.7), layout="constrained")
    for m in ("qe", *MODELS):
        grid = (
            np.array(m6["qe"]["center_grid_ev"])
            if m == "qe"
            else np.array(m6["models"][m]["grid_ev"])[2:7, 2:7]
        )
        grid = grid - grid[2, 2]
        values = [0.0]
        for j in (1, 0):
            contrast = (
                (grid[0, j] + grid[4, j] + grid[0, 4 - j] + grid[4, 4 - j]) / 4
                - (grid[2, j] + grid[2, 4 - j]) / 2
                - (grid[0, 2] + grid[4, 2]) / 2
            )
            values.append(1000 * contrast)
        ax.plot([0, 0.5, 1], values, "o-", label=LABELS[m], color=COLORS[m])
    ax.axhline(0, color="#888", lw=0.7)
    ax.set(
        xlabel=r"$|Q_\Gamma|$ ($\AA\sqrt{amu}$)",
        ylabel="Even mixed energy contrast (meV)",
        title=r"Gamma8-M6: $|Q_q|=1$; raw-energy diagnostic",
        xticks=[0, 0.5, 1],
    )
    ax.legend(fontsize=8)
    save(fig, "ws2_dft_even_mixed.png")
    # Each surface is centered on its own reference energy. No false pointwise MAE.
    first = pairs[0]
    fig = plt.figure(figsize=(11, 7.5))
    xx, yy = np.meshgrid(np.linspace(-1, 1, 5), np.linspace(-1, 1, 5))
    surfaces = {}
    for m in ("qe", *MODELS):
        grid = (
            np.array(first["qe"]["center_grid_ev"])
            if m == "qe"
            else np.array(first["models"][m]["grid_ev"])[2:7, 2:7]
        )
        if m != "qe":
            if first["models"][m]["gamma_sign_to_qe"] < 0:
                grid = grid[:, ::-1]
            sign = first["models"][m]["finite_q_sign_to_qe"]
            if sign is None:
                raise ValueError(
                    "PES comparison needs an explicitly aligned real phase"
                )
            if sign < 0:
                grid = grid[::-1, :]
        surfaces[m] = (grid - grid[2, 2]) * 1000
    zlim = (
        min(float(z.min()) for z in surfaces.values()),
        max(float(z.max()) for z in surfaces.values()),
    )
    for i, m in enumerate(("qe", *MODELS), 1):
        zz = surfaces[m]
        ax = fig.add_subplot(2, 2, i, projection="3d")
        ax.plot_surface(
            xx, yy, zz, cmap="viridis", linewidth=0.2, edgecolor="white", alpha=0.95
        )
        ax.set(
            xlabel=r"$Q_\Gamma$",
            ylabel=r"$Q_q$",
            zlabel="Relative E (meV)",
            title=LABELS[m],
            zlim=zlim,
        )
        ax.view_init(26, -55)
    fig.subplots_adjust(wspace=0.15, hspace=0.25)
    save(fig, "ws2_dft_pes.png")
    first = next(p for p in pairs if p["rank"] == 1)
    if "window_2" in first["qe"]:
        fig, axes = plt.subplots(1, 2, figsize=(11, 3.8), layout="constrained")
        for i, key in enumerate((KEY3, KEY4)):
            vals = [
                first["qe"][k]["physics"][key]
                for k in ("center", "window_1_5", "window_2")
            ]
            if i == 0:
                vals = np.abs(vals)
            axes[i].plot([1, 1.5, 2], vals, "ko-")
            axes[i].set(
                xlabel=r"Fit half-window ($\AA\sqrt{amu}$)",
                xticks=[1, 1.5, 2],
                ylabel=U3 if i == 0 else U4,
                title="DFT strongest pair: cubic"
                if i == 0
                else "DFT strongest pair: quartic",
            )
        save(fig, "ws2_dft_window.png")
    qe_times = [
        p
        for p in d["qe_performance"]
        if p["wall_seconds"] is not None and p["profile"] == "base"
    ]
    perf = []
    for nat in (12, 27):
        v = [p["wall_seconds"] for p in qe_times if p["nat"] == nat]
        if v:
            perf.append(
                f"| QE，{nat} 原子，64 MPI 核 | {len(v)} | {np.median(v):.2f} | {min(v):.2f} - {max(v):.2f} |"
            )
    for m in MODELS:
        perf.append(
            f"| {LABELS[m]}，4 推理线程 | 405 | {sum(p['models'][m]['sum_point_seconds'] for p in d['pairs']) / 405:.4f} | 每点均值；不含装载 |"
        )
    screening = [
        "| 路线 | 六点相对中央拟合最大偏差 | 五对内部 Spearman（对 DFT） |",
        "| --- | --- | --- |",
    ]
    for m in MODELS:
        coarse = np.array([abs(p["models"][m]["six_point_phi122"]) for p in pairs])
        refined = np.array(
            [abs(p["models"][m]["central"]["physics"][KEY3]) for p in pairs]
        )
        screening.append(
            f"| {LABELS[m]} | {100 * np.max(abs(coarse / refined - 1)):.3f}% | {d['metrics'][m]['cubic']['spearman']:.3f} |"
        )
    fig, axes = plt.subplots(1, 2, figsize=(11, 3.8), layout="constrained")
    for m in MODELS:
        coarse = np.array([abs(p["models"][m]["six_point_phi122"]) for p in pairs])
        refined = np.array(
            [abs(p["models"][m]["central"]["physics"][KEY3]) for p in pairs]
        )
        axes[0].plot(
            x, 100 * (coarse / refined - 1), "o-", label=LABELS[m], color=COLORS[m]
        )
        order = np.argsort(np.argsort(-refined)) + 1
        axes[1].plot(x, order, "o--", label=LABELS[m], color=COLORS[m])
    ref = np.array([abs(p["qe"]["center"]["physics"][KEY3]) for p in pairs])
    axes[1].plot(x, np.argsort(np.argsort(-ref)) + 1, "kx-", label="QE-DFT")
    axes[0].set(
        xticks=x,
        xticklabels=tick,
        ylabel="Six-point bias vs 25-point fit (%)",
        title="Selected pairs only",
    )
    axes[1].set(
        xticks=x,
        xticklabels=tick,
        ylabel="Rank within the selected subset",
        yticks=np.arange(1, len(pairs) + 1),
        title="Final cubic strength ranking",
    )
    axes[1].invert_yaxis()
    axes[1].legend(fontsize=8)
    save(fig, "ws2_dft_screening.png")
    memory = [s for s in d.get("memory_samples", []) if s["process_count"] == 64]
    memory_note = "未取得可用节点内存采样。"
    if memory:
        rss = max(s["sum_rss_kib"] for s in memory) / 2**20
        pss = [s["sum_pss_kib"] / 2**20 for s in memory if s["sum_pss_kib"] is not None]
        memory_note = f"取得 {len(memory)} 次各含 64 个 pw.x 进程的采样，最大观察 RSS 汇总 {rss:.2f} GiB。"
        if pss:
            memory_note += f"可用 PSS 汇总最大观察值 {max(pss):.2f} GiB；这是瞬时采样值，不是全作业峰值。"
    geometry = ["| 参考／Stage1 | 面内晶格 a / Å | 相对 QE |", "| --- | --- | --- |"]
    for m in ("qe", *MODELS):
        a = d["structures"][m]["a_A"]
        geometry.append(
            f"| {LABELS[m]} | {a:.6f} | {100 * (a / d['structures']['qe']['a_A'] - 1):+.3f}% |"
        )
    cubic_rows = [
        "| 路线 | 三阶 MAE | RMSE | MaxAE | 平均相对误差 |",
        "| --- | --- | --- | --- | --- |",
    ]
    quartic_rows = ["| 路线 | 四阶 MAE | RMSE | MaxAE |", "| --- | --- | --- | --- |"]
    grid_rows = [
        "| 路线 | 321 模态频率 MAE / THz | RMSE / THz | 孤立模重叠中位数 | 最差简并子空间重叠 |",
        "| --- | --- | --- | --- | --- |",
    ]
    for m in MODELS:
        a = d["metrics"][m]["cubic"]
        b = d["metrics"][m]["quartic"]
        g = d["full_grid"][m]
        f = g["frequency_optical_gamma_and_all_finite_q"]
        cubic_rows.append(
            f"| {LABELS[m]} | {a['mae']:.3f} | {a['rmse']:.3f} | {a['maxae']:.3f} | {a['mape_percent']:.2f}% |"
        )
        quartic_rows.append(
            f"| {LABELS[m]} | {b['mae']:.3f} | {b['rmse']:.3f} | {b['maxae']:.3f} |"
        )
        grid_rows.append(
            f"| {LABELS[m]} | {f['mae']:.4f} | {f['rmse']:.4f} | {g['isolated_median_overlap_squared']:.5f} | {g['degenerate_minimum_subspace_overlap_squared']:.5f} |"
        )
    detail = [
        "| DFT 模式对 | DFT 三阶强度 | TECE＋MS | Prophet＋MS | EquiformerV3＋MS |",
        "| --- | --- | --- | --- | --- |",
    ]
    detail4 = [
        "| DFT 模式对 | DFT 四阶 | TECE＋MS | Prophet＋MS | EquiformerV3＋MS |",
        "| --- | --- | --- | --- | --- |",
    ]
    for p in pairs:
        vals = [abs(p["qe"]["center"]["physics"][KEY3])] + [
            abs(p["models"][m]["central"]["physics"][KEY3]) for m in MODELS
        ]
        detail.append(
            f"| Γ8–{NAMES[p['rank'] - 1]} | "
            + " | ".join(f"{v:.3f}" for v in vals)
            + " |"
        )
        vals4 = [p["qe"]["center"]["physics"][KEY4]] + [
            p["models"][m]["central"]["physics"][KEY4] for m in MODELS
        ]
        detail4.append(
            f"| Γ8–{NAMES[p['rank'] - 1]} | "
            + " | ".join(f"{v:.3f}" for v in vals4)
            + " |"
        )
    status = (
        "完整结果"
        if d["complete"]
        else f"阶段快照：{d['completed_pairs']}/5 对，{d['completed_qe_points']}/193 点"
    )
    lines = [
        "# WS₂：声子模式匹配、非线性耦合与 DFT 定量对照",
        f"数据范围：{status}。证据截止 {d['as_of_utc']}。",
        "## 主要结果",
        "本次复用已完成的 6×6 QE 声子，从当前 TECE＋MatterSim 排名前五选取物理通道；三条 MLFF 路线各自在自己的弛豫结构和模态基底上计算，再与匹配的 DFT 模式对比较最终耦合。",
        f"三阶强度的平均相对误差为：TECE＋MS {d['metrics']['tece']['cubic']['mape_percent']:.2f}%，Prophet＋MS {d['metrics']['prophet']['cubic']['mape_percent']:.2f}%，EquiformerV3＋MS {d['metrics']['equiformer-v3']['cubic']['mape_percent']:.2f}%。这组选择由 TECE 排名确定，不能作为所有模型或全候选的无偏精度估计。",
        "三阶接近不能保证四阶正确：Γ8–M6 的 DFT 四阶混合导数为负，而三条 MatterSim Stage2 路线为正。三条路线共享 MatterSim，因此相近误差不足以单独归因于 Stage1 网络。结构变化与模态变化共同参与比较。",
        "排序也有实质变化：在这五对内部，DFT 的 Γ8–K9 排第二，而 TECE＋MS、EquiformerV3＋MS 排第五，Prophet＋MS 排第四。全组 Spearman 分别为 0.3、0.7、0.3（TECE、Prophet、EquiformerV3）。这说明粗筛与自身精算一致，仍不能保证与 DFT 排序一致。",
        "## 1. 五个物理通道的三阶、四阶结果",
        "三阶比较 |Φ122|，消去 Γ 模式整体相位的任意符号；四阶比较有符号 Φ1122，其符号不随单个模式翻转。三阶单位为 meV/(Å³·amu^(3/2))，四阶为 meV/(Å⁴·amu²)。",
        *detail,
        "逐对四阶混合导数：",
        *detail4,
        "![逐对耦合](figures/ws2_dft_couplings.png)",
        *cubic_rows,
        *quartic_rows,
        "![误差](figures/ws2_dft_errors.png)",
        "所有指标只覆盖已有 DFT 标签的选定通道。有限动量分支编号采用 DFT 基底；不同模型的同编号分支可能不是同一物理振动，原始编号和映射保存在随附 JSON/CSV。",
        "本项目 q 点标记：M=(1/2,0)，K=(1/3,1/3)，L=(1/3,2/3)，均为倒格矢分数坐标。PES 使用最小相容超胞：M 为 2×2（12 原子），K/L 为 3×3（27 原子），并非 6×6 的 108 原子超胞。Q 为该超胞上质量范数为 1 的实模坐标；各模型对同一物理通道采用相同超胞大小，表中系数依赖此归一化。",
        "## 2. 全网格频率与本征矢",
        "完整 36 个 q 点用于匹配；精度汇总排除 Γ 的三个平移声学模，保留 Γ 光学模及全部有限 q 模，共 321 个模态。近简并组比较投影子空间，不将任意单支方向作为唯一物理本征矢。",
        *grid_rows,
        "![全网格](figures/ws2_dft_full_grid.png)",
        "三条路线孤立模的最差重叠平方依次为 "
        + "、".join(
            f"{d['full_grid'][m]['isolated_minimum_overlap_squared']:.4f}"
            for m in MODELS
        )
        + "；全网格并非每个单模都能可靠一一对应。此次选定五对的有限 q 模重叠平方均大于 0.984，且选中 Γ 与有限 q 分支不落入当前阈值的近简并组。",
        "![五对频率](figures/ws2_dft_frequencies.png)",
        "右图的 PES 频率来自中央 [-1,1] 坐标区间的轴向拟合。MLFF Stage1 频率分别来自 TECE／Prophet／EquiformerV3，Stage2 轴曲率则全部来自 MatterSim，二者不是同一个势的二阶响应；因此 Γ8–M6、Γ8–M5 等通道出现的明显差值不能只解释为拟合窗口误差。有限窗口及高阶项也可能贡献差异。频率、模式方向和高阶耦合的误差分别报告，不能相互替代。",
        "## 3. 势能面、拟合残差与窗口",
        "![势能面](figures/ws2_dft_pes.png)",
        "四幅图各自减去本方法的 Q=0 能量，Γ 和 M 模式符号按本征矢内积对齐，采用相同纵轴和质量归一化坐标；结构与模态允许等价而不要求原子位移逐点相同。因此这里不把跨图能量差定义成相同构型上的 MLFF 能量 MAE。",
        "同理，不将不同结构／模态上的逐原子力差记作力 MAE。所有 QE 原子力仍完整保存在原始输出中，用于收敛、残余力与位移检查；本报告的 MAE/RMSE 比较对象是匹配通道的最终耦合或频率。",
        "![拟合与映射](figures/ws2_dft_fit_and_mapping.png)",
        "拟合残差衡量约定的 13 项多项式对离散势能面的描述误差；即使残差很小，模型对 DFT 的物理系数误差仍可显著。所有中央拟合的设计矩阵必须为满秩 13。表中导数由该有限窗口拟合的多项式求导得到，不等于已证明收敛到零振幅的精确 Taylor 导数，也不声称这 13 项包含所有允许的高阶单项式。",
        f"按中央设计矩阵伪逆估算，QE 打印能量舍入对三阶与四阶系数的最坏绝对扰动界分别为 {d['central_fit_rounding_bounds']['phi122_absolute']:.5f}、{d['central_fit_rounding_bounds']['phi1122_absolute']:.5f}（各自导数单位），不足以解释 Γ8–M6 的四阶符号差异。此界只覆盖打印舍入，不包含 SCF、窗口截断或模型误差。",
        "![偶次混合项](figures/ws2_dft_even_mixed.png)",
        "对 Γ8–M6，直接取四个 (±QΓ,±Qq) 角点能量的均值，再减去两条轴上 ±Q 的能量均值并加回原点能量，可消去纯轴项及奇次项，剩余主要为 Φ1122·QΓ²·Qq²/4 和更高偶次混合项。此检验不使用多项式拟合；两个非零振幅下 DFT 均为负，三条 MLFF 路线均为正。因此符号差异也存在于原始能量组合，仍须区别于四阶导数已在任意窗口收敛的断言。",
        "![窗口](figures/ws2_dft_window.png)",
        "窗口诊断目前只针对最强 Γ8–M9。其他四对只有中央 5×5 DFT 标签，不据此声称其四阶系数也经过窗口收敛验证。",
        "## 4. DFT 数值收敛与数据来源",
        f"最强通道六点 Φ122：100/1000 Ry、等效 24×24 k 网格 {d['pilot']['phi122_mev_per_A3amu32']['base']:.6f}；120/1200 Ry {d['pilot']['phi122_mev_per_A3amu32']['cutoff']:.6f}；等效 30×30 k 网格 {d['pilot']['phi122_mev_per_A3amu32']['kmesh']:.6f}。截断能引起 {100 * d['pilot']['relative_change']['cutoff']:.4f}% 变化，低于预先设定的 2% 门槛。",
        f"k 网格检查的六点组合在当前打印精度内相等，不代表数学上完全无误差。QE 总能量以 10⁻⁸ Ry 输出，量化步长 {d['qe_print_energy_quantum_ev']:.3g} eV；两个六点三阶估计之差的保守舍入界约 {d['pilot_difference_rounding_absolute_bound_mev']:.3g}（三阶单位）。检查的是组合后的耦合量，原始单点能量并不完全相同。",
        f"另核对了不同声子对中重合的纯 Γ 轴构型：{len(d['shared_gamma_axis_repeat_checks'])} 组、每组 2–3 次独立 QE 计算，最大能量差 {max(r['energy_spread_ev'] for r in d['shared_gamma_axis_repeat_checks']):.3g} eV（受相同输出精度限制）。这些重复物理构型是各对网格原有的轴点，不是控制器重复提交同一任务；193 是作业／网格记录数，并非193个互不相同的原子构型。",
        "参考使用 2026-03-31 WS₂ PZ-LDA 归档，DFPT 100/1000 Ry、30×30×1 k 网格、6×6×1 q 网格；本次从既有力常数导出完整声子网格，没有新增 DFPT。q2r/matdyn 使用 simple 平移 ASR，不能视为二维 ZA 转动求和规则已满足。没有引入 SOC 或非解析 LO–TO 修正。",
        "旧 BFGS 曾因历史重置终止，未满足当时严格弛豫条件，末态残余力约 4.13×10⁻⁴ eV/Å。此次沿用该实际 DFPT 结构，保留这一限制。新势能面所有能量、原子力与输入哈希已核验；坐标检查允许周期边界下的整数晶格平移。",
        *geometry,
        "这是一组对项目既有 PZ-LDA 参考的复现测试。MatterSim 训练标签采用 PBE（部分氧化物／氟化物加 U），与此处硫化物的 PZ-LDA 参考并非同一泛函；势、弛豫结构和模态基底的差异同时参与误差，不能把偏差全部归因于网络拟合。MatterSim 原始论文的训练设置见 [arXiv:2405.04967，S5](https://arxiv.org/html/2405.04967v1#S5)。本轮不追加另一套泛函计算。",
        "## 5. 计算成本与工程结论",
        "| 计算 | 样本点数 | 单点秒数 | 范围／口径 |",
        "| --- | --- | --- | --- |",
        *perf,
        "QE 使用独占节点、64 个物理核 MPI 和 8 个 k 点池；MatterSim 每进程 4 推理线程、权重常驻内存。这里是实际运行配置的成本，不是相同核数下的架构速度比。QE 单点时间含启动与 SCF；MatterSim 点时间不含模型首次装载。",
        "服务器关闭了 Slurm 作业内存统计；计时器记录到的数 MB 是 mpirun 启动器，不能当作 QE 内存。本次另做节点内进程采样，RSS 汇总会重复计入共享页，也不能代表整个作业生命周期的峰值。",
        memory_note,
        f"控制器记录 {d['execution']['unique_slurm_jobs']} 个唯一 QE 作业 ID；逐轮观测的全账号排队／运行节点最大值为 {d['execution']['max_observed_global_nodes']}，单轮最多提交 {d['execution']['max_submitted_in_cycle']} 个。观测到的失败数为 {len(d['execution']['observed_failures'])}。这是控制器轮询记录，非连续调度器轨迹。",
        f"Slurm 记账中的 QE 已完成作业合计分配 {d['execution']['qe_allocated_core_hours']:.2f} 核时，包含启动与收尾开销。三条 MLFF 路线的 1215 点在一个 {d['execution']['mlff_job']['AllocCPUS']} 核作业中完成，作业墙钟 {d['execution']['mlff_job']['ElapsedRaw']} 秒，使用两个各 4 线程的工作进程；该总时间包含装载和收尾，不能与上表的不含装载点均值混用。",
        "## 6. 六点粗筛能解决什么",
        *screening,
        "![粗筛与排序](figures/ws2_dft_screening.png)",
        "上述六点数据由同一批 9×9 MLFF 原始能量抽取，不需要新推理。六点相对于 25 点将候选逐对的评估点数减少 76%；这只是每个候选的点数节省，不含 Stage1 和最终精算。Spearman 仅衡量这五个既定通道内部的排序；并非全候选 top5 召回率。",
        "DFT 在此子集的顺序为 M9 > K9 > L9 > M5 > M6；TECE＋MS 为 M9 > L9 > M6 > M5 > K9。Γ8–K9 的 DFT 三阶强度为 71.607，三条 MLFF 路线约 55.8–55.9，误差约 22%。因此进一步加密同一 MLFF 的网格难以消除主要排序偏差；高通量筛选应保留较宽的入选范围，再针对候选阈值附近和代表性失配通道做有限 DFT 校验。",
        "目前适合继续采用六点全候选粗筛、少量通道补齐中央网格的流程。此次 DFT 仅覆盖选出的五对，能检查这些通道的最终精度，不能证明粗筛在整个候选集上的 DFT top-k 召回率。四阶系数出现符号差异，应作为后续校正的单独目标，而非被三阶平均误差掩盖。",
        "## 可复核数据",
        "[完整证据与来源哈希](acceptance_data/ws2_dft_top5.json)；[逐对数值表](acceptance_data/ws2_dft_top5.csv)。研究计算与原始 QE 输出保留在隔离测试目录，公开稳定包仍只包含两阶段 MLFF 流程。",
    ]
    if not d["complete"]:
        lines.insert(
            2, "本页为未完成的内部快照。待余下 DFT 点完成后，重新核验并生成正式版本。"
        )
    markdown = ""
    previous = ""
    for line in lines:
        separator = (
            "\n" if line.startswith("|") and previous.startswith("|") else "\n\n"
        )
        markdown += separator + line
        previous = line
    (out / "WS2_DFT_COMPARISON.md").write_text(markdown.lstrip() + "\n")
    print(out / "WS2_DFT_COMPARISON.md")


if __name__ == "__main__":
    main()
