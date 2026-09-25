"""Compare the accepted WS2 phonon mesh with traceable monolayer literature.

This is a read-only analysis of the published acceptance JSON. No force or
energy calculation is performed here.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

CM_PER_THZ = 33.356409519815
MODELS = ("qe", "tece", "prophet", "equiformer-v3")
LABELS = {
    "qe": "Archived QE LDA",
    "tece": "TECE",
    "prophet": "Prophet",
    "equiformer-v3": "EquiformerV3",
}
COLORS = {
    "qe": "#333333",
    "tece": "#26778e",
    "prophet": "#d27330",
    "equiformer-v3": "#7556a2",
}
SI_URL = "https://nano.fizyka.pw.edu.pl/_uploads/j.actamat.2022.118299_SI.pdf"
RAMAN_URL = "https://pmc.ncbi.nlm.nih.gov/articles/PMC6065453/"
BANDS_URL = "https://arxiv.org/abs/1109.5499"
PATH = ((0, 0), (1, 0), (2, 0), (3, 0), (2, 2), (1, 1), (0, 0))


def round2(value: float) -> str:
    return f"{value:.2f}"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    evidence = json.loads(args.evidence.read_text())
    if not evidence["complete"] or evidence["material"] != "WS2":
        raise ValueError("The complete, audited WS2 evidence is required")
    output = args.output
    figures = output / "figures"
    data_folder = output / "acceptance_data"
    figures.mkdir(parents=True, exist_ok=True)
    data_folder.mkdir(exist_ok=True)

    rows: dict[str, dict[tuple[int, int], dict[int, dict]]] = {m: {} for m in MODELS}
    qe_reference: dict[tuple[tuple[int, int], int], float] = {}
    seen: set[tuple[str, tuple[int, int], int]] = set()
    for row in evidence["mode_rows"]:
        q = tuple(row["q_index"])
        key = (row["model"], q, row["ml_mode"])
        if key in seen:
            raise ValueError(f"Duplicate model/q/mode entry: {key}")
        seen.add(key)
        qe_key = (q, row["qe_mode"])
        qe_value = row["qe_frequency_thz"]
        if qe_key in qe_reference and abs(qe_reference[qe_key] - qe_value) > 1e-8:
            raise ValueError(f"Inconsistent archived QE frequency: {qe_key}")
        qe_reference[qe_key] = qe_value
        rows[row["model"]].setdefault(q, {})[row["ml_mode"]] = row
        if row["model"] == "tece":
            rows["qe"].setdefault(q, {})[row["qe_mode"]] = row
    if len(seen) != 3 * 36 * 9 or len(qe_reference) != 36 * 9:
        raise ValueError("Expected three complete 36-q by nine-mode MLFF meshes")
    for model in MODELS:
        if len(rows[model]) != 36 or any(
            set(rec) != set(range(1, 10)) for rec in rows[model].values()
        ):
            raise ValueError(f"Incomplete or duplicate q/mode data for {model}")

    def frequency(model: str, q: tuple[int, int], mode: int) -> float:
        rec = rows[model][q][mode]
        key = "qe_frequency_thz" if model == "qe" else "ml_frequency_thz"
        return rec[key] * CM_PER_THZ

    # These four channels are selected by physical mode character and the
    # previously audited eigenvector mapping, not by a closest-frequency rule.
    channels = (
        {"id": "Eprime_G", "label": "E′(Γ)", "q": (0, 0), "qe_modes": (6, 7)},
        {"id": "A1prime_G", "label": "A₁′(Γ)", "q": (0, 0), "qe_modes": (8,)},
        {"id": "LA_M", "label": "LA(M)", "q": (3, 0), "qe_modes": (3,)},
        {"id": "TA_K", "label": "TA(K)", "q": (2, 2), "qe_modes": (1,)},
    )
    literature = {
        "Eprime_G": {
            "lda": 357.2,
            "pbe": 340.8,
            "raman_300k": 357.11,
            "direct": True,
            "source": "Acta Materialia 240 (2022) 118299, SI Tables S1/S3",
        },
        "A1prime_G": {
            "lda": 418.4,
            "pbe": 403.6,
            "raman_300k": 417.15,
            "direct": True,
            "source": "Acta Materialia 240 (2022) 118299, SI Tables S1/S3",
        },
        "LA_M": {
            "lda": 367 / 2,
            "pbe": None,
            "raman_300k": 369 / 2,
            "direct": False,
            "source": "Scientific Reports 8 (2018) 11398, Table 1, 2LA(M) / 2",
        },
        "TA_K": {
            "lda": 296 / 2,
            "pbe": None,
            "raman_300k": 298 / 2,
            "direct": False,
            "source": "Scientific Reports 8 (2018) 11398, Table 1, 2TA(K) / 2",
        },
    }
    selected = []
    for channel in channels:
        q = channel["q"]
        modes = channel["qe_modes"]
        vals = {}
        for model in MODELS:
            mapped = []
            for mode in modes:
                ref = (
                    rows[model][q][mode]
                    if model == "qe"
                    else next(
                        rec for rec in rows[model][q].values() if rec["qe_mode"] == mode
                    )
                )
                if model != "qe" and ref["subspace_minimum_overlap_squared"] < 0.99:
                    raise ValueError(f"Unreliable mode matching: {model}, {q}, {mode}")
                mapped.append(mode if model == "qe" else ref["ml_mode"])
            vals[model] = float(np.mean([frequency(model, q, i) for i in mapped]))
        selected.append(
            {
                **channel,
                "frequencies_cm1": vals,
                "literature": literature[channel["id"]],
            }
        )

    gaps = {}
    for model in MODELS:
        highest_acoustic = max(frequency(model, q, 3) for q in rows[model])
        lowest_optical = min(frequency(model, q, 4) for q in rows[model])
        gaps[model] = {
            "max_acoustic_cm1": highest_acoustic,
            "min_optical_cm1": lowest_optical,
            "gap_cm1": lowest_optical - highest_acoustic,
        }

    audit = {
        "schema": 1,
        "source_audit_sha256": evidence["code_sha256"],
        "source_evidence_sha256": hashlib.sha256(
            args.evidence.read_bytes()
        ).hexdigest(),
        "source_ws2_evidence": str(args.evidence),
        "conversion_cm1_per_thz": CM_PER_THZ,
        "literature": {
            "acta_si": SI_URL,
            "resonant_raman": RAMAN_URL,
            "lda_bands": BANDS_URL,
        },
        "channels": selected,
        "six_by_six_grid_gaps": gaps,
        "scope": "Γ rows are direct harmonic DFT comparisons. M/K values halve reported overtone peaks and are indicative, not independent single-phonon benchmarks.",
    }
    (data_folder / "ws2_literature_phonons.json").write_text(
        json.dumps(audit, ensure_ascii=False, indent=2) + "\n"
    )
    with (data_folder / "ws2_literature_phonons.csv").open("w", newline="") as stream:
        writer = csv.writer(stream, lineterminator="\n")
        writer.writerow(
            [
                "channel",
                "q_index",
                "literature_LDA",
                "literature_PBE",
                "Raman_300K_or_overtone_half",
                *MODELS,
                "literature_direct",
            ]
        )
        for item in selected:
            ref = item["literature"]
            writer.writerow(
                [
                    item["id"],
                    str(item["q"]),
                    ref["lda"],
                    ref["pbe"],
                    ref["raman_300k"],
                    *[item["frequencies_cm1"][m] for m in MODELS],
                    ref["direct"],
                ]
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
    fig, axes = plt.subplots(1, 2, figsize=(11, 4.0), layout="constrained")
    for ax, item in zip(axes, selected[:2]):
        values = [item["frequencies_cm1"][m] for m in MODELS]
        ax.scatter(
            np.arange(4), values, color=[COLORS[m] for m in MODELS], s=85, zorder=4
        )
        ref = item["literature"]
        ax.axhline(
            ref["lda"],
            color="#bb4444",
            ls="--",
            label=f"Literature LDA {ref['lda']:.1f}",
        )
        ax.axhline(
            ref["pbe"],
            color="#438b58",
            ls="--",
            label=f"Literature PBE {ref['pbe']:.1f}",
        )
        ax.plot(
            [0, 3],
            [ref["raman_300k"]] * 2,
            "k:",
            label=f"300 K Raman {ref['raman_300k']:.2f}",
        )
        ax.set(
            xticks=np.arange(4),
            xticklabels=["QE", "TECE", "Prophet", "EquiformerV3"],
            ylabel=r"Frequency (cm$^{-1}$)",
            title=item["label"],
        )
        ax.set_ylim(
            min(values + [ref["lda"], ref["pbe"]]) - 9,
            max(values + [ref["lda"], ref["pbe"]]) + 10,
        )
        ax.legend(fontsize=7.5, loc="lower right")
    fig.savefig(figures / "ws2_literature_gamma.png", bbox_inches="tight")
    plt.close(fig)

    fig, axes = plt.subplots(
        1, 3, figsize=(12.5, 4.1), sharey=True, layout="constrained"
    )
    x = np.arange(len(PATH))
    for ax, model in zip(axes, MODELS[1:]):
        for mode in range(1, 10):
            ax.plot(
                x,
                [frequency("qe", q, mode) for q in PATH],
                color="#444444",
                lw=0.7,
                alpha=0.48,
            )
            ax.plot(
                x,
                [frequency(model, q, mode) for q in PATH],
                color=COLORS[model],
                lw=0.9,
                marker="o",
                ms=2.5,
            )
        ax.scatter(
            [3, 4],
            [literature["LA_M"]["lda"], literature["TA_K"]["lda"]],
            marker="D",
            facecolors="none",
            edgecolors="#b64242",
            s=45,
            zorder=5,
        )
        ax.set(
            xticks=[0, 3, 4, 6],
            xticklabels=["Γ", "M", "K", "Γ"],
            xlabel="6×6 mesh path sample",
            title=LABELS[model],
        )
    axes[0].set_ylabel(r"Frequency (cm$^{-1}$)")
    fig.savefig(figures / "ws2_literature_path.png", bbox_inches="tight")
    plt.close(fig)

    def fmt_reference(value: float | None) -> str:
        return "—" if value is None else round2(value)

    direct_table = [
        "| 模式 | LDA | PBE | Raman | QE | TECE | Prophet | EquiV3 |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for item in selected[:2]:
        ref = item["literature"]
        vals = item["frequencies_cm1"]
        direct_table.append(
            "| "
            + item["label"]
            + " | "
            + " | ".join(
                [
                    fmt_reference(ref["lda"]),
                    fmt_reference(ref["pbe"]),
                    fmt_reference(ref["raman_300k"]),
                    *[round2(vals[m]) for m in MODELS],
                ]
            )
            + " |"
        )
    indirect_table = [
        "| 模式 | LDA 峰/2 | Raman 峰/2 | QE | TECE | Prophet | EquiV3 |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for item in selected[2:]:
        ref = item["literature"]
        vals = item["frequencies_cm1"]
        indirect_table.append(
            "| "
            + item["label"]
            + " | "
            + " | ".join(
                [
                    round2(ref["lda"]),
                    round2(ref["raman_300k"]),
                    *[round2(vals[m]) for m in MODELS],
                ]
            )
            + " |"
        )
    gap_table = [
        "| 路线 | 最高声学模 | 最低光学模 | 间隙 |",
        "| --- | ---: | ---: | ---: |",
    ]
    for model in MODELS:
        gap = gaps[model]
        gap_table.append(
            f"| {LABELS[model]} | {gap['max_acoustic_cm1']:.2f} | {gap['min_optical_cm1']:.2f} | {gap['gap_cm1']:.2f} |"
        )
    gamma_mae = {
        model: {
            reference: float(
                np.mean(
                    [
                        abs(
                            item["frequencies_cm1"][model]
                            - item["literature"][reference]
                        )
                        for item in selected[:2]
                    ]
                )
            )
            for reference in ("lda", "pbe")
        }
        for model in MODELS
    }
    lines = [
        "# WS₂ 声子谱与单层文献结果的定量对照",
        "本报告读取已核验的 36 点 QE 与三条自行弛豫 MLFF 声子数据，所有数值统一换算为 cm⁻¹（1 THz = 33.35640952 cm⁻¹）。表中 TECE、Prophet、EquiV3 均为 Stage1 力常数与 Phonopy 声子结果；MatterSim 只参与后续 Stage2 势能面，不影响本报告的声子频率。不新增声子或 DFT 计算。",
        "## 1. Γ 点：同层数、同模态的直接数据",
        "模式由原子振动方向和已核验的本征矢映射决定：E′ 为 Γ 第 6/7 支的面内双重态，A₁′ 为第 8 支的 S 原子面外振动。模型双重态取两支的平均；没有按频率最近邻临时换支。文献来自 Wilczyński 等的单层 WS₂ 补充材料表 S1/S3：LDA-PW92、PBE 谐波频率及 300 K Raman 实测值。",
        *direct_table,
        "![Γ 点文献对照](figures/ws2_literature_gamma.png)",
        f"这两个 Γ 模的平均绝对差（cm⁻¹）对文献 LDA：QE {gamma_mae['qe']['lda']:.2f}、TECE {gamma_mae['tece']['lda']:.2f}、Prophet {gamma_mae['prophet']['lda']:.2f}、EquiformerV3 {gamma_mae['equiformer-v3']['lda']:.2f}；对文献 PBE：QE {gamma_mae['qe']['pbe']:.2f}、TECE {gamma_mae['tece']['pbe']:.2f}、Prophet {gamma_mae['prophet']['pbe']:.2f}、EquiformerV3 {gamma_mae['equiformer-v3']['pbe']:.2f}。这些只是两项 Γ 模指标，不能推广成整条声子谱的 MAE。",
        "本项目 QE 的晶格常数 3.12073 Å，比文献 LDA 的 3.142 Å 小 0.68%；模型自弛豫值约 3.190–3.192 Å，比该文 PBE 的 3.206 Å 小约 0.43–0.48%。QE 的两个亮模比文献 LDA 高约 3–4 cm⁻¹；三条 MLFF 的值整体更接近文献 PBE 范围。这与结构和泛函差异方向一致，但不能据此确定各自误差的因果比例。归档 QE 结构的 BFGS 未达到严格终止条件，残力约 4.13×10⁻⁴ eV/Å；300 K Raman 还包含温度、衬底和测量条件影响。",
        "## 2. M、K 点：二阶 Raman 的间接参照",
        "另一篇单层 WS₂ 工作的表 1 将 367 cm⁻¹ 计算峰指认为 2LA(M)、296 cm⁻¹ 指认为 2TA(K)；相应实测峰为 369、298 cm⁻¹。下表将峰位置除以 2，用于判断相关声学模量级。它们来自共振二阶过程，可能受声子非谐性和实际散射动量影响，因此不纳入直接声子频率 MAE。M 的 LA 对应 QE 第 3 支；K 选择以面内运动为主的 QE 第 1 支作为 TA 类分支，高对称 K 点的本征态不必具有纯横向极化，因此这项对应属于参考性判断。三模型对所选 QE 单模的重叠平方均超过 0.999。",
        *indirect_table,
        "![路径声子点](figures/ws2_literature_path.png)",
        "图中仅使用现有 6×6 网格上的 Γ–M–K–Γ 七个采样点，黑色是本项目 QE，彩色是对应 MLFF，空心菱形是文献二阶峰折半值。线只连接按频率排序的支序，未做逐段本征矢连续追踪，故交叉处不作分支拓扑判断；它也不是把文献曲线逐点数字化后的全谱误差。",
        "## 3. 声学—光学分隔与解释",
        *gap_table,
        "间隙定义为全 6×6 网格最低第 4 支减去最高第 3 支。Molina-Sánchez 与 Wirtz 的单层 WS₂ LDA 声子谱文字报告约 110 cm⁻¹ 的声学—光学间隙；本项目四条线为约 106–114 cm⁻¹，均保持这一整体结构。四条线在 Γ、M、K 个别模态的绝对频率仍可差十几到二十几 cm⁻¹，因此间隙接近不能替代逐模检验。",
        "本项目仅使用 Phonopy 平移声学求和规则，未证明二维 ZA 的转动规则；Γ 邻域 ZA 曲率不用于此处打分。文献中的结构优化、赝势、真空、k 网格、SOC/非解析修正和 Raman 温度不同。上述数据应按模态、参考泛函与测量类型阅读，不能把全部差值归因于模型本身。已有完整 321 模态 QE 对照见[WS₂ DFT 定量报告](WS2_DFT_COMPARISON.md)。",
        "## 来源与可复算数据",
        f"1. Wilczyński 等，*Acta Materialia* 240 (2022) 118299，[原作者补充材料，表 S1/S3]({SI_URL})；DOI: 10.1016/j.actamat.2022.118299。",
        f"2. Liu 等，*Scientific Reports* 8 (2018) 11398，[单层 WS₂ 深紫外 Raman，表 1]({RAMAN_URL})；DOI: 10.1038/s41598-018-29587-0。",
        f"3. Molina-Sánchez 与 Wirtz，*Physical Review B* (2011)，[单层 WS₂ 声子谱]({BANDS_URL})；arXiv:1109.5499。",
        "[逐点数值与来源类型](acceptance_data/ws2_literature_phonons.csv)；[来源、模式选择及计算值 JSON](acceptance_data/ws2_literature_phonons.json)。",
    ]
    markdown = ""
    previous = ""
    for line in lines:
        markdown += (
            "\n" if line.startswith("|") and previous.startswith("|") else "\n\n"
        ) + line
        previous = line
    path = output / "WS2_LITERATURE_PHONONS.md"
    path.write_text(markdown.lstrip() + "\n")
    print(path)


if __name__ == "__main__":
    main()
