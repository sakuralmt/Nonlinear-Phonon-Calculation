"""Build the supplementary acceptance report from inspected run evidence."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import shutil
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np


MODELS = ("tece", "prophet", "equiformer-v3")
LABELS = ("TECE", "Prophet", "EquiformerV3")
COLORS = ("#26778e", "#d27330", "#7556a2")


def read(path):
    return json.loads(path.read_text())


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--evidence", type=Path, required=True)
    p.add_argument("--comparison", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    a = p.parse_args()
    a.output.mkdir(parents=True, exist_ok=True)
    data_dir = a.output / "acceptance_data"
    data_dir.mkdir(exist_ok=True)
    figures = a.output / "figures"
    figures.mkdir(exist_ok=True)
    comparison = read(a.comparison / "matched_comparisons.json")
    archive = read(a.comparison / "archived_dft_comparison.json")
    campaign = read(a.evidence / "campaign_audit_accepted.json")
    details = read(a.evidence / "release_details_accepted.json")
    parallel = read(a.evidence / "parallel_accepted.json")
    orbit = {}
    old_orbit = {}
    orbit_sources = {}
    for model in MODELS:
        for mat in ("mose2", "ws2"):
            key = f"{model}/{mat}"
            corrected_path = a.evidence / f"accepted-orbit-{model}-{mat}.json"
            original_path = a.evidence / f"orbit-check-{model}-{mat}.json"
            corrected, original = read(corrected_path), read(original_path)
            orbit[key], old_orbit[key] = corrected["summary"], original["summary"]
            if (
                orbit[key]["failed"]
                or orbit[key]["checks"] < 87
                or not set(original["checks"]) <= set(corrected["checks"])
            ):
                raise ValueError(f"Unpassed real-orbit acceptance: {key}")
            orbit_sources[key] = {
                "original_checks_fully_retested": True,
                "corrected_sha256": hashlib.sha256(
                    corrected_path.read_bytes()
                ).hexdigest(),
                "original_sha256": hashlib.sha256(
                    original_path.read_bytes()
                ).hexdigest(),
            }
    total_orbit_checks = sum(v["checks"] for v in orbit.values())
    precision_metrics = read(a.evidence / "paired_precision_metrics.json")
    for path in a.comparison.glob("*.csv"):
        shutil.copy2(path, data_dir / path.name)
    for name in ["archived_dft_comparison.json"]:
        shutil.copy2(a.comparison / name, data_dir / name)
    metric = {k: v["metrics"] for k, v in comparison.items()}
    manifest = {
        "version": "1.0.1",
        "contract": 5,
        "energy_accumulation": "atomic_float32_sum_float64_v2",
        "campaign": campaign,
        "model_comparison": metric,
        "orbit_acceptance": orbit,
        "orbit_sources": orbit_sources,
        "paired_precision_metrics": precision_metrics,
        "old_float32_orbit_failures": old_orbit,
        "parallel": parallel,
        "run_details": details,
        "historical_dft_metrics": archive["runs"],
        "precision_probe": read(a.evidence / "precision-probe.json"),
        "cross_basis_probe": read(a.evidence / "cross-basis-ws2-m6.json"),
        "limits": archive["limitations"],
    }
    (a.output / "acceptance_v5_1.json").write_text(
        json.dumps(manifest, indent=2, allow_nan=False) + "\n"
    )
    plt.rcParams.update(
        {
            "font.size": 10,
            "axes.spines.top": False,
            "axes.spines.right": False,
            "figure.dpi": 150,
        }
    )
    fig, axes = plt.subplots(2, 2, figsize=(10, 7), layout="constrained")
    for c, mat in enumerate(("mose2", "ws2")):
        for r, (field, title, unit) in enumerate(
            [
                (
                    "refined_phi122_norm",
                    "Third-order Gamma norm",
                    "meV / (A^3 amu^1.5)",
                ),
                (
                    "refined_phi1122_trace",
                    "Fourth-order Gamma trace",
                    "meV / (A^4 amu^2)",
                ),
            ]
        ):
            ax = axes[r, c]
            maximum = 0
            for model, label, color in zip(MODELS[1:], LABELS[1:], COLORS[1:]):
                rows = comparison[f"{mat}_tece_vs_{model}"]["channels"]
                x = [
                    row["reference_" + field]
                    for row in rows
                    if row["reference_" + field] is not None
                    and row["candidate_" + field] is not None
                ]
                y = [
                    row["candidate_" + field]
                    for row in rows
                    if row["reference_" + field] is not None
                    and row["candidate_" + field] is not None
                ]
                ax.scatter(
                    x, y, label=f"{label} (n={len(x)})", color=color, s=28, alpha=0.8
                )
                maximum = max(maximum, max(x), max(y))
            ax.plot([0, maximum], [0, maximum], color="#777777", lw=1, ls="--")
            ax.set(
                title=f"{mat.upper()} | {title}",
                xlabel="TECE + MatterSim",
                ylabel=f"Other Stage1 + MatterSim\n{unit}",
            )
            ax.legend(fontsize=8)
    fig.savefig(figures / "matched_couplings.png")
    plt.close(fig)
    fig, axes = plt.subplots(1, 2, figsize=(10, 3.6), layout="constrained")
    for ax, mat in zip(axes, ("mos2", "wse2")):
        x = np.arange(3)
        for shift, geometry, label, color in [
            (-0.18, "shared_dft", "Archived shared input", "#9fbcc7"),
            (0.18, "model_relaxed", "Own relaxed", "#26778e"),
        ]:
            vals = [
                archive["runs"][f"{mat}/{geometry}/{model}"][
                    "finite_q_frequency_mae_thz"
                ]
                for model in MODELS
            ]
            bars = ax.bar(x + shift, vals, 0.34, label=label, color=color)
            ax.bar_label(bars, fmt="%.3f", fontsize=8)
        ax.set(
            xticks=x,
            xticklabels=LABELS,
            ylabel="Finite-q frequency MAE vs archived QE / THz",
            title=mat.upper(),
        )
        ax.set_ylim(0, 0.66)
        ax.legend(fontsize=8)
    fig.savefig(figures / "historical_frequency_geometry.png")
    plt.close(fig)
    fig, ax = plt.subplots(figsize=(9, 3.6), layout="constrained")
    x = np.arange(6)
    keys = list(orbit)
    ax.bar(
        x - 0.18,
        [old_orbit[k]["max_phi122_difference_mev"] for k in keys],
        0.34,
        label="Previous float32 sum",
        color="#bd7043",
    )
    ax.bar(
        x + 0.18,
        [orbit[k]["max_phi122_difference_mev"] for k in keys],
        0.34,
        label="Corrected float64 sum",
        color="#26778e",
    )
    ax.axhline(0.2, color="#444444", linestyle="--", label="Unchanged tolerance: 0.2")
    ax.set(
        xticks=x,
        xticklabels=[k.replace("/", "\n") for k in keys],
        ylabel="Maximum equivalent-pair third-order difference",
        title=f"All original samples rechecked; {total_orbit_checks} corrected comparisons",
    )
    ax.legend(fontsize=8)
    fig.savefig(figures / "orbit_precision.png")
    plt.close(fig)
    lines = [
        "# 六方二维声子筛选：1.0.1 补充验收与完整对照",
        "",
        "本报告修正初版验收范围不足的问题。正式方案保持三条路线：**Phonopy＋TECE／Prophet／EquiformerV3 Stage1，各接 MatterSim Stage2**；每模型只用自行弛豫结构，Γ 只取光学模。MoSe₂、WS₂ 是两种验收材料，共六组实验。未新增 DFT 或 MD。",
        "",
        "## 原计划验收清单",
        "",
        "| 工作项 | 状态与证据 |",
        "|---|---|",
        "| 实际原子对称性与 q 轨道 | 已实现；60°/120°、1/2/3 原子、BN、TMD、Janus、降对称结构测试；不固定六类。 |",
        "| Γ 及有限 q 模态协变 | 已修复漏检 Γ 的问题；每个操作及时间反演均检查，失败退回 q/−q；六组候选均保持 324 对。 |",
        f"| 实际 MLFF 等价点耦合 | 每组起始12个代表通道，覆盖全部6个q轨道及其成员；追加旧排名样本后共{total_orbit_checks}/{total_orbit_checks}通过，覆盖全部原522次检查。不是全部324候选的穷举。 |",
        "| 全量六点与入选精算 | 六组均 324/324 候选完成；前20通道展开分量及简并伙伴，5×5 满秩；最强五通道 9×9。 |",
        "| 故障恢复／分片／来源隔离 | 自动测试与1/4工作进程实算对照通过；新累加协议入检查点身份，不混用旧点。 |",
        "| 软件稳定性与可读性 | 服务器39项测试通过；本地38通过、Torch依赖测试跳过1项；Ruff、指定入口mypy及干净wheel安装检查。 |",
        "| 模型比较与历史DFT | 本报告及CSV提供全网格频率、模态重叠、可靠通道的三/四阶差异。 |",
        "| CPU成本归因 | 已有同节点108原子基准及profile；见初版记录的成本章节。 |",
        "| 历史数据边界 | 四条WSe₂历史四阶原始网格未找回；明确缺失，不以新DFT补齐。历史QE结构来源不完全独立核实。 |",
        "",
        "## 1. 数值修正及复验",
        "",
        "原先只测两个 M 点不足以支持全网格等价性结论。扩展检查发现旧 float32 总能量求和有明显顺序依赖。两个最差案例在相同进程中复算：三阶差约 0.671／0.977；只提升累加精度后为 0.0191／0.0143；完整 float64 诊断为约 10⁻⁸。实际修复仅将逐原子能量在最终求和前提升为 float64，保留原始 float32 模型和特征；不修改权重或安装环境。本文三阶单位为meV/(Å³·amu³ᐟ²)，四阶单位为meV/(Å⁴·amu²)。",
        "",
        "| 路线/材料 | 修正前失败/87 | 修正后失败/检查数 | 最大能量差/eV | 最大三阶差 |",
        "|---|---:|---:|---:|---:|",
    ]
    for k, v in orbit.items():
        lines.append(
            f"| {k} | {old_orbit[k]['failed']} | {v['failed']}/{v['checks']} | {v['max_energy_difference_ev']:.3g} | {v['max_phi122_difference_mev']:.5f} |"
        )
    lines += [
        "",
        "预设容差始终为 10⁻⁴ eV 和三阶 0.2，未调宽。上述检查比较同一模型结构上的对称等价位移，不要求跨模型的位移完全重合。",
        "数值修正改变了部分近零通道的先后顺序；复验既覆盖新排序选样，也补回了全部旧样本，避免用换样本掩盖失败。生产前20通道仅MoSe₂/TECE改变一项：line_p2_m8__Gamma_6替代M_p6_m9__Gamma_7_8，分量精算由24对变23对。其余五组入选通道集合不变。",
        "",
        "![全轨道数值精度复验](figures/orbit_precision.png)",
        "",
        f"独立1与4工作进程比较 {parallel['energy_points_compared']} 个点，最大能量差 {parallel['max_abs_energy_delta_ev']:.3g} eV，最大三阶代理差 {parallel['max_abs_phi122_proxy_delta_mev']:.3g}。新数值运行目录为服务器 `runs-accepted`，旧 `runs-final` 完整保留。Stage1 沿用已算力常数和模态，仅用修正后的算法重新核验并生成来源可追溯的候选，没有重复弛豫。",
        "",
        "Γ 双重态指两个零动量偏振。排名是各分量 Γ–q–(−q) 三阶系数的向量范数，不是 Γ₁–Γ₂–q 的三声子系数。有限 q 下 Φ112 对应 QΓ²Qq，受动量守恒禁止；保留为数值／拟合误差诊断，绝不用于排名。动量允许不代表点群一定允许。",
        "",
        "在修正前后都完成精算的相同通道上，四阶迹的模型间MAE也减小，说明原先部分模型差异实际混有总能量累加误差。这仍然不是对DFT准确性的证明。",
        "",
        "| 材料/模型比较 | 相同通道数 | 修正前四阶迹MAE | 修正后四阶迹MAE |",
        "|---|---:|---:|---:|",
        *[
            f"| {key} | {value['same_channels']} | {value['old_quartic_trace_difference_mae']:.3f} | {value['new_quartic_trace_difference_mae']:.3f} |"
            for key, value in precision_metrics.items()
        ],
        "",
        "## 2. 新材料完整性和计算量",
        "",
        "| Stage1/材料 | 六点候选 | 精算模式对 | 9×9模式对 | 独立能量点 | 累计推理秒 |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for k, v in campaign["runs"].items():
        lines.append(
            f"| {k} | {v['candidate_pairs']}/324 | {v['refined_pairs']} | {v['audited_pairs']} | {v['all_unique_energy_points']} | {v['sum_point_compute_seconds']:.1f} |"
        )
    lines += [
        "",
        "所有中心拟合均为13列满秩，来源哈希、点数、有限值及唯一性均已核对。累计推理秒是各进程工作量之和，不是作业墙钟。作业采用CPU、每任务16 CPU与4×4线程分片，数组最多同时3项；加独立验证任务也低于十节点上限。",
        "",
        "## 3. 三条路线的可靠逐通道比较",
        "",
        "以本征矢重叠及近简并子空间建立映射。排除有限 q 简并单支、Γ多重态不能完整匹配及重叠不足的通道。三阶比较完整 Γ 向量范数；四阶比较完整 Γ 的 Φ1122 对角迹，不能用对角元素范数代替旋转不变量。有限窗口拟合仍受高阶项影响。下表是模型间差异，不是 DFT 误差。",
        "",
        "| 材料/比较 | 共同精算通道 | 三阶 MAE / RMSE | 四阶迹 MAE / RMSE |",
        "|---|---:|---:|---:|",
    ]
    for k, m in metric.items():
        t = m["refined_phi122_norm"]
        f = m["refined_phi1122_trace"]
        lines.append(
            f"| {k} | {t['count']} | {t['mae']:.3f} / {t['rmse']:.3f} | {f['mae']:.3f} / {f['rmse']:.3f} |"
        )
    lines += [
        "",
        "三阶单位：meV/(Å³·amu³ᐟ²)；四阶单位：meV/(Å⁴·amu²)。",
        "",
        "![匹配后三四阶对照](figures/matched_couplings.png)",
        "",
        "WS₂ 的 M 第6支与 Γ 第6/7光学双重态是值得保留的反例：旧总能量结果下 TECE、Prophet 路线三阶范数分别为30.116、44.369，虽然有限 q 模重叠平方为0.98487。交叉诊断给出“TECE结构＋Prophet模式”44.316、“Prophet结构＋TECE模式”30.148。这里约47%的差异主要随模式方向变化，几何交换仅改变约0.03–0.05。该诊断只解释模型间差异，不说明哪个更接近DFT；正式生产没有增加共用结构路线。",
        "",
        "## 4. 历史QE全网格对照：明确区分结构线",
        "",
        "共读取两材料×两条历史结构线×三模型×36q×9支＝3888条模式记录。主流程采用自行弛豫；共用结构仅作离线历史参照。QE结构来源未完全核实，WSe₂ Γ声学处理有问题，因此下表使用35个有限q点。完整CSV同时保留Γ数据，不能把这些差异写成严格同结构误差。",
        "",
        "| 材料/模型 | 自弛豫频率 MAE / RMSE THz | 历史共用结构 MAE THz | 自弛豫孤立模重叠中位数 |",
        "|---|---:|---:|---:|",
    ]
    for mat in ("mos2", "wse2"):
        for model in MODELS:
            own = archive["runs"][f"{mat}/model_relaxed/{model}"]
            shared = archive["runs"][f"{mat}/shared_dft/{model}"]
            lines.append(
                f"| {mat}/{model} | {own['finite_q_frequency_mae_thz']:.4f} / {own['finite_q_frequency_rmse_thz']:.4f} | {shared['finite_q_frequency_mae_thz']:.4f} | {own['isolated_finite_q_median_overlap_squared']:.6f} |"
            )
    lines += [
        "",
        "![历史结构影响](figures/historical_frequency_geometry.png)",
        "",
        "自弛豫便于高通量，但“候选排名相近”不等于“频率及耦合数值相同”。本数据中自弛豫相对历史 QE 的频率差明显增大，应作为结构敏感性记录，不能套用共用结构下更好的误差数字。",
        "",
        "## 5. 历史高耦合声子对：三阶及四阶",
        "",
        "逐对从旧本征矢恢复实际质量归一化实位移，能量显式按Ry转换。MoS₂五条、WSe₂一条原始网格可重拟合；WSe₂其余四条只保留归档三阶幅值，四阶留空。K类旧坐标的实模范数约1/√2，实际拟合采样窗口不同；四阶的窗口差异尤其需要保留。下表只列当前采用的自弛豫结构方向对照，完整CSV另含历史共用结构。",
        "",
        "| 材料/旧QE方向 | QE 三阶 / 四阶 | TECE＋MS | Prophet＋MS | Equiformer＋MS |",
        "|---|---:|---:|---:|---:|",
    ]
    couplings = list(
        csv.DictReader((a.comparison / "archived_dft_couplings.csv").open())
    )
    for mat in ("mos2", "wse2"):
        codes = list(archive["legacy_fits"][mat])
        for code in codes:
            rows = [
                next(
                    r
                    for r in couplings
                    if r["material"] == mat
                    and r["geometry"] == "model_relaxed"
                    and r["model"] == model
                    and r["qe_pair"] == code
                )
                for model in MODELS
            ]

            def fmt(x):
                return f"{float(x):.3f}" if x not in ("", None) else "缺失"

            short = code.split("__")[1]
            vals = [f"{fmt(r['ml_abs_phi122'])} / {fmt(r['ml_phi1122'])}" for r in rows]
            lines.append(
                f"| {mat}/{short} | {fmt(rows[0]['qe_abs_phi122'])} / {fmt(rows[0]['qe_phi1122'])} | {' | '.join(vals)} |"
            )
    lines += [
        "",
        "每格依次为三阶幅值／四阶Φ1122。这里比较同一高重叠声子方向的最终耦合，不比较未经对齐的绝对总能量，也不把不同结构／模态基底硬称为同一DFT势能面。有限q简并分量依旧标记基底相关。",
        "",
        "## 6. 粗筛选择与物理限制",
        "",
        "保留默认“所有候选六点＋前20完整通道5×5＋最强五通道9×9”。历史MoS₂/WSe₂六组自弛豫9×9数据回放中，筛20对中心精算前20的召回为19/20或20/20；筛30均为20/20。提高召回可设 `--top-channels 30`，但小样本不保证任意材料的召回。六点不能给出可靠四阶系数。",
        "",
        "在相同108原子、16线程节点基准中，Prophet热推理约2.862s，TECE约1.871s；profile指向模型与力求导，而非适配器或邻居表准备，是主要差别。分级筛选后Stage1与Stage2处于同一时间量级。对WS₂最强通道，四阶系数从中心5×5到9×9仍有明显变化，软件验收通过不代表四阶物理收敛。Phonopy平移ASR也不等于二维ZA转动求和规则已满足。",
        "",
        "## 数据与复现",
        "",
        "- [汇总、来源哈希与修正诊断](acceptance_v5_1.json)",
        "- [逐通道与逐模式CSV目录](acceptance_data/)",
        "- [历史DFT重拟合及来源](acceptance_data/archived_dft_comparison.json)",
        "- [初版基准与归档回放记录](VALIDATION.md)",
        "",
        "离线脚本位于 `validation/compare_relaxed_campaign.py`、`compare_archived_dft.py` 和 `build_acceptance_report.py`，不包含Stage3输入生成或作业提交。机器相关路径通过参数传入。39项测试包含解析势、动量诊断、Γ协变退回、相位／简并子空间、检查点和浮点求和梯度回归。",
        "",
        "服务器证据根目录：`/home/lmtsakura/qiyan_shared/testing/hex-phonopy-stable`。正式重算及首轮等价性：1019600；不同分片复验：1019605；最终源码测试：1019608／1019615；补齐原等价性样本：1019609。数值重算源码快照为code-acceptance，最终接口测试快照为code-verify；计算结果及所有旧文件保留。",
        "",
    ]
    (a.output / "ACCEPTANCE.md").write_text("\n".join(lines))
    print(a.output / "ACCEPTANCE.md")


if __name__ == "__main__":
    main()
