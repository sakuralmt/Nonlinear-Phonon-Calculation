#!/usr/bin/env python3
"""Build the illustrated MoS2/WSe2 MLFF comparison PDF from reviewed figures."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from PIL import Image as PILImage
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.styles import ParagraphStyle
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.pdfgen import canvas
from reportlab.platypus import Paragraph, Table, TableStyle


PAGE_W, PAGE_H = landscape(A4)
LEFT = 42
RIGHT = PAGE_W - LEFT
WIDTH = RIGHT - LEFT
INK = colors.HexColor("#193442")
TEAL = colors.HexColor("#226b89")
RUST = colors.HexColor("#b97843")
MUTED = colors.HexColor("#59717e")
LIGHT = colors.HexColor("#eaf2f4")
FONT_PATH = Path("/System/Library/Fonts/Supplemental/Arial Unicode.ttf")


def register_fonts() -> None:
    if not FONT_PATH.is_file():
        raise FileNotFoundError(f"Chinese PDF font not found: {FONT_PATH}")
    pdfmetrics.registerFont(TTFont("ArialUnicode", str(FONT_PATH)))
    pdfmetrics.registerFontFamily("ArialUnicode", normal="ArialUnicode",
                                  bold="ArialUnicode", italic="ArialUnicode",
                                  boldItalic="ArialUnicode")


BODY = ParagraphStyle("body", fontName="ArialUnicode", fontSize=10.2, leading=15.7,
                      textColor=INK, wordWrap="CJK")
SMALL = ParagraphStyle("small", parent=BODY, fontSize=8.6, leading=12.7)
CAPTION = ParagraphStyle("caption", parent=BODY, fontSize=8.2, leading=11.6,
                         textColor=MUTED)


def header(c: canvas.Canvas, label: str, page: int) -> None:
    c.setFillColor(TEAL)
    c.rect(0, PAGE_H - 11, PAGE_W, 11, fill=1, stroke=0)
    c.setFont("ArialUnicode", 8.2)
    c.setFillColor(MUTED)
    c.drawString(LEFT, PAGE_H - 31, "MLFF 静态声子与高阶耦合 | MoS₂ / WSe₂")
    c.drawRightString(RIGHT, PAGE_H - 31, "2026-09-24")
    c.setStrokeColor(colors.HexColor("#d7e3e7"))
    c.line(LEFT, PAGE_H - 39, RIGHT, PAGE_H - 39)
    c.line(LEFT, 35, RIGHT, 35)
    c.setFont("ArialUnicode", 8)
    c.drawString(LEFT, 22, label)
    c.drawRightString(RIGHT, 22, f"{page:02d} / 10")


def title(c: canvas.Canvas, value: str, subtitle: str = "") -> float:
    c.setFillColor(INK)
    c.setFont("ArialUnicode", 18.5)
    c.drawString(LEFT, PAGE_H - 68, value)
    y = PAGE_H - 82
    if subtitle:
        c.setFillColor(MUTED)
        c.setFont("ArialUnicode", 9.2)
        c.drawString(LEFT, y, subtitle)
        y -= 13
    return y - 6


def para(c: canvas.Canvas, y: float, content: str, style=BODY, indent: float = 0,
         gap: float = 7) -> float:
    block = Paragraph(content, style)
    _, height = block.wrap(WIDTH - indent, y - 40)
    block.drawOn(c, LEFT + indent, y - height)
    result = y - height - gap
    if result < 43:
        raise ValueError(f"PDF page content overflowed: {content[:65]} (y={result:.1f})")
    return result


def bullet(c: canvas.Canvas, y: float, content: str, style=BODY) -> float:
    c.setFillColor(TEAL)
    c.circle(LEFT + 4, y - 7.2, 2.3, fill=1, stroke=0)
    return para(c, y, content, style, indent=15, gap=5)


def figure(c: canvas.Canvas, y: float, path: Path, caption: str,
           max_height: float = 246) -> float:
    if not path.is_file():
        raise FileNotFoundError(path)
    with PILImage.open(path) as image:
        size = image.size
    width = WIDTH
    height = min(max_height, width * size[1] / size[0])
    if height < width * size[1] / size[0]:
        width = height * size[0] / size[1]
    x = LEFT + (WIDTH - width) / 2
    c.drawImage(str(path), x, y - height, width=width, height=height,
                preserveAspectRatio=True, mask="auto")
    y = y - height - 8
    return para(c, y, caption, CAPTION, gap=9)


def table(c: canvas.Canvas, y: float, rows: list[list[str]], widths: list[float],
          row_height: float | None = None) -> float:
    cells = [[Paragraph(str(item), SMALL) for item in row] for row in rows]
    t = Table(cells, colWidths=widths, rowHeights=row_height, hAlign="LEFT")
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), LIGHT),
        ("GRID", (0, 0), (-1, -1), .35, colors.HexColor("#d5e0e4")),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("LEFTPADDING", (0, 0), (-1, -1), 7),
        ("RIGHTPADDING", (0, 0), (-1, -1), 7),
        ("TOPPADDING", (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
    ]))
    _, height = t.wrapOn(c, WIDTH, y)
    t.drawOn(c, LEFT, y - height)
    result = y - height - 10
    if result < 43:
        raise ValueError("PDF page table overflowed")
    return result


def metric(c: canvas.Canvas, x: float, y: float, width: float, value: str, label: str,
           color=TEAL) -> None:
    c.setFillColor(LIGHT)
    c.roundRect(x, y - 82, width, 82, 8, fill=1, stroke=0)
    c.setFillColor(color)
    c.setFont("ArialUnicode", 24)
    c.drawString(x + 14, y - 37, value)
    c.setFillColor(INK)
    c.setFont("ArialUnicode", 9.4)
    c.drawString(x + 14, y - 61, label)


def flow(c: canvas.Canvas, y: float) -> float:
    items = [
        ("Stage1", "Phonopy + ASR\n36 q, 486 对"),
        ("粗筛", "全部模式对\n每对 6 点"),
        ("完整拟合", "前 30 通道\n每对补 19 点"),
        ("精查", "少量强对\n9×9 窗口诊断"),
        ("DFT", "同位移复核\n当前暂停"),
    ]
    gap = 18
    box = (WIDTH - gap * (len(items) - 1)) / len(items)
    for i, (head, body) in enumerate(items):
        x = LEFT + i * (box + gap)
        c.setFillColor(LIGHT if i < 4 else colors.HexColor("#f4f0e9"))
        c.roundRect(x, y - 96, box, 96, 7, fill=1, stroke=0)
        c.setFillColor(TEAL if i < 4 else RUST)
        c.setFont("ArialUnicode", 12)
        c.drawCentredString(x + box / 2, y - 26, head)
        c.setFont("ArialUnicode", 9.0)
        c.setFillColor(INK)
        for j, line in enumerate(body.split("\n")):
            c.drawCentredString(x + box / 2, y - 53 - 17 * j, line)
        if i < len(items) - 1:
            c.setStrokeColor(TEAL)
            c.setLineWidth(1.5)
            c.line(x + box + 3, y - 48, x + box + gap - 4, y - 48)
            c.line(x + box + gap - 8, y - 44, x + box + gap - 4, y - 48)
            c.line(x + box + gap - 8, y - 52, x + box + gap - 4, y - 48)
    return y - 112


def build(figures: Path, campaign: dict, replay: dict, output: Path) -> None:
    register_fonts()
    runs = [d for gs in replay["materials"].values() for ms in gs.values() for d in ms.values()]
    if (len(runs) != 16 or sum(d["grid_count"] for d in runs) != 7776
            or any(d["screening"]["six_point_h1"]["reference_top20_recall_by_proxy_top_n"]["30"] != 20
                   for d in runs)):
        raise ValueError("Reviewed replay evidence does not match the report claims")
    output.parent.mkdir(parents=True, exist_ok=True)
    c = canvas.Canvas(str(output), pagesize=(PAGE_W, PAGE_H), pageCompression=1)
    c.setTitle("MoS2/WSe2 MLFF 全量测试与低成本粗筛定量分析")
    c.setAuthor("Codex scientific workflow analysis")

    header(c, "证据范围与核心结论", 1)
    y = title(c, "MoS₂ / WSe₂：全量模型测试与低成本粗筛", "静态声子及非线性势能面 | 图解定量报告")
    y = para(c, y, "<b>决策结论：</b>在已完成的 16 条 MatterSim 全量运行中，±1 Å√amu 的六点三阶代理用于 486 对全量粗筛，再把前 30 个物理通道补齐到中央 25 点，均覆盖中央 25 点参照的前 20 通道。", BODY)
    top = y - 7
    cards = [("16 条", "完整 Stage1 + Stage2 线"), ("7,776", "经核验的 9×9 势能网格"),
             ("20 / 20", "16 条线的前列最差召回"), ("约 9.1%", "相对全量 9×9 的能量点数")]
    card_gap = 9
    card_w = (WIDTH - 3 * card_gap) / 4
    for i, (value, label) in enumerate(cards):
        metric(c, LEFT + i * (card_w + card_gap), top, card_w, value, label,
               color=RUST if i == 3 else TEAL)
    y = top - 101
    y = bullet(c, y, "四种 Stage1：Prophet、EquiformerV3、TECE、EquFlashV2；两种材料、共用输入与各模型自行弛豫结构分开分析。全部 16 条 MatterSim Stage2 均完成 486 对 × 81 点。")
    y = bullet(c, y, "TECE 的 MoS₂ 历史 QE 频率误差最小；WSe₂ 的 EquiformerV3 更兼顾频率和 CPU 成本。但 Stage1 频率更接近 QE，不代表与 MatterSim 势能面更自洽。")
    y = bullet(c, y, "粗筛召回只针对同一 MatterSim 中央 25 点拟合。历史 QE 高阶标签仅两材料各五个已选方向；不能给出 DFT 全域召回或四阶准确度。", SMALL)
    y = bullet(c, y, "<b>单位：</b>图表中的三阶 Φ122 数值为 meV/(Å³·amu^(3/2))，四阶 Φ1122 为 meV/(Å⁴·amu²)；图中坐标轴为节省空间简写为 meV。实模 Q 为 Å√amu，频率为 THz。", SMALL)
    c.showPage()

    header(c, "Stage1 谐波比较", 2)
    y = title(c, "1. Stage1：谐波频率、模式与 CPU 代价", "共用输入结构；对历史 QE 的描述性参照")
    y = figure(c, y, figures / "01_stage1.png",
               "图 1  全 36×9 模式的匹配频率 MAE、孤立模本征矢重叠²中位数及 MoS₂ Stage1 CPU 耗时。", 300)
    y = table(c, y, [
        ["Stage1", "MoS₂ MAE (THz)", "WSe₂ MAE (THz)", "适用判断"],
        ["Prophet", "0.1645", "0.1625", "已完整回归的默认线"],
        ["EquiformerV3", "0.0822", "0.1079", "WSe₂ 成本/准确度折中"],
        ["TECE", "0.0660", "0.1259", "MoS₂ 谐波对照最优"],
        ["EquFlashV2 CPU", "0.0898", "0.1061", "CPU 兼容路径需 CUDA 核验"],
    ], [145, 145, 145, WIDTH - 435])
    y = para(c, y, "<b>物理边界：</b>归档 QE 几何未独立核实；WSe₂ 旧 QE 的 Γ 声学支未使用本轮相同 ASR。Phonopy 平移 ASR 不自动修复二维转动求和规则，故近 Γ 的 ZA 色散不能由这张 6×6 网格宣称收敛。", SMALL)
    c.showPage()

    header(c, "结构敏感性", 3)
    y = title(c, "2. 自行弛豫结构改变频率与耦合幅值", "结构效应单独成线，不能与固定结构模型误差混排")
    y = figure(c, y, figures / "02_structure.png",
               "图 2  同一模型在共用输入结构与自身弛豫结构上的频率变化及最强三阶耦合比值。", 315)
    y = bullet(c, y, "MoS₂ 的全网格频率平均变化为 0.50–0.57 THz，WSe₂ 为 0.41–0.43 THz；均大于部分固定结构的模型间差异。")
    y = bullet(c, y, "最强 |Φ122| 从 MoS₂ 的约 99 降至约 89 meV，从 WSe₂ 的约 31.2 降至约 26 meV。三种先进模型的面内弛豫尺度相对共用输入增加约 1.9%/2.35%（MoS₂/WSe₂）。")
    y = bullet(c, y, "Prophet 自行弛豫线的 ±0.01 与 ±0.005 Å 力差分频率最大变化为 MoS₂ 0.02447、WSe₂ 0.00703 THz；只读复查表明最大差都在 Γ9 光学支。精细频率引用必须注明此步长敏感性。", SMALL)
    c.showPage()

    header(c, "Stage2 强耦合通道", 4)
    y = title(c, "3. 换 Stage1 后，强通道排名保持稳定", "MatterSim Stage2 权重固定；跨模型先映射本征矢和 Γ 子空间")
    y = figure(c, y, figures / "03_ranking.png",
               "图 3  相对 Prophet Stage1，其他 Stage1 的物理通道前列重合及可靠通道 Spearman 相关。", 305)
    y = bullet(c, y, "共用结构上，MoS₂ 三个替代模型的前 20 通道全部与 Prophet 重合；WSe₂ 为 20、20、19 个。前 5 均为 5/5。")
    y = bullet(c, y, "全域相关只有 0.86–0.92，说明弱通道次序敏感；WSe₂/EquiformerV3 有 10 个单模重叠²不足 0.9 的有限 q 通道，均未进入任一前 30。")
    y = bullet(c, y, "Γ 近简并组比较的是完整三阶向量范数。有限 q 目标支若近简并，单支排名仍可能受基底选择影响；图中前列通过现有重叠检查，不能外推所有材料。", SMALL)
    c.showPage()

    header(c, "模型配对与计算成本", 5)
    y = title(c, "4. 谐波更接近 QE，不等于势能面更自洽", "固定 MatterSim Stage2 后的二阶曲率诊断与模型成本")
    y = figure(c, y, figures / "04_curvature.png",
               "图 4  MatterSim 中央 25 点拟合的有限 q 频率与 Stage1 的差，以及逐对耗时累加。", 300)
    y = bullet(c, y, "MoS₂ 的频率差中位数：Prophet 0.210、EquiformerV3 0.332、TECE 0.306、EquFlashV2 0.271 THz；WSe₂ 分别为 0.166、0.203、0.197、0.215 THz。该差还包含有限窗口拟合和能量量化，不能单独归因于某个模型。", SMALL)
    y = table(c, y, [
        ["旧同模式基 Stage2", "Prophet 逐对耗时累加", "MatterSim 逐对耗时累加", "前 20 通道重合"],
        ["MoS₂", "137.2 小时", "3.06 小时", "16 / 20"],
        ["WSe₂", "118.4 小时", "2.48 小时", "19 / 20"],
    ], [120, 205, 205, WIDTH - 530])
    y = para(c, y, "这些是旧自写 Stage1 相同模式文件上的两模型比较，不能与新 Phonopy 模式文件逐点混用。不同节点/实现使耗时比不是普适速度律，但本轮没有证据支持再加一遍 Prophet 全网格粗筛。", SMALL)
    c.showPage()

    header(c, "历史 QE 五对", 6)
    y = title(c, "5. 旧 QE 五个已选方向支持强通道，但不支持全域精度", "同结构位移定义仍不完整；图中仅为映射后的方向强度")
    y = figure(c, y, figures / "05_archived_dft.png",
               "图 5  MoS₂、WSe₂ 各五个旧 QE 已选强方向与四种 Stage1 + MatterSim 的 |Φ122|。虚线为等值。", 310)
    y = table(c, y, [
        ["材料", "QE 首位 |Φ122|", "MatterSim 四线首位范围", "五方向幅值相对差中位数"],
        ["MoS₂", "106.13 meV", "98.62–99.28 meV", "5.65–5.93%"],
        ["WSe₂", "30.58 meV", "31.13–31.27 meV", "0.21–0.78%"],
    ], [100, 185, 220, WIDTH - 505])
    y = para(c, y, "<b>不可推断：</b>五对本来就是旧筛选中的强方向，且 QE 模式基与 v3 实模归一化需映射；因此没有 486 对的 DFT 排名、完整三/四阶 MAE/RMSE 或同位移能量/力 MAE/RMSE。MoS₂ Γ8–K9 的幅值差约 12%，说明排名一致也不保证定量系数准确。", SMALL)
    c.showPage()

    header(c, "六点粗筛模板", 7)
    y = title(c, "6. 六点中心差分：±1 的精度与召回最均衡", "16 条全量 MatterSim 网格离线回放；参照为中央 25 点、13 参数拟合")
    y = figure(c, y, figures / "06_stencils.png",
               "图 6  四种六点步长的三阶误差、通道秩相关和代理前 30 对参照前 20 的最差召回。", 300)
    y = table(c, y, [
        ["h (Å√amu)", "|ΔΦ122| 均值", "通道相关均值 / 最低", "代理前 30 的最差召回"],
        ["0.5", "0.825 meV", "0.859 / 0.794", "16 / 20"],
        ["1.0", "0.0665 meV", "0.949 / 0.933", "20 / 20"],
        ["1.5", "0.118 meV", "0.896 / 0.847", "20 / 20"],
        ["2.0", "0.135 meV", "0.886 / 0.849", "20 / 20"],
    ], [125, 150, 225, WIDTH - 500])
    y = para(c, y, "±0.5 对 MatterSim float32 总能量的量化台阶过敏；较大位移更易混入高阶非局域项。16 条结果只有两种材料，±1 是本批次推荐值，不能当成普适最佳步长。", SMALL)
    c.showPage()

    header(c, "分阶段代价", 8)
    y = title(c, "7. 六点全量 + 前 30 通道补点：约十分之一的调用量", "所有 Γ 分量保留；选中通道展开为 34–37 个模式对")
    y = figure(c, y, figures / "07_cost.png",
               "图 7  代理选样数量对参照前 20 的召回和实际能量调用点数的影响。阴影是 16 条线的范围。", 300)
    y = table(c, y, [
        ["方案（每 486 对）", "能量点数", "相对 9×9 全量", "所能给出的结果"],
        ["81 点全量", "39,366", "100%", "完整外圈/窗口诊断"],
        ["25 点全量", "12,150", "30.9%", "与现有主拟合完全相同"],
        ["6 点全量", "2,916", "7.41%", "三阶排序代理"],
        ["6 点全量 + 前 30 补至 25", "3,562–3,619", "9.05–9.19%", "这批数据覆盖参照前 20"],
    ], [215, 110, 115, WIDTH - 440])
    y = para(c, y, "六点本来就在中央 25 点内，每个被选模式对只需再补 19 点。点数约降 11 倍不等于实测墙钟降 11 倍；模型装载、批量推理、并行和 I/O 尚需真实 CPU 试运行。", SMALL)
    c.showPage()

    header(c, "四阶数值稳定性", 9)
    y = title(c, "8. 四阶系数仍受能量量化与拟合窗口影响", "不能由六点三阶粗筛直接导出定量 Φ1122")
    y = figure(c, y, figures / "08_fourth_order.png",
               "图 8  九点中心差分、7×7 与 9×9 多项式拟合，相对中央 25 点四阶结果的误差与秩相关。", 315)
    y = bullet(c, y, "7,776 个 MatterSim 网格中，2,778 个（35.7%）被标记为有重复能量值。代表网格的最小非零间隔与 float32 总能量精度一致。")
    y = bullet(c, y, "九点 ±1 模板的四阶 MAE 平均约 0.462 meV；将拟合窗口扩大至 7×7/9×9，四阶 MAE 平均约 0.510/0.554 meV，绝对值秩相关仅 0.751/0.715。", SMALL)
    y = bullet(c, y, "MoS₂ Γ8–M9 的 Φ1122 从中央 5×5 约 16.28 变到完整 9×9 约 6.98 meV/(Å⁴·amu²)。精查必须报告窗口敏感性；新的 DFT 四阶复核仍暂停。", SMALL)
    c.showPage()

    header(c, "推荐流程与证据边界", 10)
    y = title(c, "9. 建议的生产筛选路线与验收边界", "当前已完成离线数值验证；分阶段 Stage2 调度尚待实现")
    y = flow(c, y)
    y = bullet(c, y, "<b>第一道门槛：</b>在独立测试工作树实现 6 点检查点、通道范数排名和前 30 通道补 19 点。验证结构/权重/模式哈希，确保续算不重复或漏点。")
    y = bullet(c, y, "<b>第二道门槛：</b>与现有完整 25 点排名逐对核对，再测同一 CPU 节点上的真实墙钟、峰值内存与并发。若第 20–30 名得分接近或补点后排名变化，扩大到前 40。")
    y = bullet(c, y, "<b>物理门槛：</b>只对最终少量强对检查 9×9 窗口、四阶系数和 Stage1/Stage2 二阶曲率。DFT Stage3 按用户要求仍暂停，恢复后必须用同结构、同实际位移的能量和力检验真正 DFT 召回。", SMALL)
    y = para(c, y, "<b>分组修正：</b>新 Stage1 默认以 0.01 THz 判断 Γ 光学近简并。旧 0.1 THz 会误将 Prophet 自行弛豫 WSe₂ 的 Γ6/7（约 7.084 THz）与 Γ8（约 7.177 THz）合并，使通道数由 270 变为 216；本报告重算所有对比但不改写旧 486 对原始结果。", SMALL)
    y = para(c, y, "<b>可复核材料：</b>仓库 docs/full_model_and_coarse_screening_analysis_20260924.md；reports/phonopy_model_campaign_gamma001_20260924.json；reports/coarse_screening_replay_20260924.json；scripts/analyze_coarse_screening_campaign.py。原始网格保留于 huairou 的 testing/prophet-stage12-v3/validation，并有本地只读打包副本。", SMALL)
    c.showPage()
    c.save()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--campaign", type=Path, required=True)
    parser.add_argument("--replay", type=Path, required=True)
    parser.add_argument("--figure-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    build(args.figure_dir, json.loads(args.campaign.read_text()),
          json.loads(args.replay.read_text()), args.output)
    print(args.output)


if __name__ == "__main__":
    main()
