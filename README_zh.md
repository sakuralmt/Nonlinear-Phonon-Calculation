# 非线性声子计算（`npc`）

`npc` 用机器学习力场寻找晶格振动之间较强的非谐耦合。具体的问题是：一个 Γ 点光学模式发生位移后，哪些有限动量模式的恢复力变化最明显？程序先计算结构和谐波模式，再沿两个模式采样势能面（PES），提取三阶和四阶系数，供 DFT 比较或后续动力学建模使用。默认使用 **TECE 计算结构和模式，MatterSim 计算非谐势能面**；已有的 QE–DFPT 模式也可以直接导入。

这套方法最初围绕六角 **TMDC 单层**开发，自动对称性筛选的详细实算验证也主要来自 TMDC。h-BN、蓝磷和石墨烯后来做过选定模式对的 DFT–MLFF 势能面比较；其他六角材料的完整筛选还没有经过同等详细的验证。晶格形状之外，还需核对实际原子排列、模式和简并关系。详见[对称性筛选的验证范围](docs/SYMMETRY_SCOPE.md)。

[English](README.md) · [快速开始](docs/QUICKSTART_zh.md) · [安装与权重](docs/INSTALL.md) · [方法架构](ARCHITECTURE.md) · [迁移到 1.0.4](docs/MIGRATION.md) · [稳定库与公开版比较](docs/CODE_COMPARISON_20261029.md) · [本轮发布验证](docs/STABILITY_1.0.4.md)

## 安装

需要 Python **3.10 或更新版本**。以下示例安装公开的 1.0.4 版；已有源码时可从创建环境开始。

```bash
git clone https://github.com/sakuralmt/Nonlinear-Phonon-Calculation.git
cd Nonlinear-Phonon-Calculation
git checkout v1.0.4
python3.12 --version
python3.12 -m venv .venv
. .venv/bin/activate
python -m pip install .
npc --version
npc --help
```

核心依赖包括 ASE 和 Phonopy **2.38.0**。模型依赖和权重按[安装说明](docs/INSTALL.md)另外准备。MatterSim 阶段使用 `mattersim==1.2.1` 和 **v1.0.0-5M** 权重；包版本与权重版本分别记录。

开发和回归测试使用源码检出或源码发行包：

```bash
python -m pip install '.[test]'
python -m pytest -q tests
```

源码和源码发行包包含示例结构、回归数据与报告。wheel 提供计算包。使用下方的仓库内数据路径时，请先取得源码。

## 选择计算路线

| 路线 | Stage1 输入 | Stage2 内容 | 入口 |
| --- | --- | --- | --- |
| 默认 MLFF 路线 | QE 格式单层结构、固定 TECE 源码与权重 | MatterSim 粗筛、精算及可选 audit | `npc stage1 --model tece`；`npc stage2 …` |
| 替代 MLFF Stage1 | 固定 Prophet OAME-MBD 或 EquiformerV3+DeNS-OAM | 相同的 MatterSim Stage2 | `npc stage1 --model prophet` 或 `equiformer-v3` |
| 已有 DFT 模式 | 已核验的 QE–DFPT JSON 导出及原 DFT 结构 | 不做 MLFF 弛豫，接入原筛选流程 | `npc stage1 --model qe-dft` |
| 同构型 DFT 对照 | 已选导入模式及已有 QE PES 网格 | 在完全相同的构型上计算 MatterSim，比较对齐能量和力 | `npc stage2 reference` |

公开命令包括 `stage1`、`stage2 screen|refine|audit|reference` 和 `status`，也可通过 `python -m nonlinear_phonon_calculation.cli` 调用。`status` 查看已保存结果的数量。QE 提交和实时队列监控由研究执行层处理。

## 从 MoS₂ 开始

下例使用源码自带的 `ibrav=0` 显式胞 MoS₂ 起始几何；Stage1 仍会按所选模型重新弛豫。Stage1 在 TECE 环境运行，Stage2 在 MatterSim 环境运行；两个环境都要安装本包。请按示例路径准备模型源码与权重。

```bash
npc stage1 --model tece \
  --structure docs/reference_data/pbe_20260925/full_model_structures/mos2-tece.scf.inp \
  --checkpoint weights/TECE-OAM-RRA-1.0.pt \
  --source-root model-sources/tace \
  --output-dir runs/mos2/tece/stage1

npc stage2 screen \
  --mode-pairs-json runs/mos2/tece/stage1/mode_pairs.selected.json \
  --structure runs/mos2/tece/stage1/relax/optimized_structure.scf.inp \
  --checkpoint weights/mattersim-v1.0.0-5M.pth \
  --output-dir runs/mos2/tece/stage2
```

[快速开始](docs/QUICKSTART_zh.md)继续给出 `refine`、可选 `audit`、续算和已有 DFT 路线。这些命令会实际计算；检查安装时先使用 `--help` 和不需要模型的 DFT 导入示例。

## 每个阶段做什么

1. **Stage1：** 所选 MLFF 对单层作受限弛豫，再计算默认 6×6×1 q 网格的谐波模式；DFT 导入路线则保留原结构和模式。Γ 平移模通过质量加权重叠识别并排除，有限 q 的全部分支保留。只有结构与本征矢协变性核验通过后才归并 q 点。
2. **Screen：** 每个候选计算六点，`QΓ=±1`、`Qq=−1,0,+1` Å√amu。三阶差分值只是初筛代理量。
3. **Refine：** 默认选取前 20 个完整耦合组，在中央 **5×5** 网格上拟合；范围 `|Q|≤1`，步长 0.5。
4. **Audit，可选：** 将最强五个精算组扩展到 **9×9**，范围 `|Q|≤2`，步长仍为 0.5。它是**扩大振幅窗口**，不是在同一窗口内加密。

q 网格决定考察哪些波矢，PES 网格决定沿两个模式取哪些振幅。三阶描述 Γ 位移对目标曲率的一阶调制，混合四阶给出二次调制。解释结果时应同时看系数符号、势能面和实际振幅下的恢复力。

“具体模式对”指定一个 Γ 方向和一个有限 q 方向；“耦合组”可能包括多个 Γ 简并方向和有限 q 简并伙伴。因此，20 个组不一定只有 20 对。完整 Γ 分量向量的范数，也不能替代完整有限 q 简并张量。文档中的三原子 TMD 示例有 324 个候选，这不是所有材料的固定数量。

## 输出和续算

Stage1 输出 `phonon_dataset.json`、`mode_pairs.selected.json`；MLFF 弛豫还输出 `relax/optimized_structure.scf.inp` 及 `relax_summary.json`。DFT 导入有独立来源信息，已选对照可输出 `mode_pairs.reference.json`。

标准 screen/refine/audit 检查点保存能量，不是每个点都保存完整原子力。Stage2 在 `pairs/` 下保存这些逐点检查点，并生成 `screen_ranking.json`、`refine_ranking.json`、`audit_ranking.json` 和选择记录。对照入口输出 `reference_result.json`。

中断后，使用相同输入和输出目录重跑原命令即可续算。程序核对结构、模式、权重、网格、归一化与能量求和方式，复用符合原设置的已完成点。改变设置时使用新目录，保留旧结果供比较。screen/refine/audit 可按完整模式对分片；reference 每材料使用一个可续算进程。详见[迁移与续算](docs/MIGRATION.md)。

## 模式和系数怎样解释

原胞含 N 个原子时，每个 q 点有 3N 条声子支：TMDC 三原子原胞有九支，h-BN、蓝磷和石墨烯的两原子原胞有六支。较大的采样超胞用来满足波矢的周期条件。

拟合采用约定的 13 项能量多项式。坐标单位为 Å√amu，能量为 eV/超胞；Taylor 导数按阶乘关系换算。系数描述给定模式方向和振幅窗口内的响应，完整非谐张量和零振幅导数还需要相应的计算。标准拟合使用能量，reference 中的原子力另用于比较。

不同 Stage1 路线使用各自弛豫的结构和模式，比较系数前需要先匹配模式或简并子空间。同构型的相对能量与原子力比较单独列出。当前静态参数可供动力学模型使用；光场驱动力、阻尼、寿命和输运需要另外的物理输入。

[方法说明](ARCHITECTURE.md)给出位移归一化、拟合方程与数值设置。[验证范围](docs/SYMMETRY_SCOPE.md)区分 TMDC 的实际筛选、其他材料的选定模式对研究，以及解析势和弹簧模型的软件测试。

## 已公开的基准材料

仓库提供 **WS₂、MoS₂ 和 WSe₂ 的 GGA-PBE 比较**、可编辑报告及公开数据。1.0.4 发布时沿用这些已完成的研究。自动筛选与等价点的系统验证主要来自 MoSe₂ 和 WS₂，范围见[补充验证记录](docs/ACCEPTANCE.md)。

| 归档比较 | 回答的问题 | 详细材料 |
| --- | --- | --- |
| PBE DFT 与 TECE/Prophet/Equiformer Stage1＋MatterSim | 谐波模式、每材料五个匹配非谐通道，以及明确同构型的诊断 | [PBE 参考](docs/PBE_REFERENCE.md) |
| TECE＋TECE、Prophet＋Prophet、EquiformerV3＋EquiformerV3 | 独立模块提供同模型 PES 和固定 PBE 构型诊断 | [同模型基准](docs/FULL_MODEL_BENCHMARK.md) |
| 固定 PBE 构型上的 GRACE、DPA、Eqnorm | 可选 Stage2 诊断，不是默认 CLI 新后端 | [小模型基准](docs/SMALL_MODEL_BENCHMARK.md) |
| 旧 LDA 与 WS₂ 专题 | 保留原结构、单位及边界的历史参考 | [LDA 比较](docs/MODEL_DFT_COMPARISON.md)、[WS₂ 补充](docs/WS2_DFT_COMPARISON.md) |

PBE 研究包含 579 个选定 QE 点，以及已归档的 543 构型 DFT Stage1＋MatterSim 基线。WS₂ Γ8–M9 的 17×17 检查只是一条通道的证据，不表示所有模式均完成密网格收敛。不同结构、窗口、泛函或硬件的误差和时间不能混成一个精度或加速比。

[图文报告](output/pdf/tmd_gga_pbe_mlff_latex_report.pdf) · [公开参考数据](docs/reference_data/pbe_20260925/) · [报告源码](reports/pbe_three_materials/) · [历史验证记录](docs/VALIDATION.md)

## 代码与引用

可安装代码位于 `nonlinear_phonon_calculation/` 与 `mlff_modepair_workflow/`，测试位于 `tests/`。报告生成器和参考数据不进入 wheel。同模型、小模型基准模块各有独立输入和依赖；隔离研究用的 QE 控制器不是公开 CLI 功能。

项目代码采用 [MIT 许可证](LICENSE)。模型源码、权重和训练数据保留上游许可证与引用要求。本项目负责模式投影与工作流，预训练网络架构和基础训练应归功于相应模型作者。
