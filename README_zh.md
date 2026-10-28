# 非线性声子计算（`npc`）

`npc` 面向**六方单层材料的静态 Γ–q–(−q) 非谐耦合筛选**：用 Phonopy 构造谐波模式，用预训练机器学习力场计算冻结模式势能面（PES），再拟合约定的三、四阶项。默认路线为 **TECE Stage1＋MatterSim Stage2**；也可导入已经完成的 QE–DFPT 结构和模式，保留原 DFT 几何。

这里计算的是模式投影势能面。公开命令不启动 QE、不运行分子动力学，也不把耦合排名当作实验泵浦阈值。模型权重需要另行下载。

[English](README.md) · [快速开始](docs/QUICKSTART_zh.md) · [安装与权重](docs/INSTALL.md) · [方法架构](ARCHITECTURE.md) · [迁移到 1.0.4](docs/MIGRATION.md) · [稳定库与公开版比较](docs/CODE_COMPARISON_20261029.md) · [本轮发布验证](docs/STABILITY_1.0.4.md)

## 安装核心包

执行 pip 前先确认已有 **Python 3.10 或更新版本**；示例使用 `python3.12`。下面选择正式发布后的 `v1.0.4` 标签；已经取得该版本源码时，可跳过前三条命令。

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

核心包声明 Python 3.10+，Phonopy 固定为 **2.38.0**。这一步安装工作流，不会安装全部模型或下载权重。正式计算前，需在兼容环境中安装所选模型，并取得 [INSTALL.md](docs/INSTALL.md) 列出的精确权重。MatterSim Stage2 要求 `mattersim==1.2.1` 和 **v1.0.0-5M** 权重；上游默认的 1M 不能直接替代。

开发和回归测试使用源码检出或源码发行包：

```bash
python -m pip install '.[test]'
python -m pytest -q tests
```

源码发行包包含公开回归数据、参考数据和报告源文件。wheel 只包含两个 Python 包，不包含完整数据和报告目录。安装 wheel 后检查 `npc --help`，验证的是核心安装，不代表归档物理基准或模型推理已重新完成。

## 选择计算路线

| 路线 | Stage1 输入 | Stage2 内容 | 入口 |
| --- | --- | --- | --- |
| 默认 MLFF 路线 | QE 格式单层结构、固定 TECE 源码与权重 | MatterSim 粗筛、精算及可选 audit | `npc stage1 --model tece`；`npc stage2 …` |
| 替代 MLFF Stage1 | 固定 Prophet OAME-MBD 或 EquiformerV3+DeNS-OAM | 相同的 MatterSim Stage2 | `npc stage1 --model prophet` 或 `equiformer-v3` |
| 已有 DFT 模式 | 已核验的 QE–DFPT JSON 导出及原 DFT 结构 | 不做 MLFF 弛豫，接入原筛选流程 | `npc stage1 --model qe-dft` |
| 同构型 DFT 对照 | 已选导入模式及已有 QE PES 网格 | 在完全相同的构型上计算 MatterSim，比较对齐能量和力 | `npc stage2 reference` |

完整公开命令为 `stage1`、`stage2 screen|refine|audit|reference` 和 `status`。模块入口是 `python -m nonlinear_phonon_calculation.cli`。`status` 读取已保存结果的数量，不是集群调度器或实时作业监控器。

## 默认路线示例

下例使用源码自带的 `ibrav=0` 显式胞 MoS₂ 起始几何；Stage1 仍会按所选模型重新弛豫。Stage1 在 TECE 环境运行，Stage2 在 MatterSim 环境运行；两个环境都要安装本包。路径只是检出目录下的示例，不表示权重已经提供。

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

**q 网格**采样动量，**PES 网格**采样两个模式振幅。网格变密或拟合残差变小，不会改变力场自身的能量函数。

“具体模式对”指定一个 Γ 方向和一个有限 q 方向；“耦合组”可能包括多个 Γ 简并方向和有限 q 简并伙伴。因此，20 个组不一定只有 20 对。完整 Γ 分量向量的范数，也不能替代完整有限 q 简并张量。文档中的三原子 TMD 示例有 324 个候选，这不是所有材料的固定数量。

## 输出和续算

Stage1 输出 `phonon_dataset.json`、`mode_pairs.selected.json`；MLFF 弛豫还输出 `relax/optimized_structure.scf.inp` 及 `relax_summary.json`。DFT 导入有独立来源信息，已选对照可输出 `mode_pairs.reference.json`。

标准 screen/refine/audit 检查点保存能量，不是每个点都保存完整原子力。Stage2 在 `pairs/` 下保存这些逐点检查点，并生成 `screen_ranking.json`、`refine_ranking.json`、`audit_ranking.json` 和选择记录。对照入口输出 `reference_result.json`。

续算时，重复**相同输入、相同输出目录的同一条命令**。结构、模式文件、权重、网格、归一化和能量累加协议必须一致。改变这些输入后，不要改写保存的身份记录或混入旧点。screen/refine/audit 支持按完整模式对分片；reference 每材料使用一个可续算进程。详见[迁移与续算](docs/MIGRATION.md)。

## 物理适用范围

- 已验证的几何流程面向非磁性六方单层，要求显式真空和周期边界；不是通用三维变胞优化器。
- 原胞决定每个 q 的 `3N` 条声子支。采样超胞只是满足周期相容性，不会增加原胞声子支。
- 拟合使用约定的 **13 个单项式**，不是完整二元四阶 Taylor 展开或完整声子顶角。拟合目标是能量，不是能量与力联合拟合。
- 单位为 Å、amu、eV/超胞、eV/Å、THz；实模式坐标为 Å√amu。Taylor 导数按文档中的阶乘关系换算。
- Γ 光学候选不自动具备 IR 活性。筛选计算没有提供 Born 电荷、Raman 张量、泵浦、阻尼、声子寿命或输运基准。
- 平移 ASR 不等于二维转动求和规则。当前不包括非解析长程修正、SOC、外场或完整简并非对角张量。
- 不同 Stage1 模型使用各自弛豫结构和模式基。跨路线比较系数前需匹配模式或子空间；逐点能量和力误差必须在相同构型上计算。

[ARCHITECTURE.md](ARCHITECTURE.md)给出方程、归一化和数值边界。拒绝无效输入或身份不一致是有意的保护；降低核验阈值不是恢复方法。

## 已公开的基准材料

仓库保留 **WS₂、MoS₂ 和 WSe₂ GGA-PBE** 归档研究、可编辑报告及公开数据。它们是既有科学结果，不是 1.0.4 维护版重新执行模型推理的测量。

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
