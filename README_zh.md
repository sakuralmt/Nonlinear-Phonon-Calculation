# 六方二维材料非线性声子筛选

`npc` 筛选 Γ–q–(−q) 高阶声子耦合。它先用机器学习力场自行弛豫单层结构，再通过 Phonopy 计算完整 6×6×1 声子网格，最后用 MatterSim 粗筛并拟合冻结模式的势能面。默认组合为 **TECE＋MatterSim**；Stage1 还支持 Prophet、EquiformerV3。公开命令只有 Stage1、Stage2，不运行 QE 或 MD。

[English](README.md) · [安装及模型来源](docs/INSTALL.md) · [数值架构](ARCHITECTURE.md) · [当前 PBE 对照](docs/PBE_REFERENCE.md) · [图文 PDF](output/pdf/tmd_gga_pbe_mlff_latex_report.pdf)

## 计算流程

1. **Stage1：自行弛豫与声子。** 所选模型弛豫输入结构，Phonopy 计算 36 个 q 点。Γ 点只取光学模，有限 q 保留全部分支。从实际原子结构寻找对称操作，并核验频率与本征矢映射；失败时保守地减少归并。已验证的三原子 TMD、六个 q 类有 324 对，其他结构不强制采用该数。
2. **Stage2 粗筛：** 每对计算六个能量点，`QΓ=±1`、`Qq=−1,0,+1` Å√amu，以满足动量守恒的三阶 Γ–q–(−q) 耦合范数排序。
3. **Stage2 精算与审计：** 默认前 20 个完整物理通道补至中央 5×5 网格（`|Q|≤1`，步长 0.5）拟合三、四阶导数；可选 `audit` 为最强五个通道补至 9×9（`|Q|≤2`，步长 0.5）检查窗口敏感性。六点结果只作粗筛，不能当作四阶拟合。

**6×6×1 是声子动量网格**；**5×5、9×9、研究中的 17×17 是二维模式位移网格**，两者的收敛含义不同。

## 快速使用

使用 Python 3.10+，执行 `pip install -e .`。Phonopy 固定为 2.38.0。需另装所选 Stage1 模型及 MatterSim，权重不随仓库提供；已验证源码提交、权重检查和 CPU/Slurm 说明见[安装文档](docs/INSTALL.md)。可用 `npc --help` 检查入口。

以下示例的输入是带原子约束标记的 QE 格式二维结构：

```bash
npc stage1 --model tece \
  --structure /data/mose2/structure.scf.inp \
  --checkpoint /models/TECE-OAM-RRA-1.0.pt \
  --source-root /models/tace \
  --output-dir /runs/mose2/tece/stage1

npc stage2 screen \
  --mode-pairs-json /runs/mose2/tece/stage1/mode_pairs.selected.json \
  --structure /runs/mose2/tece/stage1/relax/optimized_structure.scf.inp \
  --checkpoint /models/mattersim-v1.0.0-5M.pth \
  --output-dir /runs/mose2/tece/stage2

npc stage2 refine \
  --mode-pairs-json /runs/mose2/tece/stage1/mode_pairs.selected.json \
  --structure /runs/mose2/tece/stage1/relax/optimized_structure.scf.inp \
  --checkpoint /models/mattersim-v1.0.0-5M.pth \
  --output-dir /runs/mose2/tece/stage2
```

`screen`、`refine`、可选的 `audit` 必须用**同一**模式对文件、自行弛豫结构、MatterSim 权重和输出目录。把 `stage1 --model` 换为 `prophet` 或 `equiformer-v3` 即可选择其他路线，源码参数见[安装文档](docs/INSTALL.md)。Stage2 支持逐点断点续算和 `--shard-index I --shard-count N` 分片；分片完成后用 `--finalize-only` 核验完整性、生成排名。结构或模型哈希变更会拒绝续算。

Stage1 的 `phonon_dataset.json` 记录全网格与 ASR 诊断，`mode_pairs.selected.json` 记录等价类和模式映射。Stage2 保存 `pairs/*/points.json`、`screen_ranking.json`、`selection.json`、`refine_ranking.json` 与可选的 `audit_ranking.json`。结果契约是 v5，不接受旧模式对作为新计算输入。

## 当前 DFT 标准：GGA-PBE

[WS₂、MoS₂、WSe₂ 三材料 PBE 对照](docs/PBE_REFERENCE.md)采用 QE 7.4.1、PBE 超软赝势、PBE 自弛豫结构、120/1200 Ry、原胞 30×30×1 k 网格及重新计算的 6×6×1 DFPT。**579/579** 个 QE 单点已经完成：每种材料五个匹配物理通道的中央 5×5 势能面、最强通道的 9×9 网格以及收敛检查。仓库包含[图文完整 PDF](output/pdf/tmd_gga_pbe_mlff_latex_report.pdf)、[机器可读数据](docs/reference_data/pbe_20260925/)和[可重跑的报告代码](reports/pbe_three_materials/)。

| 材料 | TECE＋MS 相对 PBE 的频率 MAE (THz) | 三阶 `|Φ122|` MAE | 带符号四阶 `Φ1122` MAE |
| --- | ---: | ---: | ---: |
| WS₂ | 0.0928 | 6.104 | 2.594 |
| MoS₂ | 0.0880 | 4.328 | 2.939 |
| WSe₂ | 0.0654 | 1.180 | 0.628 |

三阶、四阶单位分别是 meV/(Å³·amu³ᐟ²)、meV/(Å⁴·amu²)。耦合误差只针对五个匹配通道。旧 PZ-LDA 参考换成 PBE 后三阶差距缩小，但 WS₂ Γ8–M6 的四阶**符号差异依然存在**，包括在完全相同的 QE 输入结构上比较时。旧[模型／DFT 报告](docs/MODEL_DFT_COMPARISON.md)与[WS₂ 补充](docs/WS2_DFT_COMPARISON.md)只作 LDA 历史对照。截至 2026-09-26，另一个 WS₂ 固定窗口 **17×17 QE 位移密网格测试**仍在运行；此处没有宣称其 DFT 已收敛。

## 适用边界

长度 Å、质量 amu、超胞能量 eV、力 eV/Å、频率 THz，实模坐标 Å√amu。Γ 简并态以完整耦合向量范数排序，单支编号不是唯一的物理方向。有限 q 的 `Φ112` 违反动量守恒，只作为拟合诊断。Phonopy 此处处理平移 ASR，**不代表**二维 ZA 转动求和已满足；不含非解析修正、SOC、外场和 MD。

各 Stage1 模型自行弛豫，跨模型差异同时包含结构变化；比较耦合前须可靠地匹配模式或简并子空间。PBE-USPP 比 LDA 更接近 MLFF 训练泛函，但不完全复制训练标签设置。公开包位于 `nonlinear_phonon_calculation/` 与 `mlff_modepair_workflow/`；`tests/` 是软件测试。PBE 报告代码不进入安装包。权重、原始 QE 输出和 Slurm 控制器不随仓库发布。
