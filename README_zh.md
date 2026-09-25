# 六方二维非线性声子筛选

`npc` 用 Phonopy 计算完整 6×6×1 声子网格，再用 MatterSim 对 Γ–q–(−q) 耦合排序。Stage1 默认 TECE-OAM-RRA-1.0，也可选择 Prophet OAME-MBD 或 EquiformerV3+DeNS-OAM。本版本不运行 DFT 或分子动力学。

## 两阶段流程

Stage1 保留全部非 Γ 的 q 点和所有有限 q 声子分支；在 Γ 点只保留光学模，不生成三条声学平移模的耦合对。声学模由质量加权本征矢与整体平移子空间的重叠识别，不只看频率序号。spglib 从**实际原子结构**求对称操作，只有变换后的频率和复本征矢也匹配时才归并 q 点。六方晶胞的原子排布若降低对称性，候选类会增加；检验失败时记录原因，保守退回 q/−q 归并。不按小群规则、有限 q 的声学／光学分支或预计耦合强度删除候选。每个 q 类保存原子映射、操作、时间反演及模态重叠；近简并单支编号不视作唯一物理方向。

Stage2 先对**全部**候选计算六个能量点：`QΓ=±1`、`Qq=−1,0,+1` Å√amu，以完整 Γ 子空间的三阶 `Φ122` 范数排序。默认取前 **20 个通道**，展开其全部分量，在中央 5×5 网格（−1 至 +1、间隔 0.5）拟合三、四阶系数；可选 `audit` 将精算排名最强的五个通道补至 9×9。六点只是三阶排序代理，不能代替四阶拟合。最后确定的前 20 默认值覆盖了早期草案的前 30。

单位统一为 Å、amu、eV/超胞、eV/Å、eV/Å²、THz，实模坐标为 Å√amu；三、四阶导数分别为 meV/(Å³·amu³ᐟ²)、meV/(Å⁴·amu²)。结果使用 **v5** 契约，不接受包含 Γ 声学候选的旧 v4 或更早模式对作为新计算输入。

## 安装与运行

安装、模型哈希和 Slurm 示例见 [安装说明](docs/INSTALL.md)，数值约定见
[架构说明](ARCHITECTURE.md)。N 原子原胞、C 个有限 q 等价类对应
`C × (3N−3) × 3N` 个候选；本次验证的三原子 TMD、六个 q 类为 **324 对**。
单原子原胞没有 Γ 光学候选。自行弛豫保留输入原子约束和真空长度，优化允许的
原子自由度与面内等比缩放；具体收敛和搜索边界随运行保存。

用 Python 3.10+ 执行 `pip install -e .`，Phonopy 固定为 2.38.0，并安装 spglib。各 Stage1 模型使用独立环境及已验证源码／权重；Stage2 需要 MatterSim 1.2.1 和已验证的 5M 权重。精确的提交与哈希见模型适配器常量。

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

把 `stage1 --model` 改为 `prophet` 或 `equiformer-v3` 即可切换 Stage1；TECE／Equiformer 需要 `--source-root`，Prophet 使用固定源码安装或 `NPC_PROPHET_SOURCE`。`stage2 audit` 使用与前两阶段完全相同的参数。Stage1 总是先用所选模型自行弛豫；Stage2 必须使用 `stage1/relax/optimized_structure.scf.inp`，并核验弛豫摘要与结构哈希。现有受约束弛豫写入器要求带原子约束标记的 QE 格式输入，但不会启动 QE 计算。

Stage2 可设置 `--shard-index I --shard-count N` 分片并行。同一阶段全部分片结束后，用 `--finalize-only` 核对完整性并产生排名。每点检查点可断点续算；结构、权重或前 20 等设置变化会触发哈希拒错，不可在原目录暗中覆盖。

## 结果与限制

`stage1/phonon_dataset.json` 记录完整网格、ASR 和步长诊断；`stage1/mode_pairs.selected.json` 记录实际结构的 q 类、模态映射和全部候选。Stage2 保存逐对能量检查点、粗筛排名、精算选择与排名及可选 9×9 审计。权重、源码、结构、归一化和筛选参数随运行记录。

Phonopy 的此处力常数修正处理平移声学求和及指标交换，**不**等于已满足二维 ZA 的转动求和规则。本版未加入非解析修正，只针对无磁、无外场的标量势。每个 Stage1 模型使用各自弛豫结构，跨模型差异也包含结构效应；声子和耦合比较仍须可靠的模态／子空间映射。旧 QE 结果未核实几何及位移约定时不能称为严格同结构标签。

代码结构：`nonlinear_phonon_calculation/cli.py` 是公开入口；`mlff_modepair_workflow/` 实现 Phonopy、结构对称性、模型接口、实模位移及势能面拟合；`tests/` 为解析和稳定性测试；`scripts/` 保留可选 CPU 计时工具。输入、模型权重与运行结果均放在仓库之外。

详细数值验收见[验证报告](docs/VALIDATION.md)和[可追溯数据](docs/validation_v5.json)。

以DFT为参照的科学结果见[模型路线与DFT精算的定量比较](docs/MODEL_DFT_COMPARISON.md)（[PDF](output/pdf/model_dft_comparison_1_0_1.pdf)）：按论文框架比较五对精算耦合、MAE/RMSE/最大误差、频率、本征矢、势能面、拟合残差、三阶余项、四阶窗口敏感性与CPU成本，保留原QE＋MatterSim历史基线。软件稳定性和模型间一致性不替代DFT精度标准。
这里是三条模型路线，在 MoSe₂、WS₂ 上共六组实验；全部自行弛豫。
Γ 光学简并阈值可用 `--gamma-degeneracy-thz` 设置，默认 0.01 THz。
已完成的 Stage1 目录拒绝覆盖，修改参数需要新目录。

## 1.0.1 数值修正与验收

完整逐通道对照及原计划验收表见[补充验收报告](docs/ACCEPTANCE.md)。Γ 与有限 q 本征矢都必须满足协变检验，不能只核对有限 q。MatterSim 保留原 float32 模型推理，将逐原子能量在求和前提升为 float64，避免超胞总能量累加的主要舍入误差；这不是全模型双精度推理。累加协议进入检查点身份，旧 v5 能量点与缺少 Γ 协变检查的 Stage1 文件不能直接续算。

Γ 双重态的两个分量分别与 q、−q 耦合，排名使用这两个 Φ122 的范数，不是两个 Γ 与一个有限 q 的三声子耦合。有限 q 下 Φ112 对应的 QΓ²Qq 项违反动量守恒，只保留为数值诊断，不参加排名。完整 Γ 多重态的三阶向量范数与四阶 Φ1122 迹用于跨基底对照；有限 q 简并支仍需标记基底依赖。
