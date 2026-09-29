# 已有 DFT Stage1 接入 MatterSim Stage2

本入口读取已完成的 QE 弛豫结构及 DFPT 频率／质量加权复本征矢，不调用 QE，不由 MatterSim 重新弛豫。Stage2 使用相同结构和模式构造实际位移，采用当前单位范数及三四阶导数定义。Γ声学模仍不参与候选。

## 完整网格：复用现有筛选接口

从仓库根目录执行（MatterSim权重需另行提供）：

```bash
npc stage1 --model qe-dft \
  --structure docs/reference_data/pbe_20260925/structures/mos2.scf.inp \
  --phonon-dataset docs/reference_data/pbe_20260925/qe_phonon_dataset_mos2.json \
  --output-dir /runs/mos2/pbe/stage1

npc stage2 screen \
  --mode-pairs-json /runs/mos2/pbe/stage1/mode_pairs.selected.json \
  --structure docs/reference_data/pbe_20260925/structures/mos2.scf.inp \
  --checkpoint /models/mattersim-v1.0.0-5M.pth \
  --output-dir /runs/mos2/pbe/stage2
```

后续 `refine`、`audit` 使用相同模式文件和原DFT结构。对称轨道由实际结构及模式重新核验，保守退回规则与MLFF Stage1相同；已核验三种材料均为324个Γ光学候选。此导入不是声子谱重算，也不重新施加ASR，保留DFPT来源记录中的ASR处理。

## 已选通道：直接详细比较

```bash
npc stage1 --model qe-dft \
  --structure docs/reference_data/pbe_20260925/structures/mos2.scf.inp \
  --phonon-dataset docs/reference_data/pbe_20260925/qe_phonon_dataset_mos2.json \
  --selection-json docs/reference_data/pbe_20260925/selection_mos2.json \
  --output-dir /runs/mos2/pbe/reference-stage1

npc stage2 reference \
  --mode-pairs-json /runs/mos2/pbe/reference-stage1/mode_pairs.reference.json \
  --structure docs/reference_data/pbe_20260925/structures/mos2.scf.inp \
  --checkpoint /models/mattersim-v1.0.0-5M.pth \
  --reference-results docs/reference_data/pbe_20260925/results_mos2.json \
  --output-dir /runs/mos2/pbe/reference-stage2
```

WS₂/WSe₂替换材料名即可。当前每种材料为五个已选物理通道：最强通道81点，其余四通道各25点，共181点。它不按MatterSim排名重新选择声子对。每个点保存能量及全部原子力，可中断后恢复。一个材料用一个工作进程，一次加载模型；材料之间可并行。结构、模式、权重及QE参考结果的哈希不同则拒绝混合续算。

输出 `reference_result.json` 包括中心5×5／宽9×9拟合、13列秩诊断、三四阶导数和曲率频率、同构型相对中心能量MAE/RMSE、原子力分量MAE/RMSE、耗时及资源。完整网格和中心窗口的E/F误差分别报告；未经对齐的绝对总能量不比较。原子约束标记仅用于历史弛豫，不遮蔽冻结模式计算的力。

## 已验证的DFPT导出格式

`qe_phonon_dataset_*.json` 包含完整的 `q_points`（q坐标、THz频率、质量加权复本征矢）、Γ平移子空间分组及 `source_stage1`（原输入、赝势、弛豫、DFPT、ASR来源）。结构输入和eig哈希必须与选择文件匹配；逐模式核对q、频率、本征矢及动量守恒。本版导入适配器验证的是现有PBE导出格式，不直接解析任意QE `matdyn.modes`，也不宣称仅指定结构就能生成DFT本征矢。新材料须先以同一契约导出既有DFPT数据；不能直接沿用旧v2实模。

ASE显式晶胞格式与QE六方 `ibrav=4` 的A/C或celldm格式均可读取。打包的 `.scf.inp` 只用于恢复结构；其中历史赝势路径不用于发起新计算。
