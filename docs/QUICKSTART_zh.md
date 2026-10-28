# 快速开始

[English](QUICKSTART.md) · [安装依赖和权重](INSTALL.md) · [DFT 数据契约](DFT_STAGE1.md)

适用范围：以下示例使用 TMDC。其他六角材料的完整对称性筛选尚未做同等详细的验证，参见[范围说明](SYMMETRY_SCOPE.md)。
以下命令从源码检出或解压后的源码发行包根目录执行，使用公开的 MoS₂ 几何。MLFF 示例采用 `ibrav=0` 显式胞起始结构，仍会按所选模型自行弛豫；DFT 导入示例保留原 DFT 结构。wheel 不包含这一数据目录；核心安装也不提供模型权重。

## 1. 先检查安装，不启动计算

```bash
npc --version
npc --help
npc stage1 --help
npc stage2 --help
npc status --help
```

模块入口为 `python -m nonlinear_phonon_calculation.cli`。下面的 **DFT 数据导入**也不需要模型：它核验已经计算的模式，不进行 DFT 或模型推理。

```bash
npc stage1 --model qe-dft \
  --structure docs/reference_data/pbe_20260925/structures/mos2.scf.inp \
  --phonon-dataset docs/reference_data/pbe_20260925/qe_phonon_dataset_mos2.json \
  --output-dir runs/mos2/pbe/stage1
```

独立实验使用新的输出目录。只给结构文件，不能生成 DFT 声子本征矢。

## 2. 默认 TECE＋MatterSim 路线

按 [INSTALL.md](INSTALL.md)准备固定源码、权重和模型环境，再指定两个阶段各自的解释器：

```bash
export NPC_STAGE1_PYTHON=.venv-tece/bin/python
export NPC_STAGE2_PYTHON=.venv-ms/bin/python
```

下面的 Stage1 命令会实际弛豫并计算谐波，不是安装自检：

```bash
"$NPC_STAGE1_PYTHON" -m nonlinear_phonon_calculation.cli stage1 --model tece \
  --structure docs/reference_data/pbe_20260925/full_model_structures/mos2-tece.scf.inp \
  --checkpoint weights/TECE-OAM-RRA-1.0.pt \
  --source-root model-sources/tace \
  --output-dir runs/mos2/tece/stage1
```

接着在 MatterSim 环境中粗筛和精算：

```bash
"$NPC_STAGE2_PYTHON" -m nonlinear_phonon_calculation.cli stage2 screen \
  --mode-pairs-json runs/mos2/tece/stage1/mode_pairs.selected.json \
  --structure runs/mos2/tece/stage1/relax/optimized_structure.scf.inp \
  --checkpoint weights/mattersim-v1.0.0-5M.pth \
  --output-dir runs/mos2/tece/stage2

"$NPC_STAGE2_PYTHON" -m nonlinear_phonon_calculation.cli stage2 refine \
  --mode-pairs-json runs/mos2/tece/stage1/mode_pairs.selected.json \
  --structure runs/mos2/tece/stage1/relax/optimized_structure.scf.inp \
  --checkpoint weights/mattersim-v1.0.0-5M.pth \
  --output-dir runs/mos2/tece/stage2
```

默认精算 20 个完整耦合组，范围 `[-1,1]`、步长 0.5。组内伙伴扩展后，具体模式对可能超过 20 对。若使用 `--top-channels`，应在计算前确定，并在全部阶段保持一致；修改它会改变运行身份。

只有需要**扩大振幅窗口**时才执行：

```bash
"$NPC_STAGE2_PYTHON" -m nonlinear_phonon_calculation.cli stage2 audit \
  --mode-pairs-json runs/mos2/tece/stage1/mode_pairs.selected.json \
  --structure runs/mos2/tece/stage1/relax/optimized_structure.scf.inp \
  --checkpoint weights/mattersim-v1.0.0-5M.pth \
  --output-dir runs/mos2/tece/stage2
```

Audit 对最强五个精算组扩展到 `[-2,2]`，步长仍为 0.5。它不是同窗口的 5→9 加密，也不重算 q 网格。

查看已保存的结果数量：

```bash
"$NPC_STAGE2_PYTHON" -m nonlinear_phonon_calculation.cli status \
  --output-dir runs/mos2/tece/stage2
```

## 3. 替代 Stage1 模型

Prophet 使用自己的环境及固定 OAME-MBD 权重；已安装模型包时不需要 `--source-root`：

```bash
npc stage1 --model prophet \
  --structure docs/reference_data/pbe_20260925/full_model_structures/mos2-prophet.scf.inp \
  --checkpoint weights/prophet-oame-mbd.pt \
  --output-dir runs/mos2/prophet/stage1
```

固定 EquiformerV3+DeNS-OAM 路线为：

```bash
npc stage1 --model equiformer-v3 \
  --structure docs/reference_data/pbe_20260925/full_model_structures/mos2-equiformer-v3.scf.inp \
  --checkpoint weights/omat24-mptrj-salex_gradient.pt \
  --source-root model-sources/equiformer-v3 \
  --output-dir runs/mos2/equiformer-v3/stage1
```

后续仍使用 MatterSim Stage2，但必须换成**该路线自身**的模式文件和优化结构。不能把 TECE 模式文件与其他模型结构混用。

## 4. 保留已有 DFT 结构和模式

第 1 步导入后得到 `runs/mos2/pbe/stage1/mode_pairs.selected.json`，可在 MatterSim 环境接入完整的 screen/refine/audit 路线：

```bash
npc stage2 screen \
  --mode-pairs-json runs/mos2/pbe/stage1/mode_pairs.selected.json \
  --structure docs/reference_data/pbe_20260925/structures/mos2.scf.inp \
  --checkpoint weights/mattersim-v1.0.0-5M.pth \
  --output-dir runs/mos2/pbe/stage2
```

若要比较**原有已选 QE 构型**，将选择文件导入单独目录：

```bash
npc stage1 --model qe-dft \
  --structure docs/reference_data/pbe_20260925/structures/mos2.scf.inp \
  --phonon-dataset docs/reference_data/pbe_20260925/qe_phonon_dataset_mos2.json \
  --selection-json docs/reference_data/pbe_20260925/selection_mos2.json \
  --output-dir runs/mos2/pbe/reference-stage1

npc stage2 reference \
  --mode-pairs-json runs/mos2/pbe/reference-stage1/mode_pairs.reference.json \
  --structure docs/reference_data/pbe_20260925/structures/mos2.scf.inp \
  --checkpoint weights/mattersim-v1.0.0-5M.pth \
  --reference-results docs/reference_data/pbe_20260925/results_mos2.json \
  --output-dir runs/mos2/pbe/reference-stage2
```

Reference 使用已有坐标，比较中心对齐的能量、全部原子力分量及拟合导数；不重新筛选这些模式对，也不提交 QE。每材料使用一个可续算进程；此阶段不支持 `--shard-count` 大于 1 或 `--finalize-only`。

## 5. 续算与可选分片

中断后重复相同输入、相同输出目录的命令。能否复用取决于保存的身份，不是文件名看起来是否相同。

screen/refine/audit 可按完整模式对分片。下例是两个 screen 分片之一；另一个用相同命令将 `--shard-index` 改为 1。等待两者均正常退出后，再完成汇总：

```bash
npc stage2 screen \
  --mode-pairs-json runs/mos2/tece/stage1/mode_pairs.selected.json \
  --structure runs/mos2/tece/stage1/relax/optimized_structure.scf.inp \
  --checkpoint weights/mattersim-v1.0.0-5M.pth \
  --output-dir runs/mos2/tece/stage2 \
  --shard-index 0 --shard-count 2

npc stage2 screen \
  --mode-pairs-json runs/mos2/tece/stage1/mode_pairs.selected.json \
  --structure runs/mos2/tece/stage1/relax/optimized_structure.scf.inp \
  --checkpoint weights/mattersim-v1.0.0-5M.pth \
  --output-dir runs/mos2/tece/stage2 \
  --finalize-only
```

Screen 尚未完成汇总时，不要开始 refine。每个分片都会加载一个模型，需据此分配内存和线程。[INSTALL.md](INSTALL.md)介绍可选 Slurm 示例；迁移旧结果前先读 [MIGRATION.md](MIGRATION.md)。
