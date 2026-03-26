# Hex Q-Gamma Q-Pair Workflow

该目录保存 QE 前处理流程，用于生成后续所有工作流共享的 q 点、eigenvector 和模式对参考。

## 核心输出

- `hex_qgamma_qpair_run/screening/selected_qpoints.csv`
- `hex_qgamma_qpair_run/extracted/screened_eigenvectors.json`
- `hex_qgamma_qpair_run/mode_pairs/selected_mode_pairs.json`

## 主要脚本

- `screen_hex_qgamma_qpair_points.py`
- `prepare_matdyn_hex_qgamma_qpair.py`
- `extract_screened_eigs.py`
- `select_modes_qgamma_qpair.py`
- `generate_mode_pairs_qgamma_qpair.py`
- `step1_simple.py`
- `step2_simple.py`
- `step3_simple.py`

## 用法

先看结果目录：

- `hex_qgamma_qpair_run/`

再看脚本：

1. q 点筛选
2. 本征矢提取
3. 模式选择
4. 模式对生成

## 下游依赖

下列目录默认读取这里的输出：

- `../mlff_modepair_workflow/`
- `../qe_modepair_validation_workflow/`
- `../qe_modepair_handoff_workflow/`
- `../hex_qpair_nonlinear_workflow/`
