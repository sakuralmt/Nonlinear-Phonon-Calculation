# MLFF Mode-Pair Workflow

该目录包含当前主线 ML 工作流。稳定版当前主要保留的是 `stage2` screening 所需能力。

主要内容包括：

- 全模式对 screening
- 加速版 CHGNet screening
- ML 模式重建
- 若干 benchmark / 对照脚本

`run_pair_screening_optimized.py` 是当前稳定版推荐的筛选入口。

## 主要脚本

- `benchmark_golden_pair.py`
  - 额外诊断脚本，不属于稳定版默认运行链
- `benchmark_qe_structure.py`
  - 额外诊断脚本，不属于稳定版默认运行链
- `run_pair_screening.py`
  - 全模式对 screening
- `run_pair_screening_optimized.py`
  - CHGNet-only 加速筛选
  - 支持 `coarse_to_fine`
  - 支持 batch / worker 并行 / CPU affinity
  - 支持 portable CPU runtime config 和 assessed fixed config
- `build_ml_mode_pairs.py`
  - 基于 ML 声子结果重建模式对
- `run_ml_mode_source_test.py`
  - 比较 QE-mode 与 ML-mode 流程
- `run_native_dualtrack_workflow.py`
  - native dual-track 主控制器
- `refresh_native_dualtrack_summaries.py`
  - 刷新已有 summary 字段

## reference 口径

summary 中可能出现三类 reference：

- `mode_pair_frequency_compare`
  - 当前 track 的模式频率
- `qe_mode_reference_compare`
  - 原始 QE 模式频率
- `golden_pes_compare`
  - `n7` 黄金对二维 PES 拟合参考

注意：

- 稳定版默认运行链已经不再依赖 `n7`
- `golden_pes_compare` 只在你显式提供黄金参考时才有意义

## 输入

稳定版默认运行链的上游输入来自：

- `stage1_manifest.json`
- `stage1_inputs/mode_pairs/selected_mode_pairs.json`
- `../nonlocal phonon/scf.inp`

如果你要跑 benchmark / 诊断脚本，才需要额外提供 QE 对照或黄金参考。

## 运行

```bash
python run_pair_screening_optimized.py --backend chgnet --model r2scan
```

也可以显式指定 portable CPU profile：

```bash
python run_pair_screening_optimized.py --backend chgnet --model r2scan --runtime-profile medium
```

或者显式指定某个固定 runtime config：

```bash
python run_pair_screening_optimized.py --backend chgnet --model r2scan --runtime-config ../server_highthroughput_workflow/env_reports/chgnet_runtime_config.json
```

## 说明

当前结论很简单：

- `CHGNet r2scan` 可用于预筛选；
- `GPTFF v2` 不作为当前课题的主筛选器。

运行时配置优先级：

1. 显式 `--runtime-config`
2. 显式 `--runtime-profile`
3. `../server_highthroughput_workflow/env_reports/chgnet_runtime_config.json`
4. `../server_highthroughput_workflow/portable_cpu_config.json`
5. 脚本内置的自动 CPU 推断
