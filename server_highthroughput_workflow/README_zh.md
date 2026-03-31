# Server High-Throughput Workflow

这个目录是 beta TUI 背后的编排层。

在 beta 结构里，用户应该从：

- 一个外部输入根目录
- 一个 `system_id`
- `npc` 启动器

开始使用。这里的脚本是启动器背后的执行引擎，不是用户的主入口。

## 这个目录负责什么

- 发现并校验一个体系目录
- 为一次运行准备 runtime tree
- 生成和解析内部 stage contract
- 运行面向 stage1 的 family-aware 收敛性测试
- 执行 `stage2` 筛选
- 执行 `stage3` 的 QE top-5 准备与提交

## 运行目录结构

beta 的运行树固定为：

```text
runs/<system_id>/<run_id>/
  contracts/
    stage1.manifest.json
    stage2.manifest.json
    stage3.manifest.json
  logs/
  stage1/
  stage2/
  stage3/
```

`stage2` 读取 `contracts/stage1.manifest.json`。  
`stage3` 读取 `contracts/stage2.manifest.json`。

正常使用时，用户不需要手工把这些文件路径敲到命令行里。

## 主要文件

- `run_modular_pipeline.py`
  - `npc` 背后的分阶段驱动器
- `system_runtime.py`
  - 从 `structure.cif`、`system.json` 和 `pseudos/` 生成内部运行快照
- `real_stage1_phonon.py`
  - 把真实 stage1 声子前端和 tuning 阶段接进共享 runtime tree
- `stage23_pipeline.py`
  - `run_modular_pipeline.py` 在 stage2/stage3 时调用的内部 helper
- `stage_contracts.py`
  - contract 结构和路径处理
- `qe_input_utils.py`
  - CIF 到 QE 输入的生成工具

`server_highthroughput_workflow/ops/` 下面的文件属于运维/诊断辅助，不是
正常 `npc` 主线的一部分。

## Stage2 输出

筛选层输出目录：

```text
stage2/outputs/<backend>/screening/
```

关键文件：

- `pair_ranking.csv`
- `pair_ranking.json`
- `single_backend_ranking.json`
- `runtime_config_used.json`
- `run_meta.json`
- `contracts/stage2.manifest.json`

## Stage3 输出

QE 复核层输出目录：

```text
stage3/qe/<backend>/
```

关键文件：

- `selected_top_pairs.csv`
- `run_manifest.json`
- `modular_stage3_status.json`
- `contracts/stage3.manifest.json`

`contracts/stage3.manifest.json` 会在 prepare 完成后立即写出。

## 说明

- 这个 beta 仍然保留跨机器 handoff，但 handoff 已经收进 runtime tree，不再让用户去理解旧 bundle 内部目录。
- 启动器在只给出 `system_id` 时，可以自动选这个体系最近一次的运行目录，继续跑 `stage2` 或 `stage3`。
