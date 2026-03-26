# 服务器高通量工作流

该目录是三层工作流里的 `stage2` 和 `stage3` 运行侧入口。

当前默认约定是：

- `stage1` 在老机器 `159.226.208.67:33223`
- `stage2/3` 在新机器 `100.101.235.12`

这层不会自动 SSH 到别的机器，而是读取上游 handoff 文件继续跑。

## 包含的脚本

- `portable_cpu_config.json`
  - 可跨机器迁移的 CPU runtime profile
  - 默认用于没有做过本机 benchmark 的新机器
- `portable_cpu_small.json`
- `portable_cpu_medium.json`
- `portable_cpu_large.json`
  - 分别对应小型 / 均衡 / 大型 CPU 机器
- `CPU_QUICKSTART_zh.md`
  - 极简中文上手说明

- `assess_chgnet_env.py`
  - 在目标服务器上扫描 CHGNet 推理速度
  - 输出 JSON / Markdown 报告
  - 给出推荐线程数、batch size 和 worker 配置
  - 生成 `env_reports/chgnet_runtime_config.json` 作为本机固定配置
  - 同时识别整台机器的 Slurm 分区配置
  - 生成 `env_reports/slurm_cluster_assessment.json`
  - 生成 `env_reports/slurm_runtime_config.json`
  - 生成可直接 `source` 的 `env_reports/slurm_submit_defaults.sh`

- `run_server_pipeline.py`
  - 服务器端控制器
  - 当前默认走 CHGNet-only 的加速 screening 路线

- `continue_after_screening.py`
  - server controller 的 continuation helper
- `scheduler.py`
  - 调度抽象层
  - 当前支持 `auto|slurm|local`

- `bootstrap_server_env.sh`
  - 在服务器上准备 `qiyan-ht` conda 环境

## 推荐使用顺序

```bash
bash bootstrap_server_env.sh
python assess_chgnet_env.py
python run_modular_pipeline.py --stage stage2 --run-root <release_run>
```

如果要继续 QE top5 复核，再运行：

```bash
python run_modular_pipeline.py --stage stage3 --run-root <release_run>
```

其中 `<release_run>` 至少要先带上：

- `stage1_manifest.json`
- `stage1_inputs/`

## 当前加速默认配置

- `strategy = coarse_to_fine`
- `coarse_grid_size = 5`
- `full_grid_size = 9`
- `refine_top_k = 24`
- `batch_size = 16`
- `num_workers = 2`
- `torch_threads = 16`
- `worker_affinity = auto`

## 运行时配置优先级

当前 CHGNet 加速筛选会按下面顺序选 runtime 配置：

1. 显式 `--runtime-config`
2. 显式 `--runtime-profile`
3. `server_highthroughput_workflow/env_reports/chgnet_runtime_config.json`
4. `server_highthroughput_workflow/portable_cpu_config.json`
5. 脚本内置的自动 CPU 推断

这意味着：

- 如果一台机器已经跑过 `assess_chgnet_env.py`，后续会自动优先使用它自己的固定配置；
- 如果是新机器，没有评估报告，也仍然可以先靠 portable CPU profile 自动迁移运行。

如果你想直接选 profile，可以用：

```bash
python ../mlff_modepair_workflow/run_pair_screening_optimized.py --runtime-profile medium
python run_server_pipeline.py --runtime-profile medium --scheduler auto
```

快速开始文档：

```bash
server_highthroughput_workflow/CPU_QUICKSTART_zh.md
```

如果机器没有 Slurm：

- `run_pair_screening_optimized.py` 可以直接本地运行
- `run_server_pipeline.py --scheduler auto`
  会自动退化成“只做 screening，不继续 QE recheck”

如果机器有 Slurm，但 bundle 默认分区/时限和这台机不匹配：

- runtime 现在会先识别当前 Slurm 配置，再自动回退到可用 partition / walltime
- 你也可以先跑：

```bash
python server_highthroughput_workflow/assess_chgnet_env.py
source server_highthroughput_workflow/env_reports/slurm_submit_defaults.sh
```

## 说明

这层默认只依赖契约输入继续运行：

- `stage2` 只吃 `stage1_manifest + stage1_inputs`
- `stage3` 只吃 `stage2_manifest`

稳定版已经去掉黄金参考和本地历史运行目录，不再把它们作为运行时刚需。
