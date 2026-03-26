# CPU 配置快速上手

这份说明只针对 CPU 机器，不涉及 GPU。

## 目标

把加速版 CHGNet screening 迁移到一台新的 CPU 机器时，尽量不手工调线程参数。

## 三种用法

### 1. 最推荐：先评估，再固定

适合：

- 这台机器会反复使用
- 你希望后续每次都用同一套稳定参数

步骤：

```bash
bash server_highthroughput_workflow/bootstrap_server_env.sh
python server_highthroughput_workflow/assess_chgnet_env.py
python mlff_modepair_workflow/run_pair_screening_optimized.py --backend chgnet --model r2scan
```

效果：

- 会先生成：
  - `server_highthroughput_workflow/env_reports/chgnet_env_assessment.json`
  - `server_highthroughput_workflow/env_reports/chgnet_env_assessment.md`
  - `server_highthroughput_workflow/env_reports/chgnet_runtime_config.json`
- 后续 `run_pair_screening_optimized.py` 和 `run_server_pipeline.py`
  会自动优先使用这份固定配置

### 2. 不做评估，直接用 portable profile

适合：

- 只是临时在一台新 CPU 机器上跑
- 先要一个可迁移、可工作的默认配置

可选 profile：

- `default`
- `small`
- `medium`
- `large`

示例：

```bash
python mlff_modepair_workflow/run_pair_screening_optimized.py \
  --backend chgnet \
  --model r2scan \
  --runtime-profile medium
```

或者：

```bash
python server_highthroughput_workflow/run_server_pipeline.py \
  --scheduler auto \
  --runtime-profile medium
```

### 3. 手工指定某个 JSON 配置

适合：

- 你已经有一份想锁定复用的配置文件

示例：

```bash
python mlff_modepair_workflow/run_pair_screening_optimized.py \
  --backend chgnet \
  --model r2scan \
  --runtime-config server_highthroughput_workflow/portable_cpu_large.json
```

### 4. 三层断点续算

适合：

- stage1 / stage2 / stage3 不在同一台机器上跑
- 你希望通过中间文件手动接力

示例：

```bash
python server_highthroughput_workflow/run_modular_pipeline.py --stage stage1 --run-root /path/to/run_root
python server_highthroughput_workflow/run_modular_pipeline.py --stage stage2 --run-root /path/to/run_root --runtime-profile medium
python server_highthroughput_workflow/run_modular_pipeline.py --stage stage3 --run-root /path/to/run_root --qe-mode prepare_only --scheduler local
```

中间文件：

- `stage1_manifest.json`
- `stage2_manifest.json`
- `stage3_manifest.json`

## 配置优先级

当前运行时会按下面顺序选 CPU 配置：

1. 你显式传入的 `--runtime-config`
2. 你显式传入的 `--runtime-profile`
3. `server_highthroughput_workflow/env_reports/chgnet_runtime_config.json`
4. 默认 `portable_cpu_config.json`
5. 内置 auto CPU 推断

## 调度器选择

当前 controller 支持：

- `--scheduler auto`
  - 有 Slurm 时走 Slurm
  - 没有 Slurm 时自动退化到本地 screening
- `--scheduler slurm`
  - 强制要求 `sbatch/squeue`
- `--scheduler local`
  - 强制本地运行 CHGNet screening
  - 默认不会继续进入 QE recheck

## profile 含义

- `small`
  - 偏保守
  - 适合小核数 CPU 或多人共享机器

- `medium`
  - 比较均衡
  - 适合作为大多数普通 CPU 机器的起点

- `large`
  - 更激进
  - 适合核心数较多、内存较充足的 CPU 服务器

## 建议

- 新机器第一次跑，优先用“先评估，再固定”
- 临时机器，优先用 `--runtime-profile medium`
- 如果机器很小，用 `small`
- 如果是大 CPU 服务器，再考虑 `large`
