# QE 声子 Stage1 运行时

这个目录就是 beta 里真实的 `stage1`。

它只负责声子前端，不负责 CHGNet 筛选，也不负责 QE top5 复核。

## Stage1 最终会产出什么

`stage1` 从结构输入出发，最终产出后续阶段真正需要的内容：

- `qeph.eig`
- `qeph.freq`
- q 点筛选结果
- `selected_mode_pairs.json`
- `stage1_manifest.json`

这些东西之后会交给 `stage2`。

现在 q 点筛选、本征矢提取、模式选择和 mode-pair 生成脚本已经并回这个目录下的 `qpair_tools/`，不再拆成另一个顶层工作流目录。

## Quick Start

这个运行时默认对应：

- 宿主：适合 QE 声子前端的 Slurm 机器
- 调度器：Slurm

推荐顺序：

```bash
python3 ops/assess_stage1_env.py
python3 run_all.py
```

`ops/assess_stage1_env.py` 会探测：

- QE 可执行文件
- Slurm 分区
- launcher 可用性
- 各个 frontend 子步骤的资源布局

`run_all.py` 才是真正执行 stage1 的入口。

beta 启动器还额外提供了一个 tuning 路径：

```bash
python3 ../start_release.py --input-root <input_root> --system <system_id> --stage tune
```

这条路径会运行 `convergence/autotune.py`，按 `workflow_family` 选 profile，
并写出：

```text
qe_phonon_pes_run/results/selected_profiles.json
```

`step1_frontend.py` 如果看到这个文件，会自动读取。

对 TMDS 单层体系，这里的声子分支现在采用更严格的几何和受力阈值，不再沿用前一版 beta
里偏松的口径；即使进入 fallback，也只会做有限度放宽。

## 运行流程

```mermaid
flowchart TD
    A["scf.inp"] --> B["可选 vc-relax"]
    B --> C["筛 q 点集合"]
    C --> D["pw.x"]
    D --> E["ph.x"]
    E --> F["q2r.x"]
    F --> G["matdyn.x"]
    G --> H["提取 eig / freq"]
    H --> I["选择 mode pairs"]
    I --> J["stage1 manifest"]
```

## 当前稳定版默认参数

稳定版默认声子参数是前面收敛测试选出来的 `phonon.balanced`：

- `ecutwfc = 100`
- `ecutrho = 1000`
- `primitive_k_mesh = 12x12x1`
- `conv_thr = 1.0d-10`
- `degauss = 1.0d-10`
- `q-grid = 6x6x1`

如果走 release launcher 里的预松弛，前面的 `vc-relax` 仍然保持更严格的默认值。

## 当前默认资源拆分

资源是按 frontend 子步骤拆开的，不是全程用一套 MPI：

- `pw`：`1 node x 48 MPI`
- `ph`：`1 node x 24 MPI`
- `q2r`：`1 node x 48 MPI`
- `matdyn`：`1 node x 48 MPI`

这样做是因为 `ph.x` 和 `matdyn.x` 的并行行为本来就不一样。

## 主要输出

运行目录写在：

```bash
qe_phonon_pes_run/
```

关键文件有：

- `qe_phonon_pes_run/frontend_manifest.json`
- `qe_phonon_pes_run/results/stage1_env_assessment.json`
- `qe_phonon_pes_run/results/stage1_env_assessment.md`
- `qe_phonon_pes_run/results/stage1_runtime_config.json`
- `qe_phonon_pes_run/results/stage1_summary.json`
- `qe_phonon_pes_run/matdyn/qeph.eig`
- `qe_phonon_pes_run/matdyn/qeph.freq`

当这层通过 beta launcher 进入后续打包步骤时，会进一步生成：

- `stage1/outputs/mode_pairs.selected.json`
- `contracts/stage1.manifest.json`

## 说明

- 稳定版源码包不会自带预先生成好的 `inputs/`、`qe_phonon_pes_run/` 或验证快照。
- 环境探测不是额外调试工具，而是正常运行流程的一部分。
- 稳定版已经不再把预置 mode-pair 文件当作默认 stage1 路径。
