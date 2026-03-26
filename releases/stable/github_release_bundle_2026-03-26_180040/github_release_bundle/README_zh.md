# nonlinear-phonon-calculation 安装入口

推荐安装后直接使用：

```bash
npc
```

在 bundle 目录内仍保留兼容入口：

```bash
./tui
python3 start_release.py
```

当前 stage3 默认会基于 stage2 的 ranking 自动挑选 top5 做 QE 复核。

推荐安装方式：

```bash
./install.sh
```

## 三层主线

- `stage1`：从 `scf.inp` 真实跑 QE 声子前端，并生成 `selected_mode_pairs.json`
- `stage2`：读取 `stage1_manifest.json` 和 `stage1_inputs/`，完成 MLFF screening
- `stage3`：读取 `stage2_manifest.json`，自动准备或提交 QE top5 复核

## 默认宿主

- `stage1` 默认在老机器 `159.226.208.67:33223`
- `stage2/3` 默认在新机器 `100.101.235.12`
- 包内不做自动跨机 SSH 编排，靠契约文件接力

标准 handoff 文件是：

- `release_run/stage1_manifest.json`
- `release_run/stage1_inputs/`
- `release_run/stage2_manifest.json`

## WSe2 Example

包内新增了 `examples/wse2/`：

- 带 `scf.inp`
- 带 `*.UPF`
- 带小型 `stage1_manifest.json`
- 带小型 `stage2_manifest.json`
- 带 screening ranking 文件

它的作用是演示目录结构和跨机 handoff 契约，不是把整套真实运行结果塞进稳定版。
