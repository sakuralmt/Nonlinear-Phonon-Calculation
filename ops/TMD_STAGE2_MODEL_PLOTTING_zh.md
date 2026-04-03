# TMD 多模型 stage2 对照离线绘图工具

本目录新增的这组脚本，用于把 `MoS2 / MoSe2 / WS2` 的多模型 `stage2` 对照固定成可复现的离线分析工具。

它们的职责只有两类：

- 从已经归档的正式结果包里抽取统一格式的数据表
- 生成与 `WSe2` 同口径的四张定量图

这些脚本不接入主工作流，不写 contract，也不修改 `stage2 / stage3` 运行逻辑。

## 1. 一键入口

脚本：

- `ops/run_tmd_stage2_model_analysis.py`

用途：

- 作为 `MoS2 / MoSe2 / WS2` 多模型对照的一键入口
- 顺序调用：
  - 数据抽取脚本
  - 柱状图脚本

默认输出目录：

- `reports/output/tmd_stage2_model_analysis/`

默认数据来源：

- `/Users/lmtsakura/qiyan_shared/result/`

默认会读取每个材料包中的四条结果线：

- `stage2_models/gptff_v2/.../pair_ranking.json`
- `stage2_models/gptff_v1/.../pair_ranking.json`
- `stage2_models/chgnet/.../pair_ranking.json`
- `baseline_local/.../stage3_qe/gptff/results/qe_ranking.csv`

## 2. 数据抽取

脚本：

- `ops/extract_tmd_stage2_model_metrics.py`

用途：

- 以 `QE stage3` 为参考排序
- 默认截取前 `5` 个进入 QE 复核的 pair
- 对每个 pair 统一抽取：
  - `GPTFF v2`
  - `GPTFF v1`
  - `CHGNet stage2`
  - `QE stage3`

输出：

- `JSON`
- `CSV`
- `Markdown`

同时计算：

- `MAE`
- `RMSE`
- `MaxAE`

误差定义固定为：

- `phi122_method - phi122_qe`

## 3. 柱状图

脚本：

- `ops/plot_tmd_stage2_model_metrics.py`

用途：

- 复用 `WSe2` 离线分析的同一套图表口径
- 对每个材料生成四张图：
  - `phi122` 绝对值柱状图
  - 相对 `QE` 的 `Δphi122` 柱状图
  - `rmse_ev_supercell / qe_rmse_ev_supercell` 对比图
  - `MAE / RMSE / MaxAE` 汇总柱状图

当前统一单位口径：

- `phi122` 与其误差：`meV/(Å amu)^(3/2)`
- 拟合残差：`eV/supercell`

## 4. 使用边界

这组脚本是分析工具，不进入生产工作流：

- 不修改 `npc`
- 不改 `stage2`
- 不改 `stage3`
- 不写运行 contract

它们的职责是把多模型对照分析固定成仓库内可重跑、可复核的脚本。
