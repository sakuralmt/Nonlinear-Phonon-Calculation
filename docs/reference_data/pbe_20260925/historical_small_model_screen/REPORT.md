# Stage2 小模型本机单模式对测试（2026-09-25）

## 测试对象与方法

- 材料：MoS₂；模式对：`Gamma_p0_m8__M_q_0.500_0.000_0.000_m9`，12 原子超胞。
- 所有模型使用同一个 QE 结构、同一个 Stage1 模式对及同一个位移构造器；9×9 网格坐标为 `[-2, 2] Å√amu`、间隔 0.5，中心 5×5 用于 13 参数拟合，`fit_window=1.0`。
- QE 参考 Φ₁₂₂ = **106.1340 meV/(Å³·amu³ᐟ²)**。该值来自已有的同模式对 QE Stage3 结果，没有新跑 QE。
- 本机 Apple Silicon arm64，全部模型采用 CPU；设置 `OMP_NUM_THREADS=2`、`MKL_NUM_THREADS=2`、`OPENBLAS_NUM_THREADS=2`。不同框架内部的线程池/JIT 仍可能不同，时间仅作本机初筛。
- “81 点耗时”从第一个能量点至第 81 个点，包含首点编译，不包含 Python 启动、模型加载或首次下载。“加载耗时”从构造 calculator 开始，9×9 正式计时运行时权重均已在本机缓存。首次下载与环境安装未计入。

## 结果

| 模型 | Φ₁₂₂ | 对 QE 绝对误差 | 81 点耗时 | 单点中位耗时（去首点） | 加载耗时 |
|---|---:|---:|---:|---:|---:|
| MatterSim v1 5M | 98.428 | 7.706 | 2.143 s | 25.1 ms | 4.880 s |
| Nequix MP PFT | 128.770 | 22.636 | 2.713 s | 14.3 ms | 3.839 s |
| Nequix MP | 123.407 | 17.273 | 2.637 s | 13.9 ms | 2.920 s |
| SevenNet-l3i5* | 137.484 | 31.350 | 7.540 s | 91.0 ms | 3.194 s |
| Eqnorm MPtrj | 109.057 | 2.923 | 8.718 s | 107.0 ms | 2.187 s |
| DPA-3.1-3M-FT | 103.777 | **2.357** | 26.462 s | 141.0 ms | 2.777 s |
| GRACE-1L-OAM | 111.547 | 5.413 | **1.381 s** | **6.9 ms** | 6.569 s |

Φ₁₂₂ 及误差的单位均为 meV/(Å³·amu³ᐟ²)。SevenNet-l3i5* 使用官方 `7net-l3i5` 权重，星号沿用用户所列模型名。

## 判断

- **更好兼顾速度与精度的候选：GRACE-1L-OAM。** 在这一模式对上，它的耦合误差比 MatterSim 低 2.293 meV（约 30%），81 点网格耗时约为 MatterSim 的 64%。其加载较慢；单次“加载＋81 点”约 7.95 s，MatterSim 约 7.02 s。若一个 calculator 连续处理多对，加载成本可摊薄。
- **优先精度：DPA-3.1-3M-FT 或 Eqnorm MPtrj。** 两者误差分别为 2.357 和 2.923 meV，但完整网格分别需 MatterSim 的约 12.4 倍和 4.1 倍时间。
- **优先稳定单点推理：Nequix MP/PFT。** 去首点中位耗时约为 MatterSim 的 55–57%；然而 JAX 首点编译使这次 81 点总耗时略长，而且两者在这个模式对上的 Φ₁₂₂ 误差更大。PFT 在此三阶耦合样本上未优于原版 MP；不能据此推断它在二阶声子或其他样本上的表现。
- SevenNet-l3i5* 在这个样本的速度和误差均不优于 MatterSim。

这是**单个模式对的初筛**，不能证明模型在其他材料、波矢、模式基底或耦合类型上有同样排序。不同模型训练参考的 DFT 设置也未在本轮统一。若决定更换默认 Stage2 模型，下一步应在同一套有 QE 参考的多模式对上复测，并检查强耦合排序和近简并子空间，而非仅凭此处的单点 Φ₁₂₂ 误差。

## 数据与核验

- [benchmark.py](benchmark.py)：相同结构和模式构造、81 点模型能量计算、中心 5×5 拟合。
- [comparison.csv](comparison.csv) 与 [comparison.json](comparison.json)：原始对比指标；各模型完整能量矩阵和逐点计时在 `results/<model>/`。
- [model_manifest.json](model_manifest.json) 与 [runtime_manifest.json](runtime_manifest.json)：模型文件 SHA-256、路径和环境版本。GRACE 哈希对应 SavedModel 变量主文件。
- [summarize.py](summarize.py)：已核对七个模型各有 81 个有限能量值、相同输入 SHA-256、拟合设计满秩（13），且 9×9 中心 5×5 与独立初筛的 Φ₁₂₂ 一致。

模型入口参考：[Nequix](https://github.com/atomicarchitects/nequix)、[SevenNet](https://sevennet.readthedocs.io/en/latest/user_guide/pretrained.html)、[Eqnorm](https://github.com/yzchen08/eqnorm)、[DPA-3.1-3M-FT 权重](https://huggingface.co/deepmodelingcommunity/DPA-3.1-3M-FT)、[GRACE](https://github.com/ICAMS/grace-tensorpotential)。
