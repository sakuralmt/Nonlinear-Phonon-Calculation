# Prophet 与 MatterSim Stage2 排名交叉验证

默认主线是 Prophet Phonopy Stage1 给出模式，MatterSim Stage2 对 486 个候选模式对计算并排序，再只对入选的强耦合通道做 DFT 精算。DFT Stage3 当前按要求暂停。本报告用另一条 Prophet Stage2 线检查 MLFF 排名是否稳健；Prophet Stage2 不是默认主线的必经筛选步骤，MatterSim 的排名也不是 DFT 真值。

MoS₂ 和 WSe₂ 共用结构的两种 Stage2 均已各完成 486/486 对。在各材料内部，Prophet 与 MatterSim 使用完全相同的旧 v3 模式文件、结构和 9×9 势能面网格，且都以中央 5×5 拟合 Φ₁₂₂。MoS₂ 的模式/结构 SHA256 分别是 `b5a6c9caff2e50dfa81953e49b490236058d8b459ee8ed98e008fcaeb0bdd913` / `e1bac29284dbd23a43efa043e321e6344c43da3fb09ced93ed91de021c5ab89a`；WSe₂ 分别是 `78b7355c4e093855c2069f5ba34a59c56fc5f097a0eccffd0f56d9f64081c2c7` / `239f9f0288a55eacf854bc39bab2a641bbdfd614f0822b16e0348af3f830eed1`。因此下表比较的是模型差异，不混用模式定义或拟合窗口。

| 排名指标 | MoS₂ | WSe₂ |
| --- | ---: | ---: |
| 486 个原始模式对的 `|Φ₁₂₂|` Spearman 相关 | 0.7755 | 0.7481 |
| 原始模式对前 5 / 10 / 20 / 30 重合 | 4 / 9 / 16 / 27 | 5 / 9 / 19 / 27 |
| Prophet 前 20 / 30 对覆盖 MatterSim 前 20 对 | 16 / 20；20 / 20 | 19 / 20；20 / 20 |
| 270 个 Γ 子空间/q 模通道的三阶范数 Spearman 相关 | 0.8669 | 0.8771 |
| 等价通道前 5 / 10 / 20 / 30 重合 | 4 / 9 / 16 / 27 | 5 / 9 / 19 / 28 |
| Prophet 前 20 / 30 通道覆盖 MatterSim 前 20 通道 | 16 / 20；20 / 20 | 19 / 20；20 / 20 |

物理通道按 Γ 近简并子空间内的 `||Φᵢqq||` 排名，不把任意选取的简并单支方向当成唯一物理量。MoS₂ 被 Prophet 前 20 漏掉的四个 MatterSim 前 20 通道，在 Prophet 排名中分别为第 21、23、25、29；WSe₂ 唯一漏掉的是 Γ8–K(1/3,1/3) 第 4 支，MatterSim 第 12、Prophet 第 24。因此这两条线的 Prophet 前 30 均覆盖 MatterSim 前 20，但当前的 DFT 候选仍应以默认 MatterSim 排名生成。新 Prophet Phonopy Stage1＋MatterSim Stage2 的独立排名和候选文件见 `docs/prophet_phonopy_stage12_validation.md`；不应混用旧自写 Stage1 的模式文件哈希。

旧同网格 CPU 作业累积的模式对计算时间为 MoS₂ Prophet 约 494,010 秒、MatterSim 约 11,019 秒；WSe₂ 分别约 426,300 秒、8,941 秒。实现、并行和节点条件不同，比值不是普适速度定律；它足以否定仅凭模型参数量把 Prophet Stage2 当作低成本必经筛选的设想。

对物理耦合比较，有限 q 模的整体复相位、q↔−q 约定和近简并 Γ 模的子空间内基底旋转都应按相应变换处理。本轮**新旧 Prophet 接口**的等价性审计只需在相同 q 点处理复相位与 Γ 子空间旋转；它没有声称验证任意点群旋转后的模式映射。逐点相减两个固定坐标的 PES 网格才要求实际位移坐标先对齐；这种网格回归限制不应转化为声子模式或耦合是否等价的判据。两种 MLFF 排名一致性也不能替代未来 DFT 复核的准确度检验。本轮审计 JSON 保存在本地验证目录 `screening/mos2/` 和 `screening/wse2/`。
