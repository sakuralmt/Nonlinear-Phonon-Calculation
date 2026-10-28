# 对称性筛选：方法和验证范围

[中文 README](../README_zh.md) · [English README](../README.md) · [算法细节](../ARCHITECTURE.md)

## 已经验证到哪里

本项目最初研究六角 TMDC 单层。自动候选生成、对称等价点约化、简并模式处理和分级筛选的详细实算验证，主要在这一材料族内完成。其他六角材料的系统筛选验证仍不充分。

| 已有工作 | 实际覆盖 | 可以支持的结论 |
| --- | --- | --- |
| TMDC 完整筛选实验 | MoSe₂、WS₂；TECE、Prophet、EquiformerV3 三条 Stage1 路线，各接 MatterSim；每组 324 个六点候选及入选精算 | 在这些材料和设置下，流程可以完成筛选、等价点检查和入选势能面计算。详见[补充验证](ACCEPTANCE.md)。 |
| TMDC 的 DFT 比较 | MoS₂、WS₂、WSe₂ 的谐波与选定非谐模式对 | 评价已选模式的频率、耦合及势能面误差，范围见[PBE 基准](PBE_REFERENCE.md)。 |
| 软件单元测试 | 60°/120° 晶胞表示；一、二、三原子测试结构；BN、Janus 及降低原子对称性的结构；解析势和弹簧模型 | 检查原子置换、波矢轨道、本征矢规范、简并子空间与回退逻辑。测试结构的名称不表示对该真实材料做过完整 MLFF–DFT 筛选验证。 |
| 后续非 TMDC 研究 | h-BN、蓝磷和石墨烯的固定模式对 PES 比较，数据在独立研究记录中保存 | 提供这些方向上的 DFT、MatterSim 和 GRACE 比较；尚不足以将完整自动对称性筛选推广到其他六角材料。 |

表中的工作检验不同问题。例如，使用 BN 几何搭建的弹簧模型，可以检验程序在一个已知对称性模型上是否正确；真实 h-BN 势能面的等价点检查仍需使用真实计算器。

## 程序怎样使用对称性

`structure_symmetry.py` 从晶胞、原子种类和坐标出发，用 spglib 获取实际结构的空间操作。程序核对原子置换与晶格平移，再检查 Γ 点和有限 q 点的频率、本征矢变换。简并模式通过子空间重叠比较。

通过这些检查的操作才用于合并 q 点。空间协变性检查失败时，程序记录原因并退回 q/−q 约化；时间反演关系也失败时停止。实现面向无外场的非磁性标量势。六角晶格常数或晶格角度本身不足以决定可用操作，原子基元和模式同样重要。

谐波协变性通过后，模型在有限振幅下是否仍给出相等的势能和耦合，需要另看等价位移的实算结果。TMDC 已有相应记录；其他六角材料尚缺同等系统的检查。

## 适用范围的表述

方法以六角 TMDC 单层为主要验证对象，代码按实际原子结构和模式检查对称性。其他六角材料已有选定模式对的探索，完整筛选仍需单独验证。

对于新材料，应保留程序输出的对称操作、轨道和模式映射，再根据研究问题检查必要的等价位移。检查范围由物理目标和所需精度决定；本次文档修订没有增加计算。

## English summary

Detailed end-to-end validation of symmetry-reduced screening is concentrated on hexagonal TMDC monolayers. The documented MoSe₂/WS₂ campaigns cover three Stage1 models, complete screening and selected refinements. MoS₂/WS₂/WSe₂ DFT studies test harmonic and selected anharmonic accuracy.

Tests using one-, two- and three-atom cells, BN-like or Janus motifs, analytic potentials and spring Hessians check software behaviour. They are not equivalent to material-specific MLFF–DFT validation. Later fixed-pair PES comparisons for h-BN, blue phosphorus and graphene also address a narrower question than full screening equivalence.

The implementation uses the atomic motif and phonon covariance to decide which spatial operations to retain, with conservative q/−q fallback where required. Other hexagonal materials have not received equally detailed screening validation.
