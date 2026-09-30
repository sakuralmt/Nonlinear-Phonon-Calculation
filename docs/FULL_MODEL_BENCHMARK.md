# 全流程大模型与 MatterSim Stage2 对照

全流程指 TECE＋TECE、Prophet＋Prophet、EquiformerV3＋EquiformerV3。复用已有模型自行弛豫结构和匹配声子模式，仅更换 Stage2；Γ 声学模式仍排除。

每材料每模型有两组任务：

- `own_relaxed_full_flow`：五个已匹配物理通道，各 9×9 点，共 405 点；和原同 Stage1＋MatterSim 比较最终三四阶、曲率频率、残差、窗口变化与五通道内排序。
- `fixed_pbe_dft_diagnostic`：相同 PBE DFT 结构及模式，最强通道 81 点，其余各 25 点，共 181 点；与 QE 逐构型比较中心对齐能量及完整原子力。

不同结构／模式下的全流程不能报告同构型能量／力 MAE。Stage1 的频率与本征矢没有重算，也不因为多一条 Stage2 而成为独立样本。五通道筛选集不是全网格排名评估。

可复现入口：

```bash
python -m mlff_modepair_workflow.full_model_benchmark \
  --manifest /runs/manifests/ws2-tece.json \
  --model tece \
  --checkpoint /models/TECE-OAM-RRA-1.0.pt \
  --source-root /models/tace \
  --device cpu --output /runs/results/ws2-tece
```

清单显式记录材料、模型、源结构及哈希、质量归一化模式、通道映射和位移轴。TECE／EquiformerV3 核验固定源码及权重；Prophet 使用既有固定适配器。每个工作进程只加载一次模型，逐点保存能量、原子力和耗时；输入或权重变化会拒绝混合续算。服务器示例资源为 regular 分区、单节点独占、8 推理线程、64 GiB 内存；全账号排队和运行任务仍合计不超过十节点，每轮最多提交五个。服务器提交脚本属于隔离研究目录。

报告可通过 `full_model_assets.py` 从原始网格独立重拟合并重新计算误差，随后运行既有 `generate_assets.py` 及 LaTeX 编译。结果数据、表格和图形的哈希均保留在报告目录中。

打包的清单保留原始服务器路径以证明来源。在其他机器运行时，先把对应结构复制到实际运行目录，更新清单中的结构路径并保留原始文件哈希，再使用新的输出目录；不能直接照用失效的服务器绝对路径。
