# WSe2 样例

这个样例的目的，是在不塞进一个巨大运行目录的前提下，让你看清楚一套真实 handoff 契约到底长什么样。

它适合阅读、测试和交接说明，不是为了替代完整生产计算。

## 包含什么

在 `contract_handoff/release_run/` 下面，这个样例带了：

- `stage1_inputs/structure/scf.inp`
- `stage1_inputs/pseudos/*.UPF`
- `stage1_inputs/mode_pairs/selected_mode_pairs.json`
- `stage1_manifest.json`
- `stage2_manifest.json`
- `stage2_outputs/chgnet/screening/pair_ranking.csv`
- `stage2_outputs/chgnet/screening/pair_ranking.json`
- `stage2_outputs/chgnet/screening/single_backend_ranking.json`
- `stage2_outputs/chgnet/screening/runtime_config_used.json`
- `stage2_outputs/chgnet/screening/run_meta.json`

## 这个样例适合做什么

- 看清标准目录结构
- 测试 `stage2` 和 `stage3` 的契约加载
- 看 manifest 里的相对路径应该怎么写
- 先用样例结构起步，再换成自己的真实输入

## 这个样例不是什么

- 不是完整归档计算结果
- 不是说这些 manifest 里的值必须原样照抄
- 不是用来替代真实 `stage1` 的

## 怎么读这个样例

1. 先看 `stage1_inputs/structure/scf.inp`
2. 再看 `stage1_manifest.json`，理解 `stage2` 会读什么
3. 再看 `stage2_manifest.json`，理解 `stage3` 会读什么
4. 最后看 ranking 文件，理解 `stage2` 的输出形状

## 怎么用它

### 当作契约样例看

把这里的 manifest 结构和你自己运行出来的 manifest 对照。重点是结构和字段，而不是照搬里面的具体数值。

### 当作小型 stage2/stage3 smoke 输入

如果你只是想确认契约读取逻辑，这个样例足够小，不需要拖一整套完整运行目录。

### 当作 WSe2 起步输入

你可以直接复用：

- `scf.inp`
- `*.UPF`

然后把 contract 文件替换成你自己真实 `stage1` 跑出来的输出。

## 运行上的提醒

真实生产分工仍然是：

- `stage1` 在 `159.226.208.67:33223`
- `stage2/3` 在 `100.101.235.12`

两边之间的交接，是靠复制契约文件完成的，不是靠包内自动 SSH。
