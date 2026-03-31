# WSe2 输入样例

这是 beta 工作流的**用户输入样例**。

它展示的是：外部输入根目录下，一个体系目录应该长什么样。这里没有运行时产物，也没有手工维护的 stage contract。

## 文件内容

- `structure.cif`
- `system.json`
- `pseudos/W.pz-spn-rrkjus_psl.1.0.0.UPF`
- `pseudos/Se.pz-n-rrkjus_psl.0.2.UPF`

## 建议放置方式

把这个目录复制到输入根目录下：

```text
Nonlinear-Phonon-Calculation-inputs/
  wse2/
    structure.cif
    system.json
    pseudos/
      W.pz-spn-rrkjus_psl.1.0.0.UPF
      Se.pz-n-rrkjus_psl.0.2.UPF
```

然后运行：

```bash
npc --input-root /path/to/Nonlinear-Phonon-Calculation-inputs --system wse2
```

## `system.json` 是做什么的

`system.json` 刻意保持很小。

- `system_id`
  - 体系的稳定短名
- `formula`
  - 人可读的化学式
- `workflow_family`
  - 选择代码内部使用的工作流家族
- `preferred_pseudos`
  - 元素到赝势文件的显式映射
- `already_relaxed`
  - 默认是否跳过 QE relax
- `notes`
  - 可选备注

## 这个样例不是什么

它不是：

- 一整套运行目录的冻结快照
- stage1 或 stage2 contract 的教学样例
- 旧 WSe2 生产结果的打包转存

这个目录的唯一目的，是清楚地展示用户输入边界。
