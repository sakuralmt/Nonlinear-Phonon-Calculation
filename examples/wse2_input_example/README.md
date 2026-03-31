# WSe2 Input Example

This is the user-facing example for the beta workflow.

It shows what one system directory should look like under the external input
root. Nothing here is a runtime artifact. Nothing here is a hand-edited stage
contract.

## Files

- `structure.cif`
- `system.json`
- `pseudos/W.pz-spn-rrkjus_psl.1.0.0.UPF`
- `pseudos/Se.pz-n-rrkjus_psl.0.2.UPF`

## Intended placement

Copy this directory under your input root:

```text
Nonlinear-Phonon-Calculation-inputs/
  wse2/
    structure.cif
    system.json
    pseudos/
      W.pz-spn-rrkjus_psl.1.0.0.UPF
      Se.pz-n-rrkjus_psl.0.2.UPF
```

Then run:

```bash
npc --input-root /path/to/Nonlinear-Phonon-Calculation-inputs --system wse2
```

## What `system.json` means

`system.json` is intentionally small.

- `system_id`
  - short stable identifier for the system
- `formula`
  - human-readable formula
- `workflow_family`
  - selects the workflow family used by the code
- `preferred_pseudos`
  - explicit element-to-file mapping
- `already_relaxed`
  - whether the structure should skip the QE relax stage by default
- `notes`
  - optional free-form note

## What this example is not

It is not:

- a frozen copy of a full runtime tree
- a stage1 or stage2 contract example
- a dump of old WSe2 production results

The purpose of this directory is to show the clean user input boundary.
