# TUI Cross-Machine Build Spec

Date: 2026-03-31

## Purpose

This file is the packaging handoff for the next `tui/npc` release.

It records:

- the stable baseline repo
- the beta tree where the new behavior was implemented
- the exact user-facing command surface that was validated
- the machine/environment assumptions that were actually tested
- the code areas that must be merged or wrapped by the packaging session

This document is intended to be handed to another Codex session so that session
can finish the final encapsulation work without reconstructing context from
chat history.

## Stable Baseline

Canonical stable repo:

- `/Users/lmtsakura/qiyan_shared/stable_release_manager`

Current beta implementation tree:

- `/Users/lmtsakura/qiyan_shared/testing/Nonlinear-Phonon-Calculation-tui-beta`

Do not treat transient worktrees as the source of truth for the final package.
Use the stable repo above as the merge target.

## Product Goal

The packaging target is a user-friendly top-level `npc` interface that supports:

- normal interactive run selection
- `--status`
- `stage3` resume / reuse semantics
- explicit cross-machine handoff with export/import bundles

The target machine split is:

- `stage1` on `159.226.208.67:33223`
- `stage2` and `stage3` on `100.101.235.12`

Recommended runtime environment on `100.101.235.12`:

- `qiyan-ht`

## User-Facing Command Surface

These are the intended top-level commands for the packaged `npc`:

```bash
npc --status
npc --input-root <input_root> --system <system_id> --status
npc --run-root <run_root> --status

npc --handoff-export stage1 --run-root <run_root> --output <stage1_bundle.tar.gz>
npc --handoff-export stage2 --run-root <run_root> --output <stage2_bundle.tar.gz>
npc --handoff-import --bundle <bundle.tar.gz> --run-root <new_run_root>
```

Interactive path remains:

```bash
npc
```

The prompt language remains English.

## Behavior Already Implemented In Beta

### 1. `--status`

Implemented in:

- `/Users/lmtsakura/qiyan_shared/testing/Nonlinear-Phonon-Calculation-tui-beta/start_release.py`

Capabilities:

- resolve latest run root or use explicit `--run-root`
- read-only status inspection
- does not create a new run root
- prints:
  - discovered stage manifests
  - stage2 ranking summary
  - stage3 QE summary

### 2. `stage3` resume / reuse

Implemented in:

- `/Users/lmtsakura/qiyan_shared/testing/Nonlinear-Phonon-Calculation-tui-beta/server_highthroughput_workflow/run_modular_pipeline.py`

Resume modes:

- `fresh_prepare`
- `resume_existing_prepare`
- `reuse_completed`
- `submit_collect`

Semantics:

- if `run_manifest.json` exists and QE ranking is not complete:
  - reuse prepared QE batch
- if `qe_ranking.json` exists and submission is complete:
  - reuse completed batch
- rerunning `stage3` after prepare does not regenerate QE inputs

### 3. explicit handoff export/import

Implemented in:

- `/Users/lmtsakura/qiyan_shared/testing/Nonlinear-Phonon-Calculation-tui-beta/server_highthroughput_workflow/handoff_bundle.py`
- `/Users/lmtsakura/qiyan_shared/testing/Nonlinear-Phonon-Calculation-tui-beta/start_release.py`

Capabilities:

- export stage1 handoff bundle
- export stage2 handoff bundle
- import bundle into a new run root
- validate imported manifest references remain inside the new run root

### 4. contract-relative portability rule

The beta implementation preserves the correct invariant:

- manifest file paths remain relative to `run_root`

That is the required basis for cross-machine continuation.

## Files To Port Or Wrap Into Stable Packaging

Primary files to merge from beta:

- `/Users/lmtsakura/qiyan_shared/testing/Nonlinear-Phonon-Calculation-tui-beta/start_release.py`
- `/Users/lmtsakura/qiyan_shared/testing/Nonlinear-Phonon-Calculation-tui-beta/server_highthroughput_workflow/run_modular_pipeline.py`
- `/Users/lmtsakura/qiyan_shared/testing/Nonlinear-Phonon-Calculation-tui-beta/server_highthroughput_workflow/handoff_bundle.py`

Supporting docs already updated in the stable repo:

- `/Users/lmtsakura/qiyan_shared/stable_release_manager/README.md`
- `/Users/lmtsakura/qiyan_shared/stable_release_manager/README_zh.md`

Recommended merge strategy:

1. keep the stable repo as the merge target
2. port only the validated CLI/status/handoff/resume behavior
3. do not back-port unrelated beta architecture experiments unless needed
4. preserve the existing stable packaging layout

## Real Validation Already Completed

### Local checks

Completed against beta:

- `python3 -m py_compile` for:
  - `start_release.py`
  - `server_highthroughput_workflow/run_modular_pipeline.py`
  - `server_highthroughput_workflow/handoff_bundle.py`
- `--status` in an empty context returns read-only output
- stage3 prepared rerun reuses existing QE batch
- stage3 completed rerun reuses completed results
- CLI export/import works locally

### Remote machine connectivity

Confirmed:

- `lmtsakura@159.226.208.67 -p 33223`
- `server@100.101.235.12`

### Remote environment finding

On `100.101.235.12`:

- system `/usr/bin/python3` does not include `ase`
- `qiyan-ht` does include the required workflow stack:
  - `ase`
  - `numpy`
  - `scipy`
  - `matplotlib`
  - `pymatgen`
  - `phonopy`
  - `chgnet`
  - `torch`

Therefore packaged remote execution on `100.101.235.12` should target:

- `~/miniconda3/bin/conda run -n qiyan-ht ...`

or the equivalent activated environment behavior.

### Real cross-machine handoff acceptance

This was validated with real stage1/stage2 outputs, not a synthetic toy schema.

Source data came from the previously completed real experiment on `.67`:

- `/home/lmtsakura/codex_tui_stage_contract_refactor_67_retry_20260324_221602/tui_stage_contract_refactor/contracts/stage1_contract.json`
- `/home/lmtsakura/codex_tui_stage_contract_refactor_67_retry_20260324_221602/tui_stage_contract_refactor/contracts/stage2_contract.json`

Validation steps that succeeded:

1. on `.67`, wrap the real stage1/stage2 outputs into a beta-style run root
2. export stage2 handoff bundle with:
   - `python3 start_release.py --handoff-export stage2 --run-root ~/npc_beta_real_contract_run --output ~/npc_beta_real_contract_stage2.tar.gz`
3. copy the bundle to `.12`
4. on `.12`, import with:
   - `python3 start_release.py --handoff-import --bundle ~/npc_beta_real_contract_stage2.tar.gz --run-root ~/npc_beta_real_import`
5. on `.12`, inspect with:
   - `python3 start_release.py --run-root ~/npc_beta_real_import --status`
6. on `.12`, continue in `qiyan-ht` with:
   - `conda run -n qiyan-ht python server_highthroughput_workflow/run_modular_pipeline.py --stage stage3 --run-root ~/npc_beta_real_import --system wse2 --input-root /tmp/unused_input_root --qe-relax no --qe-mode prepare_only --scheduler local --backend chgnet`

Observed success:

- handoff import succeeded
- status summary showed imported stage1 and stage2 results
- stage3 `prepare_only` succeeded
- produced:
  - `stage3/qe/chgnet/run_manifest.json`
  - `contracts/stage3.manifest.json`
- rerunning stage3 reused the prepared QE batch

Observed status output after prepare:

- stage3 manifest found
- QE run root printed
- prepared QE jobs: `405`
- final QE state: `prepared`
- resume mode: `fresh_prepare`

Observed rerun behavior:

- `[stage3] reusing prepared QE batch: ...`

This is the key proof that the cross-machine handoff model is already viable for
user-facing packaging.

## User-Friendly Design Constraints

The packaging session should preserve these rules:

- users must not manually rewrite paths inside JSON
- users must not manually assemble a run root by copying random files
- export/import must remain explicit top-level commands
- status must surface useful summaries directly in terminal output
- stage3 reruns must not silently regenerate a prepared QE batch

Do not regress the flow back into “copy directories and know the hidden schema”.

## Remaining Work For Packaging Session

The remaining work is packaging-oriented, not proof-of-concept workflow design.

### 1. merge validated beta behavior into stable repo

Target:

- `/Users/lmtsakura/qiyan_shared/stable_release_manager`

### 2. choose the remote Python strategy

For the packaged launcher, add one of these:

- explicit environment-aware interpreter selection for stage2/3 machines
- or documented wrapper behavior that runs under `qiyan-ht`

The tested target on `.12` is:

- `qiyan-ht`

### 3. expose handoff commands through the final packaged `npc`

Needed user behavior:

- export on stage1 or stage2 machine
- import on downstream machine
- check `--status`
- continue directly with `npc`

### 4. document the machine split in the final package docs

Must mention:

- `.67` for `stage1`
- `.12` for `stage2/3`
- `qiyan-ht` on `.12`

### 5. final packaging smoke tests

Minimum recommended acceptance after merge:

1. local `npc --status`
2. local export/import
3. `.67 -> .12` real stage2 handoff import
4. `.12` stage3 `prepare_only` in `qiyan-ht`
5. rerun stage3 and confirm `resume_existing_prepare`

## Suggested Merge Checklist

1. port `handoff_bundle.py` into stable repo
2. port `start_release.py` status and handoff CLI
3. port stage3 resume/reuse behavior into stable runner
4. add environment-selection handling for `.12`
5. update stable install/docs/examples if needed
6. run the acceptance steps above

## Important Paths

Stable repo:

- `/Users/lmtsakura/qiyan_shared/stable_release_manager`

Beta repo:

- `/Users/lmtsakura/qiyan_shared/testing/Nonlinear-Phonon-Calculation-tui-beta`

This handoff spec:

- `/Users/lmtsakura/qiyan_shared/stable_release_manager/packaging/TUI_CROSS_MACHINE_BUILD_SPEC_2026-03-31.md`
