# Server High-Throughput Workflow

This directory contains the orchestration layer used by the beta TUI.

In the beta layout, users should start from:

- one external input root
- one `system_id`
- the `npc` launcher

This directory is the engine behind that launcher. It is not the main user
entrypoint.

## Responsibilities

- discover and validate one system directory
- prepare the runtime tree for a run
- generate and resolve internal stage contracts
- run family-aware convergence tuning for stage1 presets
- run `stage2` screening
- run `stage3` QE top-5 preparation and submission

## Runtime layout

The beta runtime tree is:

```text
runs/<system_id>/<run_id>/
  contracts/
    stage1.manifest.json
    stage2.manifest.json
    stage3.manifest.json
  logs/
  stage1/
  stage2/
  stage3/
```

`stage2` reads `contracts/stage1.manifest.json`.

`stage3` reads `contracts/stage2.manifest.json`.

Users should not need to point at those files manually during a normal TUI run.

## Main files

- `run_modular_pipeline.py`
  - stage-aware driver behind `npc`
- `system_runtime.py`
  - builds an internal runtime snapshot from `structure.cif`, `system.json`, and
    `pseudos/`
- `real_stage1_phonon.py`
  - connects the stage1 phonon frontend and tuning stage to the shared runtime tree
- `stage23_pipeline.py`
  - internal helper used by `run_modular_pipeline.py` for stage2/stage3 execution
- `stage_contracts.py`
  - contract schema and path handling
- `qe_input_utils.py`
  - CIF-to-QE input generation helpers

Files under `server_highthroughput_workflow/ops/` are operator helpers, not the
normal `npc` path.

## Stage2 outputs

The screening layer writes under:

```text
stage2/outputs/chgnet/screening/
```

Important files:

- `pair_ranking.csv`
- `pair_ranking.json`
- `single_backend_ranking.json`
- `runtime_config_used.json`
- `run_meta.json`
- `contracts/stage2.manifest.json`

## Stage3 outputs

The QE recheck layer writes under:

```text
stage3/qe/chgnet/
```

Important files:

- `selected_top_pairs.csv`
- `run_manifest.json`
- `modular_stage3_status.json`
- `contracts/stage3.manifest.json`

`contracts/stage3.manifest.json` is written as soon as preparation completes.

## Notes

- This beta keeps cross-machine handoff, but it moves that handoff into the
  runtime tree instead of asking users to understand old bundle-internal
  directories.
- The launcher can resume stage2 or stage3 by selecting the latest run root for
  the given `system_id`.
