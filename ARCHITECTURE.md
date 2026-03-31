# Beta Architecture

This file describes the current beta file structure by **actual call path**,
not by historical folder names.

## Main entry chain

```text
npc
  -> nonlinear_phonon_calculation/cli.py
  -> start_release.py
  -> server_highthroughput_workflow/run_modular_pipeline.py
  -> server_highthroughput_workflow/handoff_bundle.py (for export/import only)
```

From there, the mainline splits by stage.

## Tuning mainline

```text
run_modular_pipeline.py
  -> server_highthroughput_workflow/real_stage1_phonon.py
      -> qe_phonon_stage1_server_bundle/convergence/autotune.py
      -> qe_phonon_stage1_server_bundle/convergence/family_profiles.py
```

Meaning:

- `tune` is a TUI-driven stage, not a user-run helper script
- it writes reusable profile selections into the stage1 runtime bundle
- `step1_frontend.py` automatically consumes `qe_phonon_pes_run/results/selected_profiles.json`

## Stage 1 mainline

```text
run_modular_pipeline.py
  -> nonlinear_phonon_calculation/system_inputs.py
  -> server_highthroughput_workflow/system_runtime.py
  -> server_highthroughput_workflow/qe_relax_preflight.py
  -> server_highthroughput_workflow/real_stage1_phonon.py
      -> qe_phonon_stage1_server_bundle/run_all.py
      -> qe_phonon_stage1_server_bundle/run_all_impl.py
      -> qe_phonon_stage1_server_bundle/step1_frontend.py
      -> qe_phonon_stage1_server_bundle/stage1_env.py
      -> qe_phonon_stage1_server_bundle/common.py
      -> qe_phonon_stage1_server_bundle/config.py
      -> qe_phonon_stage1_server_bundle/scf_settings.py
      -> qe_phonon_stage1_server_bundle/qpair_tools/*
```

Meaning:

- `structure.cif`, `system.json`, and `pseudos/*.UPF` are read from the
  external input tree
- internal `system.scf.inp` is generated under the run root
- the phonon frontend runs inside `stage1/runtime/phonon_bundle/`
- q-point screening and mode-pair generation live under
  `qe_phonon_stage1_server_bundle/qpair_tools/`

## Stage 2 mainline

```text
run_modular_pipeline.py
  -> mlff_modepair_workflow/run_pair_screening_optimized.py
      -> mlff_modepair_workflow/core.py
```

Meaning:

- `stage2` reads only `contracts/stage1.manifest.json`
- ranking outputs are written under `stage2/outputs/chgnet/screening/`
- `contracts/stage2.manifest.json` is written from those outputs
- `npc --handoff-export stage2` packages the minimal stage2 continuation payload

## Stage 3 mainline

```text
run_modular_pipeline.py
  -> server_highthroughput_workflow/stage23_pipeline.py
      -> qe_modepair_handoff_workflow/prepare_top_pairs.py
      -> qe_modepair_handoff_workflow/submit_top_pairs.py
      -> qe_modepair_handoff_workflow/collect_top_pairs.py
      -> qe_modepair_handoff_workflow/common.py
      -> qe_modepair_handoff_workflow/scf_settings.py
```

Meaning:

- `stage3` reads only `contracts/stage2.manifest.json`
- QE recheck work is written under `stage3/qe/chgnet/`
- `contracts/stage3.manifest.json` is written as soon as prepare finishes
- rerunning stage3 reuses `run_manifest.json` when preparation already exists
- rerunning stage3 after collection reuses `results/qe_ranking.json` instead of resubmitting
- `modular_stage3_status.json` records `final_state`, `stage3_manifest`, and `resume_mode`

## Status and handoff control path

```text
start_release.py --status
  -> resolve latest or explicit run root
  -> read contracts/stage*.manifest.json
  -> read stage3/qe/<backend>/run_manifest.json
  -> read stage3/qe/<backend>/submission_log.json
  -> read stage3/qe/<backend>/modular_stage3_status.json
  -> read stage3/qe/<backend>/results/qe_ranking.json
```

```text
start_release.py --handoff-export stage1|stage2
  -> server_highthroughput_workflow/handoff_bundle.py
  -> tar.gz bundle with run-root-relative manifests and required payloads
```

```text
start_release.py --handoff-import
  -> server_highthroughput_workflow/handoff_bundle.py
  -> secure extract
  -> imported manifest validation
```

## Cross-machine invariant

Cross-machine handoff depends on one beta rule:

- paths recorded in `contracts/*.manifest.json` stay relative to `run_root`

That is why import does not rewrite manifest payloads. It validates that the
referenced files exist inside the newly imported run root.

The current acceptance split is:

1. `stage1` on `159.226.208.67`
2. export `stage1` handoff bundle
3. `stage2` and `stage3` on `100.101.235.12`

## Files kept on purpose but not on the mainline

These files are still useful, but they are not part of the normal `npc` path:

- `qe_phonon_stage1_server_bundle/ops/assess_stage1_env.py`
- `server_highthroughput_workflow/ops/assess_chgnet_env.py`
- `server_highthroughput_workflow/ops/bootstrap_server_env.sh`
- `server_highthroughput_workflow/ops/continue_after_screening.py`
- `server_highthroughput_workflow/ops/CPU_QUICKSTART_zh.md`
- `mlff_modepair_workflow/ops/benchmark_golden_pair.py`

They are diagnostics or operator helpers.

## Files and structures already removed from beta

- top-level `nonlocal phonon/`
- `hex_qgamma_qpair_workflow/`
- old contract example under `examples/wse2/`
- package-local duplicate `nonlinear_phonon_calculation/resources/nonlocal phonon/`
- stage2 benchmarking and historical comparison scripts not used by the beta mainline

## Current design rule

If a file is not:

1. on the `npc` mainline,
2. a direct helper for that mainline, or
3. a clearly labeled diagnostic helper,

it should not stay in the beta tree.
