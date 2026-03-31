# Stage2 Screening Module

This directory is the stage2 screening module used by the beta workflow.

It has been reduced to the pieces that are still on the main path:

- `core.py`
  - runtime selection, batching, CPU affinity, and screening utilities
- `run_pair_screening_optimized.py`
  - the stage2 screening entrypoint
- `ops/benchmark_golden_pair.py`
  - retained only as a focused diagnostic helper

The beta TUI does not ask users to run these files directly in normal use.
`npc` calls them through the stage orchestrator.

## Main output

Stage2 writes its results under:

```text
stage2/outputs/chgnet/screening/
```

with:

- `pair_ranking.csv`
- `pair_ranking.json`
- `single_backend_ranking.json`
- `runtime_config_used.json`
- `run_meta.json`
