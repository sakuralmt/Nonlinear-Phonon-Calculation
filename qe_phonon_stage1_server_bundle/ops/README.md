# Stage1 Ops Helpers

This directory contains stage1-side diagnostics that are useful for operators
but are not on the normal `npc` mainline.

- `assess_stage1_env.py`
  - probes the current machine, Slurm setup, and executable availability for
    the phonon frontend

The normal beta path does not require users to run these files directly.
