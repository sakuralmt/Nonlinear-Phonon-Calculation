# Ops Helpers

This directory holds operator-side helpers and diagnostics that are **not**
part of the normal `npc` mainline.

Current contents:

- `assess_chgnet_env.py`
  - inspect CHGNet runtime behavior on a server
- `continue_after_screening.py`
  - legacy continuation helper for the older controller path
- `bootstrap_server_env.sh`
  - convenience environment bootstrap script
- `CPU_QUICKSTART_zh.md`
  - operator notes for the older server-side workflow

These files are kept because they are still useful for deployment and
diagnostics, but they are intentionally outside the main beta call path.
