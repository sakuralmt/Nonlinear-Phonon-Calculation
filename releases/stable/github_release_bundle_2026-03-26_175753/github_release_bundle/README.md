# Nonlinear Phonon Calculation CLI

This bundle provides an installable command-line entrypoint for the staged nonlinear-phonon workflow.

## Install

Recommended local install:

```bash
./install.sh
```

This installs the `npc` command and keeps the bundle directory as the working source of truth for workflow files and defaults.

If you want a non-editable install for packaging validation:

```bash
NPC_INSTALL_MODE=wheel ./install.sh
```

## Run

Preferred entrypoint after install:

```bash
npc
```

Bundle-local compatibility entrypoints:

```bash
./tui
python3 start_release.py
```

## Workflow stages

- `stage1`: run the real QE phonon frontend from `scf.inp` and generate `selected_mode_pairs.json`
- `stage2`: run MLFF screening from an existing stage1 manifest
- `stage3`: prepare or submit QE top5 recheck jobs from an existing stage2 manifest
- `all`: optional QE relax, then `stage1 -> stage2 -> stage3`

## Default host split

- `stage1` is intended to run on the older multi-node server: `159.226.208.67:33223`
- `stage2` and `stage3` are intended to run on the newer server: `100.101.235.12`
- Cross-machine handoff is file-contract based. We do not automate SSH handoff inside the bundle.

The normal handoff files are:

- `release_run/stage1_manifest.json`
- `release_run/stage1_inputs/`
- `release_run/stage2_manifest.json`

## Example

`examples/wse2/` contains a bundled WSe2 example with:

- a minimal input structure and pseudopotentials
- a small contract-style `stage1_manifest.json`
- a small contract-style `stage2_manifest.json`
- screening ranking files for handoff demonstration

It is meant to show directory shape and handoff semantics, not to replace a real full run.

## Notes

- `npc` is the intended operator-facing command.
- `./tui` remains as a bundle-local compatibility alias.
- Scientific runtime dependencies such as QE, CHGNet, and server-side scheduler tools are still environment-specific and are not bundled from PyPI.
- The stable bundle intentionally excludes caches, local runs, historical benchmark outputs, and golden-reference datasets.
