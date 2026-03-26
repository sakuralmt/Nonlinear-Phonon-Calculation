# WSe2 Example

This example shows what a real handoff contract looks like without shipping a
large run directory.

It is meant for reading, testing, and onboarding. It is not meant to replace a
real production run.

## What Is Included

Under `contract_handoff/release_run/` the example ships:

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

## What This Example Is Good For

- understanding the expected file layout
- testing `stage2` and `stage3` contract loading
- checking how relative paths are represented inside the manifests
- replacing the example structure with your own real inputs

## What This Example Is Not

- not a full archived calculation
- not a claim that these exact manifest values must be reused unchanged
- not a replacement for a real stage1 run

## How To Read It

1. Start with `stage1_inputs/structure/scf.inp`.
2. Check `stage1_manifest.json` to see what stage2 expects.
3. Check `stage2_manifest.json` to see what stage3 expects.
4. Read the ranking files to see the output shape produced by stage2.

## How To Use It

### Use it as a contract sample

Open the example manifests and compare them with the manifests from your own
run. The point is to match the structure, not to copy the numbers.

### Use it as a small stage2/stage3 smoke input

If you only want to validate contract loading, this example is small enough to
inspect without dragging along a full run directory.

### Use it as a WSe2 starter input

You can reuse:

- `scf.inp`
- `*.UPF`

and then replace the contract files with the outputs from your own stage1 run.

## Operational Reminder

The real production split is still:

- `stage1` on `159.226.208.67:33223`
- `stage2/3` on `100.101.235.12`

The handoff is done by copying the contract files, not by automatic SSH inside
the package.
