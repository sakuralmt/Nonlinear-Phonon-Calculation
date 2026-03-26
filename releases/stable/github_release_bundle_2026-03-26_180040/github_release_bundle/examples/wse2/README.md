# WSe2 Example

This example bundles a small contract-style handoff for the WSe2 workflow.

It includes:

- `release_run/stage1_inputs/structure/scf.inp`
- `release_run/stage1_inputs/pseudos/*.UPF`
- `release_run/stage1_inputs/mode_pairs/selected_mode_pairs.json`
- `release_run/stage1_manifest.json`
- `release_run/stage2_manifest.json`
- `release_run/stage2_outputs/chgnet/screening/*`

This example is intentionally small:

- it is suitable for reading the file layout
- it is suitable for testing stage2/stage3 contract loading
- it is not a bundled full run result set

Recommended usage:

1. Use the bundled `scf.inp` and pseudos as a minimal WSe2 input example.
2. Read `stage1_manifest.json` to see the expected stage1 handoff shape.
3. Read `stage2_manifest.json` to see the expected stage2 handoff shape.
4. Replace the structure or manifests with your own run outputs when doing a real workflow.

Important notes:

- The manifests here are example contracts, not a claim that these exact files must be reused verbatim.
- The real production workflow is still:
  - `stage1` on `159.226.208.67:33223`
  - `stage2/3` on `100.101.235.12`
- Cross-machine handoff is done by copying `release_run/` contract files, not by automatic SSH inside the bundle.
