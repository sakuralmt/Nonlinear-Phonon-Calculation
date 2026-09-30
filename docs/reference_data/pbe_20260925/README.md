# Published PBE reference data

The current interpretation and units are in [PBE_REFERENCE.md](../../PBE_REFERENCE.md). This directory contains the complete **derived, machine-readable** comparison used in the updated report. Raw QE output directories and server job controllers remain in the isolated research campaign.

- `campaign_audit.json`: 579 QE jobs, convergence/hash checks and measured node-hours.
- `coupling_comparison.json`: the fifteen matched channels, final three-/fourth-order coefficients and error metrics for TECE, Prophet and EquiformerV3 Stage1 with MatterSim Stage2.
- `results_{ws2,mos2,wse2}.json`: QE PES point energies/forces, central and wide fits and per-point provenance.
- `selection_{ws2,mos2,wse2}.json`: fixed physical-channel selection and mode-pair identities.
- `qe_phonon_dataset_*.json`, `mlff_phonon_dataset_*.json`, `*_phonon_comparison.json`: complete 36-point mode grids and matched frequency/eigenvector diagnostics.
- `structure_comparison.json`: old LDA, new PBE and MLFF-relaxed structures.
- `ws2_pbe_geometry_mattersim_m6.json`, `even_mixed_diagnostics.json`: identical-input WS₂ Γ8–M6 PES and fit-independent sign checks.
- ws2_17x17_qe.json: 289-point WS₂ Γ8–M9 QE PBE grid, including 208 new and 81 hash-verified reused points, all energies/forces, fits and raw even-mixed contrasts.
- ws2_17x17_mattersim.json: MatterSim on the same 289 atomic configurations, with matched finite-window fits.
- ws2_17x17_resource_audit.json: Slurm identities and elapsed times for the 208 new QE point jobs and one cancelled attempt.

Run `python validation/check_pbe_reference.py` from the repository root to verify channel counts, grid completeness and published MAEs. The report generator in `reports/pbe_three_materials/` records SHA256 hashes of its inputs in `sources.json`. Old LDA labels under `docs/acceptance_data/` are read only for historical report columns.

- `full_model_{material}_{model}.json`: nine complete same-model Stage2 results, each with 405 own-relaxed plus 181 fixed-PBE-input energy/force points.
- `full_model_comparison.json`: independently recomputed signed quartic/cubic metrics, fixed-input E/F errors, residuals and fit-independent contrasts.
- `full_model_manifests/`, `full_model_structures/`: original matched-mode inputs and exact own-relaxed geometries; absolute server paths describe provenance.
- `full_model_resource_audit.json`, `full_model_preflight.json`: completed/cancelled Slurm attempts, allocations and repeat-inference/force-energy checks.

`small_model_*` publishes nine completed small-model runs (1,629 energy/force configurations), manifests, preflights, resource records, and independently recomputed metrics. `historical_small_model_screen/` preserves the older single-channel screen used to shortlist models; its old QE labels and local timing are not the new PBE comparison.
