# Small Stage2 models on identical PBE configurations

This diagnostic reuses existing QE PBE relaxed structures, DFPT modes, energies and forces. It adds **no DFT jobs, relaxation, MD or new Stage1**. GRACE-1L-OAM, Eqnorm MPtrj and DPA-3.1-3M-FT were shortlisted from the archived single-channel MoS2 comparison. That older screen is under `docs/reference_data/pbe_20260925/historical_small_model_screen/`; its reference and timing are not interchangeable with the new PBE benchmark.

The new benchmark covers five selected channels in each of WS2, MoS2 and WSe2: 181 configurations per material/model, 1,629 total. The leading channel uses 9×9 over ±2 Å√amu; the other four use central 5×5 over ±1. All central derivatives use the same 13-column fit. Raw energies, forces, manifest/structure/checkpoint hashes, repeat-inference and finite-difference preflights are published. `small_model_assets.py` independently refits the arrays and checks that modes/grids equal the previously audited fixed-PBE TECE diagnostic.

## Results and interpretation

On WS2 Γ8–M6, signed quartic coefficients are QE −1.4141, TECE −1.4250, GRACE −2.9585, MatterSim +1.9304, DPA +1.6197, Eqnorm +4.4992. GRACE recovers the sign, but overestimates the negative amplitude. The wrong sign is therefore neither unique to MatterSim nor inevitable for all MLFFs. Unfitted even mixed energy contrasts corroborate the sign differences.

Across five channels, GRACE's signed-quartic MAE is 0.675/1.642/0.476 for WS2/MoS2/WSe2, compared with MatterSim's 2.572/3.302/0.620. Eqnorm and DPA are material dependent; a good older cubic result does not establish quartic accuracy. These are selected-channel diagnostics, not new full-candidate rankings, and no new small-model 17×17 density test was performed.

GRACE used three completed Slurm CPU jobs, 8 inference threads, exclusive nodes, 64 GiB request, with account-wide reservations capped at 10 nodes and this batch occupying 3. Eqnorm/DPA reused existing Apple Silicon CPU environments sequentially with 2 threads; their timing must not be ranked against server timings. The default production Stage2 remains MatterSim.

## Reproduction

Keep small-model dependencies in separate environments; they are **optional**, not core installation requirements. Versions and weight SHA-256 are locked in `mlff_modepair_workflow/small_model_benchmark.py`. Published input manifests contain original absolute paths. Relocate the structure paths to the published identical files, retain their structure hashes, and use a new output directory because the manifest identity changes.

```bash
python -m mlff_modepair_workflow.small_model_benchmark \
  --model grace-1l-oam --checkpoint /path/GRACE-1L-OAM/variables/variables.data-00000-of-00001 \
  --manifest /path/ws2-grace-1l-oam.json --output /path/new-run
python -m reports.pbe_three_materials.small_model_assets
```

Set `GRACE_CACHE` to the parent of the pinned `GRACE-1L-OAM` SavedModel directory; the official `grace_fm` factory loads that cache. Eqnorm's official factory resolves `~/.cache/eqnorm/eqnorm-mptrj.pt`, whose hash is also checked. DPA takes the explicit checkpoint without specifying a head, matching the archived model test. Set `OMP_NUM_THREADS`, `MKL_NUM_THREADS` and `OPENBLAS_NUM_THREADS` before launch. Point checkpoints and output identities prevent silent reuse after model, structure or manifest changes.

The complete source-backed PDF and machine-readable coefficients are linked from [PBE_REFERENCE.md](PBE_REFERENCE.md).
