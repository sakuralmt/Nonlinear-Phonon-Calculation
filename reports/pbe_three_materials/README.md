# Rebuild the three-material PBE/MLFF report

This directory contains the portable source for the [reviewed PDF](../../output/pdf/tmd_gga_pbe_mlff_latex_report.pdf). The input data are in [docs/reference_data/pbe_20260925](../../docs/reference_data/pbe_20260925/); old LDA values under `docs/acceptance_data/` supply historical columns only. The scripts run from any working directory by finding the repository relative to their own location.

Run from the repository root after installing the package dependencies and a XeLaTeX-capable TeX Live or MacTeX runtime:

```bash
python reports/pbe_three_materials/density_audit.py
python reports/pbe_three_materials/dft_mattersim_assets.py
python reports/pbe_three_materials/full_model_assets.py
python reports/pbe_three_materials/small_model_assets.py
python reports/pbe_three_materials/generate_assets.py
python reports/pbe_three_materials/dense_ws2_assets.py
xelatex -interaction=nonstopmode -output-directory=reports/pbe_three_materials/build reports/pbe_three_materials/main.tex
```

For reliable cross-references, run XeLaTeX twice or use latexmk -xelatex. The output is reports/pbe_three_materials/build/main.pdf. The baseline density_audit.py re-fits the original three 9×9 QE grids; generate_assets.py checks the 579-point baseline and regenerates its tables and figures. Run python reports/pbe_three_materials/dense_ws2_assets.py before compilation to verify the completed 289-point WS₂ QE/MatterSim dataset and 208-job Slurm audit, then regenerate two figures, one table and dense_ws2_sources.json. All source hashes use repository data. Raw QE output directories, controllers and model weights are not needed to rebuild the published report.

Historical QE Stage1 + MatterSim Stage2 results for MoS₂/WSe₂ are bundled in `historical_qe_mattersim.json`. The report includes this LDA-geometry/mode baseline in the cubic channel tables and plots, normalized LDA error comparison, and an old-coordinate vs unit-norm audit. WS₂ and historical quartic entries remain missing. These are not newly computed PBE-mode MatterSim grids.

The new PBE DFT Stage1 + MatterSim Stage2 baseline contains 543 identical-QE configurations. `dft_mattersim_assets.py` re-fits all grids and independently recomputes aligned energy/force errors before adding the baseline to the channel tables and error plots. No new DFT calculations or model relaxation occur.

The same-model benchmark adds 5,274 E/F configurations in nine completed CPU jobs: TECE+TECE, Prophet+Prophet and EquiformerV3+EquiformerV3. It reuses each Stage1, so the existing 6x6 phonon comparison is shared. All per-channel cubic/quartic tables, selected-set ranking, PBE/LDA metrics, scatter and residual comparisons include the added workflows. Separate figures and tables show fixed-DFT E/F errors, Stage2 curvature frequencies, fit windows and raw WS2 M6 even-mixed signs. `full_model_assets.py` independently reconstructs the 13-term fits and errors from raw data.

The additional small-model section independently checks 1,629 fixed-PBE configurations, including raw mixed contrasts and signed quartic errors. GRACE server timings and local Eqnorm/DPA timings are explicitly separated.
