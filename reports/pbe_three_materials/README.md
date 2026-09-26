# Rebuild the three-material PBE/MLFF report

This directory contains the portable source for the [reviewed PDF](../../output/pdf/tmd_gga_pbe_mlff_latex_report.pdf). The input data are in [docs/reference_data/pbe_20260925](../../docs/reference_data/pbe_20260925/); old LDA values under `docs/acceptance_data/` supply historical columns only. The scripts run from any working directory by finding the repository relative to their own location.

Run from the repository root after installing the package dependencies and a XeLaTeX-capable TeX Live or MacTeX runtime:

```bash
python reports/pbe_three_materials/density_audit.py
python reports/pbe_three_materials/generate_assets.py
python reports/pbe_three_materials/dense_ws2_assets.py
xelatex -interaction=nonstopmode -output-directory=reports/pbe_three_materials/build reports/pbe_three_materials/main.tex
```

For reliable cross-references, run XeLaTeX twice or use latexmk -xelatex. The output is reports/pbe_three_materials/build/main.pdf. The baseline density_audit.py re-fits the original three 9×9 QE grids; generate_assets.py checks the 579-point baseline and regenerates its tables and figures. Run python reports/pbe_three_materials/dense_ws2_assets.py before compilation to verify the completed 289-point WS₂ QE/MatterSim dataset and 208-job Slurm audit, then regenerate two figures, one table and dense_ws2_sources.json. All source hashes use repository data. Raw QE output directories, controllers and model weights are not needed to rebuild the published 19-page report.
