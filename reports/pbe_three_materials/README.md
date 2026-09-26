# Rebuild the three-material PBE/MLFF report

This directory contains the portable source for the [reviewed PDF](../../output/pdf/tmd_gga_pbe_mlff_latex_report.pdf). The input data are in [docs/reference_data/pbe_20260925](../../docs/reference_data/pbe_20260925/); old LDA values under `docs/acceptance_data/` supply historical columns only. The scripts run from any working directory by finding the repository relative to their own location.

Run from the repository root after installing the package dependencies and a XeLaTeX-capable TeX Live or MacTeX runtime:

```bash
python reports/pbe_three_materials/density_audit.py
python reports/pbe_three_materials/generate_assets.py
xelatex -interaction=nonstopmode -output-directory=reports/pbe_three_materials/build reports/pbe_three_materials/main.tex
```

For reliable cross-references, run the final command twice or use `latexmk -xelatex`. The output is `reports/pbe_three_materials/build/main.pdf`. `density_audit.py` checks nested 5×5 versus 9×9 fits on the same `|Q|≤2` QE data. `generate_assets.py` checks the 579-point campaign summary and five channels per material, then regenerates 18 tables, ten figures and `sources.json` with repository-relative SHA256 keys. The raw QE job directories, model weights and unpublished 17×17 QE test are not needed to rebuild this completed report.
