# GGA-PBE reference for WS₂, MoS₂ and WSe₂

This is the **current DFT comparison** for the public TECE/Prophet/EquiformerV3 Stage1 + MatterSim Stage2 workflow. The old PZ-LDA [MoS₂/WSe₂ comparison](MODEL_DFT_COMPARISON.md) and [WS₂ supplement](WS2_DFT_COMPARISON.md) remain historical records, not the reference used below. The [17-page illustrated report](../output/pdf/tmd_gga_pbe_mlff_latex_report.pdf) contains the complete per-channel tables, phonon and PES figures, residuals, convergence checks and measured cost.

## Scope and provenance

QE 7.4.1 used GGA-PBE, scalar-relativistic PBE ultrasoft pseudopotentials, PBE-relaxed 1H monolayers, 120/1200 Ry and a primitive 30×30×1 k mesh. The relaxation preserved the vacuum and converged the permitted atomic and in-plane cell degrees of freedom. Each material received a fresh 6×6×1 DFPT calculation; q2r/matdyn used **simple translational ASR**. It did not enforce the 2D rotational ZA sum rule. There is no SOC or non-analytic correction.

The established five high-coupling physical channels per material were matched across PBE and the accepted MLFF lines by mass-weighted eigenvector overlap, with near-degenerate subspaces handled together. All fifteen mapped PBE channels have squared overlap above 0.996. Branch numbers alone did not define a match. Each method's final third- and fourth-order coefficients use its own relaxed structure and matched modes. Pointwise energy/force errors are reported only for the separate WS₂ Γ8–M6 **identical-input** check.

All **579/579 QE single points** finished and passed input/output hash, convergence, energy and atomic-force checks: 193 per material. Each includes five central 5×5 PES grids, the strongest channel's full 9×9 grid, plus twelve cutoff/k-mesh checks. The 18-point pilot per material passed a prespecified 2% bound on changes in the leading six-point third-order estimate. This bound does **not** prove fourth-order density convergence. The 579 jobs used 104.09 measured node-hours, excluding DFPT and controller time. The [campaign audit](reference_data/pbe_20260925/campaign_audit.json) gives provenance and measurement scope; launcher MaxRSS is not asserted to be node peak memory.

## Main numerical comparison

Frequency MAE covers 321 of the 324 harmonic modes on the 36-point grid, excluding Γ acoustic translations. Coupling MAE covers the five matched selected channels in each material. Values for all three model routes:

| Material | Route + MatterSim | Frequency MAE (THz) | `|Φ122|` MAE | Signed `Φ1122` MAE |
| --- | --- | ---: | ---: | ---: |
| WS₂ | TECE | 0.0928 | 6.104 | 2.594 |
| WS₂ | Prophet | 0.1962 | 5.244 | 2.689 |
| WS₂ | EquiformerV3 | 0.1165 | 6.143 | 2.553 |
| MoS₂ | TECE | 0.0880 | 4.328 | 2.939 |
| MoS₂ | Prophet | 0.2004 | 3.951 | 2.920 |
| MoS₂ | EquiformerV3 | 0.1614 | 4.367 | 2.875 |
| WSe₂ | TECE | 0.0654 | 1.180 | 0.628 |
| WSe₂ | Prophet | 0.1240 | 1.117 | 0.610 |
| WSe₂ | EquiformerV3 | 0.0892 | 1.223 | 0.607 |

Third-order units are meV/(Å³·amu³ᐟ²); fourth-order units are meV/(Å⁴·amu²). The [comparison JSON](reference_data/pbe_20260925/coupling_comparison.json) also gives RMSE, bias, maxima, selected-set ranking and each coefficient. Absolute `Φ122` removes a mode-sign ambiguity; `Φ1122` remains **signed**. The table does not imply full-material or unscreened-channel error.

For TECE + MatterSim, five-channel third-order MAE against old LDA → new PBE changed from **10.33 → 6.10** (WS₂), **12.54 → 4.33** (MoS₂) and **3.94 → 1.18** (WSe₂). Signed fourth-order MAE did not improve for WS₂ (2.36 → 2.59) or MoS₂ (2.43 → 2.94). Old WSe₂ LDA supplies just one recoverable fourth-order label, so no five-channel old/new fourth-order comparison is claimed. The reference change combines functional, pseudopotential and relaxed-geometry effects; it does not isolate one cause.

In WS₂ Γ8–M6, PBE gives signed `Φ1122 = −1.4141` while the three own-relaxed MatterSim routes give approximately `+1.89` to `+1.93`. MatterSim evaluated on the **same 25 PBE QE input configurations** gives `+1.9304`; relative-to-center energy MAE/RMSE is 19.404/24.629 meV per supercell, and Cartesian force-component MAE/RMSE is 0.02856/0.05486 eV/Å. Direct even-mixed energy contrasts at two amplitudes also retain opposite signs. Thus absolute-value ranking, phase and a different fitted grid do not account for this specific sign discrepancy. QE PBE-USPP and MatterSim's training labels may still differ in other details.

## Displacement density

The 6×6 phonon q mesh is independent of the two-dimensional PES grid. For the leading channel in each material, the **same** QE 9×9 data at `|Q|≤2` were refit using a nested sparse 5×5 subset (step 1) and all 9×9 points (step 0.5). The resulting fourth-order changes were +0.168% WS₂, +0.199% MoS₂ and −0.029% WSe₂. This retrospective check isolates sampling density at a fixed window; the central 5×5 `|Q|≤1` is a separate window comparison. See [density data](../reports/pbe_three_materials/density_audit.json) and the [recomputing script](../reports/pbe_three_materials/density_audit.py).

The new WS₂ Γ8–M9 **17×17**, step-0.25 QE calculation at the same maximum `|Q|≤2` is **in progress as of 2026-09-26** and is not included in the completed DFT result. Its [MatterSim-only 289-point grid](reference_data/pbe_20260925/ws2_17x17_mattersim.json) has finished: the signed fourth-order fit changes from 8.0011 (9×9, step 0.5) to 8.3945 (17×17, step 0.25), or +4.92%, at `|Q|≤2`. On the central `|Q|≤1` window it changes from 11.2105 (5×5) to 11.1666 (9×9), or −0.39%. This is a **model-only sampling result**, not a completed QE/MLFF convergence comparison. Do not infer fourth-order convergence for all channels from the three nested rank-one QE 9×9 checks.

## Reproduce and inspect

The [machine-readable directory](reference_data/pbe_20260925/) contains the audited comparison, all three 36-point QE and nine MLFF phonon datasets, mode selections, three QE PES results with point energies/forces and fit diagnostics, the same-geometry WS₂ check, and provenance. The old LDA JSON files under `docs/acceptance_data/` are required only for the **historical** columns in the report. Research QE job controllers and raw outputs are stored outside the public repository; they are not part of the installed package.

From the repository root, with NumPy, Matplotlib and a working XeLaTeX installation:

```bash
python reports/pbe_three_materials/density_audit.py
python reports/pbe_three_materials/generate_assets.py
python3 /path/to/latex-plugin/scripts/compile_latex.py reports/pbe_three_materials/main.tex --compiler texlive --engine xelatex --output-directory reports/pbe_three_materials/build
```

The first command verifies the rank-13 PES fit and original wide-grid coefficient. The second validates the 579-point audit and 15 selected channels, then regenerates 18 tables, ten figures and [relative-path source hashes](../reports/pbe_three_materials/sources.json). The compiled PDF is `reports/pbe_three_materials/build/main.pdf`; the reviewed copy is linked above. `reports/pbe_three_materials/README.md` gives prerequisites and source-file roles.
