# GGA-PBE reference for WS₂, MoS₂ and WSe₂

This is the **current DFT comparison** for the public TECE/Prophet/EquiformerV3 Stage1 + MatterSim Stage2 workflow. The old PZ-LDA [MoS₂/WSe₂ comparison](MODEL_DFT_COMPARISON.md) and [WS₂ supplement](WS2_DFT_COMPARISON.md) remain historical records, not the reference used below. The [19-page illustrated report](../output/pdf/tmd_gga_pbe_mlff_latex_report.pdf) contains the complete per-channel tables, phonon and PES figures, residuals, convergence checks and measured cost.

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

The WS₂ Γ8–M9 **17×17** QE PBE/MatterSim test is now complete on exactly the same 289 atomic configurations. It holds the maximum displacement at |Q|≤2 Å√amu, reducing the step from 0.5 (9×9) to 0.25 (17×17). All 81 old QE points were reused after geometry and hash checks; 208 new QE points converged with 12 atomic forces each. The published [QE result](reference_data/pbe_20260925/ws2_17x17_qe.json) includes all energies, forces, per-point output hashes, fitted windows and raw even-mixed contrasts. [MatterSim](reference_data/pbe_20260925/ws2_17x17_mattersim.json) uses the identical input grid.

| Fixed window | QE signed Φ1122: coarse → dense | QE change | MatterSim signed Φ1122: coarse → dense | MatterSim change |
| --- | ---: | ---: | ---: | ---: |
| Extent ≤2, 9×9 → 17×17 | 7.6905 → 7.7022 | +0.152% | 8.0011 → 8.3945 | +4.917% |
| Extent ≤1, 5×5 → 9×9 | 7.7985 → 7.8005 | +0.025% | 11.2105 → 11.1666 | −0.392% |

The wide-window cubic strength |Φ122| is 94.7903→94.7967 for QE and 87.2202→87.4178 for MatterSim. On all 289 identical configurations, relative-to-center energy MAE/RMSE is 10.072/12.036 meV per supercell; 10,404 Cartesian force components have MAE/RMSE 0.02136/0.04047 eV/Å. The corresponding 81-point central-window errors are 5.834/6.923 meV and 0.01145/0.01917 eV/Å. The complete [Slurm audit](reference_data/pbe_20260925/ws2_17x17_resource_audit.json) records 208 completed jobs, one cancelled overloaded-node attempt, and 21.059 completed QE node-hours (controllers and MatterSim excluded).

The added grid supports **density stability of the finite-window 13-term QE fit for this one channel**. MatterSim is more sampling-sensitive over the wide window, while the center-window fit changes less. Direct even-mixed contrasts and fit residuals in the PDF show that higher-order PES shape or numerical precision remain relevant. These data do not establish an exact zero-displacement Taylor derivative or settle the separate Γ8–M6 sign discrepancy; the other fourteen channels have not received a 17×17 test.

## Reproduce and inspect

The [machine-readable directory](reference_data/pbe_20260925/) contains the audited comparison, all three 36-point QE and nine MLFF phonon datasets, mode selections, three QE PES results with point energies/forces and fit diagnostics, the same-geometry WS₂ check, and provenance. The old LDA JSON files under `docs/acceptance_data/` are required only for the **historical** columns in the report. Research QE job controllers and raw outputs are stored outside the public repository; they are not part of the installed package.

From the repository root, with NumPy, Matplotlib and a working XeLaTeX installation:

```bash
python reports/pbe_three_materials/density_audit.py
python reports/pbe_three_materials/generate_assets.py
python reports/pbe_three_materials/dense_ws2_assets.py
python3 /path/to/latex-plugin/scripts/compile_latex.py reports/pbe_three_materials/main.tex --compiler texlive --engine xelatex --output-directory reports/pbe_three_materials/build
```

The first command verifies the retrospective rank-13 QE density fits. The second checks the 579-point baseline and regenerates its tables and figures; the third verifies the completed WS₂ 289-point QE/MatterSim grid and Slurm audit, then builds the added density table and two figures. The generators record source hashes in reports/pbe_three_materials/sources.json and dense_ws2_sources.json. The compiled PDF is reports/pbe_three_materials/build/main.pdf; the reviewed copy is linked above.
