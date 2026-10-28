# Numerical method and public architecture

[README](README.md) · [安装与权重](docs/INSTALL.md) · [Migration](docs/MIGRATION.md)

The physical calculation starts with a Γ-point optical displacement and asks how it changes a finite-q mode's energy and restoring force. Harmonic modes define the displacement directions; a two-coordinate PES supplies the anharmonic coefficients.

The implementation was developed and tested most extensively on hexagonal TMDC monolayers. Detailed screening validation for other hexagonal materials remains limited. Selected-pair PES studies and synthetic symmetry tests cover narrower questions; [SYMMETRY_SCOPE.md](docs/SYMMETRY_SCOPE.md) lists the evidence for each.

## Public interfaces and model roles

The public commands are:

- `npc stage1`: MLFF structure/modes, or import of audited existing DFT modes.
- `npc stage2 screen|refine|audit`: checkpointed **MatterSim** PES sampling.
- `npc stage2 reference`: MatterSim on a supplied, matched existing QE grid.
- `npc status`: read saved completion counts.
- `npc --version` and `npc --help`: installation/command inspection without model loading.

`pyproject.toml` maps `npc` to `nonlinear_phonon_calculation.cli:main`. The equivalent module is `python -m nonlinear_phonon_calculation.cli`. Core calculations, report generation and cluster execution have separate modules. The optional `full_model_benchmark` and `small_model_benchmark` runners read their own manifests. They provide archived model comparisons, while the main Stage2 CLI uses MatterSim. QE submission and cluster control are handled by the research execution layer.

Stage1 defaults to TECE-OAM-RRA-1.0; Prophet OAME-MBD and EquiformerV3+DeNS-OAM are alternatives. Stage2 uses MatterSim-v1.0.0-5M with package 1.2.1. Model environments can remain separate because files, not in-memory model objects, cross the Stage1/Stage2 boundary. [INSTALL.md](docs/INSTALL.md) records the source and weight identities.

## Geometry and harmonic calculation

An MLFF Stage1 run first relaxes its explicit-cell (`ibrav=0` plus `CELL_PARAMETERS`) QE input with its own calculator. The DFT import has a separate reader supporting the archived `ibrav=4` representation; these input examples are not interchangeable. Developed for the TMDC calculations, this protocol preserves the input's atomic constraints, vacuum length and hexagonal shape. It optimizes permitted ionic coordinates with BFGS (`fmax=0.02 eV/Å`) and searches an isotropic in-plane scale in `[0.94,1.06]`. A failed ionic optimization or a lattice optimum at a search boundary stops the run. The fixed-vacuum and fixed-shape constraints are part of the calculation protocol.

The **qe-dft route is different**: it imports the original DFT-relaxed structure and audited frequencies/complex eigenvectors without another relaxation, QE call, phonon calculation or ASR modification. It validates the published JSON export contract; it does not parse every arbitrary QE output format. DFT-source and model-relaxation provenance remain distinct.

Phonopy 2.38.0 constructs force constants from Cartesian central differences on a commensurate supercell. The default q mesh is 6×6×1, with displacement steps 0.01 and 0.005 Å. Raw and ASR-corrected force constants are retained. Translational/index-exchange symmetrization does not enforce the rotational sum rule needed for ideal long-wavelength 2D ZA behavior. There is no non-analytic long-range correction in this implementation.

The primitive cell fixes the branch count, `3N` per q. A two-atom primitive cell has six branches; a three-atom primitive cell has nine. Sampling supercells have more atoms so the chosen q fits periodic boundaries, not because extra primitive-cell branches have been introduced.

## Mode identity, spatial symmetry and translations

`structure_symmetry.py` verifies spglib's atom permutations and lattice translations, rejects operations that fail the layer/mesh requirements, and tests phonon covariance with transformed complex eigenvectors at Γ and finite q. Frequencies must agree. Degenerate groups use overlap-matrix singular values rather than identical branch numbers. Γ covariance is required for each retained spatial operation and its time-reversed partner. Failed spatial covariance falls back to conservative q/−q grouping; failed time reversal stops the run.

These checks assume a nonmagnetic scalar potential without external fields. For the real-material validation, the main screening and equivalent-displacement campaigns use TMDCs. Passing harmonic covariance checks in another material does not by itself establish finite-amplitude PES equivalence. Corresponding modes from different relaxed structures are matched by eigenvector or subspace overlap, with their frequencies checked as well.

Only Γ optical directions enter candidates. The three translation directions are identified by normalized mass-weighted vectors:

```text
t_alpha(i,beta) = sqrt(m_i / sum(m)) * delta_alpha,beta
```

An excluded mode needs at least 0.9 squared overlap with the translation subspace; a retained mode needs at most 0.1. Ambiguous translation/optical mixing stops the run. This handles an unstable optical mode that sorts below translations. Optical activity and the coupling to a chosen pump require effective charges or Raman tensors in addition to this displacement classification.

For `N` primitive atoms and `C` verified finite-q orbits, the candidate count is `C × (3N−3) × 3N`. The archived three-atom TMD case has `C=6`, hence 324 pairs. The count is not hard coded; a one-atom primitive cell has no Γ optical candidates. Finite-q acoustic and optical branches remain eligible.

## A pair, a coupling group and a complete tensor are different

A pair is one Γ direction and one finite-q direction in a stated basis. Near-degenerate Γ optical modes are grouped with a default 0.01 THz threshold. Stage2 ranks the norm of the **complete Γ component vector** for a fixed target direction. Required finite-q degenerate siblings are included in selected groups.

That Γ norm does not recover missing off-diagonal components of a finite-q degenerate tensor. Individual directions and their rankings can change with a rotation of the degenerate basis. Therefore a selected group may contain several pairs, and a sum of computed diagonal projections is not a general basis-invariant full tensor.

`core.py` constructs a real q/−q standing wave and normalizes the actual supercell displacement direction to unit mass-weighted norm. Self-conjugate q points and a general q/−q pair have different real-part norms; a universal factor of √2 is not valid. The coordinate describes a fixed real direction, not the complete complex Bloch three-phonon vertex.

Whole-supercell normalization also sets the scale of a bare coefficient. For the same primitive mode, q, gauge and periodically repeated displacement pattern, total energy is extensive while a unit-mass coordinate gives atomic amplitudes proportional to `N_c^(-1/2)`. Thus bare `Phi3 ∝ N_c^(-1/2)` and `Phi4 ∝ N_c^(-1)`. Comparing raw scores across supercell conventions is not a size-independent intrinsic-coupling ranking. Keep the supercell and coordinate convention with each result; do not apply an undocumented rescaling or infer a full Bloch vertex from these numbers.

## Sampling grids and fitted quantities

The six-point screen uses `QΓ=±1`, `Qq=−1,0,+1` Å√amu. It measures a finite-step proxy for the change in target curvature with Γ displacement; it is not a fourth-order fit.

For selected complete groups, refine uses `Q∈{-1,-0.5,0,0.5,1}` on each axis. Optional audit uses `Q∈{-2,-1.5,…,1.5,2}` on each axis for the strongest five refined groups. The step stays 0.5, so audit tests a wider window rather than a denser sample of the same interval. Neither operation changes the harmonic q mesh.

The energy-only linear least-squares fit has 13 columns:

```text
V(x,y) = a00 + a10*x + a01*y + a20*x² + a02*y² + a11*x*y
       + c111*x³ + c222*y³ + c112*x²*y + c122*x*y²
       + d1111*x⁴ + d2222*y⁴ + d1122*x²*y²
```

This omits `x³y` and `xy³`; it is a defined finite-window ansatz, not the full two-variable quartic Taylor model for all wavevectors. In particular, `3q=G` can make `xy³` translation-allowed; point-group analysis is still required. Added sample points can stabilize coefficients within this ansatz without establishing that every omitted term is negligible.

Reported Taylor derivatives use factorial factors:

```text
Phi122 = 2*c122          Phi112 = 2*c112
Phi111 = 6*c111          Phi222 = 6*c222
Phi1122 = 4*d1122
Phi1111 = 24*d1111       Phi2222 = 24*d2222
```

Standard screen/refine/audit point checkpoints store energies. Stage1 force checks and the reference/benchmark interfaces that store complete energy/force labels are separate paths; they must not be generalized to all checkpoints. Forces are not part of this energy-only fit. A full-rank fit determines the coefficients of this finite-window model; the residual measures interpolation error. DFT comparisons assess the potential, and window comparisons assess the sensitivity of its effective derivatives. Unidentifiable quantities are left unspecified.

`momentum_diagnostics` checks translation allowance. In a real q/−q coordinate, `y^n` contains harmonics `(2k−n)q`; a term is potentially translation-allowed when one is reciprocal. This is not a point-group selection-rule calculation. At genuinely finite q, `Phi112` is forbidden and remains a diagnostic, while `Phi122` and `Phi1122` are momentum-allowed. Diagnostic forbidden coefficients are not ranking targets.

## Units, curvature and physical interpretation

Lengths are Å; masses amu; energies eV per supercell; forces eV/Å; frequencies THz; real normal coordinates Å√amu. Cubic and quartic derivative units are eV/(Å³ amu^(3/2)) and eV/(Å⁴ amu²), or their explicitly converted meV forms. Energy conversion requires a declared source unit.

For the stated fit, the target-axis curvature at `y=0` is:

```text
kappa_q(x) = 2*a02 + 2*c122*x + 2*d1122*x²
```

This is the static curvature along the frozen target direction. Its relation to a displaced-system eigenfrequency requires the full Hessian. Optical driving, damping and mode mixing enter a subsequent dynamical calculation. Finite-q mean displacement and two-phonon correlations also describe different observables.

Compare signed fourth-order coefficients together with their actual energy or restoring-force contribution over the stated amplitude range. A large percentage error near a zero DFT coefficient may be physically small. Cross-structure total energies or pointwise force errors are not meaningful same-configuration benchmarks; those require identical geometry, modes and displacement positions.

## Floating-point protocol and run identity

The pinned MatterSim adapter uses an instance-local hook to promote atomic energies to float64 **before** their final sum. Network features and weights stay float32, and autograd remains connected. This is `atomic_float32_sum_float64_v2`, not a full-double network. Casting the final scalar alone would not undo earlier rounding. No global scatter function or installed model source is patched by that hook.

A Stage2 identity includes mode-pair and structure hashes, checkpoint, contract, normalization, grids, selected-group count and energy-accumulation protocol. MLFF runs also verify the relaxation summary; DFT imports verify their own frozen source information. Identical weight bytes alone do not justify mixing old float32-summed points with the corrected protocol.

Atomic JSON replacement and per-pair locking protect point checkpoints. Shards partition complete pairs; finishing one shard does not finalize a phase. Finalization checks required finite points and, for fitted phases, rank 13 before writing phase rankings. The reference phase uses one checkpointed process per material; it does not support normal Stage2 sharding or `--finalize-only`.

Saved timing, process peak RSS, host and optional Slurm identifiers describe the execution process. They do not measure whole-node peak memory or enforce a cluster-wide job budget. Repeat a run with unchanged inputs to resume; changed identities need a separate output directory. See [MIGRATION.md](docs/MIGRATION.md).

## Source layout

| Location | Responsibility |
| --- | --- |
| `nonlinear_phonon_calculation/cli.py` | Public command parsing and dispatch |
| `mlff_modepair_workflow/advanced_stage1.py`, `prophet_stage1.py` | Model-specific Stage1 paths |
| `model_relaxation.py`, `phonopy_bridge.py` | Constrained geometry optimization and Phonopy interface |
| `structure_symmetry.py`, `phonon_eigenvectors.py` | Primitive modes, covariance and reciprocal equivalence |
| `core.py`, `units.py`, `screening_stage2.py` | Real displacements, units, sampling, fit and restart contract |
| `dft_reference.py` | Existing DFT mode import and same-configuration MatterSim comparison |
| `full_model_benchmark.py`, `small_model_benchmark.py` | Separate optional archived benchmark runners |
| `tests/` | Software/contract regression tests; no substitute for scientific convergence |
| `reports/`, `docs/reference_data/` | Public report sources and archived reference data, outside the wheel |

Historical v3/v4 results remain diagnostic references, not v5 restart inputs. The default calculator and the scientific sampling rules are not changed merely by a maintenance-version increment.
