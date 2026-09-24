# Phonopy Stage1 stable route (2026-09-24)

## Existing computation

The active Prophet and advanced-model Stage1 paths displaced each primitive
atom by ±0.01 Å in a 6×6×1 supercell, obtained MLFF forces, formed force
constants by central difference, Fourier-transformed the force constants on
the 6×6 q mesh, and diagonalized a mass-weighted dynamical matrix. This is an
atomic finite-displacement calculation, not a lattice-strain calculation. The
implementation was local code in `phonon_eigenvectors.py` and
`prophet_stage1.py`; no Phonopy workflow was called.

## Independent audit on existing data

Phonopy 2.38.0 first read the *saved* force constants in eV/Å²; no MLFF or
DFT evaluations were repeated for this independent eigenanalysis check. Its
full-supercell force-constant order was mapped explicitly from the existing
`Phi[R, target, source]` order. The q-point solver used the eV/Å² → THz factor
15.63330423985619, with no space-group reduction, acoustic-sum-rule correction
or non-analytical correction in this **raw** comparison. The QE file supplied
the structure only; it did not determine the force-constant units.

| Prophet geometry line | q points | Max absolute frequency difference, THz | Minimum same-branch eigenvector overlap² after gauge conversion |
| --- | ---: | ---: | ---: |
| MoS₂ shared DFT | 36 | 2.96×10⁻¹² | 0.9999999999999964 |
| MoS₂ Prophet-relaxed | 36 | 4.53×10⁻¹² | 0.9999999999999969 |
| WSe₂ shared DFT | 36 | 1.38×10⁻¹² | 0.9999999999999958 |
| WSe₂ Prophet-relaxed | 36 | 7.43×10⁻¹³ | 0.9999999999999956 |

Phonopy eigenvectors have an atomic-basis Bloch phase relative to the existing
v3 Stage2 convention. For primitive fractional position τ, the v3 vector is
`v_v3(q) = exp(+2πi q·τ) v_phonopy(q)`, followed by the established global
phase choice. Without that conversion, finite-q single-mode overlaps can look
poor despite identical physical modes. The Stage2 real-displacement contract
therefore remains fixed, and old and new mode files must not be mixed solely
on the basis of matching branch numbers.

## Stable MLFF Stage1 route

The default `--phonon-engine phonopy` makes Prophet and the advanced MLFF Stage1 adapter use
Phonopy to generate all positive/negative Cartesian displacements, collect raw
ASE forces, fit full supercell force constants, and diagonalize every explicit
q point. The Phonopy route explicitly calls
`symmetrize_force_constants(level=1)` by default, enforcing translational
ASR and force-constant index exchange; `--no-phonopy-asr` is diagnostic only.
The raw and corrected force constants are both saved, and the drift and
maximum correction are recorded. This does **not** enforce rotational
invariance, use space-group symmetry, or add non-analytical corrections. Both
routes retain the existing v3 mode-pair and unit contract after the phase
conversion. The run archives `phonopy_params.yaml` containing the corrected
force-constant model. `phonopy==2.38.0` is pinned as a required dependency.
The modular runner exposes `--phonon-engine custom` only for historical
regression. Use a separate run root from old custom-engine jobs; the runner
rejects reusing a root with a different phonon engine. A verified Phonopy
Stage1 manifest can be reused on restart without recalculating the force
constants.

The analytic bridge tests cover 1/2/3-atom primitive cells, full Cartesian
displacement count, force-constant tensor permutations, ASE atom-order
preservation, explicit ASR drift closure, finite-q frequency, and eigenvector
phase conversion. A two-step Prophet-interface smoke test on MoS₂ produced all
81 pairs for a 2×2 mesh and an inspectable Phonopy archive.

A real MoS₂ shared-DFT-geometry EquiformerV3 6×6 CPU pilot completed as Slurm
job `1019380` in 58 s and produced 486 mode pairs. Its raw translational
drift was 6.83×10⁻⁵ eV/Å² and the Phonopy-corrected drift was
7.74×10⁻¹⁴ eV/Å²; the largest force-constant change was 7.73×10⁻⁴ eV/Å².
The corrected `phonopy_params.yaml` reloaded into full force constants with
maximum difference 8.9×10⁻¹⁶ eV/Å² from the saved v3 tensor. Two earlier
raw-only pilot directories remain as diagnostic evidence and must not be
presented as ASR-enabled runs.

The final finite-q tie-stable phase rule and explicit ASR were then exercised
on **both shared-DFT geometries** in one sequential 16-CPU `regular` job,
`1019396`, completed in 1 min 54 s. Both lines produced 36 q points and 486
pairs. MoS₂ raw/ASR translational drifts were 7.00×10⁻⁵ → 1.31×10⁻¹³
eV/Å²; WSe₂ 1.23×10⁻⁴ → 4.98×10⁻¹⁴ eV/Å². Against the saved custom-engine
mesh at the same model and geometry, the largest **finite-q** frequency
differences were 0.000296 THz (MoS₂) and 0.000163 THz (WSe₂); isolated-mode
median overlap² exceeded 0.99999998 in both. Each corrected Phonopy archive
reloaded with maximum force-constant difference 8.9×10⁻¹⁶ eV/Å².
The active 6×6 Prophet Stage2 jobs retain their original mode files; no
existing result has been overwritten or silently relabeled as Phonopy output.

The same final Phonopy+ASR route completed both **EquiformerV3-own-relaxed**
lines in one CPU `regular` job, `1019397`, in 2 min 1 s. Both produced 36 q
points and 486 mode pairs. MoS₂/WSe₂ raw drift fell from
7.20×10⁻⁵/6.31×10⁻⁵ to 5.10×10⁻¹⁴/1.01×10⁻¹³ eV/Å². The saved Phonopy
archives reproduce the corrected force constants within 2.5×10⁻¹⁴ eV/Å².
The two new relaxations differ from the previous EquiformerV3-own geometries
by at most 1.4×10⁻⁶ Å in cell and 1.4×10⁻⁶ Å in atom positions (MoS₂), and
1.7×10⁻⁷ Å in positions (WSe₂). Therefore their different structure hashes
are retained, and the old/new frequency comparison is marked *descriptive*:
matched all-mode MAE is 0.000193/0.0000759 THz and isolated-mode median
overlap² is 0.999999986/0.999999982. The comparison JSON is alongside each
local own-relaxed validation line.

## Stage2 handoff and equivalence

All four EquiformerV3 and all four Prophet material/geometry Phonopy lines
have passed the q-count, pair-count, ASR-residual, and archive checks. Each
line has also been handed to MatterSim Stage2 in its own run directory and
completed 486/486 pairs, 81/81 finite energies per pair. These full MatterSim
runs provide the default MatterSim ranking after Prophet supplies Stage1 modes;
only selected physical channels proceed to DFT when Stage3 resumes. Prophet
Stage2 is an optional MLFF comparison, not a mandatory first screening pass.

An old and a new *real standing-wave grid* can be subtracted pointwise only
after its displacement axes are aligned. This is a stricter condition than
physical equivalence. In the Prophet comparison, finite-q complex modes may
change global phase, and near-degenerate Gamma modes may rotate within an
almost identical subspace. The equivalence-aware audit checks complex finite-q
overlaps, Gamma subspace overlaps, the norm of the third-order coupling vector
over Gamma multiplets, and the Gamma trace of the fourth-order mixed coupling.
It finds the same top 5/10/20/30 physical channels in the two Prophet routes
for both MoS2 and WSe2. Details are in
`docs/prophet_phonopy_stage12_validation.md`.

This is a workflow regression, not a DFT accuracy claim. The user has paused
Stage3; rotational invariance, ZA behaviour near Gamma, 2D non-analytical
corrections, and fourth-order fit-window stability remain separate scientific
checks.
