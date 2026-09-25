# Two-stage workflow and contract v5

The public interface is `npc stage1`, `npc stage2 screen|refine|audit`, and `npc status`.
Stage1 defaults to TECE, with Prophet and EquiformerV3 alternatives. Stage2 uses
the pinned MatterSim 5M calculator. Model environments remain separate.

## Structure and harmonic calculation

Each Stage1 model first relaxes the input with its own calculator. The current
protocol preserves the QE-format input's atomic constraints and vacuum length,
optimizes the permitted atomic coordinates with BFGS (`fmax=0.02 eV/Å`), and
searches an isotropic in-plane scale in `[0.94, 1.06]`. It preserves the hexagonal
cell shape. A failed ionic optimization or a lattice optimum at the search bound
stops the run. This is a constrained monolayer relaxation, not a general 3D
variable-cell optimizer. Reading a QE-format structure does not run QE.

Phonopy 2.38 constructs force constants from Cartesian central differences on a
commensurate supercell. The default mesh is 6×6×1, and the two displacement sizes
are 0.01 and 0.005 Å. Raw and ASR-corrected force constants are both retained.
Phonopy's translational/index-exchange symmetrization is distinct from spatial
point-group symmetry and from the rotational sum rule required for ideal 2D ZA.

`structure_symmetry.py` verifies spglib's atomic permutations and translations,
rejects operations that do not preserve the layer/mesh, and checks phonon
covariance with transformed complex eigenvectors at Γ and finite q. Γ covariance is required for every retained spatial operation and its time-reversed partner. Matched frequencies must also
agree. Degenerate groups use singular values of the overlap matrix. Failed
spatial covariance falls back to q/−q; failed time reversal stops the run.
The workflow assumes a nonmagnetic scalar potential without external fields.

## Gamma optical candidates

Only Γ optical modes enter coupling candidates. The three translations are
identified using the normalized mass-weighted vectors
`t_alpha(i,beta) = sqrt(m_i / sum(m)) delta_alpha,beta`. Each excluded mode must
have at least 0.9 squared overlap with the translation subspace; each retained
mode must have at most 0.1. Ambiguous acoustic/optical mixing stops the run.
This also handles an unstable optical mode sorted before the acoustic modes.

For N atoms and C verified finite-q orbits, the candidate count is
`C × (3N−3) × 3N`. The validated three-atom TMD cells have C=6 and 324 candidates;
this count is not hard coded. A one-atom primitive cell has no Γ optical
candidates. Finite-q acoustic and optical branches are all retained.

Stage1 groups near-degenerate Γ optical modes (default 0.01 THz) into channels.
Stage2 ranks the norm of the complete Γ component vector. Finite-q degenerate
branches are marked basis dependent and selection expands their sibling
channels. These diagonal projections do not reconstruct the full finite-q
degenerate coupling tensor and must not be described as that tensor's invariant.

## Energies, fitting and restart

The six-point proxy uses `QΓ=±1` and `Qq=−1,0,+1` Å√amu. All candidates are
screened. The default top 20 channels, including required sibling components,
receive the central 5×5 grid; `audit` extends the strongest five refined channels
to 9×9. A 13-column polynomial fit supplies explicitly factorial-scaled third
and fourth derivatives. The 13 terms are the documented fitting ansatz, not the
full set of all symmetry-allowed quartic monomials for every possible q.

`core.py` normalizes each real supercell mode to unit mass-weighted norm,
including q/−q standing waves and self-conjugate q points. Energies are eV per
supercell; frequencies are THz. `units.py` defines all coordinate and derivative
units. Energy conversion always requires a declared source unit.

Run identity includes the pair-file hash, structure hash, MatterSim checkpoint
hash, contract, normalization, grids, top-channel count and the energy-accumulation protocol. Stage2 additionally
verifies the model-relaxation summary. An atomic JSON replacement after each
energy evaluation and a per-pair file lock provide interruption recovery and
prevent duplicate concurrent writes. Shards partition complete pairs. A phase
is finalized only after every required point is finite and every fit has rank 13.
Per-worker timing, process peak memory, host and Slurm identifiers are retained.

## Source layout

- `nonlinear_phonon_calculation/cli.py`: public command dispatch.
- `mlff_modepair_workflow/advanced_stage1.py`, `prophet_stage1.py`: model Stage1.
- `model_relaxation.py`, `phonopy_bridge.py`, `structure_symmetry.py`: geometry,
  force constants and verified reciprocal equivalence.
- `screening_stage2.py`, `core.py`, `units.py`: staged sampling and numerical contract.
- `tests/`: analytic, symmetry, displacement, checkpoint and provenance checks.

Historical v3/v4 results are diagnostic references, not v5 restart inputs.
QE/GPTFF/EquFlash and Stage3 recomputation code are outside this release tree.

## Precision and forbidden-term diagnostics

Version 1.0.1 registers an instance-local forward hook on the pinned MatterSim M3GNet atomic-energy normalizer. It casts atomic energies to float64 before the final scatter sum; model weights/features stay float32 and autograd remains connected. No installed package or global scatter function is patched. The same weight hash alone does not authorize mixing old float32-summed and corrected checkpoints.

`momentum_diagnostics` records translation allowance for each polynomial coefficient. For the real q/−q coordinate, y^n contains harmonics (2k−n)q; a term is potentially allowed when at least one is reciprocal. This does not apply point-group selection rules. In particular, Φ112 is forbidden for all finite q, whereas Φ122 and Φ1122 are momentum-allowed. Forbidden fits remain visible, never ranked.
