# Finite-q MLFF phonon eigenvector comparison

`scripts/compare_full_grid_eigenvectors.py` compares a complete explicit QE
`matdyn` q mesh with a machine-learning force field at one fixed primitive
structure. For a three-atom primitive cell it needs 18 force evaluations on
an `N x N x 1` supercell, then produces all `N²` dynamical matrices by Fourier
transforming the measured real-space force constants. It does **not** run MD.

The QE q coordinates printed in `qeph.eig` can be Cartesian even when
`q_in_cryst_coord=.true.`. The comparison therefore takes fractional q
coordinates from `matdyn.inp` and pairs its records with `qeph.eig` by order.
The parser checks the number of q records and the normalization of every QE
eigenvector. The sign in the Fourier transform is fixed by using a Bloch
displacement proportional to `exp(+i q R)`.

At every q point, individual mode overlap is
`|<e_QE|e_MLFF>|²`, after a global one-to-one overlap assignment. Adjacent
QE modes within 0.1 THz are grouped and compared by their projector overlap,
because an arbitrary rotation within a degenerate eigenspace changes
individual overlaps without changing the physical subspace. Frequencies are
reported for the overlap-matched mode, which can differ from the mode with
the same ascending frequency index. Full arrays, including force constants,
are saved alongside the JSON result in a compressed NPZ file.

For archived selected modes without a complete `qeph.eig`, use
`scripts/compare_selected_q_modes.py`. It computes the same dynamical matrices
and finds the largest overlap for each supplied QE mode, but cannot score all
branches or perform a complete one-to-one assignment.

Example:

```bash
PYTHONPATH=. python scripts/compare_full_grid_eigenvectors.py \
  --backend mattersim --checkpoint /path/to/checkpoint.pth \
  --structure /path/to/system.scf.inp \
  --matdyn-input /path/to/matdyn.inp --qe-eig /path/to/qeph.eig \
  --mesh 6 --step 0.01 --output /path/to/result.json
```

The comparison fixes the structure, model checkpoint, displacement step,
and q list. QE `asr='simple'` and the MLFF finite-difference result may use
slightly different acoustic-sum-rule treatments; compare acoustic modes near
Gamma as a subspace and inspect their frequencies rather than assigning
physical meaning to individual zero-frequency eigenvectors. Supercell
aliasing, differences in force-constant range, and any missing nonanalytic
correction can also affect frequencies. Repeat the calculation with a
smaller displacement step before interpreting a low overlap as a model error.

`ModePairFrozenPhononBuilder` now normalizes the **real** mass-weighted
supercell mode to unit norm. For a generic non-self-conjugate q point such as
K this changes the old amplitude by approximately `sqrt(2)`. Consequently,
old and new frozen-phonon curvature coefficients cannot be compared without
converting the amplitude convention. Gamma and real self-conjugate M modes
retain unit factors to numerical precision. A globally imaginary Gamma/M
eigenvector is rotated to its real quadrature before normalization; the phase
of a generic q mode is retained.
