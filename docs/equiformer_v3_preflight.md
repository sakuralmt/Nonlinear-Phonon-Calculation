# EquiformerV3 OAM preflight and own-structure relaxation (2026-09-24)

The isolated EquiformerV3 environment passed a sequential MoS₂/WSe₂ checkpoint preflight on the shared DFT structures. This is an interface and force-consistency check, **not** a 6×6 phonon accuracy result. The only successful strengthened run was Slurm job `1019360` (`gpu`, one GPU, eight CPUs, 64 GB requested, 29 s elapsed). It did not occupy any of the running Prophet CPU nodes.

| Material | Probe force, eV/Å | Energy slope, eV/Å | Absolute mismatch, eV/Å | Repeat E/F difference | Peak process RSS | Peak GPU reserved |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| MoS₂ | −0.628234 | +0.628281 | 0.000046 | 0 / 0 | 4928 MB | 436 MB |
| WSe₂ | −0.587130 | +0.587082 | 0.000048 | 0 / 0 | 4928 MB | 436 MB |

Both checks used a 0.03 Å probe displacement and ±0.005 Å central difference. The energy span is 0.006283 eV for MoS₂ and 0.005871 eV for WSe₂; the test therefore measures a nonzero response. The periodic cells contain three atoms and the correct Mo/S or W/Se elements. Raw output is at `validation/equiformer-v3/{mos2,wse2}/preflight/preflight.json` on the huairou campaign directory.

The source commit is `a7300c58df683dc99cb48027d5bfd4c887486c48`, with full source-tree SHA-256 `3e15a029e8e1ea534e979f5548293eb1e10841d4c86317fd4576ee1d7acc922e`; the checkpoint SHA-256 is `429ccded98163122e7ba588d78e2441653f37f3e091e106c432807fe373c8f98`. The current isolated package lock is `validation/equiformer-env-locked.txt` on the server. The optional `pyg_lib` wheel was removed because its binary requires GLIBC 2.27, unavailable on this cluster; `torch_sparse` and `torch_scatter` import successfully without it.

The first successful import run (`1019357`) tested the high-symmetry primitive at its equilibrium position. QE relaxation flags were read by ASE as fixed-atom constraints, so its zero Mo force was a false reassurance. The strengthened test first displaces an atom, then requests unconstrained calculator forces. The intermediate run (`1019359`) deliberately exposed the constraint masking before the force-reading fix; it is retained for diagnosis, not counted as a passed preflight. The existing Prophet Stage1 force constants are unaffected: ASE's `make_supercell` dropped those primitive constraints in the installed version, and the saved force constants have nonzero components for every atom and Cartesian direction. The test code now explicitly ignores geometry-optimization constraints for finite-displacement forces, and the builder clears them for frozen-phonon structures.

An 8-CPU preflight on the `regular` partition (`1019374`) also passed, with a MoS₂ force/energy difference of 0.000047 eV/Å and no GPU allocation. The queued GPU relaxation `1019373` was cancelled before it started. A single 8-CPU `regular` job (`1019375`) then relaxed MoS₂ and WSe₂ sequentially in 34 s, including model loading and preflight; all six in-plane scale trials per material converged. Both optimized structures passed a second force/energy preflight. The model, initial geometry, and optimized geometry hashes are recorded in each `model_relaxed/stage1/run_identity.json` and `relax/relax_summary.json` on the server.

| Material | DFT shared a, Å | Prophet-own a, Å | Equiformer-own a, Å | Equiformer-own chalcogen height, Å |
| --- | ---: | ---: | ---: | ---: |
| MoS₂ | 3.129006 | 3.191236 | 3.188232 | 1.565974 |
| WSe₂ | 3.249297 | 3.326661 | 3.326535 | 1.680781 |

The EquiformerV3 optimum scales the shared DFT in-plane lattice by 1.018928 for MoS₂ and 1.023771 for WSe₂. These are independent model relaxations under the same QE-flag-constrained protocol as Prophet, not copies of Prophet's structures. Similar lattice constants alone do not establish comparable phonon eigenvectors or nonlinear couplings.

The 6×6, 108-atom CPU energy/force benchmark (`1019376`, `regular`, 16 CPUs/64 GB) completed in 26 s. Measured per-call wall times were 3.4–3.5 s at four threads, 2.0 s at eight threads, and 1.3–1.7 s at 16 threads; peak process RSS was approximately 7.4 GB. A single 16-CPU `regular` job (`1019377`) then ran all four Stage1 lines sequentially in 3 min 45 s, without competing for the Prophet CPU allocation. Every line passed preflight and produced 36 q points and 486 Γ–q candidate pairs.

| Material and geometry | Smallest frequency, THz | Highest frequency, THz | Max ±0.01 vs ±0.005 Å change, THz |
| --- | ---: | ---: | ---: |
| MoS₂, own-relaxed | −0.00865 | 13.6023 | 0.00624 |
| MoS₂, shared DFT | −0.01062 | 14.3514 | 0.01654 |
| WSe₂, own-relaxed | −0.00647 | 8.91228 | 0.00324 |
| WSe₂, shared DFT | −0.00790 | 9.61439 | 0.00485 |

The source and checkpoint hashes matched the preflight lock in all four saved datasets. Server outputs are under `validation/equiformer-v3/{mos2,wse2}/{model_relaxed,shared_dft}/stage1/`. These ranges and convergence checks establish a completed mesh, not agreement with QE. A subsequent opt-in Phonopy+ASR Stage1 has been completed for all four lines, followed by MatterSim Stage2: every line finished 486/486 pairs and 39,366 finite energy points. The shared-structure Phonopy frequencies and eigenvectors were compared to the archival QE mesh with its geometry-provenance limitations; see `docs/phonopy_asr_dft_comparison.md`. The full MatterSim results provide the MLFF ranking from which only the strongest physical channels would be selected for DFT when Stage3 resumes; they are not DFT accuracy labels.
