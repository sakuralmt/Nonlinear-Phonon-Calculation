# Contributing

Develop and test changes in a separate checkout. Keep scientific inputs, accepted checkpoints, model weights and raw results immutable; do not repair a test by changing a published label or relaxing a physical acceptance threshold.

```bash
python -m pip install -e '.[test]'
python -m pytest -q tests
python -m build
```

The suite uses synthetic calculators and archived reference data. It does not need QE, Slurm or model downloads. One optional small Torch test is skipped in base-only environments. See the [release stability record](docs/STABILITY_1.0.4.md) for coverage and [architecture](ARCHITECTURE.md) for physical conventions.

For a new model adapter, describe its pinned source/checkpoint, units, precision, atom order and force behavior. Test imports without model side effects and make failed identity checks explicit. Do not silently substitute a different model or geometry when a dependency or mode mapping fails.

For a numerical change, include an independent analytic or physical reference and identify which conventions or old results may change. A higher fit R² alone does not establish better physics. Model accuracy, numerical repeatability, packaging and wall-clock performance need separate evidence.

User-facing commands belong in both README languages. Keep quick-start commands consistent with `npc --help`, explain whether an operation imports data or performs inference, and avoid site-specific account paths and cluster policies in general instructions.
