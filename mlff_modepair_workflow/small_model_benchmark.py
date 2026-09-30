"""Pinned small-model diagnostics on existing, matched PBE configurations.

This does not calculate Stage1 or create new QE labels. It reuses the audited
checkpointed PES runner with an explicitly verified external ASE calculator.
"""

from __future__ import annotations

import argparse
import importlib.metadata
import json
import os
import time
from pathlib import Path

from .advanced_stage1 import preflight_calculator
from .dft_reference import read_dft_structure
from .full_model_benchmark import run
from .prophet_backend import sha256_file
from .screening_stage2 import _write

SPECS = {
    "grace-1l-oam": (
        "tensorpotential",
        "0.6.1",
        "9baea82baf7de73af825993bede62f58ab5588c72c6900f2a901f0a1c50a9962",
    ),
    "eqnorm-mptrj": (
        "eqnorm",
        "0.1.0",
        "9fd5b97a069e03697e41d2e4c468c5c9b487fc42a2842861ea171a23b9706de5",
    ),
    "dpa-3.1-3m-ft": (
        "deepmd-kit",
        "3.1.3",
        "5ffb78b8e0c675b2cd73b343d56a6a3b7eeb972db85fe80bc0846b0df5d16106",
    ),
}


def make_calculator(model: str, checkpoint: Path):
    package, version, expected = SPECS[model]
    if sha256_file(checkpoint) != expected:
        raise ValueError("Small-model checkpoint hash mismatch")
    if importlib.metadata.version(package) != version:
        raise ValueError("Small-model package version mismatch")
    meta = {
        "model": model,
        "package": package,
        "package_version": version,
        "checkpoint_sha256": expected,
        "device": "cpu",
    }
    threads = int(os.environ.get("OMP_NUM_THREADS", "2"))
    if model == "grace-1l-oam":
        os.environ.setdefault("TF_USE_LEGACY_KERAS", "1")
        import tensorflow as tf

        tf.config.threading.set_intra_op_parallelism_threads(threads)
        tf.config.threading.set_inter_op_parallelism_threads(1)
        tf.config.set_visible_devices([], "GPU")
        from tensorpotential.calculator.foundation_models import grace_fm

        calc = grace_fm("GRACE-1L-OAM")
        saved = checkpoint.parent.parent
        meta["saved_model_files"] = {
            str(p.relative_to(saved)): sha256_file(p)
            for p in sorted(saved.rglob("*"))
            if p.is_file()
        }
    else:
        import torch

        torch.set_num_threads(threads)
        torch.set_num_interop_threads(1)
        if model == "eqnorm-mptrj":
            from eqnorm.calculator import EqnormCalculator

            calc = EqnormCalculator(
                model_name="eqnorm", model_variant="eqnorm-mptrj", device="cpu"
            )
            # The library resolves this variant from its cache; verify that the
            # cache is exactly the supplied pinned file rather than another one.
            cache = Path.home() / ".cache/eqnorm/eqnorm-mptrj.pt"
            if sha256_file(cache) != expected:
                raise ValueError("Eqnorm cache differs from supplied checkpoint")
        else:
            from deepmd.calculator import DP

            calc = DP(model=str(checkpoint))
            meta["head"] = None
    return calc, meta


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model", choices=SPECS, required=True)
    parser.add_argument("--checkpoint", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    manifest = json.loads(args.manifest.read_text())
    if manifest["external_adapter_sha256"] != sha256_file(Path(__file__)):
        raise ValueError("Small-model adapter changed since preparation")
    started = time.perf_counter()
    calc, meta = make_calculator(args.model, args.checkpoint)
    loading = time.perf_counter() - started
    probe = preflight_calculator(
        read_dft_structure(Path(manifest["tasks"][0]["structure"])), calc, "cpu"
    )
    args.output.mkdir(parents=True, exist_ok=True)
    _write(args.output / "preflight.json", probe)
    if not probe["passed"]:
        raise ValueError("Small-model preflight failed; no PES expansion")
    path = run(args.manifest, args.checkpoint, args.output, args.model, calculator=calc)
    result = json.loads(path.read_text())
    result.update(
        provenance=meta,
        loading_seconds=loading,
        external_adapter_sha256=manifest["external_adapter_sha256"],
        total_wall_seconds_including_load=time.perf_counter() - started,
        cpu_threads=int(os.environ.get("OMP_NUM_THREADS", "2")),
    )
    _write(path, result)


if __name__ == "__main__":
    main()
