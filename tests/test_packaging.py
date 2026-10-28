"""Exercise actual release artifacts without loading models or external runs."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tarfile
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _run(*args, cwd, env=None):
    return subprocess.run(
        args, cwd=cwd, env=env, text=True, capture_output=True, check=True
    )


def test_release_artifacts_include_reproducible_reference_inputs(tmp_path):
    source = tmp_path / "source"
    shutil.copytree(
        ROOT,
        source,
        ignore=shutil.ignore_patterns(
            ".git",
            "__pycache__",
            ".pytest_cache",
            ".ruff_cache",
            "*.egg-info",
            "build",
            "dist",
            ".runtime",
            "release_run",
        ),
    )
    dist = tmp_path / "dist"
    _run(
        sys.executable,
        "-c",
        "import setuptools.build_meta as b; "
        f"b.build_sdist({str(dist)!r}); b.build_wheel({str(dist)!r})",
        cwd=source,
    )
    archive = next(dist.glob("*.tar.gz"))
    with tarfile.open(archive) as handle:
        names = handle.getnames()
    prefix = names[0].split("/")[0] + "/"
    names = {name.removeprefix(prefix) for name in names}
    required = {
        "LICENSE",
        "CHANGELOG.md",
        "CONTRIBUTING.md",
        "install.sh",
        "tests/test_packaging.py",
        "validation/check_pbe_reference.py",
        "reports/pbe_three_materials/full_model_assets.py",
        "reports/pbe_three_materials/small_model_assets.py",
        "reports/pbe_three_materials/dft_mattersim_assets.py",
        "docs/reference_data/pbe_20260925/structures/mos2.scf.inp",
        "docs/reference_data/pbe_20260925/full_model_structures/ws2-tece.scf.inp",
        "docs/reference_data/pbe_20260925/small_model_resource_audit.txt",
        "docs/acceptance_data/archived_dft_couplings.csv",
    }
    assert required <= names

    wheel = next(dist.glob("*.whl"))
    with zipfile.ZipFile(wheel) as handle:
        wheel_names = handle.namelist()
    assert any(name.endswith("/LICENSE") for name in wheel_names)
    assert {name.split("/")[0] for name in wheel_names} <= {
        "nonlinear_phonon_calculation",
        "mlff_modepair_workflow",
        next(name.split("/")[0] for name in wheel_names if ".dist-info/" in name),
    }
    assert not any(
        name.startswith(("reports/", "validation/", "tests/", "docs/"))
        for name in wheel_names
    )

    installed = tmp_path / "wheel-install"
    _run(
        sys.executable,
        "-m",
        "pip",
        "--disable-pip-version-check",
        "install",
        "--no-index",
        "--no-deps",
        "--target",
        str(installed),
        str(wheel),
        cwd=tmp_path,
    )
    # -I excludes the checkout, PYTHONPATH and user site. Only this explicit
    # target and the environment's base dependencies remain available.
    check = _run(
        sys.executable,
        "-I",
        "-c",
        "import json, pathlib, sys; "
        f"sys.path.insert(0, {str(installed)!r}); "
        "import nonlinear_phonon_calculation as npc; "
        "from nonlinear_phonon_calculation.cli import main; "
        f"assert pathlib.Path(npc.__file__).is_relative_to({str(installed)!r}); "
        "main(['status', '--output-dir', 'nonexistent-run'])",
        cwd=tmp_path,
    )
    result = json.loads(check.stdout)
    assert set(result["rankings"]) == {"screen", "refine", "audit"}
    assert not (tmp_path / "nonexistent-run").exists()


def test_imports_do_not_load_models_or_open_network_connections(tmp_path):
    _run(
        sys.executable,
        "-I",
        "-c",
        """
import importlib, importlib.abc, pkgutil, socket, sys
sys.path.insert(0, SOURCE)
blocked = {'torch', 'tensorflow', 'mattersim', 'prophet', 'tace', 'fairchem',
           'tensorpotential', 'eqnorm', 'deepmd'}
class RejectModelImport(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split('.')[0] in blocked:
            raise AssertionError('Model stack loaded during import: ' + fullname)
sys.meta_path.insert(0, RejectModelImport())
def reject_network(*args, **kwargs):
    raise AssertionError('Network opened during package import')
socket.socket.connect = reject_network
import nonlinear_phonon_calculation.cli
import mlff_modepair_workflow
for item in pkgutil.iter_modules(mlff_modepair_workflow.__path__):
    importlib.import_module('mlff_modepair_workflow.' + item.name)
assert not blocked.intersection(sys.modules)
""".replace("SOURCE", repr(str(ROOT))),
        cwd=tmp_path,
    )


def test_installer_stops_before_pip_with_old_python(tmp_path):
    wrapper = tmp_path / "python-old"
    wrapper.write_text(
        f"#!{sys.executable}\n"
        "import sys\n"
        "assert sys.argv[1] == '-c', 'Installer reached pip with old Python'\n"
        "sys.version_info = (3, 9, 0)\n"
        "exec(sys.argv[2])\n"
    )
    wrapper.chmod(0o755)
    result = subprocess.run(
        ["bash", str(ROOT / "install.sh")],
        check=False,
        cwd=tmp_path,
        env={**os.environ, "NPC_INSTALL_PYTHON": str(wrapper)},
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0
    assert "Python 3.10+ is required" in result.stderr
