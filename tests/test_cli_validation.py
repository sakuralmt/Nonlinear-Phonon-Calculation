"""Reject invalid settings before loading a model or creating scientific output."""

from importlib.metadata import version

import pytest

from nonlinear_phonon_calculation import __version__
from nonlinear_phonon_calculation.cli import main


def test_version_is_available_without_model_dependencies(capsys):
    with pytest.raises(SystemExit) as error:
        main(["--version"])
    assert error.value.code == 0
    assert capsys.readouterr().out.strip() == f"npc {__version__}"


def test_installed_package_version_matches_cli():
    assert version("nonlinear-phonon-calculation") == __version__


@pytest.mark.parametrize(
    "setting,value",
    [
        ("--mesh-n", "0"),
        ("--mesh-n", "-1"),
        ("--step", "0"),
        ("--step", "nan"),
        ("--step", "inf"),
        ("--convergence-step", "-0.005"),
        ("--gamma-degeneracy-thz", "nan"),
    ],
)
def test_invalid_harmonic_settings_leave_no_outputs(tmp_path, setting, value):
    output = tmp_path / "not-created"
    with pytest.raises(SystemExit) as error:
        main(
            [
                "stage1",
                "--structure", str(tmp_path / "missing.in"),
                "--checkpoint", str(tmp_path / "missing.pt"),
                "--source-root", str(tmp_path / "missing-source"),
                "--output-dir", str(output),
                setting, value,
            ]
        )
    assert error.value.code == 2
    assert not output.exists()


@pytest.mark.parametrize(
    "options",
    [["--shard-index", "1"], ["--shard-count", "0"], ["--top-channels", "0"]],
)
def test_reference_rejects_invalid_shards_before_data_access(tmp_path, options):
    output = tmp_path / "not-created"
    with pytest.raises(SystemExit) as error:
        main(
            [
                "stage2", "reference",
                "--mode-pairs-json", "missing-pairs.json",
                "--structure", "missing.in",
                "--checkpoint", "missing.pt",
                "--reference-results", "missing-results.json",
                "--output-dir", str(output),
                *options,
            ]
        )
    assert error.value.code == 2
    assert not output.exists()
