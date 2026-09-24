import numpy as np
import pytest

from scripts.analyze_coarse_screening_campaign import _fit_window, _stencil1122, _stencil122


def test_sparse_stencils_recover_analytic_third_and_fourth_derivatives():
    axis = np.arange(-2.0, 2.01, 0.5)
    x, y = np.meshgrid(axis, axis)
    grid = -400 + 0.2 * x**2 + 0.4 * y**2 + 0.01 * x * y**2 + 0.005 * x**2 * y**2
    for step in (0.5, 1.0, 1.5, 2.0):
        assert _stencil122(grid, step) == pytest.approx(20.0, abs=1e-6)
        assert _stencil1122(grid, step) == pytest.approx(20.0, abs=1e-6)
    for window in (1.0, 1.5, 2.0):
        third, fourth = _fit_window(grid, window)
        assert third == pytest.approx(20.0, abs=1e-6)
        assert fourth == pytest.approx(20.0, abs=1e-6)


def test_sparse_stencil_uses_total_energy_offset_invariantly():
    axis = np.arange(-2.0, 2.01, 0.5)
    x, y = np.meshgrid(axis, axis)
    grid = 0.01 * x * y**2 + 0.005 * x**2 * y**2
    assert _stencil122(grid + 1000, 1.0) == pytest.approx(_stencil122(grid, 1.0), abs=1e-8)
    assert _stencil1122(grid + 1000, 1.0) == pytest.approx(_stencil1122(grid, 1.0), abs=1e-8)
