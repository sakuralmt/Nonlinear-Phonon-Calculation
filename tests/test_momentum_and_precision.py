"""Physical translation rules and pre-sum precision regressions."""

import numpy as np
import pytest

from mlff_modepair_workflow.core import analyze_pair_grid
from mlff_modepair_workflow.mattersim_backend import _atomic_energy_to_double


@pytest.mark.parametrize(
    "q,three_q_allowed",
    [([0.5, 0, 0], False), ([1 / 3, 1 / 3, 0], True), ([1 / 6, 0, 0], False)],
)
def test_gamma_q_momentum_roles_are_not_inferred_from_fitted_magnitude(
    q, three_q_allowed
):
    axis = np.linspace(-1, 1, 5)
    x, y = np.meshgrid(axis, axis)
    # Deliberately inject a forbidden x^2*y term: retain but never authorize it.
    energy = 0.2 * x * x + 0.3 * y * y + 0.01 * x * y * y + 0.05 * x * x * y
    pair = {
        "gamma_mode": {"freq_thz": 1, "q_frac": [0, 0, 0]},
        "target_mode": {"freq_thz": 2, "q_frac": q},
    }
    result = analyze_pair_grid(pair, energy, axis, axis)
    terms = result["momentum_diagnostics"]["terms"]
    assert terms["c12"]["translation_allowed"]
    assert terms["c22"]["translation_allowed"]
    for name in ("c21", "c11", "c01"):
        assert not terms[name]["translation_allowed"]
        assert terms[name]["role"] == "numerical_diagnostic_only"
    assert terms["c03"]["translation_allowed"] == three_q_allowed
    assert result["momentum_diagnostics"][
        "forbidden_phi112_mev_per_A3amu32"
    ] == pytest.approx(100)


def test_promoting_before_sum_retains_small_energy_and_autograd():
    torch = pytest.importorskip("torch")
    values = torch.tensor(
        [2**24, 1.0, -(2**24)], dtype=torch.float32, requires_grad=True
    )
    assert float(values.sum()) == 0
    promoted = _atomic_energy_to_double(None, (), values)
    assert promoted.dtype == torch.float64
    assert float(promoted.sum()) == 1
    promoted.sum().backward()
    assert torch.equal(values.grad, torch.ones_like(values))
