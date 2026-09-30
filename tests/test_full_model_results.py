"""Recompute every published full-model fit and same-input diagnostic."""

from reports.pbe_three_materials.full_model_assets import validate


def test_complete_full_model_comparison():
    data = validate()
    for material, item in data["materials"].items():
        assert set(item["models"]) == {"tece", "prophet", "equiformer-v3"}
        for model, run in item["models"].items():
            own = run["scopes"]["own_relaxed_full_flow"]["rows"]
            fixed = run["scopes"]["fixed_pbe_dft_diagnostic"]["rows"]
            assert sum(r["points"] for r in own) == 405
            assert sum(r["points"] for r in fixed) == 181
    rows = data["materials"]["ws2"]["models"]["tece"]["scopes"][
        "fixed_pbe_dft_diagnostic"
    ]["rows"]
    m6 = next(r for r in rows if r["physical_channel"] == "Gamma8-M6")
    assert round(m6["phi1122"], 3) == -1.425
    assert all(v["qe"] < 0 and v["model"] < 0 for v in m6["even_mixed_mev"].values())
