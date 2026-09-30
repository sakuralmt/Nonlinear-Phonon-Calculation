"""Independent arithmetic and provenance checks for the PBE small-model data."""

import json
from reports.pbe_three_materials.small_model_assets import validate, DATA


def test_small_models_same_pbe_inputs_and_signed_contrasts():
    d = validate()
    assert (
        sum(
            sum(r["points"] for r in m["rows"])
            for x in d["materials"].values()
            for m in x["models"].values()
        )
        == 1629
    )
    p = json.loads((DATA / "small_model_preflight.json").read_text())
    assert len(p) == 9 and all(r["passed"] for r in p.values())
    ws2 = d["materials"]["ws2"]["models"]
    for model, sign in [
        ("grace-1l-oam", -1),
        ("eqnorm-mptrj", 1),
        ("dpa-3.1-3m-ft", 1),
    ]:
        row = next(r for r in ws2[model]["rows"] if r["channel"] == "Gamma8-M6")
        assert row["phi1122"] * sign > 0
        assert all(
            v["qe"] < 0 and v["model"] * sign > 0
            for v in row["even_mixed_mev"].values()
        )
