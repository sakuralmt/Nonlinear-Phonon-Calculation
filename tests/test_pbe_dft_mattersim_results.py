"""Recompute the new DFT-mode MatterSim baseline from bundled E/F grids."""

from reports.pbe_three_materials.dft_mattersim_assets import validate


def test_completed_pbe_dft_mattersim_baseline():
    result = validate()
    assert set(result["materials"]) == {"ws2", "mos2", "wse2"}
    mos2 = result["materials"]["mos2"]["metrics"]
    assert round(mos2["phi122_abs"]["mae"], 3) == 4.274
    ws2 = result["materials"]["ws2"]["rows"]
    m6 = next(row for row in ws2 if row["physical_channel"] == "Gamma8-M6")
    assert round(m6["phi1122"], 4) == 1.9304
