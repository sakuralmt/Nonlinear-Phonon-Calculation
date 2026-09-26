"""Regression for the bundled PBE benchmark, independent of server state."""

from validation.check_pbe_reference import check


def test_pbe_reference_is_complete_and_metrics_recompute():
    rows = check()
    assert len(rows) == 9
    ws2_tece = next(row for row in rows if row[:2] == ("ws2", "tece"))
    assert round(ws2_tece[2], 4) == 0.0928
    assert round(ws2_tece[3], 3) == 6.104
    assert round(ws2_tece[4], 3) == 2.594
