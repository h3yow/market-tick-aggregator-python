"""聚合规则单测：窗口边界、乱序无关、并列裁决、迟到点位。"""

from __future__ import annotations

import logging

from app.aggregation import aggregate_ticks, window_start

log = logging.getLogger("tests.aggregation")


def test_window_start_floor_boundaries() -> None:
    cases = [
        (0, 0),
        (1, 0),
        (59_999, 0),
        (60_000, 60_000),
        (119_999, 60_000),
        (120_000, 120_000),
    ]
    log.info("窗口边界输入=%s", cases)
    for ts, want in cases:
        assert window_start(ts) == want


def test_order_independence() -> None:
    rows = [
        (60_000, 10.0, 2.0, None, "c:a"),
        (60_005, 13.0, 1.0, None, "c:b"),
        (60_020, 9.0, 4.0, None, "c:c"),
        (119_999, 11.0, 3.0, None, "c:d"),
    ]
    log.info("乱序无关判定: 原始=%s 逆序=%s", rows, list(reversed(rows)))
    a = [c.model_dump() for c in aggregate_ticks(rows, "S")]
    b = [c.model_dump() for c in aggregate_ticks(list(reversed(rows)), "S")]
    assert a == b
    c = a[0]
    assert (c["open"], c["high"], c["low"], c["close"]) == (10.0, 13.0, 9.0, 11.0)
    assert c["volume"] == 10.0
    assert c["tick_count"] == 4


def test_tie_break_by_seq_then_key() -> None:
    """同一毫秒：seq 小者为先（决定 open），无 seq 排最后。"""
    rows = [
        (60_000, 100.0, 1.0, 5, "c:z"),
        (60_000, 101.0, 1.0, None, "c:n"),
        (60_000, 99.0, 1.0, 0, "c:a"),
    ]
    log.info("并列裁决输入=%s", rows)
    (c,) = aggregate_ticks(rows, "S")
    assert c.open == 99.0      # seq=0
    assert c.close == 101.0   # NULLS LAST
    assert c.high == 101.0
    assert c.low == 99.0


def test_late_tick_changes_open_low() -> None:
    """迟到但更早的点位补入后，open/low 按时间重算，且只计一次量。"""
    first = [(60_010, 10.0, 2.0, None, "c:1"),
             (60_020, 12.0, 3.0, None, "c:2")]
    late = [(60_001, 8.0, 5.0, None, "c:0")]
    c1 = aggregate_ticks(first, "S")[0]
    c2 = aggregate_ticks(first + late, "S")[0]
    log.info("迟到前=%s 迟到后=%s", c1, c2)
    assert (c1.open, c1.low, c1.volume) == (10.0, 10.0, 5.0)
    assert (c2.open, c2.high, c2.low, c2.close, c2.volume) == (
        8.0, 12.0, 8.0, 12.0, 10.0)
