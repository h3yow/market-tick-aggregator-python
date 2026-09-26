"""查询边界：窗口切分、左闭右开、空结果、相邻窗口互不干扰。"""

from __future__ import annotations

import logging

from app.models import IngestRequest, TickIn

log = logging.getLogger("tests.query")


def test_window_split_and_half_open(service) -> None:
    ticks = [
        TickIn(symbol="S1", timestamp_ms=0, price=1, quantity=1, id="a"),
        TickIn(symbol="S1", timestamp_ms=59_999, price=2, quantity=1, id="b"),
        TickIn(symbol="S1", timestamp_ms=60_000, price=3, quantity=1, id="c"),
        TickIn(symbol="S1", timestamp_ms=119_999, price=4, quantity=1, id="d"),
        TickIn(symbol="S1", timestamp_ms=120_000, price=5, quantity=1, id="e"),
    ]
    service.ingest(IngestRequest(batch_id="q1", ticks=ticks))
    log.info("查询输入=%s", [t.model_dump() for t in ticks])

    all_c = service.query_candles("S1", 0, 180_000)
    assert [c.window_start_ms for c in all_c] == [0, 60_000, 120_000]
    assert [(c.open, c.close) for c in all_c] == [(1.0, 2.0), (3.0, 4.0), (5.0, 5.0)]

    # 左闭右开：start 含边界、end 不含边界
    one = service.query_candles("S1", 60_000, 120_000)
    assert [c.window_start_ms for c in one] == [60_000]

    # 空结果
    empty = service.query_candles("S1", 300_000, 360_000)
    log.info("空区间查询结果=%s", empty)
    assert empty == []

    # 不存在的 symbol
    assert service.query_candles("NOPE", 0, 180_000) == []

    # 窗口 OHLC 边界点
    first = all_c[0]
    assert (first.open, first.high, first.low, first.close, first.volume) == (
        1.0, 2.0, 1.0, 2.0, 2.0)
