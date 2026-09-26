"""无效记录分类与批内重复单测。"""

from __future__ import annotations

import logging

import pytest

from app.errors import ValidationBatchError
from app.models import IngestRequest, TickIn

log = logging.getLogger("tests.validation")


def _ingest(svc, batch_id, ticks):
    log.info("批次输入 batch_id=%s ticks=%s", batch_id,
             [t.model_dump() for t in ticks])
    resp = svc.ingest(IngestRequest(batch_id=batch_id, ticks=ticks))
    log.info("判定结果 accepted=%s rejected=%s",
             resp.accepted, [(r.reason, r.detail) for r in resp.rejected])
    return resp


def test_invalid_records_classified(service) -> None:
    ticks = [
        TickIn(symbol="S1", timestamp_ms=0, price=10, quantity=1),          # 有效
        TickIn(symbol="  ", timestamp_ms=0, price=10, quantity=1),         # 缺 symbol
        TickIn(symbol="S1", timestamp_ms=-5, price=10, quantity=1),        # 时间戳
        TickIn(symbol="S1", timestamp_ms=0, price=0, quantity=1),          # 价格
        TickIn(symbol="S1", timestamp_ms=0, price=-3, quantity=1),         # 价格
        TickIn(symbol="S1", timestamp_ms=0, price="abc", quantity=1),      # 价格
        TickIn(symbol="S1", timestamp_ms=0, price=10, quantity=-1),        # 数量
        TickIn(symbol="S1", timestamp_ms=0, price=10, quantity="nan"),     # 非有限
        TickIn(symbol="S1", timestamp_ms=0, price=10, quantity=1, id=" "), # 空 id
        TickIn(symbol="S1", timestamp_ms=0, price=10, quantity=1, seq="x"),# seq 类型
        TickIn(symbol=None, timestamp_ms=0, price=10, quantity=1),         # 缺 symbol
    ]
    resp = _ingest(service, "batch-invalid", ticks)
    reasons = {r.reason for r in resp.rejected}
    assert resp.accepted == 1
    assert {"missing_symbol", "bad_timestamp", "bad_price",
            "bad_quantity", "empty_id"} <= reasons
    assert len(resp.rejected) == 10
    # 拒绝记录有明确原因与原始数据，不静默丢弃
    for r in resp.rejected:
        assert r.detail and r.raw is not None


def test_duplicate_within_batch(service) -> None:
    ticks = [
        TickIn(symbol="S1", timestamp_ms=1000, price=10, quantity=1),
        TickIn(symbol="S1", timestamp_ms=1000, price=10, quantity=1),       # 内容重复
        TickIn(symbol="S1", timestamp_ms=1000, price=10, quantity=1, id="x"),
        TickIn(symbol="S1", timestamp_ms=2000, price=99, quantity=9, id="x"),# 同 id 批内重复
        TickIn(symbol="S1", timestamp_ms=2000, price=99, quantity=9, id="y"),# 不同 id 有效
        TickIn(symbol="S2", timestamp_ms=1000, price=10, quantity=1, id="x"),# 不同 symbol 不算
    ]
    resp = _ingest(service, "batch-dup", ticks)
    assert resp.accepted == 4
    assert [r.reason for r in resp.rejected] == [
        "duplicate_within_batch", "duplicate_within_batch"]
    # 同窗口三个有效点位：1 + 1 + 9 = 11，重复不累计
    (c,) = service.query_candles("S1", 0, 60_000)
    log.info("聚合判定 candle=%s（三个不同点位，量=11）", c)
    assert c.volume == 11.0
    assert c.tick_count == 3


def test_batch_level_validation(service) -> None:
    with pytest.raises(ValidationBatchError):
        service.ingest(IngestRequest(batch_id="  ", ticks=[]))
    with pytest.raises(ValidationBatchError):
        service.query_candles("S1", 100, 50)
