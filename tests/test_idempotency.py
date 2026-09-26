"""重复批次、乱序/迟到、跨批内容幂等、区间重建的确定性测试。"""

from __future__ import annotations

import itertools
import logging

from app.models import IngestRequest, ReplaceRangeRequest, TickIn

log = logging.getLogger("tests.idempotency")

BASE_TICKS = [
    TickIn(symbol="S1", timestamp_ms=60_000, price=10, quantity=2),
    TickIn(symbol="S1", timestamp_ms=60_010, price=12, quantity=3),
    TickIn(symbol="S1", timestamp_ms=119_999, price=9, quantity=1),
    TickIn(symbol="S2", timestamp_ms=60_000, price=100, quantity=7),
]
EXPECTED_S1 = (10.0, 12.0, 9.0, 9.0, 6.0, 3)  # o,h,l,c,vol,count


def _candle_tuple(svc, symbol="S1"):
    (c,) = svc.query_candles(symbol, 0, 120_000)
    return (c.open, c.high, c.low, c.close, c.volume, c.tick_count)


def test_duplicate_batch_no_double_accumulation(service) -> None:
    log.info("输入批次=%s", [t.model_dump() for t in BASE_TICKS])
    r1 = service.ingest(IngestRequest(batch_id="b1", ticks=BASE_TICKS))
    r2 = service.ingest(IngestRequest(batch_id="b1", ticks=BASE_TICKS))
    r3 = service.ingest(IngestRequest(batch_id="b1", ticks=[]))
    log.info("首次 accepted=%s，重试1 duplicate=%s accepted=%s，"
             "重试2(空体) duplicate=%s", r1.accepted, r2.duplicate, r2.accepted,
             r3.duplicate)
    assert not r1.duplicate
    assert r2.duplicate and r2.accepted == r1.accepted
    assert r3.duplicate  # 相同 batch_id 即返回首次结果，不看请求体
    assert _candle_tuple(service) == EXPECTED_S1
    assert _candle_tuple(service, "S2") == (100.0, 100.0, 100.0, 100.0, 7.0, 1)


def test_arrival_order_converges(service) -> None:
    """同一集合以任意顺序到达（相同内容分不同批次），最终 K 线一致。"""
    for order in itertools.permutations(BASE_TICKS[:3]):
        for i, t in enumerate(order):
            service.ingest(IngestRequest(batch_id=f"perm-{id(order)}-{i}", ticks=[t]))
    got = _candle_tuple(service)
    log.info("乱序到达后 K 线=%s 期望=%s", got, EXPECTED_S1)
    assert got == EXPECTED_S1


def test_split_batches_in_any_order(service) -> None:
    """把同一集合拆成多批，提交顺序不影响结果。"""
    splits_a = [[BASE_TICKS[0], BASE_TICKS[1]], [BASE_TICKS[2]]]
    for chunk in splits_a:
        service.ingest(IngestRequest(
            batch_id=f"a-{chunk[0].timestamp_ms}", ticks=chunk))
    a = _candle_tuple(service)

    svc2 = service
    svc2.store = service.store  # 同一 store 无法重置；改用新 service 独立验证
    from app.service import MarketService
    import tempfile, os
    from app.store import TickStore
    db = os.path.join(tempfile.mkdtemp(), "x.db")
    st = TickStore(db); st.init_db()
    other = MarketService(st)
    splits_b = [[BASE_TICKS[2]], [BASE_TICKS[0]], [BASE_TICKS[1]]]
    for chunk in reversed(splits_b):
        other.ingest(IngestRequest(
            batch_id=f"b-{chunk[0].timestamp_ms}", ticks=chunk))
    (b,) = other.query_candles("S1", 0, 120_000)
    log.info("拆分乱序 A=%s B=%s", a, b)
    assert a == (b.open, b.high, b.low, b.close, b.volume, b.tick_count)
    st.close()


def test_late_tick_deterministic(service) -> None:
    service.ingest(IngestRequest(batch_id="on-time", ticks=BASE_TICKS[:3]))
    # 迟到 1 小时到达的更早窗口点位
    late = TickIn(symbol="S1", timestamp_ms=0, price=7, quantity=4)
    r = service.ingest(IngestRequest(batch_id="late-1", ticks=[late]))
    log.info("迟到点位=%s 判定=%s", late.model_dump(), r)
    assert r.accepted == 1
    (c0, c1) = service.query_candles("S1", 0, 120_000)
    assert (c0.open, c0.high, c0.low, c0.close, c0.volume) == (
        7.0, 7.0, 7.0, 7.0, 4.0)
    assert (c1.open, c1.high, c1.low, c1.close, c1.volume) == EXPECTED_S1[:5]


def test_replace_range_rebuild(service) -> None:
    service.ingest(IngestRequest(batch_id="init", ticks=BASE_TICKS[:3]))
    fixed = [
        TickIn(symbol="S1", timestamp_ms=60_000, price=11, quantity=10, id="t1"),
        TickIn(symbol="S1", timestamp_ms=60_030, price=13, quantity=5, id="t2"),
    ]
    req = ReplaceRangeRequest(symbol="S1", start_ms=60_000,
                              end_ms_exclusive=120_000, batch_id="fix",
                              ticks=fixed)
    rr = service.replace_range(req)
    log.info("重建删除旧点位数=%s", rr.deleted_before)
    assert rr.deleted_before == 3
    (c,) = service.query_candles("S1", 60_000, 120_000)
    assert (c.open, c.high, c.low, c.close, c.volume, c.tick_count) == (
        11.0, 13.0, 11.0, 13.0, 15.0, 2)

    # 区间外数据不受影响：先在相邻窗口写数据再重建
    service.ingest(IngestRequest(batch_id="neighbor", ticks=[
        TickIn(symbol="S1", timestamp_ms=120_000, price=20, quantity=1, id="n")]))
    rr2 = service.replace_range(req)
    log.info("重建重试 duplicate=%s deleted_before=%s", rr2.duplicate,
             rr2.deleted_before)
    assert rr2.duplicate and rr2.deleted_before == 3
    candles = service.query_candles("S1", 0, 180_000)
    assert [c.window_start_ms for c in candles] == [60_000, 120_000]
    assert candles[0].volume == 15.0 and candles[1].volume == 1.0


def test_replace_range_rejects_out_of_range(service) -> None:
    req = ReplaceRangeRequest(
        symbol="S1", start_ms=60_000, end_ms_exclusive=120_000,
        batch_id="fix2",
        ticks=[
            TickIn(symbol="S1", timestamp_ms=59_999, price=10, quantity=1),
            TickIn(symbol="S2", timestamp_ms=60_000, price=10, quantity=1),
            TickIn(symbol="S1", timestamp_ms=60_000, price=10, quantity=1, id="ok"),
        ])
    rr = service.replace_range(req)
    log.info("区间外拒绝=%s 接受=%s",
             [(r.reason, r.detail) for r in rr.rejected], rr.accepted)
    assert rr.accepted == 1
    assert {r.reason for r in rr.rejected} == {"out_of_range"}


def test_id_conflict_is_rejected_not_overwritten(service) -> None:
    t1 = TickIn(symbol="S1", timestamp_ms=60_000, price=10, quantity=1, id="x")
    t2 = TickIn(symbol="S1", timestamp_ms=60_000, price=11, quantity=2, id="x")
    service.ingest(IngestRequest(batch_id="b1", ticks=[t1]))
    r = service.ingest(IngestRequest(batch_id="b2", ticks=[t2]))
    log.info("同 id 异内容判定=%s", [(x.reason, x.detail) for x in r.rejected])
    assert r.rejected[0].reason == "id_conflict"
    (c,) = service.query_candles("S1", 0, 120_000)
    assert c.volume == 1.0 and c.open == 10.0  # 旧值保留
