"""持久化：进程重启后数据不丢；写入失败整批回滚。"""

from __future__ import annotations

import logging
import os

from app.service import MarketService
from app.store import TickStore

log = logging.getLogger("tests.persistence")


def test_data_survives_restart(tmp_path) -> None:
    db = tmp_path / "persist.db"
    ticks_payload = [
        {"symbol": "S1", "timestamp_ms": 60_000, "price": 10, "quantity": 2},
        {"symbol": "S1", "timestamp_ms": 60_030, "price": 12, "quantity": 3},
    ]
    log.info("重启前写入输入=%s", ticks_payload)
    s1 = TickStore(db); s1.init_db()
    svc1 = MarketService(s1)
    from app.models import IngestRequest, TickIn
    svc1.ingest(IngestRequest(batch_id="before-restart", ticks=[
        TickIn(**t) for t in ticks_payload]))
    s1.close()

    # 模拟进程重启：全新 store 实例
    s2 = TickStore(db); s2.init_db()
    svc2 = MarketService(s2)
    (c,) = svc2.query_candles("S1", 0, 120_000)
    log.info("重启后 K 线=%s", c)
    assert (c.open, c.high, c.low, c.close, c.volume, c.tick_count) == (
        10.0, 12.0, 10.0, 12.0, 5.0, 2)
    # 批次记录同样持久化，重放仍判重
    again = svc2.ingest(IngestRequest(batch_id="before-restart", ticks=[
        TickIn(**t) for t in ticks_payload]))
    assert again.duplicate
    s2.close()


def test_batch_rollback_on_failure(service, monkeypatch) -> None:
    """批次记录写入阶段抛错时，ticks 与 rejected 均不落库（原子回滚）。"""
    from app.models import IngestRequest, TickIn
    ticks = [TickIn(symbol="S1", timestamp_ms=60_000, price=10, quantity=1, id="r1"),
             TickIn(symbol="S1", timestamp_ms=60_001, price=11, quantity=2, id="r2")]
    log.info("回滚场景输入=%s", [t.model_dump() for t in ticks])

    real_conn = service.store.connect()

    class _FailingConn:
        """仅在 ticks 批量插入时失败一次，其余全部透传。"""

        def __init__(self, inner):
            self._inner = inner

        def executemany(self, sql, params):
            if sql.strip().startswith("INSERT INTO ticks"):
                log.info("注入失败: ticks 写入语句抛出异常，触发 ROLLBACK")
                raise RuntimeError("injected disk failure")
            return self._inner.executemany(sql, params)

        def __getattr__(self, name):
            return getattr(self._inner, name)

    monkeypatch.setattr(service.store, "connect",
                        lambda: _FailingConn(real_conn))

    try:
        service.ingest(IngestRequest(batch_id="rollback-1", ticks=ticks))
        raise AssertionError("应当抛出异常")
    except RuntimeError as e:
        log.info("调用方收到可重试错误=%r", e)
        assert "injected disk failure" in str(e)

    monkeypatch.undo()
    # 回滚后：无点位、无批次、无拒绝记录
    assert service.query_candles("S1", 0, 120_000) == []
    conn = service.store.connect()
    n_ticks = conn.execute("SELECT COUNT(*) FROM ticks").fetchone()[0]
    n_batches = conn.execute("SELECT COUNT(*) FROM batches").fetchone()[0]
    n_rej = conn.execute("SELECT COUNT(*) FROM rejected_records").fetchone()[0]
    log.info("回滚后计数 ticks=%s batches=%s rejected=%s",
             n_ticks, n_batches, n_rej)
    assert (n_ticks, n_batches, n_rej) == (0, 0, 0)

    # 失败后用同一 batch_id 重试必须成功（没有残留状态）
    retry = service.ingest(IngestRequest(batch_id="rollback-1", ticks=ticks))
    log.info("重试结果 accepted=%s duplicate=%s", retry.accepted, retry.duplicate)
    assert retry.accepted == 2 and not retry.duplicate
    assert len(service.query_candles("S1", 0, 120_000)) == 1


def test_rejected_records_persisted(service) -> None:
    """无效记录不静默丢弃：写入 rejected_records 表，含原因与原始内容。"""
    from app.models import IngestRequest, TickIn
    ticks = [
        TickIn(symbol="S1", timestamp_ms=0, price=10, quantity=1, id="good"),
        TickIn(symbol="S1", timestamp_ms=0, price=-1, quantity=1),
    ]
    service.ingest(IngestRequest(batch_id="audit-1", ticks=ticks))
    rows = service.store.connect().execute(
        "SELECT batch_id, item_index, reason, detail, raw_json "
        "FROM rejected_records ORDER BY id").fetchall()
    log.info("留痕查询结果=%s", [dict(r) for r in rows])
    assert len(rows) == 1
    r = rows[0]
    assert (r["batch_id"], r["item_index"], r["reason"]) == (
        "audit-1", 1, "bad_price")
    assert "正数" in r["detail"]
    assert '"price": -1' in r["raw_json"]
