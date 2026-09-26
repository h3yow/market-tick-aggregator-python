"""并发场景：同标的同窗口多线程写入不重复累计；读写互不出现半成品。"""

from __future__ import annotations

import logging
import random
import threading

from app.models import IngestRequest, TickIn
from app.service import MarketService
from app.store import TickStore

log = logging.getLogger("tests.concurrency")


def _mp_child(db_path: str, tid: int, n: int) -> None:
    store = TickStore(db_path)
    store.init_db()
    svc = MarketService(store)
    for j in range(n):
        svc.ingest(IngestRequest(batch_id=f"mp-{tid}-{j}", ticks=[
            TickIn(symbol="S1", timestamp_ms=60_000 + j % 30,
                   price=10, quantity=1, id=f"mp-{tid}-{j}")]))
    store.close()


def test_concurrent_writers_no_double_count(service) -> None:
    n_threads, per_thread = 8, 25

    def worker(tid: int) -> None:
        for j in range(per_thread):
            # 每个点位用唯一 id，避免"同 id"判重；全部落在同一分钟窗口
            t = TickIn(symbol="S1", timestamp_ms=60_000 + (j % 30),
                       price=10 + (j % 5), quantity=1, id=f"t-{tid}-{j}")
            resp = service.ingest(IngestRequest(
                batch_id=f"b-{tid}-{j}", ticks=[t]))
            assert resp.accepted == 1

    log.info("并发写入: 线程数=%s 每线程点位=%s 同一分钟窗口",
             n_threads, per_thread)
    threads = [threading.Thread(target=worker, args=(i,))
               for i in range(n_threads)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()

    (c,) = service.query_candles("S1", 60_000, 120_000)
    log.info("并发结果 tick_count=%s volume=%s", c.tick_count, c.volume)
    assert c.tick_count == n_threads * per_thread
    assert c.volume == float(n_threads * per_thread)  # 每条量=1，无重复累计


def test_concurrent_duplicate_batch_retries(service) -> None:
    """多线程用同一 batch_id 同时重试：恰好一次生效，其余判重。"""
    ticks = [TickIn(symbol="S1", timestamp_ms=60_000, price=10,
                    quantity=3, id="x")]
    outcomes = []
    lock = threading.Lock()

    def worker() -> None:
        # 随机抖动放大竞争
        random.seed()
        resp = service.ingest(IngestRequest(batch_id="dup-same", ticks=ticks))
        with lock:
            outcomes.append(resp.duplicate)

    log.info("同批次并发重试输入=%s 线程数=12", [t.model_dump() for t in ticks])
    threads = [threading.Thread(target=worker) for _ in range(12)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()

    log.info("判重结果=%s（期望恰好 1 个 False）", outcomes)
    assert outcomes.count(False) == 1
    assert outcomes.count(True) == 11
    (c,) = service.query_candles("S1", 60_000, 120_000)
    assert c.volume == 3.0 and c.tick_count == 1


def test_concurrent_read_during_writes(service) -> None:
    """写入过程中持续查询：返回值必须始终自洽（量=计数*单位量）。"""
    stop = threading.Event()
    errors: list[Exception] = []

    def writer() -> None:
        try:
            for i in range(100):
                service.ingest(IngestRequest(
                    batch_id=f"w-{i}",
                    ticks=[TickIn(symbol="S1", timestamp_ms=60_000 + i % 50,
                                  price=10, quantity=2, id=f"w{i}")]))
        except Exception as e:  # pragma: no cover - 仅记录
            errors.append(e)

    def reader() -> None:
        try:
            while not stop.is_set():
                for c in service.query_candles("S1", 60_000, 120_000):
                    # 每条点位量恒为 2；已提交视图必须自洽
                    assert c.volume == 2 * c.tick_count
        except Exception as e:  # pragma: no cover
            errors.append(e)

    log.info("并发读写: 100 条写入期间持续查询校验自洽性")
    w = threading.Thread(target=writer)
    readers = [threading.Thread(target=reader) for _ in range(4)]
    w.start()
    for r in readers:
        r.start()
    w.join()
    stop.set()
    for r in readers:
        r.join()

    assert not errors, errors
    (c,) = service.query_candles("S1", 60_000, 120_000)
    log.info("最终自洽视图 tick_count=%s volume=%s", c.tick_count, c.volume)
    assert c.tick_count == 100 and c.volume == 200.0


def test_concurrent_replace_range_serialization(service) -> None:
    """两个重建批次并发（不同 batch_id），结果确定且互不覆盖丢失。"""
    def rebuild(batch_id: str, price: float) -> None:
        ticks = [TickIn(symbol="S1", timestamp_ms=60_000, price=price,
                        quantity=1, id=f"{batch_id}-1")]
        from app.models import ReplaceRangeRequest
        service.replace_range(ReplaceRangeRequest(
            symbol="S1", start_ms=60_000, end_ms_exclusive=120_000,
            batch_id=batch_id, ticks=ticks))

    log.info("并发区间重建: 两个互斥事务串行化")
    t1 = threading.Thread(target=rebuild, args=("fix-a", 11.0))
    t2 = threading.Thread(target=rebuild, args=("fix-b", 12.0))
    t1.start(); t2.start(); t1.join(); t2.join()

    (c,) = service.query_candles("S1", 60_000, 120_000)
    log.info("最终窗口（后提交者生效）=%s", c)
    # 两个事务严格串行：后者整体替换前者，窗口内恰好 1 条
    assert c.tick_count == 1 and c.volume == 1.0
    assert c.open in (11.0, 12.0)


def test_multiprocess_writers_share_db(tmp_path) -> None:
    """跨进程并发写同一 SQLite 文件：最终计数精确无丢失。"""
    import multiprocessing as mp

    db = str(tmp_path / "mp.db")

    n_proc, per_proc = 4, 20
    log.info("多进程并发: 进程=%s 每进程=%s", n_proc, per_proc)
    ctx = mp.get_context("spawn")
    procs = [ctx.Process(target=_mp_child, args=(db, i, per_proc))
             for i in range(n_proc)]
    for p in procs:
        p.start()
    for p in procs:
        p.join()
        assert p.exitcode == 0

    store = TickStore(db); store.init_db()
    svc = MarketService(store)
    (c,) = svc.query_candles("S1", 60_000, 120_000)
    log.info("多进程最终 tick_count=%s volume=%s", c.tick_count, c.volume)
    assert c.tick_count == n_proc * per_proc
    assert c.volume == float(n_proc * per_proc)
    store.close()
