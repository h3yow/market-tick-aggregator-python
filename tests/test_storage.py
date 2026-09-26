"""存储层测试：本地 SQLite，覆盖持久化、窗口聚合与并发。"""

from __future__ import annotations

import sqlite3
from concurrent.futures import ThreadPoolExecutor

import pytest

from app.storage import StoredTick, TickStore, window_start_of


class TestWindowing:
    def test_window_floor_to_minute(self, request):
        request.node._log_case(input={"ts": 60_999}, rule="floor(ts/60000)*60000")
        assert window_start_of(0) == 0
        assert window_start_of(59_999) == 0
        assert window_start_of(60_000) == 60_000
        assert window_start_of(60_999) == 60_000
        assert window_start_of(120_000) == 120_000


class TestPersistence:
    def test_commit_survives_reopen(self, db_path, request):
        ticks = [StoredTick("b1", i, "AAA", 60_000 + i * 100, 10.0 + i, 1.0)
                 for i in range(3)]
        s1 = TickStore(db_path)
        s1.insert_batch("b1", "h1", [], ticks)
        s1.close()
        request.node._log_case(input={"db": db_path, "ticks": 3},
                               rule="关闭进程后重新打开，已提交批次与窗口必须仍在")

        s2 = TickStore(db_path)
        batch = s2.get_batch("b1")
        assert batch is not None and batch.accepted == 3
        rows = s2.query_candles("AAA", 0, 120_000)
        assert len(rows) == 1
        assert rows[0][2] == 10.0          # open（确定性全序第一条）
        assert rows[0][3] == 12.0          # high
        assert rows[0][4] == 10.0          # low
        assert rows[0][5] == 12.0          # close
        assert rows[0][6] == 3.0           # volume
        assert rows[0][7] == 3             # tick_count
        s2.close()

    def test_rollback_on_duplicate_batch_leaves_no_data(self, store, request):
        ticks = [StoredTick("b1", 0, "AAA", 60_000, 10.0, 1.0)]
        store.insert_batch("b1", "h1", [], ticks)
        request.node._log_case(
            input={"batch_id": "b1 twice"},
            rule="重复 batch_id 触发主键冲突 -> ROLLBACK，ticks/candles 不重复",
        )
        with pytest.raises(sqlite3.IntegrityError):
            store.insert_batch("b1", "h1", [], ticks)
        rows = store.query_candles("AAA", 0, 120_000)
        assert rows[0][6] == 1.0  # 成交量未翻倍
        assert rows[0][7] == 1


class TestRebuild:
    def test_rebuild_recomputes_from_ticks(self, store, request):
        store.insert_batch(
            "b1",
            "h1",
            [],
            [
                StoredTick("b1", 0, "AAA", 60_100, 10.0, 2.0),
                StoredTick("b1", 1, "AAA", 60_200, 13.0, 1.0),
            ],
        )
        request.node._log_case(
            input={"symbol": "AAA", "ticks": 2},
            rule="rebuild 以 ticks 为准重算全部窗口，结果与增量写入一致",
        )
        before = store.query_candles("AAA", 0, 120_000)
        n = store.rebuild_symbol("AAA")
        after = store.query_candles("AAA", 0, 120_000)
        assert n == 1
        assert before == after

    def test_rebuild_clears_stale_windows(self, store, request):
        store.insert_batch("b1", "h1", [],
                           [StoredTick("b1", 0, "AAA", 60_000, 10.0, 1.0)])
        assert len(store.query_candles("AAA", 0, 120_000)) == 1
        request.node._log_case(
            input={"action": "delete ticks then rebuild"},
            rule="事实表中消失的窗口在重建后必须从 candles 清除",
        )
        store._conn.execute("DELETE FROM ticks WHERE symbol='AAA'")
        store.rebuild_symbol("AAA")
        assert store.query_candles("AAA", 0, 120_000) == []


class TestConcurrency:
    def test_concurrent_writers_same_symbol_no_double_count(self, db_path, request):
        n_threads = 8
        per_thread = 20
        request.node._log_case(
            input={"threads": n_threads, "batches_per_thread": per_thread},
            rule="同窗口并发写入后 volume=各批 size 之和，tick_count 不重不漏",
        )

        def worker(tid: int):
            local = TickStore(db_path)
            try:
                for j in range(per_thread):
                    bid = f"t{tid}-{j}"
                    local.insert_batch(
                        bid,
                        f"h-{bid}",
                        [],
                        [StoredTick(bid, 0, "AAA", 60_000, 10.0, 1.0)],
                    )
            finally:
                local.close()

        with ThreadPoolExecutor(max_workers=n_threads) as ex:
            list(ex.map(worker, range(n_threads)))

        final = TickStore(db_path)
        rows = final.query_candles("AAA", 0, 120_000)
        assert len(rows) == 1
        assert rows[0][6] == float(n_threads * per_thread)
        assert rows[0][7] == n_threads * per_thread
        final.close()

    def test_concurrent_reads_always_consistent_snapshot(self, db_path, request):
        n_writers = 4
        n_readers = 4
        request.node._log_case(
            input={"writers": n_writers, "readers": n_readers},
            rule="并发查询不得读到部分累计；volume 必须为 1.0 的整数倍且 <= 已提交数",
        )
        errors: list[Exception] = []

        def writer(tid: int):
            try:
                w = TickStore(db_path)
                for j in range(25):
                    bid = f"w{tid}-{j}"
                    w.insert_batch(
                        bid, f"h-{bid}", [],
                        [StoredTick(bid, 0, "AAA", 60_000, 10.0, 1.0)],
                    )
                w.close()
            except Exception as exc:  # pragma: no cover - 断言时呈现
                errors.append(exc)

        def reader(_idx: int):
            try:
                r = TickStore(db_path)
                for _ in range(50):
                    for row in r.query_candles("AAA", 0, 120_000):
                        vol, cnt = row[6], row[7]
                        assert abs(vol - cnt) < 1e-9
                        assert cnt.is_integer()
                r.close()
            except Exception as exc:  # pragma: no cover
                errors.append(exc)

        with ThreadPoolExecutor(max_workers=n_writers + n_readers) as ex:
            futs = [ex.submit(writer, i) for i in range(n_writers)]
            futs += [ex.submit(reader, i) for i in range(n_readers)]
            for f in futs:
                f.result()
        assert not errors

    def test_concurrent_duplicate_batch_id_across_connections(
        self, db_path, request
    ):
        n = 6
        request.node._log_case(
            input={"connections": n, "batch_id": "same"},
            rule="多个独立连接并发提交相同 batch_id：仅一次成功，volume=1，其余冲突回滚",
        )
        outcomes: list[str] = []

        def attempt(_):
            conn = TickStore(db_path)
            try:
                conn.insert_batch(
                    "same",
                    "h-same",
                    [],
                    [StoredTick("same", 0, "AAA", 60_000, 10.0, 1.0)],
                )
                return "inserted"
            except sqlite3.IntegrityError:
                return "conflict"
            finally:
                conn.close()

        with ThreadPoolExecutor(max_workers=n) as ex:
            outcomes = list(ex.map(attempt, range(n)))
        assert outcomes.count("inserted") == 1
        assert outcomes.count("conflict") == n - 1
        final = TickStore(db_path)
        rows = final.query_candles("AAA", 0, 120_000)
        assert rows[0][6] == 1.0 and rows[0][7] == 1
        final.close()
