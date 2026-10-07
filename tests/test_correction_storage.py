"""区间整体替换式修正的存储层测试：边界归属、替换语义、并发、回滚、对照校验。"""

from __future__ import annotations

import sqlite3
from concurrent.futures import ThreadPoolExecutor

import pytest

from app.storage import StoredTick, TickStore

W = 60_000


def _tick(bid: str, ts: int, price: float = 10.0, size: float = 1.0,
          symbol: str = "AAA", index: int = 0) -> StoredTick:
    return StoredTick(bid, index, symbol, ts, price, size)


class TestReplacementSemantics:
    def test_replaces_range_ticks_not_append(self, store, request):
        store.insert_batch("bad", "h-bad", [], [
            _tick("bad", 60_000, 10.0, 100.0, index=0),
            _tick("bad", 60_500, 11.0, 100.0, index=1),
        ])
        request.node._log_case(
            input={"old": [(60000, 10, 100), (60500, 11, 100)],
                   "correction": [(60200, 20, 1)], "range": [60000, 120000]},
            rule="区间内旧点删除后写新点；volume 不得叠加（1 而非 201）",
        )
        res = store.apply_correction(
            "c1", "h-c1", "AAA", 60_000, 120_000,
            [_tick("c1", 60_200, 20.0, 1.0)],
        )
        assert res["removed"] == 2 and res["inserted"] == 1
        row = store.query_candles("AAA", 0, 120_000)[0]
        assert (row[2], row[3], row[4], row[5], row[6], row[7]) == (
            20.0, 20.0, 20.0, 20.0, 1.0, 1
        )

    def test_half_open_boundary_ts_equals_end_kept(self, store, request):
        store.insert_batch("base", "h", [], [
            _tick("base", 119_999, 10.0, 5.0, index=0),
            _tick("base", 120_000, 12.0, 7.0, index=1),
        ])
        request.node._log_case(
            input={"range": [60000, 120000], "ts_in": 119999, "ts_end": 120000},
            rule="[start,end) 左闭右开：ts=119999 被替换，ts=120000 必须原样保留",
        )
        store.apply_correction(
            "c1", "hc", "AAA", 60_000, 120_000,
            [_tick("c1", 119_999, 50.0, 1.0)],
        )
        w1 = store.query_candles("AAA", 60_000, 60_000)[0]
        w2 = store.query_candles("AAA", 120_000, 120_000)[0]
        assert (w1[2], w1[6], w1[7]) == (50.0, 1.0, 1)
        assert (w2[2], w2[6], w2[7]) == (12.0, 7.0, 1)

    def test_out_of_range_windows_and_other_symbols_untouched(self, store, request):
        store.insert_batch("base", "h", [], [
            _tick("base", 60_000, 10.0, 1.0, symbol="AAA", index=0),
            _tick("base", 180_000, 30.0, 9.0, symbol="AAA", index=1),
            _tick("base", 60_100, 5.0, 7.0, symbol="BBB", index=2),
        ])
        request.node._log_case(
            input={"symbol": "AAA", "range": [60000, 120000]},
            rule="只动区间命中窗口；其它窗口与其它标的（BBB）不得改变",
        )
        store.apply_correction("c1", "hc", "AAA", 60_000, 120_000, [])
        assert store.query_candles("AAA", 0, 240_000) == [
            ("AAA", 180_000, 30.0, 30.0, 30.0, 30.0, 9.0, 1)
        ]
        bbb = store.query_candles("BBB", 0, 240_000)[0]
        assert (bbb[2], bbb[6]) == (5.0, 7.0)

    def test_partial_overlap_window_keeps_outside_ticks(self, store, request):
        # 同一窗口 [60000,120000) 内：区间 [60300,120000) 只覆盖后半部分
        store.insert_batch("base", "h", [], [
            _tick("base", 60_100, 8.0, 2.0, index=0),   # 区间外，保留
            _tick("base", 60_900, 99.0, 50.0, index=1), # 区间内，删除
        ])
        request.node._log_case(
            input={"range": [60300, 120000], "keep_ts": 60100, "drop_ts": 60900},
            rule="区间按点位 ts 精确归属，而非整窗口替换：窗口内区间外点位保留",
        )
        store.apply_correction(
            "c1", "hc", "AAA", 60_300, 120_000,
            [_tick("c1", 60_400, 20.0, 3.0)],
        )
        row = store.query_candles("AAA", 60_000, 60_000)[0]
        assert (row[2], row[3], row[4], row[5], row[6], row[7]) == (
            8.0, 20.0, 8.0, 20.0, 5.0, 2
        )

    def test_empty_records_clears_range_only(self, store, request):
        store.insert_batch("base", "h", [], [
            _tick("base", 60_000, 10.0, 1.0, index=0),
            _tick("base", 120_000, 11.0, 1.0, index=1),
        ])
        request.node._log_case(
            input={"records": [], "range": [60000, 120000]},
            rule="空修正集合 = 清空区间；区间外窗口保留",
        )
        res = store.apply_correction("c1", "hc", "AAA", 60_000, 120_000, [])
        assert res["removed"] == 1 and res["inserted"] == 0
        assert store.query_candles("AAA", 0, 180_000) == [
            ("AAA", 120_000, 11.0, 11.0, 11.0, 11.0, 1.0, 1)
        ]

    def test_repeated_correction_raises_integrity_and_changes_nothing(
        self, store, request
    ):
        store.apply_correction("c1", "hc", "AAA", 60_000, 120_000,
                               [_tick("c1", 60_000, 10.0, 1.0)])
        request.node._log_case(
            input={"correction_id": "c1 twice"},
            rule="重复 correction_id 触发主键冲突并回滚，不叠加",
        )
        with pytest.raises(sqlite3.IntegrityError):
            store.apply_correction("c1", "hc", "AAA", 60_000, 120_000,
                                   [_tick("c1", 60_000, 10.0, 1.0)])
        row = store.query_candles("AAA", 60_000, 60_000)[0]
        assert row[6] == 1.0 and row[7] == 1

    def test_overlapping_corrections_last_one_wins_for_its_range(
        self, store, request
    ):
        store.insert_batch("base", "h", [], [
            _tick("base", 60_000, 10.0, 100.0, index=0),
            _tick("base", 120_000, 20.0, 100.0, index=1),
        ])
        request.node._log_case(
            input={"c1_range": [60000, 180000], "c2_range": [120000, 180000]},
            rule="后一次修正只替换其区间；与前次修正在该区间表现为替换而非叠加",
        )
        store.apply_correction("c1", "hc1", "AAA", 60_000, 180_000, [
            _tick("c1", 60_000, 10.0, 1.0, index=0),
            _tick("c1", 120_000, 20.0, 2.0, index=1),
        ])
        store.apply_correction("c2", "hc2", "AAA", 120_000, 180_000,
                               [_tick("c2", 120_000, 22.0, 4.0)])
        rows = store.query_candles("AAA", 0, 240_000)
        assert [(r[1], r[5], r[6]) for r in rows] == [
            (60_000, 10.0, 1.0),
            (120_000, 22.0, 4.0),
        ]


class TestTransactionRollback:
    def test_failure_after_delete_rolls_back_everything(self, store, request):
        store.insert_batch("base", "h", [], [
            _tick("base", 60_000, 10.0, 5.0, index=0),
            _tick("base", 60_100, 11.0, 6.0, index=1),
        ])
        # 人为在删除之后制造失败：同 correction_id 的点位主键冲突
        request.node._log_case(
            input={"fail": "duplicate (batch_id,batch_index) after range delete"},
            rule="事务任一步失败 -> 区间旧点恢复，candles 保持修正前结果",
        )
        with pytest.raises(sqlite3.IntegrityError):
            store.apply_correction("base", "hc", "AAA", 60_000, 120_000, [
                StoredTick("base", 0, "AAA", 60_200, 20.0, 1.0),
            ])
        rows = store.query_candles("AAA", 60_000, 60_000)
        assert (rows[0][6], rows[0][7]) == (11.0, 2)
        batch = store.get_batch("base")
        assert batch is not None and batch.kind == "ingest"


class TestCorrectionConcurrency:
    def test_concurrent_distinct_corrections_serialized(self, db_path, request):
        n = 6
        request.node._log_case(
            input={"connections": n, "ranges": "disjoint minute windows"},
            rule="多个连接并发提交互不相交区间修正：全部生效且各自区间结果正确",
        )

        def worker(tid: int):
            conn = TickStore(db_path)
            try:
                ws = (10 + tid) * W
                conn.apply_correction(
                    f"c{tid}", f"h{tid}", "AAA", ws, ws + W,
                    [StoredTick(f"c{tid}", 0, "AAA", ws + 1, 10.0, float(tid + 1))],
                )
            finally:
                conn.close()

        with ThreadPoolExecutor(max_workers=n) as ex:
            list(ex.map(worker, range(n)))
        final = TickStore(db_path)
        rows = final.query_candles("AAA", 10 * W, (10 + n) * W)
        assert len(rows) == n
        assert sum(r[6] for r in rows) == float(n * (n + 1) // 2)
        assert final.verify_symbol("AAA")["mismatches"] == []
        final.close()

    def test_concurrent_same_correction_id_applies_once(self, db_path, request):
        n = 5
        request.node._log_case(
            input={"connections": n, "correction_id": "same"},
            rule="并发同 correction_id：仅一次成功，其余冲突回滚，结果不叠加",
        )
        outcomes: list[str] = []

        def attempt(_):
            conn = TickStore(db_path)
            try:
                conn.apply_correction(
                    "same", "h-same", "AAA", 60_000, 120_000,
                    [StoredTick("same", 0, "AAA", 60_000, 10.0, 1.0)],
                )
                return "applied"
            except sqlite3.IntegrityError:
                return "conflict"
            finally:
                conn.close()

        with ThreadPoolExecutor(max_workers=n) as ex:
            outcomes = list(ex.map(attempt, range(n)))
        assert outcomes.count("applied") == 1
        assert outcomes.count("conflict") == n - 1
        final = TickStore(db_path)
        row = final.query_candles("AAA", 60_000, 60_000)[0]
        assert (row[6], row[7]) == (1.0, 1)
        final.close()


class TestVerify:
    def test_verify_clean_store_consistent(self, store, request):
        store.insert_batch("b", "h", [], [
            _tick("b", 60_000, 10.0, 2.0, index=0),
            _tick("b", 120_000, 11.0, 3.0, index=1),
        ])
        request.node._log_case(input={"action": "verify clean"},
                               rule="正常写入/修正后 verify 必须一致")
        res = store.verify_symbol("AAA")
        assert res["checked"] == 2 and res["mismatches"] == []

    def test_verify_detects_tampered_candle(self, store, request):
        store.insert_batch("b", "h", [],
                           [_tick("b", 60_000, 10.0, 2.0)])
        store._conn.execute(
            "UPDATE candles SET volume = 999.0 WHERE symbol='AAA'"
        )
        request.node._log_case(
            input={"tamper": "candles.volume=999"},
            rule="candles 与 ticks 重算不一致时 verify 必须报出该窗口及期望值",
        )
        res = store.verify_symbol("AAA")
        assert res["checked"] == 1 and len(res["mismatches"]) == 1
        m = res["mismatches"][0]
        assert m["window_start"] == 60_000
        assert m["expected"][6] == 2.0 and m["actual"][6] == 999.0

    def test_verify_detects_missing_and_orphan_candle(self, store, request):
        store.insert_batch("b", "h", [], [
            _tick("b", 60_000, 10.0, 1.0, index=0),
            _tick("b", 120_000, 11.0, 1.0, index=1),
        ])
        # 制造：缺失窗口 60000 + 幽灵窗口 180000
        store._conn.execute(
            "DELETE FROM candles WHERE symbol='AAA' AND window_start=60000"
        )
        store._conn.execute(
            "INSERT INTO candles VALUES ('AAA', 180000, 1,1,1,1,1,1)"
        )
        request.node._log_case(
            input={"missing_window": 60000, "orphan_window": 180000},
            rule="缺失/多余窗口都应被 verify 检出",
        )
        res = store.verify_symbol("AAA")
        by_ws = {m["window_start"]: m for m in res["mismatches"]}
        assert set(by_ws) == {60_000, 180_000}
        assert by_ws[60_000]["actual"] is None
        assert by_ws[180_000]["expected"] is None
