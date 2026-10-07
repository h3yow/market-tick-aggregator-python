"""区间整体替换式修正的服务层测试。

覆盖：边界归属、替换而非叠加、重复提交/重试幂等、拆批（子区间）顺序无关、
修正与普通写入并发、失败回滚、修正/补写/重放后与重算一致、本地对照校验。
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

import pytest

from app.errors import BatchConflictError, InvalidBatchError


def _candles(svc, symbol="AAA", start=0, end=600_000):
    return svc.query_candles(symbol, start, end)["candles"]


def _ohlcv(c):
    return (c["open"], c["high"], c["low"], c["close"], c["volume"], c["tick_count"])


# 一份「修正后完整集合」：跨越 3 个分钟窗口
def _correction_set(prefix: str = "c"):
    return [
        {"symbol": "AAA", "ts": 60_100, "price": 10.0, "size": 1.0},
        {"symbol": "AAA", "ts": 60_900, "price": 13.0, "size": 2.0},
        {"symbol": "AAA", "ts": 120_200, "price": 9.0, "size": 4.0},
        {"symbol": "AAA", "ts": 180_000, "price": 11.0, "size": 8.0},
    ]


class TestCorrectionBoundaries:
    def test_end_boundary_is_exclusive(self, service, request):
        service.ingest_batch("b0", [
            {"symbol": "AAA", "ts": 119_999, "price": 1, "size": 1},
            {"symbol": "AAA", "ts": 120_000, "price": 2, "size": 1},
        ])
        request.node._log_case(
            input={"range": [60000, 120000], "ts_at_end": 120000},
            rule="[start,end)：end 处点位不属于区间，必须保留且未被重算污染",
        )
        r = service.apply_correction(
            "c1", "AAA", 60_000, 120_000,
            [{"symbol": "AAA", "ts": 119_999, "price": 100, "size": 5}],
        )
        assert r["removed"] == 1
        cs = {c["window_start"]: c for c in _candles(service)}
        assert _ohlcv(cs[60_000]) == (100.0, 100.0, 100.0, 100.0, 5.0, 1)
        assert _ohlcv(cs[120_000]) == (2.0, 2.0, 2.0, 2.0, 1.0, 1)

    def test_start_boundary_is_inclusive(self, service, request):
        service.ingest_batch(
            "b0", [{"symbol": "AAA", "ts": 60_000, "price": 1, "size": 1}]
        )
        request.node._log_case(
            input={"range": [60000, 120000], "ts_at_start": 60000},
            rule="start 处点位属于区间，必须被替换",
        )
        r = service.apply_correction(
            "c1", "AAA", 60_000, 120_000,
            [{"symbol": "AAA", "ts": 60_000, "price": 7, "size": 2}],
        )
        assert r["removed"] == 1
        assert _ohlcv(_candles(service)[0]) == (7.0, 7.0, 7.0, 7.0, 2.0, 1)

    def test_tick_at_end_rejected_out_of_range(self, service, request):
        rec = {"symbol": "AAA", "ts": 120_000, "price": 1, "size": 1}
        request.node._log_case(
            input={"range": [60000, 120000], "record": rec},
            rule="区间口径由服务端强制：ts>=end 的点位以 tick_out_of_range 整次拒绝",
        )
        with pytest.raises(InvalidBatchError) as exc:
            service.apply_correction("c1", "AAA", 60_000, 120_000, [rec])
        assert exc.value.error_code == "records_out_of_range"
        assert exc.value.rejected_records[0].reason == "tick_out_of_range"
        assert _candles(service) == []

    def test_symbol_mismatch_rejected(self, service, request):
        rec = {"symbol": "BBB", "ts": 60_000, "price": 1, "size": 1}
        request.node._log_case(
            input={"path_symbol": "AAA", "record_symbol": "BBB"},
            rule="修正集合只允许包含路径标的，否则 symbol_mismatch 整次拒绝",
        )
        with pytest.raises(InvalidBatchError) as exc:
            service.apply_correction("c1", "AAA", 60_000, 120_000, [rec])
        assert exc.value.rejected_records[0].reason == "symbol_mismatch"

    @pytest.mark.parametrize(
        "start,end,code",
        [(-1, 1, "invalid_start"), (60_000, -1, "invalid_end"),
         (60_000, 60_000, "invalid_range"), (120_000, 60_000, "invalid_range")],
    )
    def test_invalid_range_params(self, service, request, start, end, code):
        request.node._log_case(input={"start": start, "end": end},
                               rule=f"非法区间参数 -> {code}")
        with pytest.raises(InvalidBatchError) as exc:
            service.apply_correction("c", "AAA", start, end, [])
        assert exc.value.error_code == code

    def test_empty_records_clears_the_range(self, service, request):
        service.ingest_batch("b0", [
            {"symbol": "AAA", "ts": 60_000, "price": 1, "size": 10},
            {"symbol": "AAA", "ts": 120_000, "price": 2, "size": 20},
        ])
        request.node._log_case(
            input={"records": [], "range": [60000, 120000]},
            rule="空集合是合法修正（区间清空），相邻窗口保留",
        )
        r = service.apply_correction("c1", "AAA", 60_000, 120_000, [])
        assert r["status"] == "applied" and r["removed"] == 1
        assert [c["window_start"] for c in _candles(service)] == [120_000]


class TestReplacementNotAppend:
    def test_correction_replaces_bad_history(self, service, request):
        # 录错的历史段：两个窗口累计了错误的大成交量
        service.ingest_batch("bad", [
            {"symbol": "AAA", "ts": 60_100, "price": 10, "size": 1_000},
            {"symbol": "AAA", "ts": 120_100, "price": 11, "size": 1_000},
        ])
        fixed = _correction_set()
        request.node._log_case(
            input={"bad_volume": 2000, "fixed": fixed},
            rule="修正后区间 K 线完全以新集合为准，旧成交量不残留、不叠加",
        )
        r = service.apply_correction("c1", "AAA", 60_000, 240_000, fixed)
        assert (r["removed"], r["inserted"]) == (2, 4)
        cs = {c["window_start"]: c for c in _candles(service)}
        assert _ohlcv(cs[60_000]) == (10.0, 13.0, 10.0, 13.0, 3.0, 2)
        assert _ohlcv(cs[120_000]) == (9.0, 9.0, 9.0, 9.0, 4.0, 1)
        assert _ohlcv(cs[180_000]) == (11.0, 11.0, 11.0, 11.0, 8.0, 1)


class TestIdempotencyAndRetry:
    def test_duplicate_correction_applies_once(self, service, request):
        fixed = _correction_set()
        request.node._log_case(
            input={"correction_id": "c1", "submit_times": 3},
            rule="同 correction_id 重复提交只生效一次：volume 不滚大",
        )
        r1 = service.apply_correction("c1", "AAA", 60_000, 240_000, fixed)
        r2 = service.apply_correction("c1", "AAA", 60_000, 240_000, fixed)
        r3 = service.apply_correction("c1", "AAA", 60_000, 240_000, list(reversed(fixed)))
        assert r1["status"] == "applied"
        assert r2["status"] == "duplicate" and r2["removed"] == 0
        assert r3["status"] == "duplicate"  # 同集合换顺序，指纹一致
        total_volume = sum(c["volume"] for c in _candles(service))
        assert total_volume == 15.0

    def test_same_id_different_payload_conflict_409(self, service, request):
        r1 = [{"symbol": "AAA", "ts": 60_000, "price": 10, "size": 1}]
        r2 = [{"symbol": "AAA", "ts": 60_000, "price": 11, "size": 1}]
        request.node._log_case(
            input={"first": r1, "second": r2},
            rule="同 correction_id 不同负载 -> 409，首次修正保持不变",
        )
        service.apply_correction("c1", "AAA", 60_000, 120_000, r1)
        with pytest.raises(BatchConflictError) as exc:
            service.apply_correction("c1", "AAA", 60_000, 120_000, r2)
        assert exc.value.error_code == "correction_id_conflict"
        assert _candles(service)[0]["open"] == 10.0

    def test_correction_id_reused_by_ingest_conflicts(self, service, request):
        service.ingest_batch("x", [
            {"symbol": "AAA", "ts": 60_000, "price": 1, "size": 1}
        ])
        request.node._log_case(
            input={"id": "x", "first_use": "ingest"},
            rule="普通批次与修正的幂等键不得互相复用",
        )
        with pytest.raises(BatchConflictError):
            service.apply_correction(
                "x", "AAA", 60_000, 120_000,
                [{"symbol": "AAA", "ts": 60_000, "price": 2, "size": 1}],
            )

    def test_ingest_batch_id_reused_by_correction_conflicts(self, service, request):
        service.apply_correction(
            "y", "AAA", 60_000, 120_000,
            [{"symbol": "AAA", "ts": 60_000, "price": 2, "size": 1}],
        )
        request.node._log_case(
            input={"id": "y", "first_use": "correction"},
            rule="修正已占用的键再用于普通写入 -> 409，修正结果不被污染",
        )
        with pytest.raises(BatchConflictError) as exc:
            service.ingest_batch(
                "y", [{"symbol": "AAA", "ts": 60_000, "price": 1, "size": 1}]
            )
        assert exc.value.error_code == "correction_id_conflict"
        assert _candles(service)[0]["open"] == 2.0

    def test_failed_correction_changes_nothing_and_safe_retry(self, service, request):
        service.ingest_batch("bad", [
            {"symbol": "AAA", "ts": 60_100, "price": 10, "size": 100},
        ])
        bad_payload = [
            {"symbol": "AAA", "ts": 60_200, "price": 11, "size": 1},
            {"symbol": "AAA", "ts": 60_300, "price": 11, "size": -1},  # 无效
        ]
        good_payload = [
            {"symbol": "AAA", "ts": 60_200, "price": 11, "size": 1},
        ]
        request.node._log_case(
            input={"attempt1": "含 size=-1", "attempt2": "同 id 修正后重试"},
            rule="失败整次回滚（旧点仍在）；同 correction_id 修好后安全重试且生效一次",
        )
        with pytest.raises(InvalidBatchError):
            service.apply_correction("retry-c", "AAA", 60_000, 120_000, bad_payload)
        # 回滚后旧数据原样存在
        assert _ohlcv(_candles(service)[0]) == (10.0, 10.0, 10.0, 10.0, 100.0, 1)
        ok = service.apply_correction(
            "retry-c", "AAA", 60_000, 120_000, good_payload
        )
        assert ok["status"] == "applied" and ok["removed"] == 1
        assert _ohlcv(_candles(service)[0]) == (11.0, 11.0, 11.0, 11.0, 1.0, 1)


class TestSplitBatches:
    @pytest.mark.parametrize("variant", [0, 1, 2])
    def test_split_into_disjoint_ranges_order_independent(
        self, db_path_factory, variant, request
    ):
        # 同一批修正集合，切成沿时间相邻、互不相交的三段提交；
        # 改变三段提交顺序，最终 OHLCV 必须与一次性整段提交完全一致。
        parts = [
            ("p1", 60_000, 120_000, [
                {"symbol": "AAA", "ts": 60_100, "price": 10.0, "size": 1.0},
                {"symbol": "AAA", "ts": 60_900, "price": 13.0, "size": 2.0},
            ]),
            ("p2", 120_000, 180_000, [
                {"symbol": "AAA", "ts": 120_200, "price": 9.0, "size": 4.0},
            ]),
            ("p3", 180_000, 240_000, [
                {"symbol": "AAA", "ts": 180_000, "price": 11.0, "size": 8.0},
            ]),
        ]
        orders = [parts, list(reversed(parts)), [parts[1], parts[2], parts[0]]]
        chosen = orders[variant]

        from app.storage import TickStore
        from app.service import TickService

        svc = TickService(TickStore(db_path_factory(f"split-{variant}.db")))
        request.node._log_case(
            input={"variant": variant, "order": [p[0] for p in chosen]},
            rule="同一份修正集合拆成相邻子区间、换序提交 -> 最终 K 线完全一致",
        )
        for cid, s, e, recs in chosen:
            r = svc.apply_correction(cid, "AAA", s, e, recs)
            assert r["status"] == "applied"
        split_candles = _candles(svc)

        svc2 = TickService(TickStore(db_path_factory("whole.db")))
        whole = [rec for _, _, _, recs in parts for rec in recs]
        svc2.apply_correction("whole", "AAA", 60_000, 240_000, whole)
        whole_candles = _candles(svc2)
        assert split_candles == whole_candles
        assert [_ohlcv(c) for c in split_candles] == [
            (10.0, 13.0, 10.0, 13.0, 3.0, 2),
            (9.0, 9.0, 9.0, 9.0, 4.0, 1),
            (11.0, 11.0, 11.0, 11.0, 8.0, 1),
        ]


class TestCorrectionConcurrency:
    def test_concurrent_same_correction_id_only_one_applies(self, service, request):
        payload = [{"symbol": "AAA", "ts": 60_000, "price": 10, "size": 1}]
        request.node._log_case(
            input={"correction_id": "same", "threads": 6},
            rule="并发同 id 重试：一次 applied，其余 duplicate，成交量为 1",
        )
        statuses: list[str] = []

        def call(_):
            return service.apply_correction("same", "AAA", 60_000, 120_000, payload)["status"]

        with ThreadPoolExecutor(max_workers=6) as ex:
            statuses = list(ex.map(call, range(6)))
        assert statuses.count("applied") == 1
        assert statuses.count("duplicate") == 5
        assert _candles(service)[0]["volume"] == 1.0

    def test_concurrent_correction_and_ingest_never_half_state(self, service, request):
        request.node._log_case(
            input={"ingesters": 4, "corrections": 2, "window": 60000},
            rule="修正与普通写入并发：最终 ticks/candles 自洽且 verify 一致，"
                 "任何重复提交不二次生效",
        )

        def ingester(tid: int):
            for j in range(10):
                service.ingest_batch(
                    f"ing-{tid}-{j}",
                    [{"symbol": "AAA", "ts": 60_000 + j, "price": 10.0, "size": 1.0}],
                )

        def correction(tid: int):
            # 两个不相交子区间，各重试两次，验证幂等与并发安全
            start = 60_000 if tid == 0 else 300_000
            recs = [{"symbol": "AAA", "ts": start + 5, "price": 20.0, "size": 3.0}]
            for _ in range(2):
                service.apply_correction(
                    f"corr-{tid}", "AAA", start, start + 60_000, recs
                )

        with ThreadPoolExecutor(max_workers=8) as ex:
            futs = [ex.submit(ingester, i) for i in range(4)]
            futs += [ex.submit(correction, i) for i in range(2)]
            for f in futs:
                f.result()

        verify = service.verify_symbol("AAA")
        assert verify["consistent"] is True
        # 并发交错下哪些普通写入最终存活取决于提交先后（可复现的序列化语义），
        # 但以下不变量与调度无关：
        # 1) 修正点（20,size3）必然是最终集合的一部分；
        # 2) 每个窗口 volume 严格等于 tick_count*1 + 3（修正点若在该窗口）；
        # 3) 无重复累计；300000 窗口只可能有修正点（无普通写入竞争）。
        w = {c["window_start"]: c for c in _candles(service, end=400_000)}
        assert w[300_000]["volume"] == 3.0 and w[300_000]["tick_count"] == 1
        c60 = w[60_000]
        assert c60["high"] == 20.0 and c60["low"] == 10.0
        assert abs(c60["volume"] - (c60["tick_count"] - 1 + 3)) < 1e-9


class TestRecomputeEquivalenceAndVerify:
    def test_correction_backfill_replay_then_rebuild_equals_recompute(
        self, service, request
    ):
        # 1) 错误历史 + 区间外数据
        service.ingest_batch("bad", [
            {"symbol": "AAA", "ts": 60_100, "price": 10, "size": 100},
            {"symbol": "AAA", "ts": 120_100, "price": 12, "size": 100},
        ])
        service.ingest_batch("outside", [
            {"symbol": "AAA", "ts": 300_000, "price": 40, "size": 6},
            {"symbol": "BBB", "ts": 60_100, "price": 5, "size": 7},
        ])
        # 2) 区间修正
        service.apply_correction("fix", "AAA", 60_000, 180_000, [
            {"symbol": "AAA", "ts": 60_200, "price": 11, "size": 2},
            {"symbol": "AAA", "ts": 120_200, "price": 13, "size": 3},
        ])
        # 3) 修正之后再来普通补写（落在区间内 → 追加在修正集合之上）
        service.ingest_batch("after", [
            {"symbol": "AAA", "ts": 60_300, "price": 15, "size": 4},
        ])
        # 4) 幂等重放修正
        again = service.apply_correction("fix", "AAA", 60_000, 180_000, [
            {"symbol": "AAA", "ts": 120_200, "price": 13, "size": 3},
            {"symbol": "AAA", "ts": 60_200, "price": 11, "size": 2},
        ])
        request.node._log_case(
            input={"steps": ["bad ingest", "correction", "later ingest",
                             "correction replay", "rebuild"]},
            rule="修正/补写/重放后 rebuild 结果 == 仅按当前有效点位重算",
        )
        assert again["status"] == "duplicate"

        before = _candles(service, end=360_000)
        n = service.rebuild_symbol("AAA")["rebuilt_windows"]
        after = _candles(service, end=360_000)
        assert before == after and n == 3

        # 期望值：窗口 60000 = 修正点(11,size2) + 后补点(15,size4)
        got = {c["window_start"]: _ohlcv(c) for c in after}
        assert got[60_000] == (11.0, 15.0, 11.0, 15.0, 6.0, 2)
        assert got[120_000] == (13.0, 13.0, 13.0, 13.0, 3.0, 1)
        assert got[300_000] == (40.0, 40.0, 40.0, 40.0, 6.0, 1)
        # 其它标的未受影响
        bbb = _candles(service, symbol="BBB", end=120_000)
        assert _ohlcv(bbb[0]) == (5.0, 5.0, 5.0, 5.0, 7.0, 1)

        # 对照校验：AAA / BBB 均一致
        assert service.verify_symbol("AAA")["consistent"] is True
        assert service.verify_symbol("BBB")["consistent"] is True

    def test_verify_reports_mismatch_detail(self, service, request):
        service.ingest_batch("b", [
            {"symbol": "AAA", "ts": 60_000, "price": 10, "size": 2},
        ])
        # 直接破坏 candles 表模拟物化结果与事实表脱节
        service.store._conn.execute(
            "UPDATE candles SET open=1.0 WHERE symbol='AAA'"
        )
        request.node._log_case(
            input={"tamper": "candles.open=1"},
            rule="本地校验必须检出不一致并给出 expected/actual 明细",
        )
        res = service.verify_symbol("AAA")
        assert res["consistent"] is False and res["checked_windows"] == 1
        m = res["mismatches"][0]
        assert m["window_start"] == 60_000
        assert m["expected"]["open"] == 10.0 and m["actual"]["open"] == 1.0
