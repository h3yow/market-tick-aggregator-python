"""区间整体替换式修正：边界归属、替换语义、分片顺序无关性。"""

from __future__ import annotations

import pytest


def _candles(svc, symbol="AAA", start=0, end=300_000):
    return svc.query_candles(symbol, start, end)["candles"]


def _ohlcv(c):
    return (c["open"], c["high"], c["low"], c["close"], c["volume"], c["tick_count"])


class TestRangeBoundary:
    def test_half_open_interval_boundary_inclusion(self, service, request):
        # 三个贴边点位：start(含)、start 同毫秒、end(不含)
        service.ingest_batch("base", [
            {"symbol": "AAA", "ts": 60_000, "price": 10, "size": 1},
            {"symbol": "AAA", "ts": 119_999, "price": 20, "size": 1},
            {"symbol": "AAA", "ts": 120_000, "price": 30, "size": 1},
        ])
        request.node._log_case(
            input={"range": [60_000, 120_000], "ticks": [60_000, 119_999, 120_000]},
            rule="半开区间 [start,end)：60000/119999 被替换，120000 必须原样保留",
        )
        r = service.submit_correction_part(
            "corr1", "AAA", 60_000, 120_000, 1, 0,
            [{"symbol": "AAA", "ts": 90_000, "price": 50, "size": 2}],
        )
        assert r["status"] == "applied"
        # 只有 60000 窗口被重算；120000 窗口（含 end 边界点位）不出现在受影响集合
        assert r["affected_windows"] == ["AAA@60000"]
        cs = {c["window_start"]: c for c in _candles(service)}
        assert _ohlcv(cs[60_000]) == (50.0, 50.0, 50.0, 50.0, 2.0, 1)
        assert _ohlcv(cs[120_000]) == (30.0, 30.0, 30.0, 30.0, 1.0, 1)

    def test_reject_tick_on_outside_boundary(self, service, request):
        records = [
            {"symbol": "AAA", "ts": 60_000, "price": 10, "size": 1},
            {"symbol": "AAA", "ts": 120_000, "price": 10, "size": 1},  # == end，越界
        ]
        request.node._log_case(
            input={"range": [60_000, 120_000], "offending_ts": 120_000},
            rule="ts == end 不属于区间 -> tick_outside_range，整本片拒绝",
        )
        from app.errors import InvalidBatchError

        with pytest.raises(InvalidBatchError) as exc:
            service.submit_correction_part(
                "c", "AAA", 60_000, 120_000, 1, 0, records
            )
        assert exc.value.rejected_records[0].reason == "tick_outside_range"
        assert _candles(service) == []

    def test_reject_ts_before_start(self, service, request):
        request.node._log_case(
            input={"range": [60_000, 120_000], "offending_ts": 59_999},
            rule="ts < start 不属于区间 -> tick_outside_range",
        )
        from app.errors import InvalidBatchError

        with pytest.raises(InvalidBatchError) as exc:
            service.submit_correction_part(
                "c", "AAA", 60_000, 120_000, 1, 0,
                [{"symbol": "AAA", "ts": 59_999, "price": 10, "size": 1}],
            )
        assert exc.value.rejected_records[0].reason == "tick_outside_range"

    def test_reject_symbol_mismatch(self, service, request):
        request.node._log_case(
            input={"path_symbol": "AAA", "record_symbol": "BBB"},
            rule="修正点位的 symbol 必须与路径标的一致 -> symbol_mismatch",
        )
        from app.errors import InvalidBatchError

        with pytest.raises(InvalidBatchError) as exc:
            service.submit_correction_part(
                "c", "AAA", 60_000, 120_000, 1, 0,
                [{"symbol": "BBB", "ts": 60_000, "price": 10, "size": 1}],
            )
        assert exc.value.rejected_records[0].reason == "symbol_mismatch"


class TestReplaceSemantics:
    def test_replace_not_append_volume(self, service, request):
        # 错误历史：窗口内累计量 1000
        service.ingest_batch("wrong", [
            {"symbol": "AAA", "ts": 60_100, "price": 99, "size": 600},
            {"symbol": "AAA", "ts": 60_200, "price": 98, "size": 400},
        ])
        corrected = [
            {"symbol": "AAA", "ts": 60_100, "price": 10, "size": 1},
            {"symbol": "AAA", "ts": 60_200, "price": 11, "size": 2},
        ]
        request.node._log_case(
            input={"wrong_volume": 1000, "corrected_volume": 3},
            rule="替换而非叠加：修正后 volume=3，旧点位不再参与聚合",
        )
        service.submit_correction_part(
            "fix", "AAA", 60_000, 120_000, 1, 0, corrected
        )
        c = _candles(service)[0]
        assert _ohlcv(c) == (10.0, 11.0, 10.0, 11.0, 3.0, 2)

    def test_out_of_range_ticks_and_other_symbols_untouched(self, service, request):
        # AAA 窗口0 的点位在区间外（59999），窗口2 的点位也在区间外；BBB 完全不动
        service.ingest_batch("base", [
            {"symbol": "AAA", "ts": 59_999, "price": 5, "size": 7},
            {"symbol": "AAA", "ts": 120_000, "price": 6, "size": 8},
            {"symbol": "AAA", "ts": 60_500, "price": 100, "size": 100},
            {"symbol": "BBB", "ts": 60_500, "price": 200, "size": 200},
        ])
        request.node._log_case(
            input={"range": [60_000, 120_000], "outside_ts": [59_999, 120_000]},
            rule="仅替换区间内 AAA 点位；区间外点位与 BBB 标的完全不变",
        )
        service.submit_correction_part(
            "fix", "AAA", 60_000, 120_000, 1, 0,
            [{"symbol": "AAA", "ts": 60_500, "price": 10, "size": 1}],
        )
        aaa = {c["window_start"]: c for c in _candles(service, "AAA")}
        assert _ohlcv(aaa[0]) == (5.0, 5.0, 5.0, 5.0, 7.0, 1)
        assert _ohlcv(aaa[60_000]) == (10.0, 10.0, 10.0, 10.0, 1.0, 1)
        assert _ohlcv(aaa[120_000]) == (6.0, 6.0, 6.0, 6.0, 8.0, 1)
        bbb = _candles(service, "BBB")[0]
        assert _ohlcv(bbb) == (200.0, 200.0, 200.0, 200.0, 200.0, 1)

    def test_cross_boundary_window_partially_replaced(self, service, request):
        # 同一个窗口 [0,60000) 内：1000 在区间外、59000 在区间内
        service.ingest_batch("base", [
            {"symbol": "AAA", "ts": 1_000, "price": 10, "size": 10},
            {"symbol": "AAA", "ts": 59_000, "price": 20, "size": 20},
        ])
        request.node._log_case(
            input={"range": [59_000, 120_000], "window": (0, 60000)},
            rule="跨界窗口只替换落在区间内的点位，ts=1000 保留参与同一窗口聚合",
        )
        service.submit_correction_part(
            "fix", "AAA", 59_000, 120_000, 1, 0,
            [{"symbol": "AAA", "ts": 59_500, "price": 30, "size": 30}],
        )
        c = _candles(service, end=120_000)[0]
        # 窗口内有效点位：1000@10(size10) 与 59500@30(size30)
        assert _ohlcv(c) == (10.0, 30.0, 10.0, 30.0, 40.0, 2)

    def test_empty_correction_wipes_range(self, service, request):
        service.ingest_batch("wrong", [
            {"symbol": "AAA", "ts": 60_000, "price": 10, "size": 10},
            {"symbol": "AAA", "ts": 120_500, "price": 20, "size": 20},
        ])
        request.node._log_case(
            input={"range": [60_000, 120_000], "records": []},
            rule="空修正集合合法：区间被清空（该窗口 K 线删除），区间外窗口保留",
        )
        r = service.submit_correction_part(
            "wipe", "AAA", 60_000, 120_000, 1, 0, []
        )
        assert r["status"] == "applied" and r["accepted"] == 0
        cs = _candles(service)
        assert [c["window_start"] for c in cs] == [120_000]


# 同一份修正集合（点位内容相同），用不同顺序、不同拆分方式提交，
# 最终 K 线必须完全一致。
_CORRECTION_SET = [
    {"symbol": "AAA", "ts": 60_000, "price": 10.0, "size": 2.0},
    {"symbol": "AAA", "ts": 60_000, "price": 10.0, "size": 5.0},
    {"symbol": "AAA", "ts": 60_999, "price": 13.0, "size": 1.0},
    {"symbol": "AAA", "ts": 60_500, "price": 9.0, "size": 4.0},
    {"symbol": "AAA", "ts": 120_100, "price": 8.0, "size": 6.0},
]
_EXPECTED = {
    60_000: (10.0, 13.0, 9.0, 13.0, 12.0, 4),
    120_000: (8.0, 8.0, 8.0, 8.0, 6.0, 1),
}


def _seed_wrong_history(svc):
    svc.ingest_batch("wrong-history", [
        {"symbol": "AAA", "ts": 60_100, "price": 99, "size": 999},
        {"symbol": "AAA", "ts": 120_200, "price": 88, "size": 888},
        {"symbol": "BBB", "ts": 60_100, "price": 1, "size": 1},
        {"symbol": "AAA", "ts": 59_000, "price": 3, "size": 3},
    ])


@pytest.mark.parametrize("variant", [0, 1, 2, 3])
def test_same_set_different_order_and_split(db_path_factory, variant, request):
    from app.service import TickService
    from app.storage import TickStore

    svc = TickService(TickStore(db_path_factory(f"corr-order-{variant}.db")))
    _seed_wrong_history(svc)

    # 变体：0=单片顺序；1=单片逆序；2=两片乱序到达；3=三片乱序到达
    if variant == 0:
        plan = [(0, 1, _CORRECTION_SET)]
        arrival = [0]
    elif variant == 1:
        plan = [(0, 1, list(reversed(_CORRECTION_SET)))]
        arrival = [0]
    elif variant == 2:
        plan = [
            (0, 2, _CORRECTION_SET[:2]),
            (1, 2, _CORRECTION_SET[2:]),
        ]
        arrival = [1, 0]  # 后片先到
    else:
        plan = [
            (0, 3, _CORRECTION_SET[:1]),
            (1, 3, _CORRECTION_SET[1:3]),
            (2, 3, _CORRECTION_SET[3:]),
        ]
        arrival = [2, 0, 1]

    request.node._log_case(
        input={"variant": variant, "arrival_order": arrival},
        rule="同一份修正集合换序/拆批，最终两根 K 线 OHLCV 完全一致",
    )
    statuses = []
    for idx in arrival:
        part_index, total_parts, recs = plan[idx]
        r = svc.submit_correction_part(
            f"corr-v{variant}", "AAA", 60_000, 180_000,
            total_parts, part_index, recs,
        )
        statuses.append(r["status"])
    assert statuses[-1] == "applied"
    assert statuses.count("applied") == 1

    cs = {c["window_start"]: c for c in _candles(svc, end=180_000)}
    for ws, expected in _EXPECTED.items():
        assert _ohlcv(cs[ws]) == expected
    # 区间外的 BBB 与 ts=59000 的窗口不受影响
    assert _ohlcv(_candles(svc, "BBB")[0]) == (1.0, 1.0, 1.0, 1.0, 1.0, 1)
    assert _ohlcv(cs[0]) == (3.0, 3.0, 3.0, 3.0, 3.0, 1)
    assert svc.verify_symbol("AAA")["ok"]


class TestCorrectionIdempotency:
    def _apply_once(self, svc, cid="fix", records=None):
        records = records or [
            {"symbol": "AAA", "ts": 60_100, "price": 10, "size": 1},
            {"symbol": "AAA", "ts": 60_200, "price": 11, "size": 2},
        ]
        return svc.submit_correction_part(
            cid, "AAA", 60_000, 120_000, 1, 0, records
        )

    def test_duplicate_submission_applies_once(self, service, request):
        service.ingest_batch("wrong", [
            {"symbol": "AAA", "ts": 60_100, "price": 99, "size": 999}
        ])
        recs = [
            {"symbol": "AAA", "ts": 60_100, "price": 10, "size": 1},
            {"symbol": "AAA", "ts": 60_200, "price": 11, "size": 2},
        ]
        request.node._log_case(
            input={"submissions": 3, "records": recs},
            rule="同一修正重复/重试提交只生效一次，volume 不滚动累加",
        )
        first = self._apply_once(service, "fix", recs)
        assert first["status"] == "applied"
        for _ in range(2):
            again = self._apply_once(service, "fix", recs)
            assert again["status"] == "duplicate"
            assert again["affected_windows"] == []
        c = _candles(service)[0]
        assert _ohlcv(c) == (10.0, 11.0, 10.0, 11.0, 3.0, 2)

    def test_duplicate_record_order_is_same_payload(self, service, request):
        request.node._log_case(
            input={"replay": "records 顺序打乱"},
            rule="同一份修正点位书写顺序不同视为同一负载，仍为 duplicate",
        )
        recs = [
            {"symbol": "AAA", "ts": 60_200, "price": 11, "size": 2},
            {"symbol": "AAA", "ts": 60_100, "price": 10, "size": 1},
        ]
        first = self._apply_once(service, "fix")  # 先按正序提交
        assert first["status"] == "applied"
        # 用逆序负载重放：单片指纹只依赖内容集合
        r = service.submit_correction_part(
            "fix", "AAA", 60_000, 120_000, 1, 0, recs
        )
        assert r["status"] == "duplicate"

    def test_conflicting_payload_returns_409(self, service, request):
        from app.errors import CorrectionConflictError

        self._apply_once(service, "fix")
        changed = [{"symbol": "AAA", "ts": 60_100, "price": 10, "size": 9}]
        request.node._log_case(
            input={"first_volume": 3, "conflict_tick": changed[0]},
            rule="同 correction_id 不同负载 -> 409，已生效结果不变",
        )
        with pytest.raises(CorrectionConflictError):
            service.submit_correction_part(
                "fix", "AAA", 60_000, 120_000, 1, 0, changed
            )
        assert _ohlcv(_candles(service)[0]) == (10.0, 11.0, 10.0, 11.0, 3.0, 2)

    def test_conflicting_range_metadata_returns_409(self, service, request):
        from app.errors import CorrectionConflictError

        rec = [{"symbol": "AAA", "ts": 60_100, "price": 10, "size": 1}]
        self._apply_once(service, "fix", rec)
        request.node._log_case(
            input={"first_range": [60_000, 120_000], "second_range": [60_000, 121_000]},
            rule="同 correction_id 的 end 元数据不一致 -> 409",
        )
        with pytest.raises(CorrectionConflictError):
            service.submit_correction_part(
                "fix", "AAA", 60_000, 121_000, 1, 0, rec
            )

    def test_partial_stages_then_duplicate_then_apply(self, service, request):
        r1 = service.submit_correction_part(
            "fix", "AAA", 60_000, 120_000, 2, 0,
            [{"symbol": "AAA", "ts": 60_100, "price": 10, "size": 1}],
        )
        assert r1["status"] == "pending"
        request.node._log_case(
            input={"parts": 2, "replay": "part0 重发后再发 part1"},
            rule="未到齐时重发已暂存分片返回 duplicate 且不重复暂存",
        )
        r1b = service.submit_correction_part(
            "fix", "AAA", 60_000, 120_000, 2, 0,
            [{"symbol": "AAA", "ts": 60_100, "price": 10, "size": 1}],
        )
        assert r1b["status"] == "duplicate"
        r2 = service.submit_correction_part(
            "fix", "AAA", 60_000, 120_000, 2, 1,
            [{"symbol": "AAA", "ts": 60_200, "price": 11, "size": 2}],
        )
        assert r2["status"] == "applied"
        assert _ohlcv(_candles(service)[0]) == (10.0, 11.0, 10.0, 11.0, 3.0, 2)

    def test_cross_part_duplicate_rejected(self, service, request):
        from app.errors import InvalidBatchError

        service.submit_correction_part(
            "fix", "AAA", 60_000, 120_000, 2, 0,
            [{"symbol": "AAA", "ts": 60_100, "price": 10, "size": 1}],
        )
        request.node._log_case(
            input={"same_tick_in": ["part0", "part1"]},
            rule="不同分片间完全相同点位 -> duplicate_tick_in_correction，整本片拒绝",
        )
        with pytest.raises(InvalidBatchError) as exc:
            service.submit_correction_part(
                "fix", "AAA", 60_000, 120_000, 2, 1,
                [{"symbol": "AAA", "ts": 60_100, "price": 10, "size": 1}],
            )
        assert exc.value.rejected_records[0].reason == "duplicate_tick_in_correction"
        # 修正仍处于 pending，旧数据未被改动
        assert service.store.get_correction("fix").status == "pending"

    def test_correction_id_clashes_with_ingest_batch(self, service, request):
        from app.errors import BatchConflictError, CorrectionConflictError

        service.ingest_batch("shared-id", [
            {"symbol": "AAA", "ts": 60_100, "price": 10, "size": 1},
        ])
        request.node._log_case(
            input={"id": "shared-id"},
            rule="correction_id 与普通 batch_id 重名 -> 409，互不污染",
        )
        with pytest.raises(CorrectionConflictError):
            service.submit_correction_part(
                "shared-id", "AAA", 60_000, 120_000, 1, 0,
                [{"symbol": "AAA", "ts": 60_500, "price": 11, "size": 1}],
            )
        # 反向：先修正后用同名普通批次，也应 409
        service.submit_correction_part(
            "corr-only", "AAA", 60_000, 120_000, 1, 0,
            [{"symbol": "AAA", "ts": 60_500, "price": 11, "size": 1}],
        )
        with pytest.raises(BatchConflictError):
            service.ingest_batch("corr-only", [
                {"symbol": "AAA", "ts": 60_600, "price": 12, "size": 1},
            ])


class TestCorrectionRollback:
    def test_invalid_part_leaves_no_trace(self, service, request):
        service.ingest_batch("base", [
            {"symbol": "AAA", "ts": 60_100, "price": 10, "size": 5}
        ])
        bad = [
            {"symbol": "AAA", "ts": 60_200, "price": 11, "size": 1},
            {"symbol": "AAA", "ts": 60_300, "price": 12, "size": -1},
        ]
        request.node._log_case(
            input={"bad_part": bad},
            rule="含无效点位 -> 修正头/暂存/取代全部不落库，可用同 id 安全重试",
        )
        from app.errors import InvalidBatchError

        with pytest.raises(InvalidBatchError):
            service.submit_correction_part(
                "fix", "AAA", 60_000, 120_000, 1, 0, bad
            )
        assert service.store.get_correction("fix") is None
        assert service.store.fetch_staging("fix") == []
        assert _ohlcv(_candles(service)[0]) == (10.0, 10.0, 10.0, 10.0, 5.0, 1)

        ok = service.submit_correction_part(
            "fix", "AAA", 60_000, 120_000, 1, 0,
            [{"symbol": "AAA", "ts": 60_200, "price": 11, "size": 1}],
        )
        assert ok["status"] == "applied"
        assert _ohlcv(_candles(service)[0]) == (11.0, 11.0, 11.0, 11.0, 1.0, 1)

    def test_staged_parts_survive_failed_late_part(self, service, request):
        service.submit_correction_part(
            "fix", "AAA", 60_000, 120_000, 2, 0,
            [{"symbol": "AAA", "ts": 60_100, "price": 10, "size": 1}],
        )
        request.node._log_case(
            input={"late_part": "越界点位被拒后重发合规末片"},
            rule="末片失败不影响已暂存分片；重发合规末片后整体生效",
        )
        from app.errors import InvalidBatchError

        with pytest.raises(InvalidBatchError):
            service.submit_correction_part(
                "fix", "AAA", 60_000, 120_000, 2, 1,
                [{"symbol": "AAA", "ts": 120_000, "price": 11, "size": 1}],
            )
        assert service.store.get_correction("fix").status == "pending"
        ok = service.submit_correction_part(
            "fix", "AAA", 60_000, 120_000, 2, 1,
            [{"symbol": "AAA", "ts": 60_900, "price": 11, "size": 1}],
        )
        assert ok["status"] == "applied"

    def test_storage_commit_failure_rolls_back(self, service, monkeypatch, request):
        service.ingest_batch("base", [
            {"symbol": "AAA", "ts": 60_100, "price": 10, "size": 5},
        ])
        request.node._log_case(
            input={"inject": "窗口重算阶段抛出异常"},
            rule="提交中途异常 -> 整体回滚：无新批次、无新点位、旧点位不被取代",
        )

        def boom(*_a, **_kw):
            raise RuntimeError("injected failure during recompute")

        monkeypatch.setattr(service.store, "_recompute_windows_locked", boom)
        with pytest.raises(RuntimeError):
            service.submit_correction_part(
                "fix3", "AAA", 60_000, 120_000, 1, 0,
                [{"symbol": "AAA", "ts": 60_700, "price": 14, "size": 4}],
            )
        monkeypatch.undo()
        assert service.store.get_batch("fix3") is None
        # 旧点位仍为 active，K 线不变
        assert _ohlcv(_candles(service)[0]) == (10.0, 10.0, 10.0, 10.0, 5.0, 1)
        assert service.verify_symbol("AAA")["ok"]


class TestCorrectionConcurrency:
    def test_same_correction_parallel_last_part(self, service, request):
        # 两片已暂存，最后一片由多个线程并发重发相同负载：只生效一次
        recs0 = [{"symbol": "AAA", "ts": 60_100, "price": 10, "size": 1}]
        recs1 = [{"symbol": "AAA", "ts": 60_200, "price": 11, "size": 2}]
        service.submit_correction_part(
            "fix", "AAA", 60_000, 120_000, 2, 0, recs0
        )
        from concurrent.futures import ThreadPoolExecutor

        request.node._log_case(
            input={"threads": 6, "part": 1},
            rule="并发送交相同末片：仅一次 applied，其余 duplicate，volume=3",
        )

        def call(_):
            return service.submit_correction_part(
                "fix", "AAA", 60_000, 120_000, 2, 1, recs1
            )["status"]

        with ThreadPoolExecutor(max_workers=6) as ex:
            statuses = list(ex.map(call, range(6)))
        assert statuses.count("applied") == 1
        assert statuses.count("duplicate") == 5
        assert _ohlcv(_candles(service)[0]) == (10.0, 11.0, 10.0, 11.0, 3.0, 2)

    def test_correction_interleaved_with_normal_ingest(self, service, request):
        from concurrent.futures import ThreadPoolExecutor

        # 区间内做一次整体替换，同时对区间外（180000 窗口）做普通写入，
        # 二者不得互相覆盖：修正只动区间窗口，普通写入只动区间外窗口。
        service.ingest_batch("old", [
            {"symbol": "AAA", "ts": 60_100, "price": 99, "size": 999},
        ])

        def do_correction():
            return service.submit_correction_part(
                "fix", "AAA", 60_000, 120_000, 1, 0,
                [{"symbol": "AAA", "ts": 60_500, "price": 10, "size": 1}],
            )["status"]

        def do_ingest(i):
            return service.ingest_batch(
                f"normal-{i}",
                [{"symbol": "AAA", "ts": 180_000 + i, "price": 20, "size": 1}],
            )["status"]

        request.node._log_case(
            input={"correction_range": [60_000, 120_000], "normal_writes_window": 180_000},
            rule="修正与普通写入并发：区间窗口=修正结果，区间外窗口=普通写入累加",
        )
        with ThreadPoolExecutor(max_workers=6) as ex:
            f_corr = ex.submit(do_correction)
            f_normals = [ex.submit(do_ingest, i) for i in range(5)]
            corr_status = f_corr.result()
            normal_statuses = [f.result() for f in f_normals]
        assert corr_status == "applied"
        assert normal_statuses == ["accepted"] * 5
        cs = {c["window_start"]: c for c in _candles(service, end=240_000)}
        assert _ohlcv(cs[60_000]) == (10.0, 10.0, 10.0, 10.0, 1.0, 1)
        assert _ohlcv(cs[180_000]) == (20.0, 20.0, 20.0, 20.0, 5.0, 5)
        assert service.verify_symbol("AAA")["ok"]

    def test_two_overlapping_corrections_serialize(self, service, request):
        from concurrent.futures import ThreadPoolExecutor

        # 两个重叠区间的修正并发提交，必须串行化、各自只生效一次、
        # 最终结果等于按某一顺序先后应用的确定结果（后提交者覆盖重叠区间）。
        service.ingest_batch("old", [
            {"symbol": "AAA", "ts": 60_100, "price": 99, "size": 999},
            {"symbol": "AAA", "ts": 120_100, "price": 88, "size": 888},
        ])
        request.node._log_case(
            input={"ranges": [[60_000, 120_000], [60_000, 180_000]]},
            rule="重叠修正并发不出现半改状态；最终可对账，两个修正均只生效一次",
        )

        def corr(cid, start, end, ts, price):
            return service.submit_correction_part(
                cid, "AAA", start, end, 1, 0,
                [{"symbol": "AAA", "ts": ts, "price": price, "size": 1}],
            )["status"]

        with ThreadPoolExecutor(max_workers=2) as ex:
            f1 = ex.submit(corr, "fix-a", 60_000, 120_000, 60_500, 10)
            f2 = ex.submit(corr, "fix-b", 60_000, 180_000, 120_500, 20)
            s1, s2 = f1.result(), f2.result()
        assert {s1, s2} == {"applied"}
        # 两个修正都在 batches 中只出现一次
        n = service.store._conn.execute(
            "SELECT COUNT(*) n FROM batches WHERE kind='correction'"
        ).fetchone()["n"]
        assert n == 2
        assert service.verify_symbol("AAA")["ok"]
        cs = {c["window_start"]: c for c in _candles(service, end=180_000)}
        # fix-a 的点 (60500@10) 落在 fix-b 的区间内，被 fix-b 整体替换掉；
        # fix-b 只携带 120500@20，故 60000 窗口为空、120000 窗口=20
        assert 60_000 not in cs
        assert _ohlcv(cs[120_000]) == (20.0, 20.0, 20.0, 20.0, 1.0, 1)

    def test_same_id_concurrent_different_metadata_conflicts(
        self, service, request
    ):
        from concurrent.futures import ThreadPoolExecutor
        from app.errors import CorrectionConflictError

        # 同一 correction_id 首片并发、但区间元数据不同：只允许一种元数据占用，
        # 另一个必须 409，不能把分片挂到错误的修正头上。
        request.node._log_case(
            input={"ranges": [[60_000, 120_000], [60_000, 180_000]]},
            rule="同名并发不同元数据 -> 一个 applied，另一个 409",
        )
        outcomes: list[str] = []

        def call(end):
            try:
                r = service.submit_correction_part(
                    "fix", "AAA", 60_000, end, 1, 0,
                    [{"symbol": "AAA", "ts": 60_500, "price": 10, "size": 1}],
                )
                return r["status"]
            except CorrectionConflictError:
                return "conflict"

        with ThreadPoolExecutor(max_workers=2) as ex:
            futs = [ex.submit(call, 120_000), ex.submit(call, 180_000)]
            outcomes = [f.result() for f in futs]
        assert outcomes.count("applied") == 1
        assert outcomes.count("conflict") == 1
        head = service.store.get_correction("fix")
        assert head.status == "applied"
        assert service.verify_symbol("AAA")["ok"]


class TestRepairBackfillRebuildConsistency:
    def test_correction_then_backfill_then_rebuild_matches_recompute(
        self, service, request
    ):
        # 1) 错误历史  2) 区间替换修正  3) 区间外补写  4) rebuild
        service.ingest_batch("wrong", [
            {"symbol": "AAA", "ts": 60_100, "price": 99, "size": 999},
            {"symbol": "AAA", "ts": 120_100, "price": 98, "size": 998},
        ])
        service.submit_correction_part(
            "fix", "AAA", 60_000, 180_000, 1, 0,
            [
                {"symbol": "AAA", "ts": 60_100, "price": 10, "size": 2},
                {"symbol": "AAA", "ts": 60_500, "price": 12, "size": 3},
                {"symbol": "AAA", "ts": 120_100, "price": 11, "size": 6},
            ],
        )
        # 区间外补写（180000 窗口新增；59000 若存在则跨边界保留）
        service.ingest_batch("backfill", [
            {"symbol": "AAA", "ts": 180_100, "price": 30, "size": 5},
        ])
        request.node._log_case(
            input={"steps": ["wrong ingest", "correction [60000,180000)",
                             "backfill outside", "rebuild"]},
            rule="修正/补写/重建后结果 == 只按当前有效点位重算的结果",
        )
        before = service.query_candles("AAA", 0, 240_000)["candles"]
        service.rebuild_symbol("AAA")
        after = service.query_candles("AAA", 0, 240_000)["candles"]
        assert before == after
        # 直接按有效点位独立重算的期望值
        by_ws = {c["window_start"]: _ohlcv(c) for c in after}
        assert by_ws[60_000] == (10.0, 12.0, 10.0, 12.0, 5.0, 2)
        assert by_ws[120_000] == (11.0, 11.0, 11.0, 11.0, 6.0, 1)
        assert by_ws[180_000] == (30.0, 30.0, 30.0, 30.0, 5.0, 1)
        assert service.verify_symbol("AAA")["ok"]

    def test_replay_everything_in_new_db_matches(self, db_path_factory, request):
        """从空库重放「普通批次 + 修正」事件日志，结果应与当前库一致。"""
        from app.service import TickService
        from app.storage import TickStore

        svc = TickService(TickStore(db_path_factory("primary.db")))
        svc.ingest_batch("b1", [
            {"symbol": "AAA", "ts": 60_100, "price": 99, "size": 999},
            {"symbol": "AAA", "ts": 120_100, "price": 88, "size": 888},
        ])
        svc.submit_correction_part(
            "c1", "AAA", 60_000, 120_000, 1, 0,
            [{"symbol": "AAA", "ts": 60_500, "price": 10, "size": 7}],
        )
        svc.ingest_batch("b2", [
            {"symbol": "AAA", "ts": 180_100, "price": 30, "size": 5},
        ])
        primary = svc.query_candles("AAA", 0, 240_000)["candles"]

        request.node._log_case(
            input={"events": ["ingest b1", "correction c1", "ingest b2"]},
            rule="在全新库按事件顺序重放（含修正幂等重放）K 线完全一致",
        )
        svc2 = TickService(TickStore(db_path_factory("replica.db")))
        svc2.ingest_batch("b1", [
            {"symbol": "AAA", "ts": 60_100, "price": 99, "size": 999},
            {"symbol": "AAA", "ts": 120_100, "price": 88, "size": 888},
        ])
        # 先重放一次修正，再幂等重放一次，结果必须相同
        corr = [{"symbol": "AAA", "ts": 60_500, "price": 10, "size": 7}]
        assert svc2.submit_correction_part(
            "c1", "AAA", 60_000, 120_000, 1, 0, corr
        )["status"] == "applied"
        assert svc2.submit_correction_part(
            "c1", "AAA", 60_000, 120_000, 1, 0,
            list(reversed(corr)),
        )["status"] == "duplicate"
        svc2.ingest_batch("b2", [
            {"symbol": "AAA", "ts": 180_100, "price": 30, "size": 5},
        ])
        replica = svc2.query_candles("AAA", 0, 240_000)["candles"]
        assert replica == primary
        assert svc2.verify_symbol("AAA")["ok"]


class TestVerify:
    def test_verify_ok_after_normal_flow(self, service, request):
        service.ingest_batch("b", [
            {"symbol": "AAA", "ts": 60_100, "price": 10, "size": 2},
            {"symbol": "AAA", "ts": 60_500, "price": 12, "size": 3},
        ])
        request.node._log_case(input={"action": "verify 正常库"}, rule="ok=True，检查窗口数=1")
        v = service.verify_symbol("AAA")
        assert v["ok"] and v["checked_windows"] == 1 and v["mismatches"] == []

    def test_verify_detects_tampered_candle(self, service, request):
        service.ingest_batch("b", [
            {"symbol": "AAA", "ts": 60_100, "price": 10, "size": 2},
        ])
        request.node._log_case(
            input={"tamper": "直接改 candles.volume=999"},
            rule="verify 必须发现 value_mismatch，并给出 stored/expected",
        )
        service.store._conn.execute(
            "UPDATE candles SET volume=999 WHERE symbol='AAA'"
        )
        v = service.verify_symbol("AAA")
        assert v["ok"] is False and len(v["mismatches"]) == 1
        m = v["mismatches"][0]
        assert m["kind"] == "value_mismatch"
        assert m["stored"]["volume"] == 999.0
        assert m["expected"]["volume"] == 2.0

    def test_verify_detects_stale_and_missing(self, service, request):
        service.ingest_batch("b", [
            {"symbol": "AAA", "ts": 60_100, "price": 10, "size": 1},
            {"symbol": "AAA", "ts": 120_100, "price": 11, "size": 1},
        ])
        request.node._log_case(
            input={"tamper": "删除一根 K 线 + 凭空插一根 K 线"},
            rule="verify 分别识别 missing_candle 与 stale_candle",
        )
        conn = service.store._conn
        conn.execute("DELETE FROM candles WHERE window_start=60000")
        conn.execute(
            "INSERT INTO candles VALUES ('AAA', 180000, 1,1,1,1,1,1)"
        )
        v = service.verify_symbol("AAA")
        kinds = {m["window_start"]: m["kind"] for m in v["mismatches"]}
        assert kinds[60_000] == "missing_candle"
        assert kinds[180_000] == "stale_candle"
        assert v["checked_windows"] == 3
