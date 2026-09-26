"""领域服务测试：校验分类、幂等、乱序/迟到、回滚重试、顺序无关性。"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

import pytest

from app.errors import BatchConflictError, InvalidBatchError
from app.models import MAX_BATCH_RECORDS


def _candle(svc, symbol="AAA", start=0, end=240_000):
    return svc.query_candles(symbol, start, end)["candles"]


class TestValidation:
    @pytest.mark.parametrize(
        "record,reason",
        [
            (["not", "dict"], "not_an_object"),
            ({"ts": 1, "price": 1.0, "size": 1.0}, "missing_symbol"),
            ({"symbol": "", "ts": 1, "price": 1.0, "size": 1.0}, "invalid_symbol"),
            ({"symbol": "x" * 65, "ts": 1, "price": 1.0, "size": 1.0}, "invalid_symbol"),
            ({"symbol": "AAA", "price": 1.0, "size": 1.0}, "missing_timestamp"),
            ({"symbol": "AAA", "ts": -1, "price": 1.0, "size": 1.0}, "invalid_timestamp"),
            ({"symbol": "AAA", "ts": "1", "price": 1.0, "size": 1.0}, "invalid_timestamp"),
            ({"symbol": "AAA", "ts": 1, "size": 1.0}, "missing_price"),
            ({"symbol": "AAA", "ts": 1, "price": 0, "size": 1.0}, "invalid_price"),
            ({"symbol": "AAA", "ts": 1, "price": float("inf"), "size": 1.0}, "invalid_price"),
            ({"symbol": "AAA", "ts": 1, "price": 1.0}, "missing_size"),
            ({"symbol": "AAA", "ts": 1, "price": 1.0, "size": -0.1}, "invalid_size"),
        ],
    )
    def test_invalid_records_classified(self, service, request, record, reason):
        request.node._log_case(input=record, rule=f"预期判定原因 {reason}")
        with pytest.raises(InvalidBatchError) as exc:
            service.ingest_batch(f"bad-{reason}", [record])
        assert exc.value.error_code == "invalid_records"
        assert exc.value.rejected_records[0].reason == reason

    def test_invalid_batch_rolls_back_entirely(self, service, request):
        records = [
            {"symbol": "AAA", "ts": 60_000, "price": 10, "size": 1},
            {"symbol": "AAA", "ts": 60_100, "price": 10, "size": -1},
        ]
        request.node._log_case(
            input=records, rule="含 1 条无效 -> 整批 422，批次与 ticks 均不得落库"
        )
        with pytest.raises(InvalidBatchError):
            service.ingest_batch("rb1", records)
        assert service.store.get_batch("rb1") is None
        assert _candle(service) == []

    def test_empty_and_oversized_batch(self, service, request):
        request.node._log_case(input={"empty": [], "oversize": MAX_BATCH_RECORDS + 1},
                               rule="空批次/超长批次使用独立批次级错误码")
        with pytest.raises(InvalidBatchError) as e1:
            service.ingest_batch("e", [])
        assert e1.value.error_code == "empty_batch"
        with pytest.raises(InvalidBatchError) as e2:
            service.ingest_batch("big", [{"x": 1}] * (MAX_BATCH_RECORDS + 1))
        assert e2.value.error_code == "batch_too_large"

    def test_invalid_batch_id(self, service):
        with pytest.raises(InvalidBatchError) as e:
            service.ingest_batch("   ", [{"symbol": "AAA", "ts": 1, "price": 1, "size": 1}])
        assert e.value.error_code == "invalid_batch_id"

    def test_duplicate_tick_within_batch_rejected(self, service, request):
        rec = {"symbol": "AAA", "ts": 60_000, "price": 10, "size": 1}
        request.node._log_case(input=[rec, rec], rule="批内完全相同点位判重并整批拒绝")
        with pytest.raises(InvalidBatchError) as e:
            service.ingest_batch("dup-in-batch", [dict(rec), dict(rec)])
        assert e.value.rejected_records[0].reason == "duplicate_tick_in_batch"


class TestIdempotency:
    def test_duplicate_batch_is_idempotent(self, service, request):
        recs = [{"symbol": "AAA", "ts": 60_000, "price": 10, "size": 2}]
        request.node._log_case(input=recs, rule="同 batch_id 同负载重放返回 duplicate，不累计")
        first = service.ingest_batch("id1", recs)
        second = service.ingest_batch("id1", recs)
        assert first["status"] == "accepted"
        assert second["status"] == "duplicate"
        assert second["accepted"] == 1 and second["affected_windows"] == []
        c = _candle(service)
        assert c[0]["volume"] == 2 and c[0]["tick_count"] == 1

    def test_conflicting_payload_raises_409(self, service, request):
        r1 = [{"symbol": "AAA", "ts": 60_000, "price": 10, "size": 2}]
        r2 = [{"symbol": "AAA", "ts": 60_000, "price": 11, "size": 2}]
        request.node._log_case(input={"first": r1, "second": r2},
                               rule="同 batch_id 不同负载 -> 409，首写保持不变")
        service.ingest_batch("cf1", r1)
        with pytest.raises(BatchConflictError):
            service.ingest_batch("cf1", r2)
        assert _candle(service)[0]["open"] == 10

    def test_failed_then_retried_batch_succeeds_once(self, service, request):
        recs = [
            {"symbol": "AAA", "ts": 60_000, "price": 10, "size": 1},
            {"symbol": "AAA", "ts": 60_100, "price": 11, "size": 1},
        ]
        request.node._log_case(
            input={"attempt1": "无效版本", "attempt2": "修正后同 batch_id 重试"},
            rule="失败回滚不落库；同一 batch_id 修正后可正常成功且只计一次",
        )
        bad = [dict(recs[0]), {"symbol": "AAA", "ts": 60_100, "price": 11, "size": "x"}]
        with pytest.raises(InvalidBatchError):
            service.ingest_batch("retry1", bad)
        ok = service.ingest_batch("retry1", recs)
        assert ok["status"] == "accepted"
        assert _candle(service)[0]["tick_count"] == 2


class TestOrderIndependence:
    def test_out_of_order_and_late_ticks(self, service, request):
        # 先写晚一分钟，再迟到写早一分钟；窗口独立，且 OHLC 由确定性全序决定
        request.node._log_case(
            input={"late": [(60_000, 10, 2)], "early_seen": [(120_000, 11, 3)]},
            rule="迟到点位正常入库并重建窗口；窗口归属 floor(ts/60000)",
        )
        service.ingest_batch("late1", [{"symbol": "AAA", "ts": 120_000, "price": 11, "size": 3}])
        service.ingest_batch("late0", [{"symbol": "AAA", "ts": 60_000, "price": 10, "size": 2}])
        cs = _candle(service)
        assert [c["window_start"] for c in cs] == [60_000, 120_000]

    @pytest.mark.parametrize("variant", [0, 1, 2])
    def test_same_set_different_arrival_order(self, db_path_factory, variant, request):
        dataset = [
            ("b-a", {"symbol": "AAA", "ts": 60_000, "price": 10.0, "size": 2.0}),
            ("b-b", {"symbol": "AAA", "ts": 60_000, "price": 10.0, "size": 5.0}),
            ("b-c", {"symbol": "AAA", "ts": 60_999, "price": 13.0, "size": 1.0}),
            ("b-d", {"symbol": "AAA", "ts": 60_500, "price": 9.0, "size": 4.0}),
        ]
        orders = [
            dataset,
            list(reversed(dataset)),
            [dataset[2], dataset[0], dataset[3], dataset[1]],
        ]
        request.node._log_case(
            input={"variant": variant, "order": [b for b, _ in orders[variant]]},
            rule="同一输入集合，仅批次到达顺序不同 -> OHLCV 完全一致",
        )
        from app.storage import TickStore
        from app.service import TickService

        svc = TickService(TickStore(db_path_factory(f"order-{variant}.db")))
        for batch_id, rec in orders[variant]:
            svc.ingest_batch(batch_id, [rec])
        c = svc.query_candles("AAA", 0, 120_000)["candles"]
        assert len(c) == 1
        c = c[0]
        # open：窗口全序第一条（60000ms 同价时 size 小者优先）；
        # close：窗口全序最后一条（60999@13）。决胜与到达顺序无关。
        assert (c["open"], c["high"], c["low"], c["close"], c["volume"],
                c["tick_count"]) == (10.0, 13.0, 9.0, 13.0, 12.0, 4)

    def test_replay_after_rebuild_matches(self, service, request):
        recs1 = [{"symbol": "AAA", "ts": 60_000, "price": 10, "size": 2}]
        recs2 = [{"symbol": "AAA", "ts": 60_500, "price": 12, "size": 1}]
        service.ingest_batch("p1", recs1)
        service.ingest_batch("p2", recs2)
        request.node._log_case(
            input={"batches": ["p1", "p2"]},
            rule="重建后 K 线与重放全部批次的增量结果一致",
        )
        service.rebuild_symbol("AAA")
        c = _candle(service)[0]
        assert (c["open"], c["high"], c["low"], c["close"], c["volume"]) == (
            10.0, 12.0, 10.0, 12.0, 3.0
        )


class TestQuery:
    def test_range_boundaries(self, service, request):
        service.ingest_batch("q1", [
            {"symbol": "AAA", "ts": 0, "price": 1, "size": 1},
            {"symbol": "AAA", "ts": 60_000, "price": 2, "size": 1},
            {"symbol": "AAA", "ts": 120_000, "price": 3, "size": 1},
        ])
        request.node._log_case(
            input={"start": 60_000, "end": 60_000},
            rule="按窗口过滤：包含 start 所在窗口与 end 所在窗口",
        )
        cs = service.query_candles("AAA", 60_000, 60_000)["candles"]
        assert [x["window_start"] for x in cs] == [60_000]

    def test_invalid_range(self, service):
        with pytest.raises(InvalidBatchError) as e:
            service.query_candles("AAA", 100, 50)
        assert e.value.error_code == "invalid_range"


class TestServiceConcurrency:
    def test_same_batch_id_parallel_retry(self, service, request):
        recs = [{"symbol": "AAA", "ts": 60_000, "price": 10, "size": 1}]
        request.node._log_case(
            input={"batch_id": "same", "threads": 6},
            rule="同 batch_id 并发重试：仅一次 accepted，其余 duplicate，成交量为 1",
        )
        results: list[str] = []

        def call(_):
            return service.ingest_batch("same", recs)["status"]

        with ThreadPoolExecutor(max_workers=6) as ex:
            results = list(ex.map(call, range(6)))
        assert sorted(results).count("accepted") == 1
        assert results.count("duplicate") == 5
        assert _candle(service)[0]["volume"] == 1.0
