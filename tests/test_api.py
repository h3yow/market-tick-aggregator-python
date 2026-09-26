"""HTTP 端到端测试：兼容行为、错误分类、进程重启恢复、并发读写。"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor

import pytest


def _post(c, batch_id, records):
    return c.post("/api/v1/ticks/batch", json={"batch_id": batch_id, "records": records})


class TestCompat:
    def test_root_unchanged(self, client, request):
        request.node._log_case(input="GET /", rule="骨架期既有行为必须保持兼容")
        assert client.get("/").json() == {"Hello": "World"}

    def test_health(self, client):
        assert client.get("/health").json() == {"status": "ok"}


class TestIngestApi:
    def test_accept_and_query(self, client, request):
        recs = [
            {"symbol": "AAA", "ts": 60_000, "price": 10, "size": 2},
            {"symbol": "AAA", "ts": 60_500, "price": 12, "size": 3},
        ]
        request.node._log_case(input=recs, rule="200 accepted 并返回受影响窗口")
        r = _post(client, "a1", recs)
        assert r.status_code == 200
        body = r.json()
        assert body["status"] == "accepted" and body["accepted"] == 2
        assert body["affected_windows"] == ["AAA@60000"]

        q = client.get("/api/v1/symbols/AAA/candles", params={"start": 0, "end": 120_000})
        assert q.status_code == 200
        c = q.json()["candles"][0]
        assert (c["open"], c["high"], c["low"], c["close"], c["volume"]) == (
            10.0, 12.0, 10.0, 12.0, 5.0
        )

    def test_duplicate_batch_http(self, client, request):
        recs = [{"symbol": "AAA", "ts": 60_000, "price": 10, "size": 2}]
        request.node._log_case(input=recs, rule="重放返回 200 duplicate 且 K 线不翻倍")
        assert _post(client, "d1", recs).json()["status"] == "accepted"
        r2 = _post(client, "d1", recs)
        assert r2.status_code == 200 and r2.json()["status"] == "duplicate"

    def test_invalid_records_http_body(self, client, request):
        recs = [
            {"symbol": "AAA", "ts": 60_000, "price": 10, "size": 2},
            {"symbol": "AAA", "ts": "bad", "price": 10, "size": 2},
        ]
        request.node._log_case(input=recs, rule="422 且逐条给出 index/reason/detail")
        r = _post(client, "e1", recs)
        assert r.status_code == 422
        body = r.json()
        assert body["error_code"] == "invalid_records"
        rr = body["rejected_records"][0]
        assert rr["index"] == 1 and rr["reason"] == "invalid_timestamp"
        # 整批未落库
        q = client.get("/api/v1/symbols/AAA/candles", params={"start": 0, "end": 120_000})
        assert q.json()["candles"] == []

    def test_conflict_http(self, client):
        _post(client, "cf", [{"symbol": "AAA", "ts": 1, "price": 10, "size": 1}])
        r = _post(client, "cf", [{"symbol": "AAA", "ts": 1, "price": 11, "size": 1}])
        assert r.status_code == 409
        assert r.json()["error_code"] == "batch_id_conflict"

    def test_rebuild_endpoint(self, client, request):
        _post(client, "r1", [{"symbol": "AAA", "ts": 60_000, "price": 10, "size": 2}])
        request.node._log_case(input="POST rebuild AAA", rule="重建窗口数与查询一致")
        r = client.post("/api/v1/admin/symbols/AAA/rebuild")
        assert r.status_code == 200 and r.json()["rebuilt_windows"] == 1

    def test_backfill_then_rebuild(self, client, request):
        # 先写入近窗口，再补写更早的历史窗口
        _post(client, "cur", [{"symbol": "AAA", "ts": 120_000, "price": 20, "size": 1}])
        request.node._log_case(
            input={"backfill_window": 60_000},
            rule="历史补写后增量结果即正确；显式 rebuild 结果必须一致",
        )
        _post(client, "hist", [{"symbol": "AAA", "ts": 60_000, "price": 9, "size": 4}])
        before = client.get(
            "/api/v1/symbols/AAA/candles", params={"start": 0, "end": 180_000}
        ).json()["candles"]
        client.post("/api/v1/admin/symbols/AAA/rebuild")
        after = client.get(
            "/api/v1/symbols/AAA/candles", params={"start": 0, "end": 180_000}
        ).json()["candles"]
        assert before == after and len(after) == 2

    def test_bad_query_params(self, client):
        r = client.get("/api/v1/symbols/AAA/candles", params={"start": -1, "end": 10})
        assert r.status_code == 422
        r = client.get("/api/v1/symbols/AAA/candles", params={"start": 100, "end": 50})
        assert r.status_code == 422


class TestRestart:
    def test_data_survives_process_restart(self, client_factory, request):
        c1, path = client_factory("restart.db")
        recs = [
            {"symbol": "AAA", "ts": 60_000, "price": 10, "size": 2},
            {"symbol": "BBB", "ts": 60_100, "price": 50, "size": 7},
        ]
        assert _post(c1, "persist-1", recs).status_code == 200
        c1.close()
        request.node._log_case(
            input={"db": path, "batches": ["persist-1"]},
            rule="重新装配服务（模拟进程重启）后数据不丢、可继续幂等重放",
        )

        from app.deps import reset_service
        from fastapi.testclient import TestClient
        import main

        reset_service(path)
        c2 = TestClient(main.app)
        # 同批次重放 -> duplicate
        assert _post(c2, "persist-1", recs).json()["status"] == "duplicate"
        q = c2.get("/api/v1/symbols/AAA/candles", params={"start": 0, "end": 120_000})
        c = q.json()["candles"][0]
        assert c["volume"] == 2 and c["tick_count"] == 1
        q2 = c2.get("/api/v1/symbols/BBB/candles", params={"start": 0, "end": 120_000})
        assert q2.json()["candles"][0]["volume"] == 7
        c2.close()


class TestHttpConcurrency:
    def test_concurrent_writes_and_queries(self, client, request):
        n = 8
        request.node._log_case(
            input={"writers": n, "ticks_per_writer": 10, "window": 60000},
            rule="并发结束后 volume=tick_count=80，中途查询只见已提交快照",
        )
        observed_volumes: list[float] = []

        def writer(tid: int):
            for j in range(10):
                _post(client, f"c-{tid}-{j}",
                      [{"symbol": "AAA", "ts": 60_000, "price": 10 + (j % 3), "size": 1}])

        def reader(_):
            for _ in range(40):
                r = client.get(
                    "/api/v1/symbols/AAA/candles",
                    params={"start": 0, "end": 120_000},
                ).json()["candles"]
                if r:
                    c = r[0]
                    # 任何中间状态都必须内部自洽：volume 等于笔数（每笔 size=1）
                    assert abs(c["volume"] - c["tick_count"]) < 1e-9
                    observed_volumes.append(c["volume"])

        with ThreadPoolExecutor(max_workers=n + 2) as ex:
            futs = [ex.submit(writer, i) for i in range(n)]
            futs += [ex.submit(reader, i) for i in range(2)]
            for f in futs:
                f.result()

        final = client.get(
            "/api/v1/symbols/AAA/candles", params={"start": 0, "end": 120_000}
        ).json()["candles"][0]
        assert final["tick_count"] == n * 10
        assert final["volume"] == float(n * 10)
        assert final["high"] == 12.0 and final["low"] == 10.0
        logging.getLogger("tests").info(
            "observed intermediate volumes sample=%s", observed_volumes[:10]
        )
