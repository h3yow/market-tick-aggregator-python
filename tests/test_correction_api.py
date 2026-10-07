"""区间整体替换式修正的 HTTP 端到端测试。"""

from __future__ import annotations


def _post_batch(c, batch_id, records):
    return c.post("/api/v1/ticks/batch",
                  json={"batch_id": batch_id, "records": records})


def _correct(c, symbol, correction_id, start, end, records):
    return c.post(
        f"/api/v1/symbols/{symbol}/corrections",
        json={"correction_id": correction_id, "start": start,
              "end": end, "records": records},
    )


def _candles(c, symbol, start=0, end=600_000):
    return c.get(f"/api/v1/symbols/{symbol}/candles",
                 params={"start": start, "end": end}).json()["candles"]


class TestCorrectionApi:
    def test_apply_correction_endpoint_replaces(self, client, request):
        _post_batch(client, "bad", [
            {"symbol": "AAA", "ts": 60_100, "price": 10, "size": 100},
            {"symbol": "AAA", "ts": 120_100, "price": 12, "size": 100},
        ])
        fixed = [
            {"symbol": "AAA", "ts": 60_200, "price": 11, "size": 2},
            {"symbol": "AAA", "ts": 120_200, "price": 13, "size": 3},
        ]
        request.node._log_case(
            input={"range": [60000, 180000], "fixed": fixed},
            rule="200 applied，区间旧点整体替换，返回 removed/inserted/窗口",
        )
        r = _correct(client, "AAA", "fix-1", 60_000, 180_000, fixed)
        assert r.status_code == 200
        body = r.json()
        assert body["status"] == "applied"
        assert (body["removed"], body["inserted"]) == (2, 2)
        assert body["affected_windows"] == ["AAA@120000", "AAA@60000"]
        cs = {c["window_start"]: c for c in _candles(client, "AAA")}
        assert (cs[60_000]["open"], cs[60_000]["volume"]) == (11.0, 2.0)
        assert (cs[120_000]["open"], cs[120_000]["volume"]) == (13.0, 3.0)

    def test_duplicate_correction_is_idempotent_http(self, client, request):
        recs = [{"symbol": "AAA", "ts": 60_000, "price": 10, "size": 2}]
        request.node._log_case(
            input={"correction_id": "fix", "times": 2},
            rule="重放 200 duplicate，区间结果不变",
        )
        r1 = _correct(client, "AAA", "fix", 60_000, 120_000, recs)
        r2 = _correct(client, "AAA", "fix", 60_000, 120_000, list(reversed(recs)))
        assert r1.json()["status"] == "applied"
        assert r2.status_code == 200 and r2.json()["status"] == "duplicate"
        c = _candles(client, "AAA")[0]
        assert c["volume"] == 2 and c["tick_count"] == 1

    def test_conflict_http_409(self, client):
        r1 = [{"symbol": "AAA", "ts": 60_000, "price": 10, "size": 1}]
        r2 = [{"symbol": "AAA", "ts": 60_000, "price": 11, "size": 1}]
        _correct(client, "AAA", "fix", 60_000, 120_000, r1)
        r = _correct(client, "AAA", "fix", 60_000, 120_000, r2)
        assert r.status_code == 409
        assert r.json()["error_code"] == "correction_id_conflict"

    def test_out_of_range_record_http_422(self, client, request):
        rec = {"symbol": "AAA", "ts": 120_000, "price": 10, "size": 1}
        request.node._log_case(
            input={"range": [60000, 120000], "record_ts": 120000},
            rule="end 处点位越界 -> 422 tick_out_of_range，且数据未改动",
        )
        r = _correct(client, "AAA", "fix", 60_000, 120_000, [rec])
        assert r.status_code == 422
        body = r.json()
        assert body["error_code"] == "records_out_of_range"
        assert body["rejected_records"][0]["reason"] == "tick_out_of_range"
        assert _candles(client, "AAA") == []

    def test_invalid_range_http_422(self, client):
        r = _correct(client, "AAA", "fix", 120_000, 120_000, [])
        assert r.status_code == 422
        assert r.json()["error_code"] == "invalid_range"

    def test_correction_then_verify_and_rebuild(self, client, request):
        _post_batch(client, "bad", [
            {"symbol": "AAA", "ts": 60_100, "price": 10, "size": 100},
        ])
        _correct(client, "AAA", "fix", 60_000, 120_000,
                 [{"symbol": "AAA", "ts": 60_200, "price": 20, "size": 4}])
        request.node._log_case(
            input={"action": ["verify", "rebuild"]},
            rule="修正后 verify 一致；rebuild 前后结果相同",
        )
        before = _candles(client, "AAA")
        v = client.post("/api/v1/admin/symbols/AAA/verify")
        assert v.status_code == 200
        assert v.json()["consistent"] is True
        rb = client.post("/api/v1/admin/symbols/AAA/rebuild")
        assert rb.status_code == 200 and rb.json()["rebuilt_windows"] == 1
        assert _candles(client, "AAA") == before

    def test_correction_does_not_touch_other_symbol_http(self, client, request):
        _post_batch(client, "b", [
            {"symbol": "AAA", "ts": 60_000, "price": 10, "size": 1},
            {"symbol": "BBB", "ts": 60_000, "price": 5, "size": 7},
        ])
        request.node._log_case(
            input={"correct": "AAA@[60000,120000)"},
            rule="只修正 AAA，BBB K 线保持不变",
        )
        _correct(client, "AAA", "fix", 60_000, 120_000,
                 [{"symbol": "AAA", "ts": 60_000, "price": 11, "size": 2}])
        bbb = _candles(client, "BBB")[0]
        assert (bbb["open"], bbb["volume"], bbb["tick_count"]) == (5.0, 7.0, 1)
