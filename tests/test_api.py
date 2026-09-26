"""HTTP API 测试：兼容性、错误码、端到端 HTTP 流程。"""

from __future__ import annotations

import logging

import pytest
from fastapi.testclient import TestClient

from app.main import create_app

log = logging.getLogger("tests.api")


@pytest.fixture
def client(tmp_path):
    app = create_app(tmp_path / "api.db")
    with TestClient(app) as c:
        yield c


def test_root_compatibility(client) -> None:
    r = client.get("/")
    log.info("GET / -> %s", r.json())
    assert r.status_code == 200
    assert r.json() == {"Hello": "World"}


def test_health(client) -> None:
    assert client.get("/health").json() == {"status": "ok"}


def test_ingest_and_query_flow(client) -> None:
    body = {
        "batch_id": "http-1",
        "ticks": [
            {"symbol": "S1", "timestamp_ms": 60000, "price": 10, "quantity": 2},
            {"symbol": "S1", "timestamp_ms": 60030, "price": 12, "quantity": 3},
            {"symbol": "S1", "timestamp_ms": 59999, "price": 1, "quantity": 99},
        ],
    }
    log.info("POST 输入=%s", body)
    r = client.post("/v1/ticks/batches", json=body)
    assert r.status_code == 200, r.text
    data = r.json()
    log.info("POST 判定=%s", data)
    assert data["accepted"] == 3 and data["rejected"] == []

    # 重试：判重
    r2 = client.post("/v1/ticks/batches", json=body)
    assert r2.json()["duplicate"] is True

    r3 = client.get("/v1/symbols/S1/candles",
                    params={"start_ms": 60000, "end_ms_exclusive": 120000})
    candles = r3.json()
    log.info("GET K 线=%s", candles)
    assert len(candles) == 1
    assert (candles[0]["open"], candles[0]["high"], candles[0]["low"],
            candles[0]["close"], candles[0]["volume"]) == (10, 12, 10, 12, 5)


def test_invalid_records_returned_not_dropped(client) -> None:
    body = {"batch_id": "http-bad", "ticks": [
        {"symbol": "", "timestamp_ms": 1, "price": 1, "quantity": 1},
        {"symbol": "S1", "timestamp_ms": "x", "price": 1, "quantity": 1},
        {"symbol": "S1", "timestamp_ms": 1, "price": 1, "quantity": 1},
    ]}
    r = client.post("/v1/ticks/batches", json=body)
    data = r.json()
    log.info("无效记录判定 accepted=%s rejected=%s",
             data["accepted"], [(x["reason"], x["detail"]) for x in data["rejected"]])
    assert data["accepted"] == 1
    assert {x["reason"] for x in data["rejected"]} == {
        "missing_symbol", "bad_timestamp"}


def test_batch_level_error_is_400(client) -> None:
    r = client.post("/v1/ticks/batches",
                    json={"batch_id": "", "ticks": []})
    log.info("空 batch_id HTTP 状态=%s 响应=%s", r.status_code, r.json())
    assert r.status_code == 400
    assert r.json()["error"] == "validation_error"

    r2 = client.get("/v1/symbols/S1/candles",
                    params={"start_ms": 100, "end_ms_exclusive": 50})
    assert r2.status_code == 400


def test_replace_range_http(client) -> None:
    client.post("/v1/ticks/batches", json={"batch_id": "init", "ticks": [
        {"symbol": "S1", "timestamp_ms": 60000, "price": 10, "quantity": 1,
         "id": "old"}]})
    body = {"symbol": "S1", "start_ms": 60000, "end_ms_exclusive": 120000,
            "batch_id": "fix", "ticks": [
                {"symbol": "S1", "timestamp_ms": 60000, "price": 20,
                 "quantity": 5, "id": "new"}]}
    r = client.post("/v1/symbols/S1/replace-range", json=body)
    log.info("重建响应=%s", r.json())
    assert r.status_code == 200
    assert r.json()["deleted_before"] == 1

    # 路径与 body symbol 不一致 -> 400
    r2 = client.post("/v1/symbols/S2/replace-range", json=body)
    assert r2.status_code == 400


def test_malformed_params_are_400_not_framework_422(client) -> None:
    r = client.get("/v1/symbols/S1/candles",
                   params={"start_ms": "abc", "end_ms_exclusive": "120000"})
    log.info("畸形查询参数状态=%s 响应=%s", r.status_code, r.json())
    assert r.status_code == 400

    r2 = client.post("/v1/symbols/S1/replace-range", json={
        "start_ms": "x", "end_ms_exclusive": 120000, "batch_id": "b", "ticks": []})
    assert r2.status_code == 400
    assert r2.json()["error"] == "validation_error"
