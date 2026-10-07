"""行情写入 / K 线查询 HTTP 路由。"""

from __future__ import annotations

from fastapi import APIRouter, Depends, Query

from app.deps import get_service
from app.models import (
    CandleQueryResponse,
    CorrectionRequest,
    CorrectionResponse,
    RebuildResponse,
    TickBatchRequest,
    TickBatchResponse,
    VerifyResponse,
)
from app.service import TickService

router = APIRouter(tags=["ticks"])


@router.post("/api/v1/ticks/batch", response_model=TickBatchResponse)
def ingest_ticks(
    payload: TickBatchRequest, svc: TickService = Depends(get_service)
) -> dict:
    """批量写入行情点。

    * 200：接受（``accepted``）或幂等重放（``duplicate``）。
    * 422：批次/记录无效，响应体 ``rejected_records`` 逐条给出原因。
    * 409：相同 batch_id 但负载不一致。
    """
    return svc.ingest_batch(payload.batch_id, payload.records)


@router.post(
    "/api/v1/symbols/{symbol}/corrections", response_model=CorrectionResponse
)
def apply_correction(
    symbol: str,
    payload: CorrectionRequest,
    svc: TickService = Depends(get_service),
) -> dict:
    """按标的与 ``[start, end)`` 区间做整体替换式修正。

    * 区间内该标的旧点位整体删除并替换为 ``records``，不叠加、不残留；
      区间外（含 ``ts == end``）点位不动。
    * 200：修正生效（``applied``）或同 correction_id + 同负载重放
      （``duplicate``，不重复生效）。
    * 422：参数/记录无效（含点位越界、symbol 不一致），整体未改动。
    * 409：相同 correction_id 但负载不一致。
    """
    return svc.apply_correction(
        payload.correction_id, symbol, payload.start, payload.end, payload.records
    )


@router.post("/api/v1/admin/symbols/{symbol}/verify", response_model=VerifyResponse)
def verify_symbol(
    symbol: str, svc: TickService = Depends(get_service)
) -> dict:
    """以 ticks 事实表重算并与 candles 物化结果逐窗口对照（本地审计用）。"""
    return svc.verify_symbol(symbol)


@router.get("/api/v1/symbols/{symbol}/candles", response_model=CandleQueryResponse)
def query_candles(
    symbol: str,
    start: int = Query(..., description="起始 Unix 毫秒（含，自动向下取整到分钟）"),
    end: int = Query(..., description="结束 Unix 毫秒（含其所在分钟窗口）"),
    svc: TickService = Depends(get_service),
) -> dict:
    """按标的与时间范围查询一分钟 K 线。"""
    return svc.query_candles(symbol, start, end)


@router.post("/api/v1/admin/symbols/{symbol}/rebuild", response_model=RebuildResponse)
def rebuild_symbol(
    symbol: str, svc: TickService = Depends(get_service)
) -> dict:
    """历史补写/修正/重放后，以 ticks 事实表重建该标的全部分钟 K 线。"""
    return svc.rebuild_symbol(symbol)
