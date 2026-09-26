"""FastAPI 应用装配。

保持兼容：``GET /`` 仍返回 ``{"Hello": "World"}``；
新增接口均位于 ``/v1`` 前缀下。
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse

from .errors import ConflictError, StorageError, ValidationBatchError
from .models import (
    Candle,
    HealthResponse,
    IngestRequest,
    IngestResponse,
    ReplaceRangeRequest,
    ReplaceRangeResponse,
)
from .service import MarketService
from .store import TickStore


def create_app(db_path: str | Path | None = None) -> FastAPI:
    db = db_path or os.environ.get("MARKET_DB", "data/market.db")
    store = TickStore(db)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        store.init_db()
        try:
            yield
        finally:
            store.close()

    app = FastAPI(title="market-tick-aggregator", lifespan=lifespan)
    service = MarketService(store)
    app.state.store = store
    app.state.service = service

    @app.exception_handler(ValidationBatchError)
    async def _on_validation(_request, exc: ValidationBatchError):
        return JSONResponse(status_code=400,
                            content={"error": "validation_error", "detail": str(exc)})

    @app.exception_handler(ConflictError)
    async def _on_conflict(_request, exc: ConflictError):
        return JSONResponse(status_code=409,
                            content={"error": "conflict", "detail": str(exc)})

    @app.exception_handler(StorageError)
    async def _on_storage(_request, exc: StorageError):
        return JSONResponse(status_code=500,
                            content={"error": "storage_error", "detail": str(exc)})

    @app.get("/", include_in_schema=False)
    def read_root() -> dict:
        return {"Hello": "World"}

    @app.get("/health")
    def health() -> HealthResponse:
        return HealthResponse(status="ok")

    @app.post("/v1/ticks/batches", response_model=IngestResponse)
    def ingest_ticks(req: IngestRequest) -> IngestResponse:
        """批量追加点位；batch_id 幂等。"""
        return service.ingest(req)

    @app.post("/v1/symbols/{symbol}/replace-range",
              response_model=ReplaceRangeResponse)
    def replace_symbol_range(symbol: str, req: ReplaceRangeRequest) -> ReplaceRangeResponse:
        # 路径参数优先；若 body 也提供 symbol 则必须一致
        if req.symbol and req.symbol != symbol:
            raise HTTPException(
                status_code=400,
                detail=f"路径 symbol={symbol} 与 body symbol={req.symbol} 不一致")
        req.symbol = symbol
        return service.replace_range(req)

    @app.get("/v1/symbols/{symbol}/candles", response_model=list[Candle])
    def query_candles(
        symbol: str,
        start_ms: str = Query(..., description="起始时间戳（含），UTC 毫秒"),
        end_ms_exclusive: str = Query(..., description="结束时间戳（不含），UTC 毫秒"),
    ) -> list[Candle]:
        try:
            start = int(start_ms)
            end = int(end_ms_exclusive)
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail="start_ms/end_ms_exclusive 必须为整数毫秒")
        return service.query_candles(symbol, start, end)

    return app


app = create_app()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000)
