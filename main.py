"""服务入口：装配路由与统一错误处理。

保留历史对外行为：``GET /`` 仍返回 ``{"Hello": "World"}``。
"""

from __future__ import annotations

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from app.errors import DomainError
from app.routers import health, ticks


def create_app() -> FastAPI:
    app = FastAPI(title="market-tick-aggregator")

    @app.exception_handler(DomainError)
    async def domain_error_handler(request: Request, exc: DomainError):
        return JSONResponse(
            status_code=exc.http_status,
            content={
                "error_code": exc.error_code,
                "message": exc.message,
                "rejected_records": [
                    r.model_dump() if hasattr(r, "model_dump") else r
                    for r in exc.rejected_records
                ],
            },
        )

    app.include_router(health.router)
    app.include_router(ticks.router)
    return app


app = create_app()


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
