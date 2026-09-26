"""健康检查与兼容根路由。"""

from __future__ import annotations

from fastapi import APIRouter

router = APIRouter(tags=["meta"])


@router.get("/")
def read_root():
    # 保持骨架期既有对外行为不变
    return {"Hello": "World"}


@router.get("/health")
def health():
    return {"status": "ok"}
