"""数据模型与错误分类（仅类型定义与常量）。"""

from __future__ import annotations

from enum import Enum

from pydantic import BaseModel, Field


WINDOW_SIZE_MS = 60_000


class RejectReason(str, Enum):
    """无效记录的拒绝原因分类。"""

    MISSING_SYMBOL = "missing_symbol"
    BAD_TIMESTAMP = "bad_timestamp"
    BAD_PRICE = "bad_price"
    BAD_QUANTITY = "bad_quantity"
    DUPLICATE_WITHIN_BATCH = "duplicate_within_batch"
    ID_CONFLICT = "id_conflict"
    EMPTY_ID = "empty_id"
    OUT_OF_RANGE = "out_of_range"
    BAD_SEQ = "bad_seq"


class TickIn(BaseModel):
    """单条行情点位（入参）。

    刻意放宽字段类型，由服务层统一分类拒绝，避免框架层静默 422 而无业务原因。
    """

    model_config = {"extra": "ignore"}

    symbol: object = None
    timestamp_ms: object = None
    price: object = None
    quantity: object = None
    id: object = None
    seq: object = None


class IngestRequest(BaseModel):
    """批量点位写入请求。"""

    batch_id: str
    ticks: list[TickIn] = Field(default_factory=list)


class RejectedItem(BaseModel):
    """被拒绝/标记的记录及其原因。"""

    index: int
    reason: str
    detail: str
    raw: dict


class IngestResponse(BaseModel):
    """批量写入结果。"""

    batch_id: str
    accepted: int
    rejected: list[RejectedItem]
    duplicate: bool = False


class ReplaceRangeRequest(BaseModel):
    """按标的与时间区间重建（补写/修正/重放）请求。

    symbol 通常取自路径参数；body 中若提供则必须与路径一致。
    """

    symbol: str | None = None
    start_ms: object
    end_ms_exclusive: object
    batch_id: str
    ticks: list[TickIn] = Field(default_factory=list)


class ReplaceRangeResponse(BaseModel):
    batch_id: str
    accepted: int
    rejected: list[RejectedItem]
    deleted_before: int
    duplicate: bool = False


class Candle(BaseModel):
    """分钟 K 线。"""

    symbol: str
    window_start_ms: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    tick_count: int


class CandleQuery(BaseModel):
    symbol: str
    start_ms: int
    end_ms_exclusive: int


class HealthResponse(BaseModel):
    status: str
