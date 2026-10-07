"""对外/对内数据模型（纯类型定义，无业务逻辑）。

时间语义
--------
所有时间戳均为 ``int`` 类型的 Unix 毫秒时间戳（UTC）。
分钟窗口由时间戳按整分钟向下取整得到，窗口起点记为 ``window_start``（毫秒）。
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

# ---------------------------------------------------------------------------
# 约束常量
# ---------------------------------------------------------------------------

MAX_BATCH_RECORDS = 10_000
MAX_SYMBOL_LENGTH = 64

# 行情点校验原因码（无效记录分类）
RejectReason = Literal[
    "not_an_object",
    "missing_symbol",
    "invalid_symbol",
    "missing_timestamp",
    "invalid_timestamp",
    "missing_price",
    "invalid_price",
    "missing_size",
    "invalid_size",
    "duplicate_tick_in_batch",
    "symbol_mismatch",
    "tick_out_of_range",
]

# 批次级失败原因码
BatchErrorCode = Literal[
    "empty_batch",
    "batch_too_large",
    "invalid_batch_id",
    "invalid_records",
    "invalid_symbol",
    "invalid_start",
    "invalid_end",
    "invalid_range",
    "batch_id_conflict",
    "invalid_correction_id",
    "records_out_of_range",
    "correction_id_conflict",
]


class Tick(BaseModel):
    """单条行情点位。"""

    symbol: str = Field(..., description="交易标的代码，非空字符串")
    ts: int = Field(..., description="Unix 毫秒时间戳（UTC）", ge=0)
    price: float = Field(..., description="成交价，必须为有限正数", gt=0)
    size: float = Field(..., description="成交量，必须为有限非负数", ge=0)


class TickBatchRequest(BaseModel):
    """批量行情写入请求。"""

    batch_id: str = Field(..., description="调用方生成的批次幂等键，非空字符串")
    records: list[dict] = Field(..., description="行情点原始列表，逐条校验")


class RejectedRecord(BaseModel):
    """无效记录的明确拒绝说明。"""

    index: int = Field(..., description="该记录在请求 records 中的下标（从 0 开始）")
    reason: str
    detail: str
    record: object | None = Field(None, description="无法解析时保留原始内容")


class TickBatchResponse(BaseModel):
    """批量写入结果。"""

    batch_id: str
    status: Literal["accepted", "duplicate"]
    received: int
    accepted: int
    rejected: int
    rejected_records: list[RejectedRecord]
    affected_windows: list[str] = Field(
        default_factory=list,
        description="本次写入实际改变的「symbol@分钟窗口」集合（重复批次为空）",
    )


class Candle(BaseModel):
    """单根一分钟 K 线。"""

    symbol: str
    window_start: int = Field(..., description="窗口起点，Unix 毫秒（整分钟）")
    open: float
    high: float
    low: float
    close: float
    volume: float
    tick_count: int


class CandleQueryResponse(BaseModel):
    symbol: str
    start: int
    end: int
    candles: list[Candle]


class RebuildRequest(BaseModel):
    """历史补写/修正后重建某标的 K 线的请求。"""

    symbol: str


class RebuildResponse(BaseModel):
    symbol: str
    rebuilt_windows: int


class CorrectionRequest(BaseModel):
    """按标的与时间区间做整体替换式修正的请求。

    区间口径为 ``[start, end)``（左闭右开，毫秒）：提交后该标的所有
    ``start <= ts < end`` 的旧点位被整体删除并替换为 ``records``；
    区间外点位（含 ``ts == end``）不受影响。
    """

    correction_id: str = Field(..., description="调用方生成的修正幂等键，非空字符串")
    start: int = Field(..., description="修正区间起点，Unix 毫秒，含", ge=0)
    end: int = Field(..., description="修正区间终点，Unix 毫秒，不含", ge=0)
    records: list[dict] = Field(
        ..., description="修正后的完整点位集合，逐条校验；可为空数组（区间清空）"
    )


class CorrectionResponse(BaseModel):
    """区间整体替换式修正结果。"""

    correction_id: str
    symbol: str
    start: int
    end: int
    status: Literal["applied", "duplicate"]
    removed: int = Field(..., description="本次替换删除的旧点位数（重复提交为 0）")
    inserted: int = Field(..., description="写入的新点位数（重复提交为首次计数）")
    affected_windows: list[str] = Field(default_factory=list)


class CandleMismatch(BaseModel):
    """candles 物化结果与 ticks 事实表重算结果不一致的一个窗口。"""

    window_start: int
    expected: Candle | None = Field(None, description="按 ticks 重算应有的 K 线；None 表示应无此窗口")
    actual: Candle | None = Field(None, description="candles 表中现存 K 线；None 表示缺失")


class VerifyResponse(BaseModel):
    """以 ticks 事实表为准对 candles 做本地对照校验的结果。"""

    symbol: str
    consistent: bool
    checked_windows: int
    mismatches: list[CandleMismatch]


class ErrorBody(BaseModel):
    """统一错误响应体。"""

    error_code: str
    message: str
    rejected_records: list[RejectedRecord] = Field(default_factory=list)
