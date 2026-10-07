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
    "tick_outside_range",
    "duplicate_tick_in_correction",
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
    "invalid_total_parts",
    "invalid_part_index",
    "correction_too_large",
    "correction_conflict",
]

MAX_BATCH_RECORDS = 10_000
MAX_SYMBOL_LENGTH = 64
# 单次区间修正允许的点位总量上限（所有分片合计）
MAX_CORRECTION_RECORDS = 100_000


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


class CorrectionPartRequest(BaseModel):
    """区间整体替换式修正的一个分片（单批修正即 total_parts=1）。

    时间口径：仅接受 ``start <= ts < end`` 的点位（半开区间，按原始时间戳
    归属，而非按分钟窗口归属）；``records`` 中每条点位的 ``symbol`` 必须
    与路径参数 ``symbol`` 完全一致。
    """

    correction_id: str = Field(..., description="调用方生成的修正幂等键，非空字符串")
    start: int = Field(..., description="修正区间起点，Unix 毫秒，**含**", ge=0)
    end: int = Field(..., description="修正区间终点，Unix 毫秒，**不含**", ge=0)
    total_parts: int = Field(..., description="本次修正共分多少片，正整数", ge=1)
    part_index: int = Field(..., description="本片序号，从 0 开始且小于 total_parts", ge=0)
    records: list[dict] = Field(..., description="本片携带的修正点位原始列表（可空）")


class CorrectionPartResponse(BaseModel):
    correction_id: str
    symbol: str
    start: int
    end: int
    total_parts: int
    part_index: int
    status: Literal["pending", "applied", "duplicate"] = Field(
        ...,
        description=(
            "pending=本片已暂存、尚有其它片未到齐；"
            "applied=本片为最后到齐的一片，修正已整体生效；"
            "duplicate=该修正此前已生效或本片此前已暂存（幂等返回）"
        ),
    )
    received: int = Field(..., description="本片携带记录数")
    accepted: int = Field(..., description="本片校验通过的点数")
    affected_windows: list[str] = Field(
        default_factory=list,
        description="修正生效时实际重算的「symbol@分钟窗口」集合（pending/duplicate 为空）",
    )


class CandleMismatch(BaseModel):
    """K 线物化结果与按当前有效点位重算结果之间的一处差异。"""

    symbol: str
    window_start: int
    kind: Literal["missing_candle", "stale_candle", "value_mismatch"]
    stored: object | None = Field(None, description="candles 表中当前值（缺失时为 null）")
    expected: object | None = Field(None, description="按有效点位重算的期望值（窗口无有效点位时为 null）")


class VerifyResponse(BaseModel):
    symbol: str
    ok: bool = Field(..., description="true 表示全部窗口与有效点位重算结果一致")
    checked_windows: int
    mismatches: list[CandleMismatch]


class ErrorBody(BaseModel):
    """统一错误响应体。"""

    error_code: str
    message: str
    rejected_records: list[RejectedRecord] = Field(default_factory=list)
