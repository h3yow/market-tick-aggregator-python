"""业务编排层：输入校验、内容键生成、幂等与事务调用。"""

from __future__ import annotations

import hashlib
import json
import math

from .aggregation import aggregate_ticks
from .errors import ValidationBatchError
from .models import (
    Candle,
    IngestRequest,
    IngestResponse,
    RejectReason,
    RejectedItem,
    ReplaceRangeRequest,
    ReplaceRangeResponse,
    TickIn,
)
from .store import AcceptedRow, TickStore

MAX_TICKS_PER_BATCH = 100_000


def _to_float(value, *, allow_int: bool) -> tuple[float | None, str | None]:
    """宽松数值解析；返回 (值, 错误原因)。"""
    if value is None:
        return None, "字段缺失"
    if isinstance(value, bool):
        return None, "布尔值不是合法数字"
    try:
        if isinstance(value, str):
            value = value.strip()
            if value == "":
                return None, "空字符串不是合法数字"
        number = float(value)
    except (TypeError, ValueError):
        return None, f"无法解析为数字: {value!r}"
    if not math.isfinite(number):
        return None, f"数字必须有限: {value!r}"
    if not allow_int and number <= 0:
        return None, "价格必须为正数"
    if allow_int and number < 0:
        return None, "数量不能为负"
    return number, None


def content_key(ts_ms: int, price: float, quantity: float, seq: int | None) -> str:
    seq_part = "" if seq is None else str(seq)
    raw = f"{ts_ms}|{price!r}|{quantity!r}|{seq_part}"
    return "c:" + hashlib.sha1(raw.encode("utf-8")).hexdigest()


def _validate_tick(
    tick: TickIn, index: int
) -> tuple[tuple | None, RejectedItem | None]:
    """校验单条点位。成功返回标准行（无 tick_key 版本），失败返回拒绝项。"""
    raw = tick.model_dump()

    symbol = tick.symbol
    if symbol is None or not str(symbol).strip():
        return None, RejectedItem(
            index=index, reason=RejectReason.MISSING_SYMBOL.value,
            detail="symbol 缺失或为空", raw=raw)
    symbol = str(symbol).strip()

    ts = tick.timestamp_ms
    if isinstance(ts, bool) or not isinstance(ts, int) or ts < 0:
        return None, RejectedItem(
            index=index, reason=RejectReason.BAD_TIMESTAMP.value,
            detail=f"timestamp_ms 必须为非负整数毫秒，实际: {ts!r}", raw=raw)

    price, err = _to_float(tick.price, allow_int=False)
    if err is not None:
        return None, RejectedItem(
            index=index, reason=RejectReason.BAD_PRICE.value,
            detail=f"price: {err}", raw=raw)

    quantity, err = _to_float(tick.quantity, allow_int=True)
    if err is not None:
        return None, RejectedItem(
            index=index, reason=RejectReason.BAD_QUANTITY.value,
            detail=f"quantity: {err}", raw=raw)

    seq = tick.seq
    if seq is not None and (isinstance(seq, bool) or not isinstance(seq, int)):
        return None, RejectedItem(
            index=index, reason=RejectReason.BAD_SEQ.value,
            detail=f"seq 必须为整数，实际: {seq!r}", raw=raw)

    if tick.id is not None and not str(tick.id).strip():
        return None, RejectedItem(
            index=index, reason=RejectReason.EMPTY_ID.value,
            detail="id 若提供则不能为空字符串", raw=raw)

    key = str(tick.id).strip() if tick.id is not None else content_key(ts, price, quantity, seq)
    has_id = 1 if tick.id is not None else 0
    # (symbol, key, has_id, ts, price, qty, seq, index, raw_json)
    return (symbol, key, has_id, ts, price, quantity, seq, index,
            json.dumps(raw, sort_keys=True, ensure_ascii=False)), None


class MarketService:
    def __init__(self, store: TickStore) -> None:
        self.store = store

    # -- 校验公共逻辑 ---------------------------------------------------
    @staticmethod
    def _validate(ticks: list[TickIn]) -> tuple[list[AcceptedRow], list]:
        accepted: list[AcceptedRow] = []
        rejected: list[RejectedItem] = []
        seen: set[tuple[str, str]] = set()
        for i, tick in enumerate(ticks):
            row, rej = _validate_tick(tick, i)
            if rej is not None:
                rejected.append(rej)
                continue
            assert row is not None
            ident = (row[0], row[1])
            if ident in seen:
                rejected.append(RejectedItem(
                    index=i,
                    reason=RejectReason.DUPLICATE_WITHIN_BATCH.value,
                    detail=(
                        f"批次内与更早记录重复: symbol={row[0]}, key={row[1]}；"
                        "提供 id 时按 (symbol,id) 判定，否则按内容"
                        "(timestamp_ms,price,quantity,seq) 判定"),
                    raw=tick.model_dump(),
                ))
                continue
            seen.add(ident)
            accepted.append(row)
        return accepted, rejected

    @staticmethod
    def _rejected_rows(rejected: list[RejectedItem]) -> list:
        return [
            (r.index, r.reason, r.detail,
             json.dumps(r.raw, sort_keys=True, ensure_ascii=False))
            for r in rejected
        ]

    # -- 追加写入 -------------------------------------------------------
    def ingest(self, req: IngestRequest) -> IngestResponse:
        if not req.batch_id or not req.batch_id.strip():
            raise ValidationBatchError("batch_id 不能为空")
        if len(req.ticks) > MAX_TICKS_PER_BATCH:
            raise ValidationBatchError(
                f"单批最多 {MAX_TICKS_PER_BATCH} 条，实际 {len(req.ticks)} 条")

        accepted, rejected = self._validate(req.ticks)
        result = self.store.insert_batch(
            req.batch_id, accepted, self._rejected_rows(rejected))
        return IngestResponse(
            batch_id=result["batch_id"],
            accepted=result["accepted"],
            rejected=[RejectedItem(**r) for r in result["rejected"]],
            duplicate=result["duplicate"],
        )

    # -- 区间重建 -------------------------------------------------------
    def replace_range(self, req: ReplaceRangeRequest) -> ReplaceRangeResponse:
        if not req.symbol or not req.symbol.strip():
            raise ValidationBatchError("symbol 不能为空")
        if not req.batch_id or not req.batch_id.strip():
            raise ValidationBatchError("batch_id 不能为空")
        for name, value in (("start_ms", req.start_ms),
                            ("end_ms_exclusive", req.end_ms_exclusive)):
            if isinstance(value, bool) or not isinstance(value, int):
                raise ValidationBatchError(
                    f"{name} 必须为整数毫秒，实际: {value!r}")
        start_ms, end_ms = req.start_ms, req.end_ms_exclusive
        if end_ms <= start_ms:
            raise ValidationBatchError(
                "end_ms_exclusive 必须大于 start_ms")
        if len(req.ticks) > MAX_TICKS_PER_BATCH:
            raise ValidationBatchError(
                f"单批最多 {MAX_TICKS_PER_BATCH} 条，实际 {len(req.ticks)} 条")

        symbol = req.symbol.strip()
        accepted, rejected = self._validate(req.ticks)

        out_of_range: list[RejectedItem] = []
        in_range: list[AcceptedRow] = []
        for row in accepted:
            if row[0] != symbol or not (start_ms <= row[3] < end_ms):
                out_of_range.append(RejectedItem(
                    index=row[7], reason=RejectReason.OUT_OF_RANGE.value,
                    detail=(
                        f"记录 symbol={row[0]}, ts={row[3]} 不属于重建区间 "
                        f"symbol={symbol}, [{start_ms},{end_ms})"),
                    raw=json.loads(row[8]),
                ))
            else:
                in_range.append(row)
        rejected.extend(out_of_range)

        result = self.store.replace_range(
            symbol, start_ms, end_ms, req.batch_id,
            in_range, self._rejected_rows(rejected))
        return ReplaceRangeResponse(
            batch_id=req.batch_id,
            accepted=result["accepted"],
            rejected=[RejectedItem(**r) for r in result["rejected"]],
            deleted_before=result["deleted_before"],
            duplicate=result["duplicate"],
        )

    # -- 查询 -----------------------------------------------------------
    def query_candles(
        self, symbol: str, start_ms: int, end_ms_exclusive: int
    ) -> list[Candle]:
        if not symbol or not symbol.strip():
            raise ValidationBatchError("symbol 不能为空")
        if end_ms_exclusive <= start_ms:
            raise ValidationBatchError("end_ms_exclusive 必须大于 start_ms")
        ticks = self.store.query_ticks(
            symbol.strip(), start_ms, end_ms_exclusive)
        return aggregate_ticks(ticks, symbol.strip())
