"""领域服务：逐条校验、批次幂等、分钟聚合与重建。

关键规则
========
* **逐条校验**：每条记录独立判定并给出 ``reason``，无效记录不会被静默丢弃；
  只要批次中存在任何无效记录，整批拒绝（HTTP 422，不写入任何数据），
  保证调用方必须显式修正后重试，语义可解释。
* **批次幂等**：相同 ``batch_id`` 携带相同负载重放返回首次结果（``duplicate``），
  不重复累计；负载不一致则返回 409（``batch_id_conflict``）。
* **乱序/迟到**：ticks 是唯一事实表，K 线由确定性全序重算，
  因此同一输入集合与到达顺序无关；迟到点位正常入库并触发窗口重算。
"""

from __future__ import annotations

import hashlib
import json
import logging
import math

from .errors import BatchConflictError, InvalidBatchError
from .models import (
    MAX_BATCH_RECORDS,
    MAX_SYMBOL_LENGTH,
    Candle,
    RejectedRecord,
)
from .storage import StoredTick, TickStore

logger = logging.getLogger("market_tick.service")


def _reject(index: int, reason: str, detail: str, record: object) -> RejectedRecord:
    return RejectedRecord(index=index, reason=reason, detail=detail, record=record)


def _is_finite_number(value: object) -> bool:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return False
    return math.isfinite(value)


def _validate_record(index: int, raw: object) -> RejectedRecord | None:
    """返回 None 表示有效；否则返回拒绝原因。"""
    if not isinstance(raw, dict):
        return _reject(index, "not_an_object", "记录必须是 JSON 对象", raw)

    # symbol
    if "symbol" not in raw:
        return _reject(index, "missing_symbol", "缺少字段 symbol", raw)
    symbol = raw["symbol"]
    if not isinstance(symbol, str) or not symbol.strip() or len(symbol) > MAX_SYMBOL_LENGTH:
        return _reject(
            index,
            "invalid_symbol",
            f"symbol 必须为长度 1..{MAX_SYMBOL_LENGTH} 的非空字符串",
            raw,
        )

    # ts
    if "ts" not in raw:
        return _reject(index, "missing_timestamp", "缺少字段 ts", raw)
    ts = raw["ts"]
    if isinstance(ts, bool) or not isinstance(ts, int) or ts < 0:
        return _reject(
            index, "invalid_timestamp", "ts 必须为非负整数（Unix 毫秒）", raw
        )

    # price
    if "price" not in raw:
        return _reject(index, "missing_price", "缺少字段 price", raw)
    price = raw["price"]
    if not _is_finite_number(price) or price <= 0:
        return _reject(index, "invalid_price", "price 必须为有限正数", raw)

    # size
    if "size" not in raw:
        return _reject(index, "missing_size", "缺少字段 size", raw)
    size = raw["size"]
    if not _is_finite_number(size) or size < 0:
        return _reject(index, "invalid_size", "size 必须为有限非负数", raw)

    return None


def _canonical_payload(
    batch_id: str,
    ticks: list[StoredTick],
    rejected: list[RejectedRecord],
) -> str:
    """对负载做规范化指纹：同语义负载（含字段顺序差异）指纹一致。"""
    body = {
        "batch_id": batch_id,
        "ticks": [
            {
                "i": t.batch_index,
                "symbol": t.symbol,
                "ts": t.ts,
                "price": t.price,
                "size": t.size,
            }
            for t in ticks
        ],
        "rejected": [r.model_dump() for r in rejected],
    }
    text = json.dumps(body, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _canonical_correction_payload(
    correction_id: str,
    symbol: str,
    start: int,
    end: int,
    ticks: list[StoredTick],
) -> str:
    """修正负载的规范化指纹。

    点位按 ``(ts, price, size)`` 排序后入指纹：**同一份修正集合无论以什么
    顺序提交，指纹都相同**，因此乱序重放会被识别为同一次修正（duplicate）。
    """
    body = {
        "correction_id": correction_id,
        "symbol": symbol,
        "start": start,
        "end": end,
        "ticks": sorted(
            (
                {"symbol": t.symbol, "ts": t.ts, "price": t.price, "size": t.size}
                for t in ticks
            ),
            key=lambda d: (d["ts"], d["price"], d["size"]),
        ),
    }
    text = json.dumps(body, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


class TickService:
    def __init__(self, store: TickStore) -> None:
        self.store = store

    # ------------------------------------------------------------------
    def ingest_batch(
        self, batch_id: object, records: object
    ) -> dict:
        """写入一批行情点。返回可直接序列化的响应字典。"""
        # ---- 批次级校验 -------------------------------------------------
        if not isinstance(batch_id, str) or not batch_id.strip():
            raise InvalidBatchError(
                "batch_id 必须为非空字符串", code="invalid_batch_id"
            )
        if not isinstance(records, list):
            raise InvalidBatchError(
                "records 必须为数组", code="invalid_records"
            )
        if len(records) == 0:
            raise InvalidBatchError("records 不能为空", code="empty_batch")
        if len(records) > MAX_BATCH_RECORDS:
            raise InvalidBatchError(
                f"单批记录数超过上限 {MAX_BATCH_RECORDS}",
                code="batch_too_large",
            )

        logger.info(
            "ingest_batch start batch_id=%s received=%d records=%s",
            batch_id,
            len(records),
            json.dumps(records, ensure_ascii=False, default=str),
        )

        # ---- 逐条校验 ---------------------------------------------------
        rejected: list[RejectedRecord] = []
        ticks: list[StoredTick] = []
        seen_keys: set[tuple[str, int, float, float]] = set()
        for i, raw in enumerate(records):
            err = _validate_record(i, raw)
            if err is not None:
                rejected.append(err)
                continue
            key = (raw["symbol"], raw["ts"], float(raw["price"]), float(raw["size"]))
            if key in seen_keys:
                rejected.append(
                    _reject(
                        i,
                        "duplicate_tick_in_batch",
                        "同一批次内完全相同的点位（symbol/ts/price/size）重复",
                        raw,
                    )
                )
                continue
            seen_keys.add(key)
            ticks.append(
                StoredTick(
                    batch_id=batch_id,
                    batch_index=i,
                    symbol=raw["symbol"],
                    ts=raw["ts"],
                    price=float(raw["price"]),
                    size=float(raw["size"]),
                )
            )

        if rejected:
            logger.info(
                "ingest_batch rejected batch_id=%s rejected=%s",
                batch_id,
                json.dumps([r.model_dump() for r in rejected], ensure_ascii=False),
            )
            raise InvalidBatchError(
                f"{len(rejected)} 条记录无效，整批拒绝（未写入任何数据）",
                rejected_records=rejected,
                code="invalid_records",
            )

        payload_hash = _canonical_payload(batch_id, ticks, rejected)

        # ---- 幂等检查 ---------------------------------------------------
        existing = self.store.get_batch(batch_id)
        if existing is not None:
            if existing.kind != "ingest":
                logger.info(
                    "ingest_batch id-reused-by-correction batch_id=%s", batch_id
                )
                raise BatchConflictError(
                    f"batch_id={batch_id} 已被区间修正占用，不能作为普通批次 ID",
                    code="correction_id_conflict",
                )
            if existing.payload_hash != payload_hash:
                logger.info(
                    "ingest_batch conflict batch_id=%s stored_hash=%s new_hash=%s",
                    batch_id,
                    existing.payload_hash,
                    payload_hash,
                )
                raise BatchConflictError(
                    f"batch_id={batch_id} 已存在但负载与本次请求不一致"
                )
            logger.info(
                "ingest_batch duplicate batch_id=%s accepted=%d -> 幂等返回",
                batch_id,
                existing.accepted,
            )
            return {
                "batch_id": batch_id,
                "status": "duplicate",
                "received": existing.received,
                "accepted": existing.accepted,
                "rejected": existing.rejected,
                "rejected_records": existing.rejected_records,
                "affected_windows": [],
            }

        # ---- 原子写入 ---------------------------------------------------
        try:
            affected = self.store.insert_batch(batch_id, payload_hash, [], ticks)
        except Exception as exc:  # 极端竞态：并发同 batch_id
            existing = self.store.get_batch(batch_id)
            if existing is not None:
                if existing.kind != "ingest":
                    raise BatchConflictError(
                        f"batch_id={batch_id} 已被区间修正占用，不能作为普通批次 ID",
                        code="correction_id_conflict",
                    ) from exc
                if existing.payload_hash != payload_hash:
                    raise BatchConflictError(
                        f"batch_id={batch_id} 已存在但负载与本次请求不一致"
                    ) from exc
                logger.info(
                    "ingest_batch duplicate(after race) batch_id=%s", batch_id
                )
                return {
                    "batch_id": batch_id,
                    "status": "duplicate",
                    "received": existing.received,
                    "accepted": existing.accepted,
                    "rejected": existing.rejected,
                    "rejected_records": existing.rejected_records,
                    "affected_windows": [],
                }
            raise

        affected_sorted = sorted(
            f"{symbol}@{ws}" for symbol, ws in affected
        )
        logger.info(
            "ingest_batch accepted batch_id=%s accepted=%d windows=%s",
            batch_id,
            len(ticks),
            affected_sorted,
        )
        return {
            "batch_id": batch_id,
            "status": "accepted",
            "received": len(records),
            "accepted": len(ticks),
            "rejected": 0,
            "rejected_records": [],
            "affected_windows": affected_sorted,
        }

    # ------------------------------------------------------------------
    def query_candles(self, symbol: str, start: object, end: object) -> dict:
        if not isinstance(symbol, str) or not symbol.strip():
            raise InvalidBatchError("symbol 必须为非空字符串", code="invalid_symbol")
        if isinstance(start, bool) or not isinstance(start, int) or start < 0:
            raise InvalidBatchError(
                "start 必须为非负整数（Unix 毫秒）", code="invalid_start"
            )
        if isinstance(end, bool) or not isinstance(end, int) or end < 0:
            raise InvalidBatchError(
                "end 必须为非负整数（Unix 毫秒）", code="invalid_end"
            )
        if end < start:
            raise InvalidBatchError("end 不能早于 start", code="invalid_range")

        logger.info("query_candles symbol=%s start=%d end=%d", symbol, start, end)
        rows = self.store.query_candles(symbol, start, end)
        candles = [
            Candle(
                symbol=r[0],
                window_start=r[1],
                open=r[2],
                high=r[3],
                low=r[4],
                close=r[5],
                volume=r[6],
                tick_count=r[7],
            ).model_dump()
            for r in rows
        ]
        return {"symbol": symbol, "start": start, "end": end, "candles": candles}

    # ------------------------------------------------------------------
    def rebuild_symbol(self, symbol: str) -> dict:
        if not isinstance(symbol, str) or not symbol.strip():
            raise InvalidBatchError("symbol 必须为非空字符串", code="invalid_symbol")
        n = self.store.rebuild_symbol(symbol)
        logger.info("rebuild_symbol symbol=%s rebuilt_windows=%d", symbol, n)
        return {"symbol": symbol, "rebuilt_windows": n}

    # ------------------------------------------------------------------
    def apply_correction(
        self,
        correction_id: object,
        symbol: object,
        start: object,
        end: object,
        records: object,
    ) -> dict:
        """按标的 + ``[start, end)`` 区间做整体替换式修正。

        校验、幂等判定与存储事务的规则见模块文档与
        :meth:`TickStore.apply_correction`。返回可直接序列化的响应字典。
        """
        # ---- 参数级校验 -------------------------------------------------
        if not isinstance(correction_id, str) or not correction_id.strip():
            raise InvalidBatchError(
                "correction_id 必须为非空字符串", code="invalid_correction_id"
            )
        if not isinstance(symbol, str) or not symbol.strip() or len(symbol) > MAX_SYMBOL_LENGTH:
            raise InvalidBatchError(
                f"symbol 必须为长度 1..{MAX_SYMBOL_LENGTH} 的非空字符串",
                code="invalid_symbol",
            )
        if isinstance(start, bool) or not isinstance(start, int) or start < 0:
            raise InvalidBatchError(
                "start 必须为非负整数（Unix 毫秒）", code="invalid_start"
            )
        if isinstance(end, bool) or not isinstance(end, int) or end < 0:
            raise InvalidBatchError(
                "end 必须为非负整数（Unix 毫秒）", code="invalid_end"
            )
        if end <= start:
            raise InvalidBatchError(
                "修正区间必须满足 start < end（左闭右开 [start, end)）",
                code="invalid_range",
            )
        if not isinstance(records, list):
            raise InvalidBatchError(
                "records 必须为数组（可为空数组，表示区间清空）",
                code="invalid_records",
            )
        if len(records) > MAX_BATCH_RECORDS:
            raise InvalidBatchError(
                f"单次修正记录数超过上限 {MAX_BATCH_RECORDS}",
                code="batch_too_large",
            )

        logger.info(
            "apply_correction start id=%s symbol=%s range=[%d,%d) received=%d records=%s",
            correction_id,
            symbol,
            start,
            end,
            len(records),
            json.dumps(records, ensure_ascii=False, default=str),
        )

        # ---- 逐条校验：结构规则与普通写入相同，另加标的/区间归属 --------
        rejected: list[RejectedRecord] = []
        ticks: list[StoredTick] = []
        seen_keys: set[tuple[str, int, float, float]] = set()
        for i, raw in enumerate(records):
            err = _validate_record(i, raw)
            if err is not None:
                rejected.append(err)
                continue
            if raw["symbol"] != symbol:
                rejected.append(
                    _reject(
                        i,
                        "symbol_mismatch",
                        f"修正记录的 symbol 必须与路径标的一致（{symbol}）",
                        raw,
                    )
                )
                continue
            ts = raw["ts"]
            if ts < start or ts >= end:
                rejected.append(
                    _reject(
                        i,
                        "tick_out_of_range",
                        f"点位 ts={ts} 不在修正区间 [{start}, {end}) 内",
                        raw,
                    )
                )
                continue
            key = (raw["symbol"], ts, float(raw["price"]), float(raw["size"]))
            if key in seen_keys:
                rejected.append(
                    _reject(
                        i,
                        "duplicate_tick_in_batch",
                        "同一修正集合内完全相同的点位（symbol/ts/price/size）重复",
                        raw,
                    )
                )
                continue
            seen_keys.add(key)
            ticks.append(
                StoredTick(
                    batch_id=correction_id,
                    batch_index=i,
                    symbol=symbol,
                    ts=ts,
                    price=float(raw["price"]),
                    size=float(raw["size"]),
                )
            )

        if rejected:
            logger.info(
                "apply_correction rejected id=%s rejected=%s",
                correction_id,
                json.dumps([r.model_dump() for r in rejected], ensure_ascii=False),
            )
            code = "records_out_of_range"
            if any(r.reason not in ("symbol_mismatch", "tick_out_of_range") for r in rejected):
                code = "invalid_records"
            raise InvalidBatchError(
                f"{len(rejected)} 条修正记录无效，整次修正拒绝（未改动任何数据）",
                rejected_records=rejected,
                code=code,
            )

        payload_hash = _canonical_correction_payload(
            correction_id, symbol, start, end, ticks
        )

        # ---- 幂等检查（同 id 同内容 -> duplicate；内容不一致 -> 409） ----
        existing = self.store.get_batch(correction_id)
        if existing is not None:
            if existing.kind != "correction":
                raise BatchConflictError(
                    f"{correction_id} 已被普通写入批次占用，不能作为修正 ID",
                    code="correction_id_conflict",
                )
            if existing.payload_hash != payload_hash:
                logger.info(
                    "apply_correction conflict id=%s stored_hash=%s new_hash=%s",
                    correction_id,
                    existing.payload_hash,
                    payload_hash,
                )
                raise BatchConflictError(
                    f"correction_id={correction_id} 已存在但修正负载与本次请求不一致",
                    code="correction_id_conflict",
                )
            logger.info(
                "apply_correction duplicate id=%s accepted=%d -> 幂等返回",
                correction_id,
                existing.accepted,
            )
            return {
                "correction_id": correction_id,
                "symbol": symbol,
                "start": start,
                "end": end,
                "status": "duplicate",
                "removed": 0,
                "inserted": existing.accepted,
                "affected_windows": [],
            }

        # ---- 原子替换 ---------------------------------------------------
        try:
            result = self.store.apply_correction(
                correction_id, payload_hash, symbol, start, end, ticks
            )
        except Exception as exc:  # 极端竞态：并发同 correction_id
            existing = self.store.get_batch(correction_id)
            if existing is not None:
                if existing.kind != "correction":
                    raise BatchConflictError(
                        f"{correction_id} 已被普通写入批次占用，不能作为修正 ID",
                        code="correction_id_conflict",
                    ) from exc
                if existing.payload_hash != payload_hash:
                    raise BatchConflictError(
                        f"correction_id={correction_id} 已存在但修正负载与本次请求不一致",
                        code="correction_id_conflict",
                    ) from exc
                logger.info(
                    "apply_correction duplicate(after race) id=%s", correction_id
                )
                return {
                    "correction_id": correction_id,
                    "symbol": symbol,
                    "start": start,
                    "end": end,
                    "status": "duplicate",
                    "removed": 0,
                    "inserted": existing.accepted,
                    "affected_windows": [],
                }
            raise

        affected_sorted = sorted(
            f"{sym}@{ws}" for sym, ws in result["affected"]
        )
        logger.info(
            "apply_correction applied id=%s removed=%d inserted=%d windows=%s",
            correction_id,
            result["removed"],
            result["inserted"],
            affected_sorted,
        )
        return {
            "correction_id": correction_id,
            "symbol": symbol,
            "start": start,
            "end": end,
            "status": "applied",
            "removed": result["removed"],
            "inserted": result["inserted"],
            "affected_windows": affected_sorted,
        }

    # ------------------------------------------------------------------
    def verify_symbol(self, symbol: object) -> dict:
        """以 ticks 事实表重算 K 线并与 candles 物化结果逐窗口对照。"""
        if not isinstance(symbol, str) or not symbol.strip():
            raise InvalidBatchError("symbol 必须为非空字符串", code="invalid_symbol")
        result = self.store.verify_symbol(symbol)

        def _to_candle(row: tuple | None):
            if row is None:
                return None
            return Candle(
                symbol=row[0],
                window_start=row[1],
                open=row[2],
                high=row[3],
                low=row[4],
                close=row[5],
                volume=row[6],
                tick_count=row[7],
            ).model_dump()

        mismatches = [
            {
                "window_start": m["window_start"],
                "expected": _to_candle(m["expected"]),
                "actual": _to_candle(m["actual"]),
            }
            for m in result["mismatches"]
        ]
        consistent = not mismatches
        logger.info(
            "verify_symbol symbol=%s consistent=%s checked=%d mismatches=%d",
            symbol,
            consistent,
            result["checked"],
            len(mismatches),
        )
        return {
            "symbol": symbol,
            "consistent": consistent,
            "checked_windows": result["checked"],
            "mismatches": mismatches,
        }
