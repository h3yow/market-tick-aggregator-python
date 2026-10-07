"""领域服务：逐条校验、批次幂等、分钟聚合、区间整体替换式修正与重建。

关键规则
========
* **逐条校验**：每条记录独立判定并给出 ``reason``，无效记录不会被静默丢弃；
  只要批次中存在任何无效记录，整批拒绝（HTTP 422，不写入任何数据），
  保证调用方必须显式修正后重试，语义可解释。
* **批次幂等**：相同 ``batch_id`` 携带相同负载重放返回首次结果（``duplicate``），
  不重复累计；负载不一致则返回 409（``batch_id_conflict``）。
* **乱序/迟到**：ticks 是唯一事实表，K 线由确定性全序重算，
  因此同一输入集合与到达顺序无关；迟到点位正常入库并触发窗口重算。
* **区间替换式修正**：对 ``(symbol, [start, end))`` 半开时间区间，以提交的
  完整点位集合**替换**区间内当前有效点位（旧点位标记失效但保留审计行）；
  区间外（含跨界窗口中落在区间外的点位）一行不动。修正支持分片提交，
  同一份修正集合任意换序、任意拆分，最终 K 线完全一致。
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
import sqlite3

from .errors import (
    BatchConflictError,
    CorrectionConflictError,
    InvalidBatchError,
)
from .models import (
    MAX_BATCH_RECORDS,
    MAX_CORRECTION_RECORDS,
    MAX_SYMBOL_LENGTH,
    Candle,
    RejectedRecord,
)
from .storage import (
    CorrectionHeadConflict,
    StagedTick,
    StoredCorrection,
    StoredTick,
    TickStore,
)

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


def _canonical_part_hash(
    symbol: str, start: int, end: int, ticks: list[StagedTick]
) -> str:
    """修正分片的规范化指纹：与片内点位书写顺序无关。"""
    body = {
        "symbol": symbol,
        "start": start,
        "end": end,
        # tick_index 是该片内的书写下标，重排后不参与指纹语义；
        # 点位内容按规范化排序后参与哈希
        "ticks": sorted(
            (t.ts, t.price, t.size) for t in ticks
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
    # 区间整体替换式修正
    # ------------------------------------------------------------------
    def submit_correction_part(
        self,
        correction_id: object,
        symbol: object,
        start: object,
        end: object,
        total_parts: object,
        part_index: object,
        records: object,
    ) -> dict:
        """提交区间替换式修正的一个分片。返回可序列化的响应字典。"""
        # ---- 批次头参数校验 --------------------------------------------
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
        if end < start:
            raise InvalidBatchError("end 不能早于 start", code="invalid_range")
        if isinstance(total_parts, bool) or not isinstance(total_parts, int) or total_parts < 1:
            raise InvalidBatchError(
                "total_parts 必须为正整数", code="invalid_total_parts"
            )
        if (
            isinstance(part_index, bool)
            or not isinstance(part_index, int)
            or part_index < 0
            or part_index >= total_parts
        ):
            raise InvalidBatchError(
                "part_index 必须满足 0 <= part_index < total_parts",
                code="invalid_part_index",
            )
        if not isinstance(records, list):
            raise InvalidBatchError(
                "records 必须为数组（允许空数组，表示该片无点位）",
                code="invalid_records",
            )

        logger.info(
            "submit_correction_part start correction_id=%s symbol=%s range=[%d,%d) "
            "part=%d/%d received=%d records=%s",
            correction_id,
            symbol,
            start,
            end,
            part_index,
            total_parts,
            len(records),
            json.dumps(records, ensure_ascii=False, default=str),
        )

        # ---- 幂等头检查 --------------------------------------------------
        head = self.store.get_correction(correction_id)
        if head is None:
            # correction_id 与普通批次共用 batches 命名空间，不允许跨类型重名
            existing_batch = self.store.get_batch(correction_id)
            if existing_batch is not None:
                raise CorrectionConflictError(
                    f"correction_id={correction_id} 已被普通写入批次占用"
                )
        if head is not None:
            if (
                head.symbol != symbol
                or head.start_ts != start
                or head.end_ts != end
                or head.total_parts != total_parts
            ):
                raise CorrectionConflictError(
                    f"correction_id={correction_id} 已存在，但 "
                    "symbol/start/end/total_parts 与首次提交不一致"
                )

        # ---- 本片逐条校验（含区间/标的归属） -----------------------------
        staged_this, rejected = self._validate_correction_records(
            symbol, start, end, part_index, records
        )
        if rejected:
            raise InvalidBatchError(
                f"{len(rejected)} 条修正点位无效，本片整体拒绝（未写入任何数据）",
                rejected_records=rejected,
                code="invalid_records",
            )

        part_hash = _canonical_part_hash(symbol, start, end, staged_this)

        # ---- 已生效 / 已暂存：幂等与冲突判定 -----------------------------
        if head is not None and head.status == "applied":
            part_hashes = self.store.get_part_hashes(correction_id)
            if part_hashes.get(part_index) != part_hash:
                raise CorrectionConflictError(
                    f"修正 {correction_id} 已生效，但第 {part_index} 片负载与"
                    "首次提交不一致"
                )
            logger.info(
                "submit_correction_part duplicate(applied) correction_id=%s part=%d",
                correction_id,
                part_index,
            )
            return self._correction_response(
                correction_id, symbol, start, end, total_parts, part_index,
                "duplicate", len(records), len(staged_this), [],
            )

        part_hashes = self.store.get_part_hashes(correction_id) if head else {}
        if part_index in part_hashes:
            if part_hashes[part_index] != part_hash:
                raise CorrectionConflictError(
                    f"修正 {correction_id} 第 {part_index} 片此前已暂存，但负载"
                    "与本次不一致"
                )
            logger.info(
                "submit_correction_part duplicate(staged) correction_id=%s part=%d",
                correction_id,
                part_index,
            )
            return self._correction_response(
                correction_id, symbol, start, end, total_parts, part_index,
                "duplicate", len(records), len(staged_this), [],
            )

        # ---- 总量上限（按本片新增 + 已暂存估算） -------------------------
        already = sum(
            1 for _ in self.store.fetch_staging(correction_id)
        )
        if already + len(staged_this) > MAX_CORRECTION_RECORDS:
            raise InvalidBatchError(
                f"单次修正点位总数超过上限 {MAX_CORRECTION_RECORDS}",
                code="correction_too_large",
            )

        # ---- 跨片点位查重 ------------------------------------------------
        # 同一份修正集合可能分片乱序到达：不同片中不允许出现完全相同的点位，
        # 否则语义与「批内重复」一致地模糊（累计 vs 去重无法解释）。
        prior_keys = {
            (t.symbol, t.ts, t.price, t.size)
            for t in self.store.fetch_staging(correction_id)
        }
        cross_dup: list[RejectedRecord] = []
        seen_this: set[tuple[str, int, float, float]] = set()
        for i, raw in enumerate(records):
            key = (raw["symbol"], raw["ts"], float(raw["price"]), float(raw["size"]))
            if key in seen_this:
                continue  # 本片内重复已在逐条校验阶段拦截
            seen_this.add(key)
            if key in prior_keys:
                cross_dup.append(
                    _reject(
                        i,
                        "duplicate_tick_in_correction",
                        "该点位与此前已暂存的另一个分片完全重复",
                        raw,
                    )
                )
        if cross_dup:
            raise InvalidBatchError(
                f"{len(cross_dup)} 条点位与其它分片重复，本片整体拒绝",
                rejected_records=cross_dup,
                code="invalid_records",
            )

        head_obj = StoredCorrection(
            correction_id=correction_id,
            symbol=symbol,
            start_ts=start,
            end_ts=end,
            total_parts=total_parts,
            payload_hash="",
            status="pending",
            created_at_ms=0,
            applied_at_ms=0,
        )
        try:
            stage_result = self.store.stage_correction_part(
                head_obj, part_index, part_hash, staged_this
            )
        except CorrectionHeadConflict as exc:
            # 并发竞态：本请求此前未见修正头，暂存时发现已被其它连接建立/提交。
            latest = self.store.get_correction(correction_id)
            if latest is not None and latest.status == "applied":
                same_part = (
                    self.store.get_part_hashes(correction_id).get(part_index)
                    == part_hash
                )
                if same_part:
                    logger.info(
                        "submit_correction_part duplicate(after race) "
                        "correction_id=%s part=%d",
                        correction_id,
                        part_index,
                    )
                    return self._correction_response(
                        correction_id, symbol, start, end, total_parts, part_index,
                        "duplicate", len(records), len(staged_this), [],
                    )
            raise CorrectionConflictError(
                f"correction_id={correction_id} 已被并发请求以不同元数据占用或提交"
            ) from exc
        complete = stage_result == "complete"
        if not complete:
            logger.info(
                "submit_correction_part pending correction_id=%s part=%d/%d",
                correction_id,
                part_index + 1,
                total_parts,
            )
            return self._correction_response(
                correction_id, symbol, start, end, total_parts, part_index,
                "pending", len(records), len(staged_this), [],
            )

        # ---- 全部分片到齐：计算整体指纹并一次性替换提交 -----------------
        payload_hash = self._build_correction_payload_hash(
            correction_id, symbol, start, end
        )
        if head is not None and head.payload_hash and head.payload_hash != payload_hash:
            # 理论不可达：pending 阶段 payload_hash 为空；保留防御
            raise CorrectionConflictError(
                f"修正 {correction_id} 的整体负载指纹与既有状态不一致"
            )
        applied = StoredCorrection(
            correction_id=correction_id,
            symbol=symbol,
            start_ts=start,
            end_ts=end,
            total_parts=total_parts,
            payload_hash=payload_hash,
            status="applied",
            created_at_ms=0,
            applied_at_ms=0,
        )
        try:
            affected = self.store.commit_correction(applied)
        except sqlite3.IntegrityError as exc:
            # 极端竞态：其它连接/线程并发提交了最后一片
            latest = self.store.get_correction(correction_id)
            if latest is not None and latest.status == "applied":
                if latest.payload_hash != payload_hash:
                    raise CorrectionConflictError(
                        f"修正 {correction_id} 已由并发请求生效，但整体负载不一致"
                    ) from exc
                logger.info(
                    "submit_correction_part duplicate(after race) correction_id=%s",
                    correction_id,
                )
                return self._correction_response(
                    correction_id, symbol, start, end, total_parts, part_index,
                    "duplicate", len(records), len(staged_this), [],
                )
            raise

        affected_sorted = sorted(f"{s}@{ws}" for s, ws in affected)
        logger.info(
            "submit_correction_part applied correction_id=%s parts=%d ticks=%d "
            "windows=%s",
            correction_id,
            total_parts,
            len(records),
            affected_sorted,
        )
        return self._correction_response(
            correction_id, symbol, start, end, total_parts, part_index,
            "applied", len(records), len(staged_this), affected_sorted,
        )

    def _validate_correction_records(
        self,
        symbol: str,
        start: int,
        end: int,
        part_index: int,
        records: list,
    ) -> tuple[list[StagedTick], list[RejectedRecord]]:
        staged: list[StagedTick] = []
        rejected: list[RejectedRecord] = []
        seen: set[tuple[str, int, float, float]] = set()
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
                        f"点位 symbol 必须与修正标的 {symbol!r} 完全一致",
                        raw,
                    )
                )
                continue
            ts = raw["ts"]
            if not (start <= ts < end):
                rejected.append(
                    _reject(
                        i,
                        "tick_outside_range",
                        f"点位 ts={ts} 必须落在半开区间 [{start}, {end}) 内",
                        raw,
                    )
                )
                continue
            key = (raw["symbol"], ts, float(raw["price"]), float(raw["size"]))
            if key in seen:
                rejected.append(
                    _reject(
                        i,
                        "duplicate_tick_in_correction",
                        "同一修正分片内完全相同的点位重复",
                        raw,
                    )
                )
                continue
            seen.add(key)
            staged.append(
                StagedTick(
                    part_index=part_index,
                    tick_index=i,
                    symbol=raw["symbol"],
                    ts=ts,
                    price=float(raw["price"]),
                    size=float(raw["size"]),
                )
            )
        return staged, rejected

    def _build_correction_payload_hash(
        self, correction_id: str, symbol: str, start: int, end: int
    ) -> str:
        """整体指纹：只依赖修正集合内容与区间，不依赖分片方式与到达顺序。"""
        part_hashes = self.store.get_part_hashes(correction_id)
        body = {
            "correction_id": correction_id,
            "symbol": symbol,
            "start": start,
            "end": end,
            # 分片内容排序后聚合；每片自身指纹已与片内顺序无关
            "part_hashes": sorted(part_hashes.items()),
        }
        text = json.dumps(body, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(text.encode("utf-8")).hexdigest()

    @staticmethod
    def _correction_response(
        correction_id, symbol, start, end, total_parts, part_index,
        status, received, accepted, affected_windows,
    ) -> dict:
        return {
            "correction_id": correction_id,
            "symbol": symbol,
            "start": start,
            "end": end,
            "total_parts": total_parts,
            "part_index": part_index,
            "status": status,
            "received": received,
            "accepted": accepted,
            "affected_windows": affected_windows,
        }

    # ------------------------------------------------------------------
    def verify_symbol(self, symbol: object) -> dict:
        """按当前有效点位重算 K 线并与物化结果逐窗口对账。"""
        if not isinstance(symbol, str) or not symbol.strip():
            raise InvalidBatchError("symbol 必须为非空字符串", code="invalid_symbol")
        result = self.store.verify_symbol(symbol)
        mismatches = result["mismatches"]
        logger.info(
            "verify_symbol symbol=%s checked_windows=%d mismatches=%d detail=%s",
            symbol,
            result["checked_windows"],
            len(mismatches),
            json.dumps(mismatches, ensure_ascii=False),
        )
        return {
            "symbol": symbol,
            "ok": not mismatches,
            "checked_windows": result["checked_windows"],
            "mismatches": mismatches,
        }
