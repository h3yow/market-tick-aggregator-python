"""本地持久化层（SQLite）。

设计要点
========
* 五张表：``batches``（批次/修正幂等键 + 负载指纹）、``ticks``（行情点事实表，
  为唯一数据真相）、``candles``（分钟 K 线物化视图，可由 ticks 完全重建）、
  ``corrections``（区间整体替换式修正元数据）、``correction_parts`` 与
  ``correction_staging``（修正分片的幂等记录与暂存点位）。
* 进程内单连接 + ``RLock`` 串行化所有写事务；每个批次/每次修正的写入与受影响
  窗口重建在**同一个事务**内完成，查询永远只能看到提交前或提交后的完整状态，
  不会出现部分可见。
* 同批次 ID 重复写入由 ``batches`` 主键拦截，杜绝重复累计。
* 区间替换式修正不会物理删除旧点位：区间内被替换的点位以
  ``superseded_by_correction`` 标记失效（保留行供审计），仅「当前有效点位」
  参与聚合；可随时用 verify 按有效点位重算与物化 K 线对账。
* OHLC 对同一毫秒内多笔成交采用与到达顺序无关的确定性决胜
  （``ts, price, size, batch_id, batch_index``），因此任意顺序重放结果一致。
"""

from __future__ import annotations

import json
import os
import sqlite3
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone

WINDOW_MS = 60_000

# 多连接并发（例如多进程/多实例指向同一文件）遭遇 SQLITE_BUSY 时的重试策略
_TX_BUSY_RETRIES = 50
_TX_BUSY_SLEEP_S = 0.01


def _is_busy_error(exc: BaseException | None) -> bool:
    if not isinstance(exc, sqlite3.OperationalError):
        return False
    msg = str(exc).lower()
    return "locked" in msg or "busy" in msg


class CorrectionHeadConflict(Exception):
    """并发暂存时发现同名修正头已按不同元数据建立，或修正已生效。"""

_SCHEMA = """
CREATE TABLE IF NOT EXISTS corrections (
    correction_id TEXT PRIMARY KEY,
    symbol        TEXT NOT NULL,
    start_ts      INTEGER NOT NULL,
    end_ts        INTEGER NOT NULL,
    total_parts   INTEGER NOT NULL,
    payload_hash  TEXT NOT NULL DEFAULT '',
    status        TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    applied_at_ms INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS batches (
    batch_id      TEXT PRIMARY KEY,
    kind          TEXT NOT NULL DEFAULT 'ingest',
    payload_hash  TEXT NOT NULL,
    received      INTEGER NOT NULL,
    accepted      INTEGER NOT NULL,
    rejected      INTEGER NOT NULL,
    rejected_json TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS correction_parts (
    correction_id TEXT NOT NULL,
    part_index    INTEGER NOT NULL,
    part_hash     TEXT NOT NULL,
    received      INTEGER NOT NULL,
    accepted      INTEGER NOT NULL,
    PRIMARY KEY (correction_id, part_index),
    FOREIGN KEY (correction_id) REFERENCES corrections(correction_id)
);

CREATE TABLE IF NOT EXISTS correction_staging (
    correction_id TEXT NOT NULL,
    part_index    INTEGER NOT NULL,
    tick_index    INTEGER NOT NULL,
    symbol        TEXT NOT NULL,
    ts            INTEGER NOT NULL,
    price         REAL NOT NULL,
    size          REAL NOT NULL,
    PRIMARY KEY (correction_id, part_index, tick_index),
    FOREIGN KEY (correction_id) REFERENCES corrections(correction_id)
);

CREATE TABLE IF NOT EXISTS ticks (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id    TEXT NOT NULL REFERENCES batches(batch_id),
    batch_index INTEGER NOT NULL,
    symbol      TEXT NOT NULL,
    ts          INTEGER NOT NULL,
    price       REAL NOT NULL,
    size        REAL NOT NULL,
    superseded_by_correction TEXT NOT NULL DEFAULT '',
    UNIQUE(batch_id, batch_index)
);

CREATE INDEX IF NOT EXISTS idx_ticks_symbol_ts ON ticks(symbol, ts);

CREATE TABLE IF NOT EXISTS candles (
    symbol       TEXT NOT NULL,
    window_start INTEGER NOT NULL,
    open         REAL NOT NULL,
    high         REAL NOT NULL,
    low          REAL NOT NULL,
    close        REAL NOT NULL,
    volume       REAL NOT NULL,
    tick_count   INTEGER NOT NULL,
    PRIMARY KEY (symbol, window_start)
);
"""

# 旧库（区间修正功能上线前）需要补齐的列
_COLUMN_MIGRATIONS = {
    "batches": [("kind", "TEXT NOT NULL DEFAULT 'ingest'")],
    "ticks": [("superseded_by_correction", "TEXT NOT NULL DEFAULT ''")],
}


@dataclass(frozen=True)
class StoredTick:
    batch_id: str
    batch_index: int
    symbol: str
    ts: int
    price: float
    size: float


@dataclass(frozen=True)
class StoredBatch:
    batch_id: str
    payload_hash: str
    received: int
    accepted: int
    rejected: int
    rejected_records: list[dict]
    created_at_ms: int
    kind: str = "ingest"


@dataclass(frozen=True)
class StoredCorrection:
    correction_id: str
    symbol: str
    start_ts: int
    end_ts: int
    total_parts: int
    payload_hash: str
    status: str  # pending / applied
    created_at_ms: int
    applied_at_ms: int


@dataclass(frozen=True)
class StagedTick:
    part_index: int
    tick_index: int
    symbol: str
    ts: int
    price: float
    size: float


def window_start_of(ts_ms: int) -> int:
    """时间戳向下取整到分钟窗口起点（毫秒）。"""
    return (ts_ms // WINDOW_MS) * WINDOW_MS


class TickStore:
    """线程安全的行情点 / 批次 / K 线 SQLite 存储。"""

    def __init__(self, db_path: str, busy_timeout_ms: int = 10_000) -> None:
        self.db_path = db_path
        self.busy_timeout_ms = busy_timeout_ms
        self._lock = threading.RLock()
        if db_path != ":memory:":
            directory = os.path.dirname(os.path.abspath(db_path))
            os.makedirs(directory, exist_ok=True)
        self._conn = sqlite3.connect(
            db_path,
            check_same_thread=False,
            isolation_level=None,  # 手工事务
            timeout=busy_timeout_ms / 1000,
        )
        self._conn.row_factory = sqlite3.Row
        self._configure()
        self._create_schema()

    def _configure(self) -> None:
        # busy_timeout 必须最先设置：后续 PRAGMA/DDL 与首个连接的 WAL 模式
        # 切换都可能与其它连接的初始化发生瞬时锁冲突。
        self._exec_with_retry(
            lambda: self._conn.execute(
                f"PRAGMA busy_timeout={int(self.busy_timeout_ms)}"
            )
        )
        for sql in (
            "PRAGMA journal_mode=WAL",
            "PRAGMA synchronous=FULL",
            "PRAGMA foreign_keys=ON",
            "PRAGMA temp_store=MEMORY",
        ):
            self._exec_with_retry(lambda sql=sql: self._conn.execute(sql))

    def _create_schema(self) -> None:
        # executescript 会先隐式 COMMIT，故表创建放在显式事务之外；
        # 所有 DDL 均为 IF NOT EXISTS，崩溃后重开可幂等补建。
        self._exec_with_retry(lambda: self._conn.executescript(_SCHEMA))
        self._run_transaction(self._migrate_columns_locked)

    def _migrate_columns_locked(self) -> None:
        """对功能上线前的旧库补齐新增列（幂等执行）。"""
        for table, columns in _COLUMN_MIGRATIONS.items():
            existing = {
                r["name"]
                for r in self._conn.execute(
                    f"PRAGMA table_info({table})"
                ).fetchall()
            }
            for name, decl in columns:
                if name not in existing:
                    self._conn.execute(
                        f"ALTER TABLE {table} ADD COLUMN {name} {decl}"
                    )

    def _exec_with_retry(self, action, attempts: int = _TX_BUSY_RETRIES):
        """执行单条语句/脚本，遇到 BUSY/LOCKED 做有限退避重试。"""
        last_exc: BaseException | None = None
        for i in range(attempts + 1):
            try:
                return action()
            except Exception as exc:
                last_exc = exc
                if i < attempts and _is_busy_error(exc):
                    time.sleep(_TX_BUSY_SLEEP_S)
                    continue
                raise
        raise last_exc  # pragma: no cover

    def close(self) -> None:
        with self._lock:
            try:
                self._conn.close()
            except sqlite3.Error:
                pass

    # ------------------------------------------------------------------
    # 批次
    # ------------------------------------------------------------------
    def get_batch(self, batch_id: str) -> StoredBatch | None:
        with self._lock:
            row = self._exec_with_retry(
                lambda: self._conn.execute(
                    "SELECT * FROM batches WHERE batch_id = ?", (batch_id,)
                ).fetchone()
            )
        if row is None:
            return None
        return StoredBatch(
            batch_id=row["batch_id"],
            payload_hash=row["payload_hash"],
            received=row["received"],
            accepted=row["accepted"],
            rejected=row["rejected"],
            rejected_records=json.loads(row["rejected_json"]),
            created_at_ms=row["created_at_ms"],
            kind=row["kind"] if "kind" in row.keys() else "ingest",
        )

    # ------------------------------------------------------------------
    # 写入（事务）
    # ------------------------------------------------------------------
    def _run_transaction(self, operation) -> None:
        """在 ``BEGIN IMMEDIATE`` 事务中执行 ``operation()``。

        进程内 RLock 串行化同一实例的写入；当另有连接（其它实例/进程）持锁时，
        依赖 busy_timeout 等待，并在收到 SQLITE_BUSY/LOCKED 时做有限次退避重试，
        保证并发写入不会因为瞬时锁而直接失败。
        """
        attempts = 0
        with self._lock:
            while True:
                try:
                    self._conn.execute("BEGIN IMMEDIATE")
                    operation()
                    self._conn.execute("COMMIT")
                    return
                except Exception:
                    try:
                        self._conn.execute("ROLLBACK")
                    except sqlite3.Error:
                        pass
                    if (
                        attempts < _TX_BUSY_RETRIES
                        and _is_busy_error(sys.exc_info()[1])
                    ):
                        attempts += 1
                        time.sleep(_TX_BUSY_SLEEP_S)
                        continue
                    raise

    def insert_batch(
        self,
        batch_id: str,
        payload_hash: str,
        rejected_records: list[dict],
        ticks: list[StoredTick],
    ) -> set[tuple[str, int]]:
        """原子写入一个已通过校验的批次并重算受影响窗口。

        返回受影响的 ``(symbol, window_start)`` 集合。
        若批次 ID 已存在则抛出 :class:`sqlite3.IntegrityError`（调用方负责转译）。
        """
        affected: set[tuple[str, int]] = {
            (t.symbol, window_start_of(t.ts)) for t in ticks
        }
        now_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)

        def _op() -> None:
            self._conn.execute(
                """
                INSERT INTO batches(batch_id, kind, payload_hash, received, accepted,
                                    rejected, rejected_json, created_at_ms)
                VALUES (?, 'ingest', ?, ?, ?, ?, ?, ?)
                """,
                (
                    batch_id,
                    payload_hash,
                    len(ticks) + len(rejected_records),
                    len(ticks),
                    len(rejected_records),
                    json.dumps(rejected_records, ensure_ascii=False, sort_keys=True),
                    now_ms,
                ),
            )
            self._conn.executemany(
                """
                INSERT INTO ticks(batch_id, batch_index, symbol, ts, price, size)
                VALUES (:batch_id, :batch_index, :symbol, :ts, :price, :size)
                """,
                [
                    {
                        "batch_id": t.batch_id,
                        "batch_index": t.batch_index,
                        "symbol": t.symbol,
                        "ts": t.ts,
                        "price": t.price,
                        "size": t.size,
                    }
                    for t in ticks
                ],
            )
            self._recompute_windows_locked(sorted(affected))

        self._run_transaction(_op)
        return affected

    # ------------------------------------------------------------------
    # 聚合 / 重建
    # ------------------------------------------------------------------
    def _fetch_window_ticks_locked(
        self, symbol: str, ws: int
    ) -> list[StoredTick]:
        rows = self._conn.execute(
            """
            SELECT batch_id, batch_index, symbol, ts, price, size
            FROM ticks
            WHERE symbol = ? AND ts >= ? AND ts < ?
              AND superseded_by_correction = ''
            ORDER BY ts ASC, price ASC, size ASC, batch_id ASC, batch_index ASC
            """,
            (symbol, ws, ws + WINDOW_MS),
        ).fetchall()
        return [
            StoredTick(
                batch_id=r["batch_id"],
                batch_index=r["batch_index"],
                symbol=r["symbol"],
                ts=r["ts"],
                price=r["price"],
                size=r["size"],
            )
            for r in rows
        ]

    def _upsert_window_locked(self, symbol: str, ws: int, ticks: list[StoredTick]) -> None:
        if not ticks:
            self._conn.execute(
                "DELETE FROM candles WHERE symbol = ? AND window_start = ?",
                (symbol, ws),
            )
            return
        first = ticks[0]
        last = ticks[-1]
        high = max(t.price for t in ticks)
        low = min(t.price for t in ticks)
        volume = sum(t.size for t in ticks)
        self._conn.execute(
            """
            INSERT INTO candles(symbol, window_start, open, high, low, close,
                                volume, tick_count)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol, window_start) DO UPDATE SET
                open=excluded.open, high=excluded.high, low=excluded.low,
                close=excluded.close, volume=excluded.volume,
                tick_count=excluded.tick_count
            """,
            (symbol, ws, first.price, high, low, last.price, volume, len(ticks)),
        )

    def _recompute_windows_locked(self, windows: list[tuple[str, int]]) -> None:
        for symbol, ws in windows:
            ticks = self._fetch_window_ticks_locked(symbol, ws)
            self._upsert_window_locked(symbol, ws, ticks)

    # ------------------------------------------------------------------
    # 区间整体替换式修正
    # ------------------------------------------------------------------
    def get_correction(self, correction_id: str) -> StoredCorrection | None:
        with self._lock:
            row = self._exec_with_retry(
                lambda: self._conn.execute(
                    "SELECT * FROM corrections WHERE correction_id = ?",
                    (correction_id,),
                ).fetchone()
            )
        if row is None:
            return None
        return StoredCorrection(
            correction_id=row["correction_id"],
            symbol=row["symbol"],
            start_ts=row["start_ts"],
            end_ts=row["end_ts"],
            total_parts=row["total_parts"],
            payload_hash=row["payload_hash"],
            status=row["status"],
            created_at_ms=row["created_at_ms"],
            applied_at_ms=row["applied_at_ms"],
        )

    def get_part_hashes(self, correction_id: str) -> dict[int, str]:
        with self._lock:
            rows = self._exec_with_retry(
                lambda: self._conn.execute(
                    "SELECT part_index, part_hash FROM correction_parts "
                    "WHERE correction_id = ?",
                    (correction_id,),
                ).fetchall()
            )
        return {int(r["part_index"]): r["part_hash"] for r in rows}

    def stage_correction_part(
        self,
        correction: StoredCorrection,
        part_index: int,
        part_hash: str,
        ticks: list[StagedTick],
    ) -> str:
        """暂存修正分片。

        返回 `"complete"` 表示本片新暂存后全部分片已到齐（应由调用方提交）；
        `"incomplete"` 表示仍有其它片未到齐。本片是否已存在由调用方先比对 part_hash。
        若并发请求已建立元数据不一致的修正头，抛出
        :class:`CorrectionHeadConflict`，由调用方转译为 409。
        """
        now_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)

        def _op() -> None:
            # 首片到达时建立修正头；头已存在则必须与本次元数据完全一致，
            # 避免并发下把分片挂到元数据不同的修正头上。
            created = self._conn.execute(
                """
                INSERT OR IGNORE INTO corrections(correction_id, symbol, start_ts,
                                                  end_ts, total_parts, payload_hash,
                                                  status, created_at_ms, applied_at_ms)
                VALUES (?, ?, ?, ?, ?, '', 'pending', ?, 0)
                """,
                (
                    correction.correction_id,
                    correction.symbol,
                    correction.start_ts,
                    correction.end_ts,
                    correction.total_parts,
                    now_ms,
                ),
            ).rowcount
            if created == 0:
                row = self._conn.execute(
                    "SELECT symbol, start_ts, end_ts, total_parts, status "
                    "FROM corrections WHERE correction_id = ?",
                    (correction.correction_id,),
                ).fetchone()
                if row is None:  # pragma: no cover - 极端竞态防御
                    raise sqlite3.OperationalError("correction head vanished")
                if row["status"] != "pending":
                    raise CorrectionHeadConflict("already applied")
                if (
                    row["symbol"] != correction.symbol
                    or int(row["start_ts"]) != correction.start_ts
                    or int(row["end_ts"]) != correction.end_ts
                    or int(row["total_parts"]) != correction.total_parts
                ):
                    raise CorrectionHeadConflict("metadata mismatch")
            self._conn.execute(
                """
                INSERT INTO correction_parts(correction_id, part_index, part_hash,
                                             received, accepted)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    correction.correction_id,
                    part_index,
                    part_hash,
                    len(ticks),
                    len(ticks),
                ),
            )
            self._conn.executemany(
                """
                INSERT INTO correction_staging(correction_id, part_index, tick_index,
                                               symbol, ts, price, size)
                VALUES (:correction_id, :part_index, :tick_index, :symbol,
                        :ts, :price, :size)
                """,
                [
                    {
                        "correction_id": correction.correction_id,
                        "part_index": part_index,
                        "tick_index": t.tick_index,
                        "symbol": t.symbol,
                        "ts": t.ts,
                        "price": t.price,
                        "size": t.size,
                    }
                    for t in ticks
                ],
            )

        self._run_transaction(_op)

        with self._lock:
            count = self._exec_with_retry(
                lambda: self._conn.execute(
                    "SELECT COUNT(*) AS n FROM correction_parts WHERE correction_id = ?",
                    (correction.correction_id,),
                ).fetchone()
            )["n"]
        return "complete" if int(count) == correction.total_parts else "incomplete"

    def fetch_staging(self, correction_id: str) -> list[StagedTick]:
        """读取某次修正当前已暂存的全部点位（供服务层跨片查重）。"""
        with self._lock:
            return self._fetch_staging_locked(correction_id)

    def _fetch_staging_locked(self, correction_id: str) -> list[StagedTick]:
        rows = self._conn.execute(
            """
            SELECT part_index, tick_index, symbol, ts, price, size
            FROM correction_staging
            WHERE correction_id = ?
            """,
            (correction_id,),
        ).fetchall()
        return [
            StagedTick(
                part_index=int(r["part_index"]),
                tick_index=int(r["tick_index"]),
                symbol=r["symbol"],
                ts=int(r["ts"]),
                price=float(r["price"]),
                size=float(r["size"]),
            )
            for r in rows
        ]

    def commit_correction(self, correction: StoredCorrection) -> set[tuple[str, int]]:
        """把暂存分片整体落库：替换区间内有效点位并重算受影响窗口。

        全部操作在单个 ``BEGIN IMMEDIATE`` 事务内完成。若修正已被并发提交，
        抛出 :class:`sqlite3.IntegrityError`（batches 主键冲突），调用方转译
        为幂等结果。
        """
        now_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
        cid = correction.correction_id

        def _op() -> None:
            staged = self._fetch_staging_locked(cid)
            # 与到达顺序/拆分方式无关：按同一确定性全序排列后分配全局序号
            staged.sort(
                key=lambda t: (t.ts, t.price, t.size, t.part_index, t.tick_index)
            )
            final_ticks = [
                StoredTick(
                    batch_id=cid,
                    batch_index=i,
                    symbol=t.symbol,
                    ts=t.ts,
                    price=t.price,
                    size=t.size,
                )
                for i, t in enumerate(staged)
            ]

            affected: set[tuple[str, int]] = {
                (t.symbol, window_start_of(t.ts)) for t in final_ticks
            }

            # 1) 找到区间内当前有效点位（必须在标记取代之前查询）
            old_rows = self._conn.execute(
                """
                SELECT ts FROM ticks
                WHERE symbol = ? AND ts >= ? AND ts < ?
                  AND superseded_by_correction = ''
                """,
                (correction.symbol, correction.start_ts, correction.end_ts),
            ).fetchall()
            for r in old_rows:
                affected.add((correction.symbol, window_start_of(int(r["ts"]))))

            # 2) 区间内有效点位整体标记失效（行保留，可审计）
            self._conn.execute(
                """
                UPDATE ticks SET superseded_by_correction = ?
                WHERE symbol = ? AND ts >= ? AND ts < ?
                  AND superseded_by_correction = ''
                """,
                (cid, correction.symbol, correction.start_ts, correction.end_ts),
            )

            # 3) 修正批次登记（与普通批次共用 batches 幂等表）
            self._conn.execute(
                """
                INSERT INTO batches(batch_id, kind, payload_hash, received, accepted,
                                    rejected, rejected_json, created_at_ms)
                VALUES (?, 'correction', ?, ?, ?, 0, '[]', ?)
                """,
                (cid, correction.payload_hash, len(final_ticks), len(final_ticks), now_ms),
            )

            # 4) 写入修正点位
            self._conn.executemany(
                """
                INSERT INTO ticks(batch_id, batch_index, symbol, ts, price, size,
                                  superseded_by_correction)
                VALUES (:batch_id, :batch_index, :symbol, :ts, :price, :size, '')
                """,
                [
                    {
                        "batch_id": t.batch_id,
                        "batch_index": t.batch_index,
                        "symbol": t.symbol,
                        "ts": t.ts,
                        "price": t.price,
                        "size": t.size,
                    }
                    for t in final_ticks
                ],
            )

            # 5) 重算所有真正受影响的窗口（只影响该标的、只影响这些窗口）
            self._recompute_windows_locked(sorted(affected))

            # 6) 修正头置为 applied；暂存数据已无用，删除（审计事实保留在 ticks）
            self._conn.execute(
                """
                UPDATE corrections
                SET status = 'applied', payload_hash = ?, applied_at_ms = ?
                WHERE correction_id = ?
                """,
                (correction.payload_hash, now_ms, cid),
            )
            self._conn.execute(
                "DELETE FROM correction_staging WHERE correction_id = ?", (cid,)
            )

        self._run_transaction(_op)

        # 返回事后计算的受影响窗口（仅用于响应展示，不参与事务）
        with self._lock:
            rows = self._exec_with_retry(
                lambda: self._conn.execute(
                    """
                    SELECT DISTINCT ts FROM ticks
                    WHERE batch_id = ? OR superseded_by_correction = ?
                    """,
                    (cid, cid),
                ).fetchall()
            )
        return {(correction.symbol, window_start_of(int(r["ts"]))) for r in rows}

    # ------------------------------------------------------------------
    # 对账：按当前有效点位重算，与物化 K 线比对
    # ------------------------------------------------------------------
    def verify_symbol(self, symbol: str) -> dict:
        """按当前有效点位重算并与物化 K 线比对。

        返回 ``{"checked_windows": int, "mismatches": [...]}``；
        ``mismatches`` 为空表示全部一致。
        """
        with self._lock:
            def _snapshot():
                # 同一只读事务内取两张表的快照，避免对账途中被其它写入撕裂
                self._conn.execute("BEGIN")
                try:
                    candle_rows = self._conn.execute(
                        """
                        SELECT symbol, window_start, open, high, low, close, volume,
                               tick_count
                        FROM candles WHERE symbol = ? ORDER BY window_start ASC
                        """,
                        (symbol,),
                    ).fetchall()
                    tick_rows = self._conn.execute(
                        """
                        SELECT batch_id, batch_index, symbol, ts, price, size
                        FROM ticks
                        WHERE symbol = ? AND superseded_by_correction = ''
                        ORDER BY ts ASC, price ASC, size ASC, batch_id ASC,
                                 batch_index ASC
                        """,
                        (symbol,),
                    ).fetchall()
                finally:
                    self._conn.execute("COMMIT")
                return candle_rows, tick_rows

            rows, active_rows = self._exec_with_retry(_snapshot)

        stored = {int(r["window_start"]): tuple(r) for r in rows}
        expected: dict[int, tuple] = {}
        for r in active_rows:
            ws = window_start_of(int(r["ts"]))
            bucket = expected.setdefault(ws, [])
            bucket.append(r)

        mismatches: list[dict] = []
        for ws in sorted(set(stored) | set(expected)):
            s = stored.get(ws)
            if ws not in expected:
                mismatches.append(
                    {
                        "symbol": symbol,
                        "window_start": ws,
                        "kind": "stale_candle",
                        "stored": self._candle_tuple_to_dict(s),
                        "expected": None,
                    }
                )
                continue
            ticks = [
                StoredTick(
                    batch_id=r["batch_id"],
                    batch_index=int(r["batch_index"]),
                    symbol=r["symbol"],
                    ts=int(r["ts"]),
                    price=float(r["price"]),
                    size=float(r["size"]),
                )
                for r in expected[ws]
            ]
            exp = self._aggregate_window(symbol, ws, ticks)
            if s is None:
                mismatches.append(
                    {
                        "symbol": symbol,
                        "window_start": ws,
                        "kind": "missing_candle",
                        "stored": None,
                        "expected": self._candle_tuple_to_dict(exp),
                    }
                )
            elif tuple(s[2:]) != tuple(exp[2:]):
                mismatches.append(
                    {
                        "symbol": symbol,
                        "window_start": ws,
                        "kind": "value_mismatch",
                        "stored": self._candle_tuple_to_dict(s),
                        "expected": self._candle_tuple_to_dict(exp),
                    }
                )
        return {"checked_windows": len(set(stored) | set(expected)),
                "mismatches": mismatches}

    @staticmethod
    def _aggregate_window(
        symbol: str, ws: int, ticks: list[StoredTick]
    ) -> tuple:
        first = ticks[0]
        last = ticks[-1]
        return (
            symbol,
            ws,
            first.price,
            max(t.price for t in ticks),
            min(t.price for t in ticks),
            last.price,
            sum(t.size for t in ticks),
            len(ticks),
        )

    @staticmethod
    def _candle_tuple_to_dict(row: tuple | None) -> object:
        if row is None:
            return None
        return {
            "symbol": row[0],
            "window_start": int(row[1]),
            "open": row[2],
            "high": row[3],
            "low": row[4],
            "close": row[5],
            "volume": row[6],
            "tick_count": int(row[7]),
        }

    # ------------------------------------------------------------------
    # 聚合 / 重建
    # ------------------------------------------------------------------
    def rebuild_symbol(self, symbol: str) -> int:
        """以 ticks 事实表为准，重建某标的全部分钟 K 线，返回重建窗口数。"""
        with self._lock:
            rows = self._exec_with_retry(
                lambda: self._conn.execute(
                    "SELECT DISTINCT (ts / ?) AS ws FROM ticks "
                    "WHERE symbol = ? AND superseded_by_correction = ''",
                    (WINDOW_MS, symbol),
                ).fetchall()
            )
        windows = [(symbol, int(r["ws"]) * WINDOW_MS) for r in rows]

        def _op() -> None:
            self._recompute_windows_locked(windows)
            # 清理事实表中已不存在的陈旧窗口（例如手工修正数据后重建）
            if windows:
                placeholders = ",".join("?" for _ in windows)
                params = [symbol] + [ws for _, ws in windows]
                self._conn.execute(
                    f"DELETE FROM candles WHERE symbol = ? AND window_start "
                    f"NOT IN ({placeholders})",
                    params,
                )
            else:
                self._conn.execute(
                    "DELETE FROM candles WHERE symbol = ?", (symbol,)
                )

        self._run_transaction(_op)
        return len(windows)

    # ------------------------------------------------------------------
    # 查询
    # ------------------------------------------------------------------
    def query_candles(
        self, symbol: str, start_ms: int, end_ms: int
    ) -> list[tuple]:
        """返回窗口起点落在 ``[floor(start), floor(end)]`` 的 K 线，按时间升序。"""
        start_ws = window_start_of(max(start_ms, 0))
        end_ws = window_start_of(max(end_ms, 0))

        def _query():
            return self._conn.execute(
                """
                SELECT symbol, window_start, open, high, low, close, volume,
                       tick_count
                FROM candles
                WHERE symbol = ? AND window_start >= ? AND window_start <= ?
                ORDER BY window_start ASC
                """,
                (symbol, start_ws, end_ws),
            ).fetchall()

        with self._lock:
            rows = self._exec_with_retry(_query)
        return [tuple(r) for r in rows]
