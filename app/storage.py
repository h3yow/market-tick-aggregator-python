"""本地持久化层（SQLite）。

设计要点
========
* 三张表：``batches``（批次幂等键 + 负载指纹）、``ticks``（行情点事实表，
  为唯一数据真相）、``candles``（分钟 K 线物化视图，可由 ticks 完全重建）。
* 进程内单连接 + ``RLock`` 串行化所有写事务；每个批次的写入与受影响窗口
  重建在**同一个事务**内完成，查询永远只能看到提交前或提交后的完整状态，
  不会出现部分可见。
* 同批次 ID 重复写入由 ``batches`` 主键拦截，杜绝重复累计。
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

_SCHEMA = """
CREATE TABLE IF NOT EXISTS batches (
    batch_id      TEXT PRIMARY KEY,
    payload_hash  TEXT NOT NULL,
    received      INTEGER NOT NULL,
    accepted      INTEGER NOT NULL,
    rejected      INTEGER NOT NULL,
    rejected_json TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS ticks (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id    TEXT NOT NULL REFERENCES batches(batch_id),
    batch_index INTEGER NOT NULL,
    symbol      TEXT NOT NULL,
    ts          INTEGER NOT NULL,
    price       REAL NOT NULL,
    size        REAL NOT NULL,
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
        def _ddl() -> None:
            self._conn.executescript(_SCHEMA)

        self._exec_with_retry(_ddl)

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
                INSERT INTO batches(batch_id, payload_hash, received, accepted,
                                    rejected, rejected_json, created_at_ms)
                VALUES (?, ?, ?, ?, ?, ?, ?)
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

    def rebuild_symbol(self, symbol: str) -> int:
        """以 ticks 事实表为准，重建某标的全部分钟 K 线，返回重建窗口数。"""
        with self._lock:
            rows = self._exec_with_retry(
                lambda: self._conn.execute(
                    "SELECT DISTINCT (ts / ?) AS ws FROM ticks WHERE symbol = ?",
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
