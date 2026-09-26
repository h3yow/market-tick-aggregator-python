"""SQLite 本地持久化层。

并发与一致性策略
----------------
- 开启 WAL（``PRAGMA journal_mode=WAL``），单写多读；``busy_timeout`` 处理
  跨进程/跨连接的写竞争。
- 进程内以 ``threading.RLock`` 串行化所有写事务，事务内使用
  ``BEGIN IMMEDIATE`` 立即获取写锁，保证"检查-写入"序列不会交错。
- 每个工作线程持有自己的连接（``threading.local``），连接不会跨线程共享。
- 点位主键：有业务 id 时为 ``(symbol, id)``；无 id 时为
  ``(symbol, "c:" + sha1(ts|price|qty|seq))`` 内容键，键相等即内容相等。
- 批次以 ``batch_id`` 幂等：重复批次直接返回首次结果，不产生二次写入。
"""

from __future__ import annotations

import json
import sqlite3
import threading
import time
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS ticks (
    symbol     TEXT    NOT NULL,
    tick_key   TEXT    NOT NULL,
    has_id     INTEGER NOT NULL,
    ts_ms      INTEGER NOT NULL,
    price      REAL    NOT NULL,
    quantity   REAL    NOT NULL,
    seq        INTEGER,
    batch_id   TEXT    NOT NULL,
    created_at_ms INTEGER NOT NULL,
    PRIMARY KEY (symbol, tick_key)
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_ticks_symbol_ts ON ticks(symbol, ts_ms);

CREATE TABLE IF NOT EXISTS batches (
    batch_id     TEXT PRIMARY KEY,
    accepted     INTEGER NOT NULL,
    rejected_json TEXT NOT NULL,
    duplicate    INTEGER NOT NULL,
    mode         TEXT NOT NULL,
    deleted_before INTEGER NOT NULL DEFAULT 0,
    created_at_ms INTEGER NOT NULL
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS rejected_records (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id   TEXT NOT NULL,
    item_index INTEGER NOT NULL,
    reason     TEXT NOT NULL,
    detail     TEXT NOT NULL,
    raw_json   TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_rejected_batch ON rejected_records(batch_id);
"""


def now_ms() -> int:
    return time.time_ns() // 1_000_000


# 已通过 service 层格式校验的标准行：
# (symbol, tick_key, has_id, ts_ms, price, quantity, seq, raw_index, raw_json)
AcceptedRow = tuple[str, str, int, int, float, float, int | None, int, str]
RejectedRow = tuple[int, str, str, str]  # index, reason, detail, raw_json


class TickStore:
    def __init__(self, db_path: str | Path = "data/market.db") -> None:
        self.db_path = str(db_path)
        self._write_lock = threading.RLock()
        self._local = threading.local()

    # -- 生命周期 -------------------------------------------------------
    def init_db(self) -> None:
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        conn = self.connect()
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=FULL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.executescript(SCHEMA)
        self._migrate(conn)

    @staticmethod
    def _migrate(conn: sqlite3.Connection) -> None:
        cols = {r[1] for r in conn.execute("PRAGMA table_info(batches)")}
        if "deleted_before" not in cols:
            conn.execute(
                "ALTER TABLE batches ADD COLUMN deleted_before "
                "INTEGER NOT NULL DEFAULT 0")

    def close(self) -> None:
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            conn.close()
            self._local.conn = None

    def connect(self) -> sqlite3.Connection:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = sqlite3.connect(
                self.db_path,
                timeout=30.0,
                isolation_level=None,  # autocommit；事务边界显式控制
                check_same_thread=False,
            )
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA busy_timeout=30000")
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA foreign_keys=ON")
            self._local.conn = conn
        return conn

    # -- 内部工具 -------------------------------------------------------
    @staticmethod
    def _existing_map(
        conn: sqlite3.Connection, symbol: str, keys: list[str]
    ) -> dict[str, sqlite3.Row]:
        if not keys:
            return {}
        marks = ",".join("?" for _ in keys)
        rows = conn.execute(
            f"SELECT tick_key, ts_ms, price, quantity, seq FROM ticks "
            f"WHERE symbol = ? AND tick_key IN ({marks})",
            [symbol, *keys],
        ).fetchall()
        return {r["tick_key"]: r for r in rows}

    def _record_rejected(
        self, conn: sqlite3.Connection, batch_id: str, rejected: list[RejectedRow]
    ) -> None:
        ts = now_ms()
        conn.executemany(
            "INSERT INTO rejected_records"
            "(batch_id, item_index, reason, detail, raw_json, created_at_ms)"
            " VALUES (?,?,?,?,?,?)",
            [(batch_id, idx, reason, detail, raw, ts) for idx, reason, detail, raw in rejected],
        )

    @staticmethod
    def _batch_payload(row: sqlite3.Row) -> dict:
        return {
            "batch_id": row["batch_id"],
            "accepted": row["accepted"],
            "rejected": json.loads(row["rejected_json"]),
            "duplicate": bool(row["duplicate"]),
            "deleted_before": row["deleted_before"],
        }

    # -- 写入：追加批次 -------------------------------------------------
    def insert_batch(
        self,
        batch_id: str,
        accepted_rows: list[AcceptedRow],
        rejected_rows: list[RejectedRow],
    ) -> dict:
        """原子写入一个追加批次。

        返回 ``{"accepted": int, "rejected": [...], "duplicate": bool}``。
        - batch_id 已存在：原样返回首次结果，不重复累计（duplicate=True）。
        - 内容键/同 id 同内容：识别为重复，不重复插入、不重复累计成交量。
        - 同 id 不同内容：拒绝（id_conflict），不覆盖既有数据。
        """
        conn = self.connect()
        with self._write_lock:
            try:
                conn.execute("BEGIN IMMEDIATE")
                prior = conn.execute(
                    "SELECT * FROM batches WHERE batch_id = ?", (batch_id,)
                ).fetchone()
                if prior is not None:
                    conn.execute("COMMIT")
                    payload = self._batch_payload(prior)
                    payload["duplicate"] = True
                    return payload

                rejected = list(rejected_rows)
                # 同一批次只涉及一个 symbol 集合；按 symbol 分组查重
                by_symbol: dict[str, list[AcceptedRow]] = {}
                for row in accepted_rows:
                    by_symbol.setdefault(row[0], []).append(row)

                to_insert: list[AcceptedRow] = []
                for symbol, rows in by_symbol.items():
                    existing = self._existing_map(conn, symbol, [r[1] for r in rows])
                    for r in rows:
                        _, key, has_id, ts_ms, price, qty, seq, idx, raw = r
                        old = existing.get(key)
                        if old is None:
                            to_insert.append(r)
                        elif (
                            old["ts_ms"] == ts_ms
                            and old["price"] == price
                            and old["quantity"] == qty
                            and (old["seq"] == seq)
                        ):
                            # 幂等重复：接受但无需再次插入
                            continue
                        else:
                            rejected.append((
                                idx,
                                "id_conflict",
                                "同一 id 已存在但内容不同；追加接口不覆盖，"
                                "修正请使用 /v1/symbols/{symbol}/replace-range",
                                raw,
                            ))

                ts = now_ms()
                conn.executemany(
                    "INSERT INTO ticks"
                    "(symbol, tick_key, has_id, ts_ms, price, quantity, seq, "
                    " batch_id, created_at_ms) VALUES (?,?,?,?,?,?,?,?,?)",
                    [
                        (s, k, h, t, p, q, sq, batch_id, ts)
                        for (s, k, h, t, p, q, sq, _i, _raw) in to_insert
                    ],
                )
                self._record_rejected(conn, batch_id, rejected)
                rejected_json = [
                    {"index": i, "reason": rs, "detail": d, "raw": json.loads(raw)}
                    for (i, rs, d, raw) in rejected
                ]
                # accepted = 提交行数中未被冲突拒绝的数量（含幂等重复）
                conflict_indexes = {
                    i for (i, rs, _d, _raw) in rejected if rs == "id_conflict"
                }
                accepted_count = len({r[7] for r in accepted_rows} - conflict_indexes)
                conn.execute(
                    "INSERT INTO batches(batch_id, accepted, rejected_json, "
                    "duplicate, mode, created_at_ms) VALUES (?,?,?,?,?,?)",
                    (batch_id, accepted_count, json.dumps(rejected_json), 0, "append", ts),
                )
                conn.execute("COMMIT")
                return {
                    "batch_id": batch_id,
                    "accepted": accepted_count,
                    "rejected": rejected_json,
                    "duplicate": False,
                }
            except Exception:
                try:
                    conn.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
                raise

    # -- 写入：区间重建 -------------------------------------------------
    def replace_range(
        self,
        symbol: str,
        start_ms: int,
        end_ms_exclusive: int,
        batch_id: str,
        accepted_rows: list[AcceptedRow],
        rejected_rows: list[RejectedRow],
    ) -> dict:
        """在单个事务内删除区间旧点位并写入新点位（补写/修正/重放）。

        - batch_id 重复：返回首次结果，绝不二次删除（避免删掉后到数据）。
        - 调用方必须保证 accepted_rows 全部属于该 symbol 且落在区间内。
        """
        conn = self.connect()
        with self._write_lock:
            try:
                conn.execute("BEGIN IMMEDIATE")
                prior = conn.execute(
                    "SELECT * FROM batches WHERE batch_id = ?", (batch_id,)
                ).fetchone()
                if prior is not None:
                    conn.execute("COMMIT")
                    payload = self._batch_payload(prior)
                    payload["duplicate"] = True
                    return payload

                deleted = conn.execute(
                    "DELETE FROM ticks WHERE symbol = ? AND ts_ms >= ? AND ts_ms < ?",
                    (symbol, start_ms, end_ms_exclusive),
                ).rowcount
                ts = now_ms()
                conn.executemany(
                    "INSERT INTO ticks"
                    "(symbol, tick_key, has_id, ts_ms, price, quantity, seq, "
                    " batch_id, created_at_ms) VALUES (?,?,?,?,?,?,?,?,?)",
                    [
                        (s, k, h, t, p, q, sq, batch_id, ts)
                        for (s, k, h, t, p, q, sq, _i, _raw) in accepted_rows
                    ],
                )
                rejected = list(rejected_rows)
                self._record_rejected(conn, batch_id, rejected)
                rejected_json = [
                    {"index": i, "reason": rs, "detail": d, "raw": json.loads(raw)}
                    for (i, rs, d, raw) in rejected
                ]
                accepted_count = len({r[7] for r in accepted_rows})
                conn.execute(
                    "INSERT INTO batches(batch_id, accepted, rejected_json, "
                    "duplicate, mode, deleted_before, created_at_ms) "
                    "VALUES (?,?,?,?,?,?,?)",
                    (batch_id, accepted_count, json.dumps(rejected_json), 0,
                     f"replace:{start_ms}:{end_ms_exclusive}", deleted, ts),
                )
                conn.execute("COMMIT")
                return {
                    "accepted": accepted_count,
                    "rejected": rejected_json,
                    "duplicate": False,
                    "deleted_before": deleted,
                }
            except Exception:
                try:
                    conn.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
                raise

    # -- 读取 -----------------------------------------------------------
    def get_batch(self, batch_id: str) -> dict | None:
        conn = self.connect()
        row = conn.execute(
            "SELECT * FROM batches WHERE batch_id = ?", (batch_id,)
        ).fetchone()
        return self._batch_payload(row) if row is not None else None

    def query_ticks(
        self, symbol: str, start_ms: int, end_ms_exclusive: int
    ) -> list[tuple]:
        """返回区间内已提交点位，按 (ts_ms, seq, tick_key) 全序排序。"""
        conn = self.connect()
        rows = conn.execute(
            "SELECT ts_ms, price, quantity, seq, tick_key FROM ticks "
            "WHERE symbol = ? AND ts_ms >= ? AND ts_ms < ? "
            "ORDER BY ts_ms ASC, seq IS NULL, seq ASC, tick_key ASC",
            (symbol, start_ms, end_ms_exclusive),
        ).fetchall()
        return [
            (r["ts_ms"], r["price"], r["quantity"], r["seq"], r["tick_key"])
            for r in rows
        ]
