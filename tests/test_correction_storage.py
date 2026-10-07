"""存储层：区间修正的取代标记、旧库迁移与跨连接持久化。"""

from __future__ import annotations

import sqlite3

from app.storage import StoredTick, TickStore


class TestCorrectionStorage:
    def test_superseded_rows_kept_for_audit(self, store, request):
        store.insert_batch("b1", "h1", [], [
            StoredTick("b1", 0, "AAA", 60_100, 99.0, 999.0),
            StoredTick("b1", 1, "AAA", 59_000, 5.0, 5.0),
        ])
        request.node._log_case(
            input={"range": [60_000, 120_000]},
            rule="被替换行物理保留但 superseded_by_correction 置为修正 id",
        )
        from app.storage import StagedTick, StoredCorrection

        c = StoredCorrection("fix", "AAA", 60_000, 120_000, 1, "HASH",
                             "pending", 0, 0)
        assert store.stage_correction_part(
            c, 0, "ph", [StagedTick(0, 0, "AAA", 60_500, 10.0, 2.0)]
        )
        applied = StoredCorrection("fix", "AAA", 60_000, 120_000, 1, "HASH",
                                   "applied", 0, 0)
        store.commit_correction(applied)

        rows = store._conn.execute(
            "SELECT batch_id, superseded_by_correction s FROM ticks "
            "ORDER BY ts"
        ).fetchall()
        marks = [(r["batch_id"], r["s"]) for r in rows]
        assert marks == [
            ("b1", ""),        # 59000 区间外，仍 active
            ("b1", "fix"),     # 60100 被取代，行保留
            ("fix", ""),       # 新修正点 active
        ]
        # 审计：旧值仍可查
        old = store._conn.execute(
            "SELECT price, size FROM ticks WHERE batch_id='b1' "
            "AND superseded_by_correction='fix'"
        ).fetchone()
        assert (old["price"], old["size"]) == (99.0, 999.0)
        # K 线只由 active 点构成（窗口 0 的 ts=59000 保留，窗口 60000 已替换）
        by_ws = {r[1]: r for r in store.query_candles("AAA", 0, 120_000)}
        candle = by_ws[60_000]
        assert (candle[2], candle[6], candle[7]) == (10.0, 2.0, 1)
        assert (by_ws[0][2], by_ws[0][6]) == (5.0, 5.0)

    def test_correction_batch_registered_in_batches(self, store):
        from app.storage import StagedTick, StoredCorrection

        c = StoredCorrection("fix", "AAA", 60_000, 120_000, 1, "HASH",
                             "pending", 0, 0)
        store.stage_correction_part(
            c, 0, "ph", [StagedTick(0, 0, "AAA", 60_500, 10.0, 2.0)]
        )
        store.commit_correction(StoredCorrection(
            "fix", "AAA", 60_000, 120_000, 1, "HASH", "applied", 0, 0))
        b = store.get_batch("fix")
        assert b is not None and b.kind == "correction" and b.accepted == 1
        head = store.get_correction("fix")
        assert head.status == "applied" and head.payload_hash == "HASH"
        # 暂存表在生效后清空
        assert store.fetch_staging("fix") == []

    def test_commit_twice_with_same_id_conflicts(self, store, request):
        from app.storage import StagedTick, StoredCorrection

        request.node._log_case(
            input={"commit": "same correction_id twice"},
            rule="底层重复提交被 batches 主键拦截并整体回滚",
        )
        c = StoredCorrection("fix", "AAA", 60_000, 120_000, 1, "H",
                             "pending", 0, 0)
        store.stage_correction_part(
            c, 0, "ph", [StagedTick(0, 0, "AAA", 60_500, 10.0, 2.0)]
        )
        store.commit_correction(StoredCorrection(
            "fix", "AAA", 60_000, 120_000, 1, "H", "applied", 0, 0))
        # 第二次准备暂存再提交
        store.stage_correction_part(
            StoredCorrection("fix2holder", "AAA", 0, 0, 1, "", "pending", 0, 0),
            0, "x", [],
        )
        import pytest

        with pytest.raises(sqlite3.IntegrityError):
            store.commit_correction(StoredCorrection(
                "fix", "AAA", 60_000, 120_000, 1, "H2", "applied", 0, 0))


class TestSchemaMigration:
    def test_old_schema_db_upgrades_and_data_preserved(self, db_path, request):
        # 用旧版（无 kind / superseded 列）schema 建库并写入
        old = sqlite3.connect(db_path)
        old.executescript(
            """
            CREATE TABLE batches (
                batch_id TEXT PRIMARY KEY, payload_hash TEXT NOT NULL,
                received INTEGER NOT NULL, accepted INTEGER NOT NULL,
                rejected INTEGER NOT NULL, rejected_json TEXT NOT NULL,
                created_at_ms INTEGER NOT NULL);
            CREATE TABLE ticks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                batch_id TEXT NOT NULL, batch_index INTEGER NOT NULL,
                symbol TEXT NOT NULL, ts INTEGER NOT NULL,
                price REAL NOT NULL, size REAL NOT NULL,
                UNIQUE(batch_id, batch_index));
            CREATE INDEX idx_ticks_symbol_ts ON ticks(symbol, ts);
            CREATE TABLE candles (
                symbol TEXT NOT NULL, window_start INTEGER NOT NULL,
                open REAL NOT NULL, high REAL NOT NULL, low REAL NOT NULL,
                close REAL NOT NULL, volume REAL NOT NULL, tick_count INTEGER NOT NULL,
                PRIMARY KEY (symbol, window_start));
            INSERT INTO batches VALUES ('old1','h',1,1,0,'[]',0);
            INSERT INTO ticks(batch_id,batch_index,symbol,ts,price,size)
                VALUES ('old1',0,'AAA',60000,10.0,3.0);
            INSERT INTO candles VALUES ('AAA',60000,10,10,10,10,3,1);
            """
        )
        old.commit()
        old.close()
        request.node._log_case(
            input={"db": "旧 schema（无 kind/superseded 列）"},
            rule="打开旧库自动迁移：数据保留、旧点位默认 active、可继续修正",
        )
        s = TickStore(db_path)
        assert s.get_batch("old1").kind == "ingest"
        rows = s.query_candles("AAA", 0, 120_000)
        assert rows[0][6] == 3.0
        # 迁移后可正常做修正
        from app.storage import StagedTick, StoredCorrection

        c = StoredCorrection("fix", "AAA", 60_000, 120_000, 1, "H",
                             "pending", 0, 0)
        s.stage_correction_part(
            c, 0, "ph", [StagedTick(0, 0, "AAA", 60_500, 11.0, 4.0)]
        )
        s.commit_correction(StoredCorrection(
            "fix", "AAA", 60_000, 120_000, 1, "H", "applied", 0, 0))
        candle = s.query_candles("AAA", 0, 120_000)[0]
        assert (candle[2], candle[6]) == (11.0, 4.0)
        s.close()

    def test_correction_survives_reopen(self, db_path, request):
        s1 = TickStore(db_path)
        from app.storage import StagedTick, StoredCorrection

        c = StoredCorrection("fix", "AAA", 60_000, 120_000, 1, "H",
                             "pending", 0, 0)
        s1.stage_correction_part(
            c, 0, "ph", [StagedTick(0, 0, "AAA", 60_500, 11.0, 4.0)]
        )
        s1.commit_correction(StoredCorrection(
            "fix", "AAA", 60_000, 120_000, 1, "H", "applied", 0, 0))
        s1.close()
        request.node._log_case(
            input={"reopen": True},
            rule="修正结果（取代标记、批次、K 线）重开数据库后保持",
        )
        s2 = TickStore(db_path)
        assert s2.get_correction("fix").status == "applied"
        candle = s2.query_candles("AAA", 0, 120_000)[0]
        assert (candle[2], candle[6]) == (11.0, 4.0)
        assert s2.verify_symbol("AAA")["mismatches"] == []
        s2.close()


class TestCorrectionAcrossConnections:
    def test_parts_from_separate_connections_then_commit(self, db_path, request):
        from app.storage import StagedTick, StoredCorrection

        request.node._log_case(
            input={"connections": ["c0", "c1", "reader"]},
            rule="不同连接分别暂存分片，最后一个连接提交；K 线只反映修正集合",
        )
        # 连接1 写入错误历史
        base = TickStore(db_path)
        base.insert_batch("wrong", "h", [], [
            StoredTick("wrong", 0, "AAA", 60_100, 99.0, 999.0),
        ])
        base.close()

        head = StoredCorrection("fix", "AAA", 60_000, 120_000, 2, "",
                                "pending", 0, 0)
        # 连接2 暂存 part1（后片先到）
        conn2 = TickStore(db_path)
        r1 = conn2.stage_correction_part(
            head, 1, "h1", [StagedTick(1, 0, "AAA", 60_900, 12.0, 4.0)]
        )
        assert r1 == "incomplete"
        conn2.close()

        # 连接3 暂存 part0 后片到齐并提交
        conn3 = TickStore(db_path)
        r0 = conn3.stage_correction_part(
            head, 0, "h0", [StagedTick(0, 0, "AAA", 60_100, 10.0, 2.0)]
        )
        assert r0 == "complete"
        applied = StoredCorrection("fix", "AAA", 60_000, 120_000, 2, "H",
                                   "applied", 0, 0)
        conn3.commit_correction(applied)
        candle = conn3.query_candles("AAA", 0, 120_000)[0]
        # 全序：60100@10 -> open，60900@12 -> close，volume=6
        assert (candle[2], candle[3], candle[4], candle[5], candle[6], candle[7]) == (
            10.0, 12.0, 10.0, 12.0, 6.0, 2)
        assert conn3.verify_symbol("AAA")["mismatches"] == []
        conn3.close()
