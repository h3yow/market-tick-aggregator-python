#!/usr/bin/env python3
"""本地对账工具：按当前有效点位重算 K 线，与物化结果逐窗口比对。

用法::

    uv run python tools/verify.py --db data/ticks.db --symbol AAA
    uv run python tools/verify.py --db data/ticks.db --all      # 对账全部标的

发现任何差异时进程以非零码退出，并打印每个不一致窗口的
``stored``（物化值）与 ``expected``（按有效点位重算值），
便于排查与审计。该过程只读，不修改数据库。
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

# 允许直接以 `python tools/verify.py` 运行：把项目根加入模块搜索路径
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.storage import TickStore  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="K 线与有效点位对账")
    parser.add_argument("--db", required=True, help="SQLite 数据库文件路径")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--symbol", help="只对账指定标的")
    group.add_argument("--all", action="store_true", help="对账库中全部标的")
    args = parser.parse_args()

    store = TickStore(args.db)
    try:
        if args.symbol:
            symbols = [args.symbol]
        else:
            rows = store._conn.execute(
                "SELECT DISTINCT symbol FROM ticks"
            ).fetchall()
            symbols = sorted(r["symbol"] for r in rows)

        report = {}
        total_mismatches = 0
        for symbol in symbols:
            result = store.verify_symbol(symbol)
            report[symbol] = result
            total_mismatches += len(result["mismatches"])
    finally:
        store.close()

    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    if total_mismatches:
        print(f"发现 {total_mismatches} 处不一致", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
