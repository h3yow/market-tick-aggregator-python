"""分钟窗口聚合。

语义
----
- 窗口：左闭右开的 60 秒区间 ``[floor(ts/60000)*60000, +60000)``，UTC 绝对
  毫秒，不涉及时区或交易日历。
- 开盘价/收盘价：窗口内点位按 ``(ts_ms, seq NULLS LAST, tick_key)`` 全序排序
  后，首条价格为开盘、末条价格为收盘。tick_key 作为最终确定性并列裁决，保证
  同一集合无论到达顺序如何结果一致。
- 最高价/最低价：窗口内全部点位价格的 max/min。
- 成交量：窗口内全部点位 quantity 之和（浮点）。
"""

from __future__ import annotations

from collections import defaultdict

from .models import WINDOW_SIZE_MS, Candle


def window_start(ts_ms: int, window_size_ms: int = WINDOW_SIZE_MS) -> int:
    """返回时间戳所属分钟窗口起点（floor 到窗口边界）。"""
    if ts_ms < 0:
        # 负数时间戳也向负无穷取整
        return -((-ts_ms + window_size_ms - 1) // window_size_ms) * window_size_ms
    return (ts_ms // window_size_ms) * window_size_ms


def _sort_key(row: tuple):
    ts_ms, _price, _qty, seq, tick_key = row
    # None 排在非 None 之后（NULLS LAST）
    return (ts_ms, seq is None, seq if seq is not None else 0, tick_key)


def aggregate_ticks(
    ticks: list[tuple], symbol: str | None = None
) -> list[Candle]:
    """把已去重点位按 (symbol, 分钟窗口) 聚合为 K 线。

    入参行：``(ts_ms, price, quantity, seq, tick_key)``。store 已按 symbol
    过滤，故这里显式传入 symbol；如 tick 行携带 symbol 可改用扩展。
    返回按 window_start 升序的 K 线列表。
    """
    groups: dict[int, list[tuple]] = defaultdict(list)
    for row in ticks:
        groups[window_start(row[0])].append(row)

    candles: list[Candle] = []
    for win in sorted(groups):
        rows = sorted(groups[win], key=_sort_key)
        prices = [r[1] for r in rows]
        volume = sum(r[2] for r in rows)
        candles.append(
            Candle(
                symbol=symbol or "",
                window_start_ms=win,
                open=rows[0][1],
                high=max(prices),
                low=min(prices),
                close=rows[-1][1],
                volume=volume,
                tick_count=len(rows),
            )
        )
    return candles
