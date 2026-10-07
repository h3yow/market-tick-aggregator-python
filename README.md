# market-tick-aggregator-python

本地行情点位批量摄取与一分钟 K 线查询服务。仅依赖本地 SQLite，
**无外部网络、无真实凭据即可独立运行并稳定复现结果**。

## 快速开始

```bash
uv sync
uv run python main.py                 # 启动 HTTP 服务，默认 127.0.0.1:8000
uv run pytest -q                      # 运行全部本地测试（默认打印每条用例输入与判定依据）
uv run python tools/verify.py --db data/ticks.db --all  # K 线与有效点位对账
```

数据库位置由环境变量 `TICK_DB_PATH` 控制（默认 `data/ticks.db`）。

## 能力概览

- `POST /api/v1/ticks/batch`：批量写入行情点；`batch_id` 幂等，重复批次
  不重复累计，乱序/迟到点位按确定性规则聚合，无效记录逐条给出原因且整批拒绝。
- `POST /api/v1/symbols/{symbol}/corrections`：**区间整体替换式修正**。
  对 `(symbol, [start, end))` 半开区间提交修正后的完整点位集合（支持分片、
  乱序到达、幂等重试）；区间内原有有效点位被整体替换（旧点位保留审计行），
  区间外数据不动，受影响窗口在单事务内重算。
- `GET /api/v1/symbols/{symbol}/candles?start=&end=`：按标的与时间范围查询
  一分钟 OHLCV K 线。
- `GET /api/v1/admin/symbols/{symbol}/verify`：按当前有效点位逐窗口重算，
  与物化 K 线对账，输出不一致明细；CLI：
  `uv run python tools/verify.py --db data/ticks.db --symbol AAA`。
- `POST /api/v1/admin/symbols/{symbol}/rebuild`：历史补写/修正/重放后，
  以行情事实表（仅当前有效点位）重建该标的全部 K 线。
- `GET /` 保持 `{"Hello": "World"}`；`GET /health` 返回 `{"status":"ok"}`。

时间窗口、区间边界与「替换而不是叠加」口径、幂等/并发规则、与补写/重建的
兼容范围、错误分类与本地验证方法详见 [docs/DESIGN.md](docs/DESIGN.md)。

## 核心一致性保证

| 场景 | 行为 |
| --- | --- |
| 相同 `batch_id` + 相同负载重放 | 返回 `duplicate`，不重复累计 |
| 相同 `batch_id` + 不同负载 | `409 batch_id_conflict` |
| 乱序 / 迟到点位 | 正常入库并重算所属窗口；OHLCV 与到达顺序无关 |
| 批次中含无效记录 | 整批 422 回滚，逐条返回 `index/reason/detail` |
| 写入失败 | 事务回滚，无部分可见；同 `batch_id` 可安全重试 |
| 进程重启 | 已 COMMIT 数据不丢失（SQLite WAL + synchronous=FULL） |
| 并发写同一窗口 | 全量重算 UPSERT，不重复累计、不互相覆盖 |
| 区间修正 `[start,end)` | 按时间戳归属，区间内有效点位整体替换；区间外/跨界窗口外点位不动 |
| 修正集合换序/拆批 | 分片可乱序提交，最终 OHLCV 与拆分方式、到达顺序无关 |
| 相同 `correction_id` 重放 | `duplicate` 只生效一次；元数据/负载不一致 `409` |
| 修正与普通写入/其它修正并发 | 事务串行化，无改了一半的中间状态，只重算被覆盖窗口 |
| 修正/补写/重建之后 | K 线 == 只按当前有效点位重算的结果（可用 verify 对账） |
