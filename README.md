# market-tick-aggregator-python

本地行情点位批量摄取与一分钟 K 线查询服务。仅依赖本地 SQLite，
**无外部网络、无真实凭据即可独立运行并稳定复现结果**。

## 快速开始

```bash
uv sync
uv run python main.py                 # 启动 HTTP 服务，默认 127.0.0.1:8000
uv run pytest -q                      # 运行全部本地测试
uv run pytest -q --log-cli-level=INFO # 同时打印每条测试的输入数据与判定依据
```

数据库位置由环境变量 `TICK_DB_PATH` 控制（默认 `data/ticks.db`）。

## 能力概览

- `POST /api/v1/ticks/batch`：批量写入行情点；`batch_id` 幂等，重复批次
  不重复累计，乱序/迟到点位按确定性规则聚合，无效记录逐条给出原因且整批拒绝。
- `GET /api/v1/symbols/{symbol}/candles?start=&end=`：按标的与时间范围查询
  一分钟 OHLCV K 线。
- `POST /api/v1/admin/symbols/{symbol}/rebuild`：历史补写/修正/重放后，
  以行情事实表重建该标的全部 K 线。
- `GET /` 保持 `{"Hello": "World"}`；`GET /health` 返回 `{"status":"ok"}`。

时间窗口、幂等/一致性规则、错误分类与本地验证方法详见
[docs/DESIGN.md](docs/DESIGN.md)。

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
