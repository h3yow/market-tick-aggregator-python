# market-tick-aggregator-python

本地行情点位批量摄取与一分钟 K 线查询服务。仅依赖本地 SQLite，
**无外部网络、无真实凭据即可独立运行并稳定复现结果**。

## 快速开始

```bash
uv sync
uv run python main.py                 # 启动 HTTP 服务，默认 127.0.0.1:8000
uv run pytest -q                      # 运行全部本地测试（默认实时打印每条
                                      # 用例的输入数据与判定依据）
uv run pytest -q --log-cli-level=WARNING  # 需要安静输出时使用
```

数据库位置由环境变量 `TICK_DB_PATH` 控制（默认 `data/ticks.db`）。

## 能力概览

- `POST /api/v1/ticks/batch`：批量写入行情点；`batch_id` 幂等，重复批次
  不重复累计，乱序/迟到点位按确定性规则聚合，无效记录逐条给出原因且整批拒绝。
- `POST /api/v1/symbols/{symbol}/corrections`：**区间整体替换式修正**。
  针对标的与 `[start, end)` 左闭右开区间提交修正后的完整点位集合，区间内旧
  点位被整体替换（不叠加、不残留），区间外（含 `ts == end`）点位不动；
  `correction_id` 幂等，可安全重试；同一份集合拆成相邻不相交子区间、换序
  提交结果完全一致；空集合表示清空该区间。
- `GET /api/v1/symbols/{symbol}/candles?start=&end=`：按标的与时间范围查询
  一分钟 OHLCV K 线。
- `POST /api/v1/admin/symbols/{symbol}/rebuild`：历史补写/修正/重放后，
  以行情事实表重建该标的全部 K 线。
- `POST /api/v1/admin/symbols/{symbol}/verify`：以 ticks 事实表重算并与
  K 线物化结果逐窗口对照，本地即可发现"K 线与原始点位对不上"的情况。
- `GET /` 保持 `{"Hello": "World"}`；`GET /health` 返回 `{"status":"ok"}`。

时间窗口、修正的边界与替换语义、幂等/并发规则、错误分类与本地验证方法详见
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
| 区间修正（`[start,end)`） | 区间内旧点整体删除替换；区间外/其它标的不动；同事务重算，无中间状态 |
| 相同 `correction_id` 重放/重试 | 返回 `duplicate`，只生效一次（换记录顺序也视为同一负载） |
| 修正集合拆批、换序提交 | 沿时间切成相邻不相交子区间即可；最终 OHLCV 与一次性提交一致 |
| 修正与普通写入并发 | 序列化为可复现的提交序列，每次提交后 ticks/K 线自洽 |
| 修正 / 补写 / 重放之后 | `rebuild` 与 `verify` 结果均等于只按当前有效点位重算 |
