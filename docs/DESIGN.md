# 本地行情摄取与分钟 K 线服务 — 设计与语义说明

本服务在**无外部网络、无真实凭据**的条件下独立运行：持久化使用内置
SQLite（WAL 模式），不依赖任何外部数据库、消息队列或第三方服务。

## 1. 时间窗口与数据语义

* 所有时间戳 `ts` / `start` / `end` 均为 **Unix 毫秒时间戳（UTC 整数）**。
* 分钟窗口：

  ```
  window_start = floor(ts / 60_000) * 60_000
  ```

  即窗口 `[window_start, window_start + 60_000)`。
* 查询 `GET /api/v1/symbols/{symbol}/candles?start=&end=` 返回窗口起点落在
  `[floor(start), floor(end)]` 的全部 K 线，按时间升序。即 `start`、`end`
  都会先向下取整到各自所在分钟，**包含两端所在窗口**。
* K 线字段：
  * `open`：窗口内第一笔成交价；
  * `high` / `low`：窗口内成交价的最大值 / 最小值；
  * `close`：窗口内最后一笔成交价；
  * `volume`：窗口内全部 `size` 之和；
  * `tick_count`：窗口内成交点位数。
* **同毫秒多笔的确定性决胜**：同一窗口内的点位按
  `(ts ASC, price ASC, size ASC, batch_id ASC, batch_index ASC)` 全序排列，
  取该序列的第一条价格为 `open`、最后一条价格为 `close`。该决胜**只依赖数据
  内容本身，与到达顺序无关**，因此同一输入集合无论以何种顺序、分多少批、
  是否重试或重放，最终 OHLCV 完全一致（由顺序无关性测试参数化覆盖）。

## 2. 数据模型与存储

SQLite 文件由 `TICK_DB_PATH` 环境变量指定，默认 `data/ticks.db`。

* `batches(batch_id PK, payload_hash, received, accepted, rejected, rejected_json, created_at_ms)`
  —— 批次幂等键与负载指纹。
* `ticks(id PK, batch_id FK, batch_index, symbol, ts, price, size, UNIQUE(batch_id, batch_index))`
  —— 行情点事实表，是唯一数据真相（source of truth），索引 `(symbol, ts)`。
* `candles(symbol, window_start, open, high, low, close, volume, tick_count, PK(symbol, window_start))`
  —— 分钟 K 线物化结果，可由 `ticks` 表随时完全重建。

## 3. 幂等与一致性规则

### 3.1 批次幂等

* `batch_id` 由调用方提供，作为批次唯一幂等键。
* 首次写入成功返回 `status="accepted"`；相同 `batch_id` + **相同负载**重放
  返回 `status="duplicate"` 与首次的计数结果，不重复累计、不改变任何窗口。
* 相同 `batch_id` 但**负载不一致**返回 `409 batch_id_conflict`，首次写入
  保持不变（防止键复用造成的语义污染）。
* 负载指纹（SHA-256）对规范化 JSON（键排序）计算，字段书写顺序不同但语义
  相同的负载视为同一负载。
* 并发使用相同 `batch_id` 重试时，由 `batches` 主键保证只有一次写入成功，
  其余调用得到幂等结果（有并发重试测试覆盖）。

### 3.2 事务与可见性

* 每个批次的「插入批次记录 + 插入全部点位 + 重算受影响分钟窗口」在**同一个
  `BEGIN IMMEDIATE` 事务**内完成，任何步骤失败即 `ROLLBACK`。
* 因此不存在部分可见：查询只能看到批次提交前或提交后的完整状态；
  失败回滚不会留下半个批次，同一 `batch_id` 修正后可以安全重试。
* 进程内单连接 + `RLock` 串行化写事务；WAL 模式下读不阻塞写。
  `PRAGMA synchronous=FULL` 保证已确认写入（事务 COMMIT 后）在进程/机器
  重启后不丢失（有重开数据库的持久化与重启测试覆盖）。
* 同一标的同一窗口的并发写入不会重复累计或互相覆盖：每次提交都以 `ticks`
  事实表为准对窗口做**全量重算并 UPSERT**，而非增量加减。

### 3.3 乱序、迟到与历史补写

* 点位不要求按时间顺序到达；乱序与迟到点位均正常接收，入库后重算其所属
  分钟窗口，先前窗口被自动修正。
* 历史数据补写：直接以新批次写入历史时间戳即可。
* 修正/重放后重建：`POST /api/v1/admin/symbols/{symbol}/rebuild` 以 `ticks`
  表为准重建该标的全部窗口，并清除事实表中已不存在的陈旧窗口。

## 4. 校验与错误分类

**只要批次中存在任何无效记录，整批拒绝（HTTP 422），不写入任何数据**，
响应体 `rejected_records` 对每条无效记录给出 `index`（在 `records` 中的
下标）、`reason`、`detail` 及原始记录内容。无效记录不会被静默丢弃，也不会
被「部分接受」。

记录级原因码（`reason`）：

| reason | 判定依据 |
| --- | --- |
| `not_an_object` | 记录不是 JSON 对象 |
| `missing_symbol` / `invalid_symbol` | 缺少 `symbol`；非字符串、空白或长度 > 64 |
| `missing_timestamp` / `invalid_timestamp` | 缺少 `ts`；非整数、布尔或负数（必须为非负 Unix 毫秒） |
| `missing_price` / `invalid_price` | 缺少 `price`；非有限数值或 <= 0 |
| `missing_size` / `invalid_size` | 缺少 `size`；非有限数值或 < 0 |
| `duplicate_tick_in_batch` | 同一批次内 `(symbol, ts, price, size)` 完全重复 |

批次级错误码（`error_code`）：

| error_code | HTTP | 含义 |
| --- | --- | --- |
| `empty_batch` | 422 | `records` 为空 |
| `batch_too_large` | 422 | 单批超过 10,000 条 |
| `invalid_batch_id` | 422 | `batch_id` 为空/非字符串 |
| `invalid_records` | 422 | 含无效记录（整批回滚，明细见响应体） |
| `invalid_symbol` / `invalid_start` / `invalid_end` / `invalid_range` | 422 | 查询参数非法或 `end < start` |
| `batch_id_conflict` | 409 | 相同 `batch_id` 负载不一致 |

## 5. HTTP API

### `POST /api/v1/ticks/batch`

```json
{
  "batch_id": "20260926-aaa-001",
  "records": [
    {"symbol": "AAA", "ts": 1789000000000, "price": 10.25, "size": 100}
  ]
}
```

成功响应：

```json
{
  "batch_id": "20260926-aaa-001",
  "status": "accepted",
  "received": 1,
  "accepted": 1,
  "rejected": 0,
  "rejected_records": [],
  "affected_windows": ["AAA@1788999960000"]
}
```

### `GET /api/v1/symbols/{symbol}/candles?start=<ms>&end=<ms>`

```json
{
  "symbol": "AAA",
  "start": 0,
  "end": 120000,
  "candles": [
    {"symbol": "AAA", "window_start": 60000, "open": 10.0, "high": 12.0,
     "low": 9.0, "close": 13.0, "volume": 12.0, "tick_count": 4}
  ]
}
```

### `POST /api/v1/admin/symbols/{symbol}/rebuild`

返回 `{"symbol": "...", "rebuilt_windows": <int>}`。

### 兼容与健康

* `GET /` 保持骨架期行为：`{"Hello": "World"}`。
* `GET /health` 返回 `{"status": "ok"}`。

## 6. 本地验证方法

```bash
# 运行全部单测（纯本地 SQLite 临时库，不访问网络/外部服务）
uv run pytest -q

# 查看测试中打印的输入数据与判定依据
uv run pytest -q --log-cli-level=INFO

# 启动服务
TICK_DB_PATH=data/ticks.db uv run python main.py   # 默认 127.0.0.1:8000
```

测试覆盖场景：

1. 重复批次（相同负载幂等、不同负载 409、并发相同 `batch_id` 重试）；
2. 乱序与迟到点位、同一集合三种到达顺序的顺序无关性；
3. 全部记录级无效分类、批内重复点位、空批次/超长批次；
4. 失败整批回滚 + 同 `batch_id` 修正后重试只生效一次；
5. 并发同窗口写入（不重复累计）、并发查询只见自洽已提交快照；
6. 进程重启后数据不丢失并可继续幂等重放；
7. 历史补写与 rebuild 重建结果一致性、陈旧窗口清理；
8. 根路由历史行为兼容。
