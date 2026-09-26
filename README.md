# 本地行情摄取与分钟 K 线聚合服务

一个**完全本地化、无外部网络、无真实凭据**即可运行**且结果可稳定复现**的行情
点位（tick）摄取与分钟 K 线查询服务。基于 FastAPI + SQLite（WAL 模式）实现：

- 批量点位写入，按标的与分钟窗口聚合 OHLC + 累计成交量；
- 重复批次、乱序、迟到点位的处理确定且可解释；无效记录按原因拒绝并留痕；
- 历史数据补写 / 修正 / 重放后可重建，最终结果与到达顺序、重试方式无关；
- 本地持久化、支持并发访问，进程重启不丢已确认数据。

## 快速开始（离线）

```bash
uv sync                                   # 依赖已通过 uv.lock 锁定
uv run python main.py                     # 启动，默认库文件 data/market.db
# 或指定数据库：
MARKET_DB=/tmp/market.db uv run python main.py
```

运行全部本地验证（不访问任何外部服务）：

```bash
uv run pytest                              # 单测，日志会打印输入数据与判定依据
uv run pytest tests/test_concurrency.py    # 仅并发场景（含多进程）
```

## HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `GET` | `/` | 兼容旧骨架：`{"Hello":"World"}` |
| `GET` | `/health` | 健康检查 |
| `POST` | `/v1/ticks/batches` | 批量追加点位（`batch_id` 幂等） |
| `POST` | `/v1/symbols/{symbol}/replace-range` | 按区间删除后重放（补写/修正） |
| `GET` | `/v1/symbols/{symbol}/candles?start_ms=&end_ms_exclusive=` | 查询分钟 K 线 |

### 写入示例

```bash
curl -s -X POST http://127.0.0.1:8000/v1/ticks/batches \
  -H 'Content-Type: application/json' \
  -d '{
    "batch_id": "b-0001",
    "ticks": [
      {"symbol":"BTC","timestamp_ms":60000,"price":100.5,"quantity":2},
      {"symbol":"BTC","timestamp_ms":60030,"price":101,"quantity":3,"id":"t-3"}
    ]
  }'
# {"batch_id":"b-0001","accepted":2,"rejected":[],"duplicate":false}
```

`price` / `quantity` 支持数字或可解析的字符串（如 `"12.5"`）；`timestamp_ms`
为自 Unix 纪元起的**非负整数毫秒**。点位可携带可选字段：

- `id`：业务方点位唯一标识（同一 symbol 内唯一）。提供时按 `(symbol,id)`
  识别同一点位；不提供时按内容键
  `sha1(timestamp_ms|price|quantity|seq)` 识别。
- `seq`：同一数据源内的单调序列号（整数），用于同毫秒并列时确定先后。

## 时间窗口与数据语义

- 窗口为**左闭右开**的 60 秒区间：
  `window_start = floor(timestamp_ms / 60000) * 60000`，
  点位 `ts` 归入 `[window_start, window_start + 60000)`。使用 UTC 绝对毫秒，
  不涉及时区、交易日历。
- 查询区间同样左闭右开：`[start_ms, end_ms_exclusive)`。
- K 线字段（按窗口聚合所有**已去重**点位）：
  - **open / close**：窗口内点位按
    `(timestamp_ms, seq NULLS LAST, tick_key)` 全序排序后的首条 / 末条价格；
    `tick_key` 是最终并列裁决，保证纯输入集合的函数性；
  - **high / low**：全部点位价格的最大值 / 最小值；
  - **volume**：全部点位 `quantity` 之和；
  - **tick_count**：参与聚合的不同点位数。
- 迟到点位：晚到但时间戳更早的点位始终被接受，聚合结果按时间戳而非到达时间
  重算（见 `test_late_tick_*`）。

## 幂等与一致性规则

1. **批次幂等（重试安全）**：以 `batch_id` 为幂等键。同一 `batch_id` 的重复
   请求（即使请求体不同或为空）直接返回**首次**处理结果（`duplicate=true`），
   绝不二次写入或二次删除。
2. **跨批次内容幂等（乱序/重放收敛）**：
   - 无 `id` 点位：内容键相同即同一点位，重复提交不会重复插入、不重复累计量；
   - 有 `id` 点位且内容完全一致：同上，视为重复；
   - 有 `id` 但内容冲突：追加接口**拒绝并标记 `id_conflict`，不覆盖旧值**；
     需要修正请使用 replace-range。
   - 因此同一输入集合无论拆分方式、到达顺序、重试次数如何，最终 K 线一致。
3. **批内去重**：同一批次内 `(symbol, key)` 相同的后续记录被拒绝并标记
   `duplicate_within_batch`。
4. **原子提交与回滚**：每个批次（含区间重建）在一个
   `BEGIN IMMEDIATE` 事务内完成"查重 + 写点位 + 写拒绝记录 + 写批次元数据"。
   任何一步失败全部回滚，不留半成品；失败后可用同一 `batch_id` 安全重试。
5. **区间重建**：replace-range 在同一事务内
   `DELETE 区间旧点位 → INSERT 新点位`，语义为"该 symbol 该区间以此集合为准"。
   重建幂等同样由 `batch_id` 保证；重复请求返回首次的 `deleted_before`，不会
   删掉重建之后才到达的相邻数据。区间外 / 其他 symbol 的记录被标记
   `out_of_range`。
6. **并发**：
   - 进程内写事务由全局写锁串行化，配合 SQLite WAL（`busy_timeout=30s`、
     `synchronous=FULL`）支持跨进程单写多读；
   - 查询只读取已提交数据，读不到部分写入；
   - 同一 `(symbol, 窗口)` 的并发写入不会出现重复累计或相互覆盖
     （见 `tests/test_concurrency.py`，含多进程用例）。
7. **重启不丢**：已提交事务在 `synchronous=FULL` + WAL 下落盘；重启后点位、
   批次幂等记录、拒绝留痕均保留。

## 错误分类

请求级错误返回 JSON `{"error": ..., "detail": ...}`：

| 场景 | HTTP 状态 | error |
| --- | --- | --- |
| `batch_id`/`symbol` 为空、区间非法、批次过大 | 400 | `validation_error` |
| 存储冲突 | 409 | `conflict`（调用方应重试） |
| 存储层故障 | 500 | `storage_error`（可重试；写入事务已回滚） |

记录级问题**不静默丢弃**，逐条出现在响应的 `rejected` 数组中，并持久化到
`rejected_records` 表（含 `batch_id`、下标、原因、说明、原始内容）：

| reason | 判定 |
| --- | --- |
| `missing_symbol` | `symbol` 缺失或为空白 |
| `bad_timestamp` | `timestamp_ms` 非非负整数毫秒 |
| `bad_price` | `price` 缺失、不可解析、非有限值或 `<= 0` |
| `bad_quantity` | `quantity` 缺失、不可解析、非有限值或为负 |
| `bad_seq` | `seq` 提供但不是整数 |
| `empty_id` | `id` 提供但为空白字符串 |
| `duplicate_within_batch` | 批次内与更早的 `(symbol,key)` 相同 |
| `id_conflict` | 跨批同一 `(symbol,id)` 但内容不同（追加不覆盖） |
| `out_of_range` | 重建记录不属于目标 symbol / 时间区间 |

## 本地验证方法

全部测试仅使用 `tmp_path` 下的临时 SQLite 文件与 127.0.0.1 回环地址，
**无任何外部服务依赖**。覆盖：

- `tests/test_aggregation.py`：窗口边界、乱序无关、同毫秒并列裁决、迟到点位；
- `tests/test_validation.py`：全部无效记录分类、批内去重、请求级 400；
- `tests/test_idempotency.py`：重复批次、乱序收敛、迟到、区间重建与重试、
  `id_conflict`、区间外拒绝；
- `tests/test_persistence.py`：重启后数据与幂等记录不丢、注入故障后整批回滚、
  同 `batch_id` 重试成功；
- `tests/test_concurrency.py`：多线程写不重复累计、同批次并发重试恰好一次生效、
  读写并发视图自洽、重建事务串行化、**多进程**写同一库精确无丢失；
- `tests/test_api.py`：HTTP 端到端、根路由兼容、错误码。

测试以 INFO 日志打印每个用例的输入数据和判定依据，例如：

```
INFO tests.idempotency | 首次 accepted=4，重试1 duplicate=True ...
INFO tests.concurrency | 并发结果 tick_count=200 volume=200.0
```

## 存储结构

- `ticks`：点位主表，主键 `(symbol, tick_key)`（无 rowid），含时间、价格、
  数量、seq、来源批次；
- `batches`：批次幂等记录（接受数、拒绝明细 JSON、模式、重建删除数）；
- `rejected_records`：被拒绝记录的持久化留痕。
