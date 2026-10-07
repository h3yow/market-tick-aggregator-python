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

## 1A. 区间整体替换式修正（correction）

当**某一段历史数据整段录错**时，普通写入（append）无法把错误数据"改对"：
新点位只会与旧点位叠加，累计成交量越滚越大。区间整体替换式修正用于解决
这一场景：针对某个标的与一段时间区间，提交一份**修正后的完整点位集合**，
这段区间的 K 线完全以这份集合为准。

### 1A.1 时间范围与边界口径（可复现）

* 修正区间为 **`[start, end)`，左闭右开**，单位 Unix 毫秒；必须满足
  `0 <= start < end`。
* 点位按其 **`ts` 精确归属**，而不是按分钟窗口整体归属：
  * 被替换（删除）：该标的所有满足 `start <= ts < end` 的旧点位；
  * 不受影响：`ts < start` 或 `ts >= end` 的点位。特别地，**`ts == end`
    的点位不属于区间**，原样保留。
  * 因此当一个分钟窗口只有一部分落在区间内时，**窗口内区间外的点位保留，
    区间内的点位被替换**，该窗口重算后等于"区间外旧点 + 本次新点"。
* 受影响（重算）的窗口集合 = 删除前区间内旧点所在窗口 ∪ 新点位所在窗口；
  其它窗口、其它标的一概不动。
* 修正集合中的每条记录都必须 `symbol == 路径标的` 且
  `start <= ts < end`，否则整次修正被拒绝（见第 4 节）。
* **`records` 允许为空数组**，语义是"把这段区间清空"。

### 1A.2 "替换而不是叠加"的数据语义

* 一次修正在**同一个 `BEGIN IMMEDIATE` 事务**内依次完成：
  1. 登记修正幂等记录（`correction_id`）；
  2. 删除区间内全部旧点位（包括更早的普通写入与更早的修正写入）；
  3. 插入本次修正点位；
  4. 对受影响窗口以 ticks 事实表**全量重算并 UPSERT**（无点则删除该窗口）。
* 因此区间窗口不可能出现新旧叠加：删除与插入在同一事务，查询只能看到
  修正前或修正后的完整状态，不存在"改了一半"的中间状态。
* 修正之后再发生的事情按既有语义处理：
  * **普通写入**（`/ticks/batch`）仍是 append——若它落在某个已修正区间内，
    它会追加到修正集合之上（这是"新来的真实成交"）；需要再次整段改对时，
    再提交一次覆盖该区间的修正即可。
  * **重叠修正**：后一次修正只替换它自己区间内的点位；对同一区间再次修正
    即"修正上一次修正"，结果以后提交的集合为准，同样不叠加。
  * `rebuild` 仍只以当前 ticks 事实表为准重算，行为与普通补写后完全一致。

### 1A.3 重复提交、重试与拆批规则

* `correction_id` 由调用方提供，作为修正的唯一幂等键（与普通 `batch_id`
  共享幂等表，**两类键不得互相复用**，复用返回 409）。
* 首次成功返回 `status="applied"` 与 `removed/inserted` 计数；相同
  `correction_id` + **相同语义负载**重放（含失败重试）返回
  `status="duplicate"`，`removed=0`、不改动任何数据、不重复生效。
* 负载指纹对点位按 `(ts, price, size)` 排序后计算 SHA-256：
  **同一份修正集合无论记录书写顺序如何，指纹都相同**；同一毫秒内同价同量
  的重复点位仍按既有规则判为批内重复（整次拒绝）。
* 相同 `correction_id` 但负载（区间或集合内容）不一致返回
  `409 correction_id_conflict`，首次修正保持不变。
* 任一步失败（含记录非法、事务冲突外的存储错误）整体回滚，区间旧数据
  原样保留，可用**同一个 `correction_id`** 修正后安全重试。
* **拆成几批提交**的口径：把修正集合沿时间切成若干**相邻、互不相交**的
  子区间（如 `[a,b)`、`[b,c)`、`[c,d)`），各自使用不同 `correction_id`
  提交。因为归属口径只依赖点位 `ts`、聚合与到达顺序无关，所以这些子修正
  **无论以什么顺序提交、是否重试**，最终几根 K 线的 OHLCV 与一次性整段
  提交完全一致（有参数化换序测试覆盖）。注意：不要把**同一子区间**拆成
  多次提交——后一次会按替换语义删掉前一次的点位。
* 并发规则与普通写入相同：进程内写事务串行化 + SQLite 事务锁，多个修正、
  修正与普通写入并发时表现为某种可复现的**串行提交序列**，每次提交后
  ticks/candles 都互相自洽；相同 `correction_id` 并发重试只有一次生效。

### 1A.4 本地对照校验

`POST /api/v1/admin/symbols/{symbol}/verify` 以 ticks 事实表重算该标的
全部窗口，与 `candles` 物化结果逐窗口比对，返回
`consistent / checked_windows / mismatches`；每个不一致窗口给出
`expected`（按 ticks 应有）与 `actual`（candles 现存），可检出
数值不符、窗口缺失、幽灵窗口三类脱节，供排查与审计。

## 2. 数据模型与存储

SQLite 文件由 `TICK_DB_PATH` 环境变量指定，默认 `data/ticks.db`。

* `batches(batch_id PK, payload_hash, received, accepted, rejected, rejected_json, created_at_ms, kind, corr_symbol, corr_start, corr_end)`
  —— 普通批次与修正共用一张幂等表：`kind='ingest'|'correction'`；
  修正行额外记录标的与 `[corr_start, corr_end)` 区间（旧版本数据库会在
  启动时自动补齐新增列，历史普通批次默认 `kind='ingest'`）。
* `ticks(id PK, batch_id FK, batch_index, symbol, ts, price, size, UNIQUE(batch_id, batch_index))`
  —— 行情点事实表，是唯一数据真相（source of truth），索引 `(symbol, ts)`。
  修正写入的点位与普通写入同表存放（`batch_id=correction_id`），因此删除、
  重建与对照校验不需要区分点位来源。
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
* 历史数据补写（**追加**缺失数据）：直接以新批次写入历史时间戳即可。
* 历史数据**整段录错需要替换**：使用区间整体替换式修正接口（见第 1A 节），
  而不是再追加一批点位。
* 修正/补写/重放后重建：`POST /api/v1/admin/symbols/{symbol}/rebuild` 以
  `ticks` 表为准重建该标的全部窗口，并清除事实表中已不存在的陈旧窗口。
  修正与普通补写共用同一张事实表，因此重建逻辑无需区分数据来源。

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
| `duplicate_tick_in_batch` | 同一批次/修正集合内 `(symbol, ts, price, size)` 完全重复 |
| `symbol_mismatch` | 修正记录的 `symbol` 与路径标的不一致 |
| `tick_out_of_range` | 修正记录的 `ts` 不满足 `start <= ts < end` |

批次级错误码（`error_code`）：

| error_code | HTTP | 含义 |
| --- | --- | --- |
| `empty_batch` | 422 | `records` 为空（仅普通写入；修正允许空集合表示清空） |
| `batch_too_large` | 422 | 单批/单次修正超过 10,000 条 |
| `invalid_batch_id` | 422 | `batch_id` 为空/非字符串 |
| `invalid_records` | 422 | 含无效记录（整批回滚，明细见响应体） |
| `invalid_symbol` / `invalid_start` / `invalid_end` / `invalid_range` | 422 | 查询或修正参数非法、`end < start`，或修正区间不满足 `start < end` |
| `invalid_correction_id` | 422 | `correction_id` 为空/非字符串 |
| `records_out_of_range` | 422 | 修正集合含 `symbol_mismatch` / `tick_out_of_range` 记录（整次修正回滚） |
| `batch_id_conflict` | 409 | 相同 `batch_id` 负载不一致 |
| `correction_id_conflict` | 409 | 相同 `correction_id` 负载不一致，或幂等键被另一类写入占用 |

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

### `POST /api/v1/symbols/{symbol}/corrections`

按标的与 `[start, end)` 区间做整体替换式修正（语义见第 1A 节）：

```json
{
  "correction_id": "20260928-aaa-fix-001",
  "start": 1789000020000,
  "end": 1789000140000,
  "records": [
    {"symbol": "AAA", "ts": 1789000020000, "price": 10.20, "size": 3},
    {"symbol": "AAA", "ts": 1789000059999, "price": 10.30, "size": 2}
  ]
}
```

成功响应：

```json
{
  "correction_id": "20260928-aaa-fix-001",
  "symbol": "AAA",
  "start": 1789000020000,
  "end": 1789000140000,
  "status": "applied",
  "removed": 5,
  "inserted": 2,
  "affected_windows": ["AAA@1789000020000"]
}
```

* `records` 可为 `[]`，表示清空该区间（`status="applied"`、`inserted=0`）。
* 同 `correction_id` + 同语义负载重放返回 `status="duplicate"`、
  `removed=0` 与首次 `inserted` 计数；负载不一致返回
  `409 correction_id_conflict`。
* 任何记录非法（含越界、symbol 不一致、批内重复）整次 422、区间不改动，
  `rejected_records` 逐条给出原因。

### `POST /api/v1/admin/symbols/{symbol}/verify`

以 ticks 事实表重算并与 candles 逐窗口对照：

```json
{
  "symbol": "AAA",
  "consistent": true,
  "checked_windows": 12,
  "mismatches": []
}
```

不一致时 `consistent=false`，`mismatches` 中每项形如
`{"window_start": ..., "expected": {<candle>|null}, "actual": {<candle>|null}}`。

### `POST /api/v1/admin/symbols/{symbol}/rebuild`

返回 `{"symbol": "...", "rebuilt_windows": <int>}`。

### 兼容与健康

* `GET /` 保持骨架期行为：`{"Hello": "World"}`。
* `GET /health` 返回 `{"status": "ok"}`。

## 6. 本地验证方法

```bash
# 运行全部单测（纯本地 SQLite 临时库，不访问网络/外部服务）
uv run pytest -q

# 默认即实时打印每条用例记录的输入数据与判定依据（tests logger），
# 以及服务侧实际收到的批次/修正负载（market_tick.service logger）；
# 需要更安静时可覆盖：
uv run pytest -q --log-cli-level=WARNING

# 启动服务
TICK_DB_PATH=data/ticks.db uv run python main.py   # 默认 127.0.0.1:8000

# 手工审计某标的：K 线与原始点位是否对得上
curl -s -XPOST http://127.0.0.1:8000/api/v1/admin/symbols/AAA/verify
```

测试覆盖场景：

1. 重复批次（相同负载幂等、不同负载 409、并发相同 `batch_id` 重试）；
2. 乱序与迟到点位、同一集合三种到达顺序的顺序无关性；
3. 全部记录级无效分类、批内重复点位、空批次/超长批次；
4. 失败整批回滚 + 同 `batch_id` 修正后重试只生效一次；
5. 并发同窗口写入（不重复累计）、并发查询只见自洽已提交快照；
6. 进程重启后数据不丢失并可继续幂等重放；
7. 历史补写与 rebuild 重建结果一致性、陈旧窗口清理；
8. 根路由历史行为兼容；
9. **区间整体替换式修正**：
   * `[start, end)` 边界归属（`ts==start` 替换、`ts==end` 保留）、
     窗口部分命中只替换区间内点位、其它窗口/其它标的不动、空集合清空；
   * 替换而非叠加（旧成交量不残留）、重叠修正以后者区间为准；
   * 同 `correction_id` 重复提交/乱序重放只生效一次、不同负载 409、
     幂等键跨类复用冲突、失败回滚后同 id 安全重试；
   * 同一修正集合拆成相邻不相交子区间、换序/换批提交结果与一次性提交一致；
   * 修正与普通写入并发（无中间状态、verify 自洽）、并发同 id 只生效一次；
   * 修正 + 补写 + 重放 + rebuild 后结果与"只按当前有效点位重算"一致；
   * 旧版本数据库自动迁移后新能力可用；
10. **本地对照校验**：一致场景通过；数值被改、窗口缺失、幽灵窗口均能被
    `verify` 检出并给出 expected/actual 明细。
