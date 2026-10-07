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

* `batches(batch_id PK, kind, payload_hash, received, accepted, rejected, rejected_json, created_at_ms)`
  —— 普通批次与修正批次共用的幂等键与负载指纹；`kind` 为 `ingest` 或
  `correction`（修正批次的 `batch_id` 即 `correction_id`）。
* `ticks(id PK, batch_id FK, batch_index, symbol, ts, price, size, superseded_by_correction, UNIQUE(batch_id, batch_index))`
  —— 行情点事实表，是唯一数据真相（source of truth），索引 `(symbol, ts)`。
  被区间修正取代的点位以 `superseded_by_correction` 标记（行保留、可审计），
  只有该字段为空的「当前有效点位」参与聚合。
* `candles(symbol, window_start, open, high, low, close, volume, tick_count, PK(symbol, window_start))`
  —— 分钟 K 线物化结果，可由 `ticks` 表当前有效点位随时完全重建。
* `corrections(correction_id PK, symbol, start_ts, end_ts, total_parts, payload_hash, status, created_at_ms, applied_at_ms)`
  —— 区间修正元数据；`status` 为 `pending`（分片未到齐）或 `applied`（已生效）。
* `correction_parts(correction_id, part_index, part_hash, received, accepted, PK(correction_id, part_index))`
  —— 每个已接收分片的幂等记录与分片指纹。
* `correction_staging(correction_id, part_index, tick_index, symbol, ts, price, size, PK(correction_id, part_index, tick_index))`
  —— 分片到齐前的暂存点位；修正整体生效后删除（审计事实保留在 `ticks`）。

打开功能上线前的旧库时会自动幂等迁移（为 `batches` 补 `kind`、为 `ticks`
补 `superseded_by_correction`），既有数据保留、旧点位默认全部有效
（有旧库迁移测试覆盖）。

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

## 3A. 区间整体替换式修正（correction）

针对「某一段历史整段录错」的场景：补写只能在错误数据上**叠加**，累计成交量
会越滚越大；而区间整体替换式修正会让指定区间内的 K 线**完全以新提交的点位
集合为准**。

### 3A.1 时间范围与数据语义

* 修正针对单个标的 `symbol` 与一个**半开时间区间**：

  ```
  start <= ts < end        # start 含、end 不含，单位 Unix 毫秒
  ```

* 归属判定**按点位的原始时间戳 `ts`，而不是按分钟窗口**。
  因此对于跨越区间边界的分钟窗口，只有落在区间内的点位被替换；窗口内落在
  区间外的点位原样保留，并继续参与该窗口聚合。
* 替换语义（**replace，不是 append**）：修正生效时——
  1. 区间内、该标的的**当前有效点位**全部被本次修正取代；
  2. 提交的修正点位成为该区间内新的有效点位集合；
  3. 被取代的旧点位**不做物理删除**，而是在 `ticks` 表中以
     `superseded_by_correction = <correction_id>` 标记，行保留用于审计追溯；
  4. 区间以外的点位、以及其它标的的数据一行不动。
* 只有「当前有效点位」（`superseded_by_correction = ''`）参与 K 线聚合、
  rebuild 枚举与 verify 对账。一次修正若与更早的修正区间重叠，后生效修正的
  区间内会连先前修正带入的点位一并取代，形成清晰的取代链。
* 修正允许提交**空点位集合**：表示把整段区间清空（对应窗口若无其它有效点位，
  其 K 线被删除）。
* 修正点位与普通点位遵守同一套确定性全序
  `(ts, price, size, batch_id, batch_index)`。修正点位的 `batch_index`
  在生效时按该全序重新分配（与分片方式、到达顺序无关），保证同一份修正集合
  无论以何种顺序、拆成多少片提交，最终 OHLCV 完全一致。

### 3A.2 分片、幂等与重试

一次修正由调用方给定 `correction_id`（幂等键），可拆成 `total_parts` 片，
每片带 `part_index`（从 0 开始）：

* 各片**允许乱序到达**；服务端暂存已到分片，`status="pending"`。
* 当最后一片到齐时，在**单个事务**内完成「标记取代 + 写入修正点位 +
  重算全部受影响窗口」，该响应 `status="applied"`。
* 重复提交：
  * 修正已生效后重放（含点位书写顺序不同的同语义负载）→ `status="duplicate"`，
    不重复累计、不改变任何窗口；
  * 修正未到齐时重发已暂存的同分片 → `status="duplicate"`。
* 同 `correction_id` 出现下列不一致返回 `409 correction_conflict`，已有状态
  保持不变：`symbol/start/end/total_parts` 元数据变化；某分片的点位内容变化；
  已生效修正的整体负载指纹不一致。
* 每个分片内的点位逐条校验（同普通写入），并额外要求：
  点位 `symbol` 必须等于路径标的（`symbol_mismatch`）、
  点位 `ts` 必须在 `[start, end)` 内（`tick_outside_range`）、
  同一片内与不同片之间不允许出现完全相同的点位
  （`duplicate_tick_in_correction`）。任一条不满足，**该分片整方拒绝**
  （422，不暂存任何内容，已暂存的其它分片不受影响），修正后可用同一
  `correction_id` 安全重试。
* 单次修正的点位总量上限为 100,000（所有分片合计）。
* 注：幂等是针对「同一种拆分方式」的重放。把同一份数据改成不同的片数/分片
  边界属于另一次提交，请使用新的 `correction_id`。

### 3A.3 并发与事务

* 普通批次写入与区间修正共用同一个写事务串行化机制（进程内 `RLock` +
  `BEGIN IMMEDIATE`）。修正与普通写入、修正与修正并发时彼此串行提交，
  不出现「改了一半」的中间状态。
* 修正重算的窗口集合 = 新点位所在窗口 ∪ 区间内被取代旧点位所在窗口，因此
  只影响真正被覆盖到的窗口；其它标的、其它时间段的数据不被触碰。
* 并发提交同一修正的「最后一片」时，由 `batches` 主键保证只有一次生效，
  其余并发请求得到 `duplicate`。

### 3A.4 与补写 / 重建的关系

* 三者可自由混用：普通补写（append）、区间替换（replace）、rebuild（以有效
  点位全量重算）操作的都是同一份 `ticks` 事实表。
* rebuild 只枚举**当前有效点位**所在窗口，因此修正后执行 rebuild，结果与
  增量聚合结果一致（有「修正→补写→重建」一致性测试覆盖）。
* 在全新数据库按相同事件顺序重放（含修正的幂等重放）得到的 K 线与原库一致。

### 3A.5 本地对账 / 审计

* `GET /api/v1/admin/symbols/{symbol}/verify`：按「当前有效点位」逐窗口重算，
  与物化 `candles` 表比对，返回每个不一致窗口的 `kind`
  （`value_mismatch` / `missing_candle` / `stale_candle`）及
  `stored`（物化值）与 `expected`（重算值）。
* 命令行（只读，不改数据库；发现差异时退出码为 1）：

  ```bash
  uv run python tools/verify.py --db data/ticks.db --symbol AAA
  uv run python tools/verify.py --db data/ticks.db --all
  ```

* 被取代的原始点位始终保留在 `ticks` 表（带 `superseded_by_correction`
  标记），可直接 SQL 审计「某次修正具体替换掉了哪些点位」。

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
| `duplicate_tick_in_batch` | 同一普通批次内 `(symbol, ts, price, size)` 完全重复 |
| `symbol_mismatch` | 修正点位的 `symbol` 与修正路径标的不一致 |
| `tick_outside_range` | 修正点位的 `ts` 不在半开区间 `[start, end)` 内 |
| `duplicate_tick_in_correction` | 同一修正的同一片内或不同片之间出现完全相同点位 |

批次级错误码（`error_code`）：

| error_code | HTTP | 含义 |
| --- | --- | --- |
| `empty_batch` | 422 | `records` 为空（仅普通批次；修正允许空集合） |
| `batch_too_large` | 422 | 单批超过 10,000 条 |
| `invalid_batch_id` | 422 | `batch_id` 为空/非字符串 |
| `invalid_records` | 422 | 含无效记录（整批/整分片回滚，明细见响应体） |
| `invalid_symbol` / `invalid_start` / `invalid_end` / `invalid_range` | 422 | 查询或修正参数非法、`end < start` |
| `batch_id_conflict` | 409 | 相同 `batch_id` 负载不一致 |
| `invalid_correction_id` | 422 | `correction_id` 为空/非字符串 |
| `invalid_total_parts` / `invalid_part_index` | 422 | 分片数非正整数，或片序号越界 |
| `correction_too_large` | 422 | 修正点位总量超过 100,000 |
| `correction_conflict` | 409 | 相同 `correction_id` 的元数据/分片/整体负载不一致 |

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

提交一个区间整体替换式修正分片（单批修正即 `total_parts=1, part_index=0`）。

```json
{
  "correction_id": "20261007-fix-aaa-001",
  "start": 1789000020000,
  "end":   1789000080000,
  "total_parts": 2,
  "part_index": 0,
  "records": [
    {"symbol": "AAA", "ts": 1789000021000, "price": 10.20, "size": 100}
  ]
}
```

成功响应：

```json
{
  "correction_id": "20261007-fix-aaa-001",
  "symbol": "AAA",
  "start": 1789000020000,
  "end": 1789000080000,
  "total_parts": 2,
  "part_index": 0,
  "status": "pending",
  "received": 1,
  "accepted": 1,
  "affected_windows": []
}
```

`status` 取值：`pending`（本片已暂存，仍有其它片未到齐）、
`applied`（本片是最后到齐的一片，区间替换已整体生效；
`affected_windows` 列出实际重算的窗口）、`duplicate`（幂等重放）。
时间口径为 `[start, end)`，按点位时间戳归属；详见 3A 节。

### `GET /api/v1/admin/symbols/{symbol}/verify`

按当前有效点位逐窗口重算并与物化 K 线对账：

```json
{
  "symbol": "AAA",
  "ok": true,
  "checked_windows": 12,
  "mismatches": []
}
```

不一致时 `ok=false`，`mismatches` 中每项给出 `window_start`、`kind`
（`value_mismatch` / `missing_candle` / `stale_candle`）以及
`stored` 与 `expected` 明细。

### `POST /api/v1/admin/symbols/{symbol}/rebuild`

返回 `{"symbol": "...", "rebuilt_windows": <int>}`。

### 兼容与健康

* `GET /` 保持骨架期行为：`{"Hello": "World"}`。
* `GET /health` 返回 `{"status": "ok"}`。

## 6. 本地验证方法

```bash
# 运行全部单测（纯本地 SQLite 临时库，不访问网络/外部服务）
# 默认即开启 INFO 级实时日志，可直接看到每条用例的输入数据与判定依据
uv run pytest -q

# 启动服务
TICK_DB_PATH=data/ticks.db uv run python main.py   # 默认 127.0.0.1:8000

# K 线与当前有效点位的本地对账（只读；发现差异退出码为 1）
uv run python tools/verify.py --db data/ticks.db --symbol AAA
uv run python tools/verify.py --db data/ticks.db --all
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
   * 半开区间边界归属（`start` 含、`end` 不含）、按时间戳而非窗口归属、
     跨界窗口只替换区间内部位、其它标的与区间外点位不动、空修正清空区间；
   * 替换而非叠加（旧点位不参与聚合，成交量不滚动），旧点位行保留可审计；
   * 同一份修正集合单片顺序变化 / 两片 / 三片乱序到齐的 OHLCV 完全一致；
   * 重复提交/重试只生效一次、元数据或负载冲突 409、分片重发幂等、跨片查重；
   * 无效分片整片回滚不留痕、暂存分片在末片失败后可重试、提交中途异常整体回滚；
   * 修正与普通写入并发、两个重叠区间修正并发：串行化、无中间状态、只影响
     被覆盖窗口；
   * 修正 → 补写 → rebuild 后结果与「只按当前有效点位重算」一致、新库重放一致；
10. **本地对账**：verify 对 `value_mismatch` / `missing_candle` /
    `stale_candle` 的识别，HTTP 端点与 `tools/verify.py` CLI（含退出码）；
11. **旧库迁移**：功能上线前的旧 schema 打开后自动补列、数据保留、可继续修正，
    修正结果跨进程重开持久化。
