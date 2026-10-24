# sql/

五层（ODS / DWD / DIM / DWS / ADS）加两张伴生表的一行一层形状表——表名、类型、分区、主键、bucket——见
[`.scratch/ecom-user-behavior-pipeline/issues/05-offline-layer-scope.md`](../.scratch/ecom-user-behavior-pipeline/issues/05-offline-layer-scope.md)
§「下半 · 五层形状」。

## 编排脚本（`make replay` 按序执行）

| 脚本 | 层 | 做什么 |
| --- | --- | --- |
| `replay_00_catalog.sql` | — | 建 Paimon catalog（`file:/warehouse`）与 `ecom_lake` 库 |
| `replay_01_ddl.sql` | — | 全部表 DDL（`IF NOT EXISTS`，可重复执行） |
| `replay_10_ods.sql` | ods | `raw.jsonl` → `ods_behavior_event`（append，`dt`=**落地日**） |
| `replay_20_dwd.sql` | dwd | ODS → `dwd_behavior_event` + `dwd_behavior_event_reject`（三值路由） |
| `replay_30_dim.sql` | dim | `dim.jsonl` → 三张拉链表 + 两张静态维（`LEAD` 算区间） |
| `replay_40_dws.sql` | dws | DWD × 时态维**区间 join** → `dws_subject_window` |
| `replay_50_ads.sql` | ads | DWS/DWD → `ads_window_metric` + `dwd_late_event_count` |
| `replay_90_maintain.sql` | — | `ALTER TABLE … SET` 快照保留 ×11 ＋ `sys.compact(full)` ×11 ＋ `sys.expire_snapshots(retain_max=>20)` ×11（impl-03d） |
| `assert_lake.sql` | — | 离线断言哨兵集（A1/A3/幂等指纹/快照/拉链/属性），由 `scripts/lake_assert.sh` 判定（impl-03d） |

顺序不可交换（后者读前者的表）。每层脚本以一行 `layer=<层> rows=…` 收尾，
`scripts/lake_replay.sh` 用它当该层跑成功的**正向证据**（marker 缺失即判失败）。

维护段（`replay_90_maintain.sql`）**不在 `LAYER=` 体系里**：它只在**全量 replay** 时跑
（`LAYER=` 截断时数据不完整，压了白做），可观察性行用 `maintain rows=`
而**不是** `layer=maintain` —— `layer=` 是五值的层词汇表，多造一个会是永远选不中的标签。

## 断言（`assert_lake.sql` + `scripts/lake_assert.sh`）

判据**不看退出码**：`sql-client.sh -f` 语句报错后仍以 0 退出
（源码事实：`CliClient` 丢弃返回值、无 `System.exit`；本仓实机复核过）。
⇒ 每条断言写成哨兵 `SELECT '<名>' AS assertion, 'ok=true' AS ok`，
脚本解析 stdout 后判「无错误标记 **且** 每条期望哨兵 `ok=true`」。

两条**输出语法**约束（改一处必改另一处 + `tests/test_lake_assert.py`）：

1. 哨兵名 **≤ 26 字符** —— TABLEAU 会把超宽单元格渲染成 `xxx...`，
   截断后按名核对会"找不到"，而真实原因从错误信息里完全看不出。
2. `sql/` 下的注释里**不得**出现带方括号的错误标记字面量 ——
   sql-client 会**逐字回显** SQL 源码（含注释），而两个判定面都在全文上找该标记
   ⇒ 注释里写一个会把一次**成功**的运行判成失败（本票实测踩了两次）。

跑法：`make assert`（只判断言）/ `make replay-idempotent`（同批重放两遍后比 DWD 指纹）。

## 三条最容易写反的约定

1. **落地日过滤**：`ods_behavior_event` 是 **append + 落地日分区**（跨天累积）
   ⇒ 读它必须过滤 `dt = DATE_FORMAT(CURRENT_TIMESTAMP,'yyyy-MM-dd')`
   （`replay_10_ods.sql` 与 `replay_20_dwd.sql` 都这么做，且表达式逐字相同）。
   `dwd_behavior_event` / `dws_subject_window` / `ads_window_metric` 是**主键表 +
   整表 `INSERT OVERWRITE`** ⇒ 读它们**不要**过滤，否则会读空。
   ★ `assert_lake.sql` 里 **A1 与 A2 的 ODS 侧**也限定本次落地日、理由同上：
     不过滤的话，A1 会依赖「历史上没落过别的批」，A2 报的是「跑过几次」而不是
     「这批有多少重复」（本票实测：首版 A2 得到 1 048 576，正是前一天那一整个分区）。
     ⚠️ 这与 spec §5.4 的「全局比对」不冲突——那句话的目的是「不按**事件日**分区比」，
     本条是「把 ODS 侧收窄到本次落地批」，粒度不同；已在 03d 票面登记。
2. **时态维 join**：拉链表用 `BETWEEN valid_from_ms AND valid_to_ms` 的 **batch
   区间 join**；**不是** lookup join（processing-time，回放必错），也不是
   `FOR SYSTEM_TIME AS OF`。Paimon 无 zipper/SCD 官方能力，区间 join 就是正解。
   ★ 注意区间**两侧都是闭区间**（`valid_to_ms = 下一版 valid_from_ms − 1`）：
   写侧 `− 1` 与读侧 `BETWEEN` 是一对，混搭会让边界那一毫秒两版都不命中。
   ⚠️ 「lookup 是 processing-time / Paimon 无 zipper」取自本仓 spec §5.2 与 [05] Q10
   的一致口径，**没有逐字版本号/URL** ⇒ 按 HLD 的分法属**设计值级**约束，不是官方保证。
3. **幂等指纹不用 `crc32`**：Flink 2.2 SQL **没有** `crc32`
   （实机逐字：`SqlValidatorException: No match found for function signature
   crc32(<CHARACTER>)`）—— spec §5.4 的伪代码里写了它，但伪代码不能执行。
   改用 `SUM(CAST(HASH_CODE(...) AS BIGINT))`：`HASH_CODE` 返回 **INT**，
   百万行求和会溢出，**先 CAST 成 BIGINT** 是必需的（溢出后的值两次跑之间照样稳定
   ⇒ 会是一条"恒绿但不校验"的假指纹）。

三条约定都有静态守卫：前两条见 [`tests/test_replay_sql.py`](../tests/test_replay_sql.py)
与 [`tests/test_replay_temporal_sql.py`](../tests/test_replay_temporal_sql.py)，
第三条见 [`tests/test_lake_assert.py`](../tests/test_lake_assert.py)
（`test_assert_sql_does_not_use_crc32` / `test_assert_sql_hash_code_is_cast_before_sum`）。
