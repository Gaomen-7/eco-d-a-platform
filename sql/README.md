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

顺序不可交换（后者读前者的表）。每层脚本以一行 `layer=<层> rows=…` 收尾，
`scripts/lake_replay.sh` 用它当该层跑成功的**正向证据**（marker 缺失即判失败）。

## 两条最容易写反的约定

1. **落地日过滤**：`ods_behavior_event` 是 **append + 落地日分区**（跨天累积）
   ⇒ 读它必须过滤 `dt = DATE_FORMAT(CURRENT_TIMESTAMP,'yyyy-MM-dd')`
   （`replay_10_ods.sql` 与 `replay_20_dwd.sql` 都这么做，且表达式逐字相同）。
   `dwd_behavior_event` / `dws_subject_window` / `ads_window_metric` 是**主键表 +
   整表 `INSERT OVERWRITE`** ⇒ 读它们**不要**过滤，否则会读空。
2. **时态维 join**：拉链表用 `BETWEEN valid_from_ms AND valid_to_ms` 的 **batch
   区间 join**；**不是** lookup join（processing-time，回放必错），也不是
   `FOR SYSTEM_TIME AS OF`。Paimon 无 zipper/SCD 官方能力，区间 join 就是正解。
   ★ 注意区间**两侧都是闭区间**（`valid_to_ms = 下一版 valid_from_ms − 1`）：
   写侧 `− 1` 与读侧 `BETWEEN` 是一对，混搭会让边界那一毫秒两版都不命中。
   ⚠️ 「lookup 是 processing-time / Paimon 无 zipper」取自本仓 spec §5.2 与 [05] Q10
   的一致口径，**没有逐字版本号/URL** ⇒ 按 HLD 的分法属**设计值级**约束，不是官方保证。

两处约定都有静态守卫：见 [`tests/test_replay_sql.py`](../tests/test_replay_sql.py)
与 [`tests/test_replay_temporal_sql.py`](../tests/test_replay_temporal_sql.py)。
