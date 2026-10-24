-- replay_90_maintain.sql — 快照保留兜底 + compact ×N + expire ×N（impl-03d）
--
-- 规格落点：spec §5.3（`replay_90_maintain.sql`）、§5.4（快照 [5,20] / 「普通 SELECT
--          拉链」）、ticket impl-03d「交付内容」第 1 项。
--
-- ★ 本文件**写数据面**（改表属性 + 触发压缩/过期），不写断言；断言在 assert_lake.sql。
--
-- ════════════════════════════════════════════════════════════════════
-- ① 为什么需要这个文件（而不是「写完数就算完」）
-- ════════════════════════════════════════════════════════════════════
-- 纯 batch 下**没有常驻 writer**，两个后果：
--   a) append 表（bucket=-1）的 writer **不自压**小文件 —— 官方逐字
--      "For a normal unaware-bucket append table, the writer does not compact
--       files itself"（流式 sink 才有 coordinator）。本仓 ODS / reject /
--      dwd_late_event_count 三张都是 append ⇒ 不压就永远是一批一个小文件。
--   b) 主键表默认的 writer 压缩是**流式 writer** 行为；batch 作业跑完即无 writer。
-- ⇒ 维护动作必须由编排链**显式**调用，不能指望「写的时候自动发生」。
--
-- ════════════════════════════════════════════════════════════════════
-- ② `retain_max` 是**上界不是钉住**（这条最容易读错，spec §5.4 逐字点名）
-- ════════════════════════════════════════════════════════════════════
-- `CALL sys.expire_snapshots(table => ..., retain_max => 20)` 的官方参数说明是
--   "retain_max: the maximum number of completed snapshots to retain"
-- ⇒ 它是**上界**：快照数**不超过** 20，但不会被钉在 20。
-- 实际删到几个，由另外两条共同决定：
--   · `snapshot.num-retained.min`（本仓显式 5）—— 保护下界，官方逐字：
--       "When the number of snapshots is less than `snapshot.num-retained.min`,
--        no snapshots will be expired (even the condition `snapshot.time-retained` meet)"
--   · `snapshot.time-retained`（官方默认 1 h，本仓**不改**）
-- ★ **可观察形态**（本机实测见过，逐表数值留在票面 Progress、本文件不内嵌）：
--   调完 `retain_max => 20` 之后，快照数**可能远低于 20** —— 它既不是 20、
--   也不是调用前的那个数。**这就是「上界不是钉住」的证据**。
--   ⚠️ 不在此处写具体数字：那是随批次/环境漂移的量，写进来必 rot
--      （impl-03c 的 code-review HARD-2 判过同类）。
--   ⚠️ `retain_min` 缺省时回落表属性 `snapshot.num-retained.min` 这一点是
--      **源码级结论**（`ProcedureUtils#fillInSnapshotOptions`），
--      **官方文档未给出**该回落关系（只有 Spark 侧 `expire_changelogs` 有逐字表述）
--      ⇒ 按 spec §8.3 登记，不当作官方保证。故本文件**显式**在每张表上写 min=5。
--
-- ════════════════════════════════════════════════════════════════════
-- ③ `sys.compact` 的三处口径（哪些是官方、哪些是本仓推断）
-- ════════════════════════════════════════════════════════════════════
-- · `compact_strategy => 'full'` **只支持 batch 模式** —— 官方逐字。故本文件钉 batch。
-- · **sort compact 不可用**于本仓主键表 —— 官方逐字 "Sort Compact currently supports
--   only bucket=-1 and batch mode"，本仓主键表固定 bucket=1。
-- · ⚠️ `sys.compact` 对**主键表**的**逐句适用声明官方没有给出**（spec §8.3 登记项）：
--   官方 procedure 示例含主键表、且 `buckets` 参数明写 "supported only for
--   fixed-bucket tables" ⇒ 只能算**推断级**，不是官方承诺。
--   影响面：本仓 11 张表里**除 3 张 append 表外都是主键表**（bucket=1）——
--   即 8 张走的是推断级适用性，不是 1~2 张的边缘情况。
--   逐表调用是**保守选择**（可判、不依赖 `compact_database` 在 batch 下的未定行为）。
-- · 返回值的两种形态**都算成功、都不可当判据**：实测同一 `compact_strategy=>'full'`
--   在不同表上返回过 `JobID=…`（真的压了）与 `Success`（没什么可压）。
--   ⇒ 判据只能看「无错误标记」+ 后续断言的哨兵，**不能**解析这个返回值。
--   ⚠️ 注意：本文件的注释会被 sql-client **逐字回显**到 stdout，而编排脚本
--      `lake_replay.sh` 是在**全文**上找错误标记的 ⇒ 注释里**不得**写出带方括号的
--      错误标记字面量（会把一次成功的 replay 判成失败；本票实测踩过）。
--      静态守卫：tests/test_lake_assert.py::test_no_error_token_in_sql。
--
-- ════════════════════════════════════════════════════════════════════
-- ④ 成本与「不做」清单
-- ════════════════════════════════════════════════════════════════════
-- · Paimon `file://` 上的 compaction **成本数字官方未给出**（spec §8.3 登记项）。
--   本仓只在**本机**量过（量级：分钟级；逐项数值留票面，不写进文件）。
--   ⚠️ 那是**本机实测**，不是官方承诺、也不是跨机保证 —— 换机/换数据量都会变。
-- · **不做**（spec §8.2 明列）：排序 compaction（对 bucket=1 主键表官方不支持）·
--   把幂等检查放进 `replay`（全量重放两遍是重操作，由 03d 离线断言承担）·
--   「离线重算覆盖实时行」。
--
-- ★ `table.dml-sync`：procedure 的 CALL 不是 DML，但紧随其后的 SELECT 要读到
--   已完成的状态，故与层脚本一样钉上（无害且消除一类异步误读）。

SET 'sql-client.execution.result-mode' = 'TABLEAU';
SET 'execution.runtime-mode' = 'batch';
SET 'table.local-time-zone' = 'UTC';
SET 'table.dml-sync' = 'true';

CREATE CATALOG IF NOT EXISTS paimon WITH (
  'type' = 'paimon',
  'warehouse' = 'file:/warehouse'
);
USE CATALOG paimon;
USE ecom_lake;

-- ════════════════════════════════════════════════════════════════════
-- ⓪ 快照保留属性兜底（幂等）
-- ════════════════════════════════════════════════════════════════════
-- ★ 为什么**两处**都写（DDL 与本文件）：
--   `replay_01_ddl.sql` 的 11 个 `CREATE TABLE IF NOT EXISTS` 只在**首次建表**
--   生效；对**已存在**的表（本机这类长期演进的集群就是这样）它一句都不改。
--   于是「新建的干净库属性正确、老库属性还是官方默认 10」—— 两种状态并存，
--   而 `10` 也落在 [5,20] 里 ⇒ **断言看不出漏写**。
--   ⇒ 本段用 `ALTER TABLE ... SET` 逐表兜底（幂等，可重复执行）。
-- ★ 实测（impl-03d）：`ALTER TABLE ... SET` **不产生新快照**（改属性不是数据提交），
--   故本段不会把快照数推高、也不会让下面的 expire 更有可压之物。
-- ⚠️ `snapshot.time-retained` 在这里**不写**：保持官方默认 1 h（spec §5.4 逐字）。

ALTER TABLE ods_behavior_event SET (
  'snapshot.num-retained.min' = '5',
  'snapshot.num-retained.max' = '20'
);
ALTER TABLE dwd_behavior_event SET (
  'snapshot.num-retained.min' = '5',
  'snapshot.num-retained.max' = '20'
);
ALTER TABLE dwd_behavior_event_reject SET (
  'snapshot.num-retained.min' = '5',
  'snapshot.num-retained.max' = '20'
);
ALTER TABLE dim_item SET (
  'snapshot.num-retained.min' = '5',
  'snapshot.num-retained.max' = '20'
);
ALTER TABLE dim_category SET (
  'snapshot.num-retained.min' = '5',
  'snapshot.num-retained.max' = '20'
);
ALTER TABLE dim_user SET (
  'snapshot.num-retained.min' = '5',
  'snapshot.num-retained.max' = '20'
);
ALTER TABLE dim_region SET (
  'snapshot.num-retained.min' = '5',
  'snapshot.num-retained.max' = '20'
);
ALTER TABLE dim_channel SET (
  'snapshot.num-retained.min' = '5',
  'snapshot.num-retained.max' = '20'
);
ALTER TABLE dws_subject_window SET (
  'snapshot.num-retained.min' = '5',
  'snapshot.num-retained.max' = '20'
);
ALTER TABLE ads_window_metric SET (
  'snapshot.num-retained.min' = '5',
  'snapshot.num-retained.max' = '20'
);
ALTER TABLE dwd_late_event_count SET (
  'snapshot.num-retained.min' = '5',
  'snapshot.num-retained.max' = '20'
);

-- ════════════════════════════════════════════════════════════════════
-- ① compact ×11（append 表不自压 + 主键表 batch 后无 writer，见文件头 ①②③）
-- ════════════════════════════════════════════════════════════════════
-- 顺序 = 数据流顺序（便于按日志断点），语义上与顺序无关（逐表独立）。
-- 返回值不解析（见文件头 ③ 末条）。
CALL sys.compact(`table` => 'ecom_lake.ods_behavior_event', compact_strategy => 'full');
CALL sys.compact(`table` => 'ecom_lake.dwd_behavior_event', compact_strategy => 'full');
CALL sys.compact(`table` => 'ecom_lake.dwd_behavior_event_reject', compact_strategy => 'full');
CALL sys.compact(`table` => 'ecom_lake.dim_item', compact_strategy => 'full');
CALL sys.compact(`table` => 'ecom_lake.dim_category', compact_strategy => 'full');
CALL sys.compact(`table` => 'ecom_lake.dim_user', compact_strategy => 'full');
CALL sys.compact(`table` => 'ecom_lake.dim_region', compact_strategy => 'full');
CALL sys.compact(`table` => 'ecom_lake.dim_channel', compact_strategy => 'full');
CALL sys.compact(`table` => 'ecom_lake.dws_subject_window', compact_strategy => 'full');
CALL sys.compact(`table` => 'ecom_lake.ads_window_metric', compact_strategy => 'full');
CALL sys.compact(`table` => 'ecom_lake.dwd_late_event_count', compact_strategy => 'full');

-- ════════════════════════════════════════════════════════════════════
-- ② expire ×11（`retain_max => 20` 收**上界**，见文件头 ②）
-- ════════════════════════════════════════════════════════════════════
-- ★ 必须**放在 compact 之后**：compact 可能产生新的提交（实测新压出快照），
--   先 expire 再 compact 会让刚压出来的快照留在上界之外。
CALL sys.expire_snapshots(`table` => 'ecom_lake.ods_behavior_event', retain_max => 20);
CALL sys.expire_snapshots(`table` => 'ecom_lake.dwd_behavior_event', retain_max => 20);
CALL sys.expire_snapshots(`table` => 'ecom_lake.dwd_behavior_event_reject', retain_max => 20);
CALL sys.expire_snapshots(`table` => 'ecom_lake.dim_item', retain_max => 20);
CALL sys.expire_snapshots(`table` => 'ecom_lake.dim_category', retain_max => 20);
CALL sys.expire_snapshots(`table` => 'ecom_lake.dim_user', retain_max => 20);
CALL sys.expire_snapshots(`table` => 'ecom_lake.dim_region', retain_max => 20);
CALL sys.expire_snapshots(`table` => 'ecom_lake.dim_channel', retain_max => 20);
CALL sys.expire_snapshots(`table` => 'ecom_lake.dws_subject_window', retain_max => 20);
CALL sys.expire_snapshots(`table` => 'ecom_lake.ads_window_metric', retain_max => 20);
CALL sys.expire_snapshots(`table` => 'ecom_lake.dwd_late_event_count', retain_max => 20);

-- ── 阶段报告（可观察性）：`maintain rows=…` ──────────────────────────────
-- ★★ 刻意**不**用 `layer=maintain`：`layer=` 是 spec §5.3 的**层**词汇表
--   （ods|dwd|dim|dws|ads 五值），`lake_replay.sh` 的 `LAYER=` 就是从它派生。
--   多造一个 `layer=maintain` 会让「层」出现一个**永远选不中**的标签
--   （与 impl-03b 拒绝表当时的取舍同源：那里用 `dwd_reject` 而**不**新造 `layer=reject`）。
--   维护段是**阶段**不是层 ⇒ 用独立前缀 `maintain`。
-- ★ 报的是**维护后的快照总数**（11 张表合计），不是「跑了两个 procedure」——
--   「执行过」不等于「达到效果」，可观察性应指向效果。
--   ⚠️ 这只是可观察性；判据（逐表区间、空表为 0）由 assert_lake.sql 的哨兵判。
SELECT CONCAT('maintain rows=', CAST(COUNT(*) AS STRING)) AS stage_report
FROM (
  SELECT snapshot_id FROM ods_behavior_event$snapshots
  UNION ALL SELECT snapshot_id FROM dwd_behavior_event$snapshots
  UNION ALL SELECT snapshot_id FROM dwd_behavior_event_reject$snapshots
  UNION ALL SELECT snapshot_id FROM dim_item$snapshots
  UNION ALL SELECT snapshot_id FROM dim_category$snapshots
  UNION ALL SELECT snapshot_id FROM dim_user$snapshots
  UNION ALL SELECT snapshot_id FROM dim_region$snapshots
  UNION ALL SELECT snapshot_id FROM dim_channel$snapshots
  UNION ALL SELECT snapshot_id FROM dws_subject_window$snapshots
  UNION ALL SELECT snapshot_id FROM ads_window_metric$snapshots
  UNION ALL SELECT snapshot_id FROM dwd_late_event_count$snapshots
) all_snapshots;
