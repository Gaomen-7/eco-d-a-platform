-- verify_paimon_layers.sql — 五层行数探针（impl-03f 交付物 3）
--
-- 规格落点：spec §4.3（P2：五项真断言——第三项「Paimon 五层有行」）、
--          §5.5（断言执行面：sql-client 退出码不可用，唯一信号是 stdout）。
-- 消费方：src/ecomgen/verify_runner.py::check_paimon_layers
--         （经 docker exec eco-flink-jm sql-client.sh -f /sql/verify_paimon_layers.sql）。
--
-- ════════════════════════════════════════════════════════════════════
-- ★★ 输出语法（与 verify_runner.py::parse_layer_rows_from_stdout 严格配对）
-- ════════════════════════════════════════════════════════════════════
-- 每层一行两列 TABLEAU 结果：
--     | ods | 1048576 |
-- 解析侧只认**以 `|` 开头**的表格行、第一列 ∈ {ods,dwd,dim,dws,ads}、
-- 第二列纯数字 —— 回显行（`>` / `Flink SQL>` 开头）与表头/分隔行天然被排除。
--
-- ★ 层名压在 3 字符、数值列是纯数字 ⇒ 不会撞 TABLEAU 30 字符列宽截断
--   （impl-03d/03e 两次实测踩过：长单元格渲染成 `xxxx...` 后解析面失明）。
-- ★ DIM 层＝五张维表**合计**（replay_30_dim.sql 的 layer=dim rows= 同一口径；
--   spec §5.3 的层词汇表只有 ods|dwd|dim|dws|ads，dim 不拆表）。
-- ★ 只读探针：本文件零写操作（无 INSERT/DELETE/ALTER），可任意重跑。
--
-- ⚠️ 若某层语句报错，sql-client 会**中止整个脚本**（03e §2 实测）⇒ 后续层
--    缺行 ⇒ 解析面得到「层缺席」⇒ 判定 FAIL。这正是期望的失败形态：
--    缺席不是通过（03e F6 已证伪的同款通道，这里由「词表五层必须齐」兜住）。

-- ★★ 并行度压到 1（实机换来的，2026-10-24）：
--   批探针与流作业共用 session 集群的 slot 池，本仓 TM 仅 4 slot 且被流作业
--   （28 任务）占满 ⇒ 默认并行度 4 的批作业 PendingSlotRequest 超时 FAILED
--   （NoRestartBackoffTimeStrategy 不重启），五层全部缺席。
--   COUNT 探针本身无 shuffle 压力，单 slot 足够。
--   （官方文档未给出「session 集群 slot 争用时批作业排队超时」的条文；
--    本仓实机观察，按 spec §8.3 登记。）
SET 'pipeline.parallelism' = '1';
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

-- ── ODS ─────────────────────────────────────────────────────────────
-- ★ 别名必须是 rows_cnt：`rows` 是 Flink SQL 保留字（实机 2026-10-24：
--   ParseException "Encountered \"rows\""），且 sql-client 报错即中止
--   整个脚本 ⇒ 五层全部缺席。解析侧只认第一列（层名），第二列列名无关。
SELECT 'ods' AS layer_name, CAST(COUNT(*) AS STRING) AS rows_cnt
FROM ods_behavior_event;

-- ── DWD ─────────────────────────────────────────────────────────────
SELECT 'dwd' AS layer_name, CAST(COUNT(*) AS STRING) AS rows_cnt
FROM dwd_behavior_event;

-- ── DIM（五张维表合计；与 replay_30_dim.sql 的 layer=dim 同口径）────────
SELECT 'dim' AS layer_name, CAST(SUM(c) AS STRING) AS rows_cnt
FROM (
  SELECT COUNT(*) AS c FROM dim_item
  UNION ALL SELECT COUNT(*) FROM dim_category
  UNION ALL SELECT COUNT(*) FROM dim_user
  UNION ALL SELECT COUNT(*) FROM dim_region
  UNION ALL SELECT COUNT(*) FROM dim_channel
) t;

-- ── DWS ─────────────────────────────────────────────────────────────
SELECT 'dws' AS layer_name, CAST(COUNT(*) AS STRING) AS rows_cnt
FROM dws_subject_window;

-- ── ADS ─────────────────────────────────────────────────────────────
SELECT 'ads' AS layer_name, CAST(COUNT(*) AS STRING) AS rows_cnt
FROM ads_window_metric;
