-- replay_10_ods.sql — 门槛批 → ods_behavior_event（impl-03b）
--
-- 规格落点：spec §5.3（dump → ods_behavior_event，append、dt=落地日、bucket=-1）
--          §5.2 形状表 ODS 行（payload 原文列；**零解析**）
--
-- ★ 输入是**生成器现产的 dump 文件**，不是 Kafka topic（票面「不重读 Kafka」）：
--   spec §5.1 已定 —— Kafka 保留窗不足以支撑 3 天历史重放，重读不成立。
--
-- ★★ payload 存**整行原文**，不是 `$.payload` 子对象（2026-10-22 修正，有实测依据）：
--   首版用 `REGEXP_EXTRACT(line, '"payload":(\{[^{}]*\})', 1)` 只摘子对象，
--   结果把 **amount / currency / quantity / order_id 四个字段丢了** ——
--   它们在 dump 行的**顶层**，不在 payload 里。实测（30 000 行抽样）：
--     top-level: amount 889 / currency 889 / quantity 6 864 / order_id 2 534
--     payload  : 只有 session_id（30 000/30 000）
--   而 ODS 表没有这四列（形状表：零解析 ⇒ 不许加解析列）
--   ⇒ 若仍只存子对象，这四个字段在 ODS 层就**永久不可恢复**，
--     下游 DWD 会把 29 979 条 purchase 全判 schema_invalid（首版真会这么错）。
--   处置：payload 列存**这一整行 JSON 原文**。
--     ① 仍是「原文」：逐字节来自 dump，未经任何 json 解析/规范化（`format='raw'`）。
--     ② 仍是「零解析」：ODS 不建任何 payload 子字段列，不解释其内容。
--     ③ 保真面更大而非更小：整行包含子对象，不存在信息丢失。
--   ⚠️ 判据（票面第 6 条）：`payload` 与 dump 行**逐字节相同** —— 现在可直接 md5 比对。
--
-- ★ 为什么不用 `format='json'`：实测它把 STRING 读成**规范化后**的 `{}`，
--   原文丢失。必须 `format='raw'` 逐行读字节。
--
-- ★ dt = **落地日**（不是事件日）：ODS 记的是「这批数据哪天到的湖」。
--   门槛批一次性 dump ⇒ 落地日 = 本次 replay 的 UTC 日期（单值）。
--   ⚠️ DWD 的 dt=事件日 是**另一种语义**，不要统一。
--
-- ★ src_seq：dump 里的 `_src_offset`，即**投递序**。落进 ODS 是给 DWD 算
--   迟到用的（迟到是到达序语义，需要真实行序；batch 下没有 ROW_NUMBER()）。
--   gdd Q25 本就要求 ODS 保留 offset 类元数据。
--
-- ★ `table.dml-sync='true'`：不设则 INSERT 异步提交，紧随的行数查询读到 0（§D5 实测）。

SET 'sql-client.execution.result-mode' = 'TABLEAU';
SET 'execution.runtime-mode' = 'batch';
-- ★ UTC 是 dt 口径的前提（spec §5.2「dt STRING = yyyy-MM-dd UTC」）。
SET 'table.local-time-zone' = 'UTC';
SET 'table.dml-sync' = 'true';

CREATE CATALOG IF NOT EXISTS paimon WITH (
  'type' = 'paimon',
  'warehouse' = 'file:/warehouse'
);
USE CATALOG paimon;
CREATE DATABASE IF NOT EXISTS ecom_lake;
USE ecom_lake;

-- ── 输入：门槛批原文（每行一条 JSON，含 `_src_offset`）────────────────
CREATE TEMPORARY TABLE src_raw (
  line_str STRING
) WITH (
  'connector' = 'filesystem',
  'path' = 'file:///data/threshold/raw.jsonl',
  'format' = 'raw'
);

-- ── 写入 ODS ─────────────────────────────────────────────────────────
-- ★ append 表 + `INSERT OVERWRITE`：先清**本 dt 分区**再写 ⇒ 重跑同一批不叠加，
--   同时**不动其它分区**（其它落地日的数据保留）。这是本编排链可重入的前提。
INSERT OVERWRITE ods_behavior_event
SELECT
  JSON_VALUE(line_str, '$.event_id')                     AS event_id,
  TRY_CAST(JSON_VALUE(line_str, '$.event_time') AS BIGINT) AS event_time,
  JSON_VALUE(line_str, '$.user_id')                      AS user_id,
  JSON_VALUE(line_str, '$.item_id')                      AS item_id,
  JSON_VALUE(line_str, '$.category_id')                  AS category_id,
  JSON_VALUE(line_str, '$.event_type')                   AS event_type,
  JSON_VALUE(line_str, '$.schema_version')               AS schema_version,
  JSON_VALUE(line_str, '$.page')                         AS page,
  JSON_VALUE(line_str, '$.source')                       AS source,
  JSON_VALUE(line_str, '$.channel')                      AS channel,
  JSON_VALUE(line_str, '$.province')                     AS province,
  -- ★ 整行原文（见文件头注）。零解析：原样落列，一个字节都不改。
  line_str                                               AS payload,
  -- ★ 投递序（到达序）—— DWD 算迟到要用
  TRY_CAST(JSON_VALUE(line_str, '$._src_offset') AS BIGINT) AS src_seq,
  -- ★ 落地日 = 本次 replay 的 UTC 日（处理时间语义，单值）。
  --   不用 CURRENT_DATE：那是**本地时区**会话日期；dt 口径是 UTC。
  DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd')           AS dt
FROM src_raw;
-- ── 层报告（spec §5.3 可观察性）：逐层 layer=<层> rows=… ──────────────
-- ★ 为什么是 SELECT 而不是 shell 里 echo：行数必须是**从这里查出来的**，
--   不是脚本猜的；sql-client 打表格，脚本 grep 出 'layer=ods rows=' 即可。
--
-- ★★ 必须统计**本次落地分区**，不是整表（impl-03b 实机发现，2026-10-23）：
--   ODS 的 dt = **落地日**，而 append 表的 INSERT OVERWRITE 只改写**本次 dt 分区**，
--   历史落地日的分区按设计保留（那正是「数据哪天到的湖」这个语义的含义）。
--   于是「隔天重跑一次」会让整表行数翻倍 —— 实测：10-22 跑一遍（1048576），
--   10-23 再跑一遍，表变 2097152 / 2 个分区，而**本次确实只写了 1048576 条**。
--   整表计数会让日志行随「跑过几次、跨不跨天」漂移，读者无法据此判断本次是否写全。
--   ⇒ 加 dt 过滤后，本行恒等于本次写入条数。
--   ⚠️ **DWD 侧也必须按同一落地日过滤**（replay_20_dwd.sql 的 v_ods_scope）：
--      DWD 的 dt 是事件日、且 INSERT OVERWRITE 整表改写，但它**读** ODS 时若不限定
--      落地日，隔天重跑仍会把前一天的行再喂一遍 ⇒ DWD 翻倍、A1 全局等式破。
--      两个读取点（本文件的层报告 / replay_20 的两个视图）必须同源，改一处必改另两处。
--   ⚠️ 不要为了「让数字好看」而把 ODS 也改成整表覆盖：落地日分区跨天累积是
--     形状表定的语义，改写它会与 spec §5.2 的 ODS 行冲突。
SELECT CONCAT('layer=ods rows=', CAST(COUNT(*) AS STRING)) AS layer_report
FROM ods_behavior_event
WHERE dt = DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd');
