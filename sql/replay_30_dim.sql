-- replay_30_dim.sql — 维度变更流 → 五张维表（三张拉链 + 两张静态）（impl-03c）
--
-- 规格落点：spec §5.2（DIM 两行形状）、§5.3（`replay_30_dim.sql`）、[05] Q10/Q14。
--
-- ★ 输入是**维度变更流**（`data/threshold/dim.jsonl`，由 `ecomgen dump --dim-out` 现产），
--   不是事件流：事件文件的字节已被 03b 的门槛批判据锁定，混进维度会把那份判据推翻。
--
-- ★★ 拉链区间在**这里**用窗口函数一次算出（一处实现）：
--     valid_from_ms = 该版本的 change_time
--     valid_to_ms   = 同 id 的**下一版** change_time − 1（没有下一版 ⇒ 9999999999999）
--   ⇒ 相邻版本首尾相接（`old.valid_to_ms + 1 = new.valid_from_ms`）、不重叠、无空洞。
--   这与 [05] Q10 签字的「新版本到达时旧行 valid_to_ms = 新 valid_from_ms − 1」逐字一致，
--   且与事件侧的 `BETWEEN valid_from_ms AND valid_to_ms`（闭区间）配套：
--   边界那一毫秒归属**新版本**。
--   ⚠️ 生成器只产出「版本行」（id + change_time + 属性），**不**自己算区间——
--     若两侧各算一遍，漂移的症状是「拉链看起来对、抽查时刻命中错版本」，极难察觉。
--
-- ★ 时间源 = 维度变更流的 `change_time`（事件时间语义，满足 canonical-truth D-3）。
--   生成器保证每个对象有一个 `change_time = 批次最早事件时刻` 的初始版本，
--   故**每一条事件**都落在某个版本区间里，区间 join 不会漏行（由构造保证，非抽样）。
--
-- ★ 为什么是 `INSERT OVERWRITE`：五张维表都**不分区**，OVERWRITE 整表改写 ⇒
--   重跑同一批不叠加（幂等）。`table.dml-sync=true` 让紧随的层报告读到真实行数。
--
-- ★ 本文件不写断言（断言归 impl-03d）。

SET 'sql-client.execution.result-mode' = 'TABLEAU';
SET 'execution.runtime-mode' = 'batch';
SET 'table.local-time-zone' = 'UTC';
SET 'table.dml-sync' = 'true';

CREATE CATALOG IF NOT EXISTS paimon WITH (
  'type' = 'paimon',
  'warehouse' = 'file:/warehouse'
);
USE CATALOG paimon;
CREATE DATABASE IF NOT EXISTS ecom_lake;
USE ecom_lake;

-- ── 输入：维度变更流原文（每行一个版本；静态维无 change_time）───────────
CREATE TEMPORARY TABLE src_dim (
  line_str STRING
) WITH (
  'connector' = 'filesystem',
  'path' = 'file:///data/threshold/dim.jsonl',
  'format' = 'raw'
);

-- ── 投影：五种维度**一次**投影成超集视图（缺的列自然为 NULL）─────────────
-- ★ entity_id 是拉链窗口的分区键：五种维度的 id 列不同名，用 COALESCE 归一，
--   使 LEAD 只算一遍。按 (dim, entity_id) 分区 ⇒ 不同种类之间不会互相串链。
CREATE TEMPORARY VIEW v_dim AS
SELECT
  JSON_VALUE(line_str, '$.dim')                              AS dim,
  COALESCE(
    JSON_VALUE(line_str, '$.item_id'),
    JSON_VALUE(line_str, '$.category_id'),
    JSON_VALUE(line_str, '$.user_id'),
    JSON_VALUE(line_str, '$.province'),
    JSON_VALUE(line_str, '$.channel')
  )                                                          AS entity_id,
  JSON_VALUE(line_str, '$.item_id')                          AS item_id,
  JSON_VALUE(line_str, '$.item_name')                        AS item_name,
  JSON_VALUE(line_str, '$.category_id')                      AS category_id,
  TRY_CAST(JSON_VALUE(line_str, '$.price_cents') AS INT)     AS price_cents,
  JSON_VALUE(line_str, '$.brand')                            AS brand,
  JSON_VALUE(line_str, '$.category_name')                    AS category_name,
  JSON_VALUE(line_str, '$.parent_id')                        AS parent_id,
  TRY_CAST(JSON_VALUE(line_str, '$.level') AS INT)           AS level,
  JSON_VALUE(line_str, '$.user_id')                          AS user_id,
  JSON_VALUE(line_str, '$.user_level')                       AS user_level,
  JSON_VALUE(line_str, '$.province')                         AS province,
  JSON_VALUE(line_str, '$.register_date')                    AS register_date,
  JSON_VALUE(line_str, '$.region')                           AS region,
  JSON_VALUE(line_str, '$.province_code')                    AS province_code,
  JSON_VALUE(line_str, '$.channel')                          AS channel,
  JSON_VALUE(line_str, '$.channel_group')                    AS channel_group,
  TRY_CAST(JSON_VALUE(line_str, '$.is_paid') AS TINYINT)     AS is_paid,
  TRY_CAST(JSON_VALUE(line_str, '$.change_time') AS BIGINT)  AS change_time
FROM src_dim;

-- ── 拉链：同 id 的版本按 change_time 排序，下一版的起点 − 1 即本版终点 ────
-- ★ 为什么用 LEAD 而不是自连接/行号：LEAD 的默认值只能给常量，而这里恰好
--   「没有下一版 ⇒ 哨兵值 9999999999999」就是一个常量 ⇒ LEAD 足够，不必多一层分组。
CREATE TEMPORARY VIEW v_zipper AS
SELECT
  dim, entity_id, change_time,
  change_time AS valid_from_ms,
  COALESCE(
    LEAD(change_time) OVER (PARTITION BY dim, entity_id ORDER BY change_time) - 1,
    9999999999999
  ) AS valid_to_ms,
  item_id, item_name, category_id, price_cents, brand,
  category_name, parent_id, level,
  user_id, user_level, province, register_date,
  region, province_code, channel, channel_group, is_paid
FROM v_dim;

-- ── ① dim_item ───────────────────────────────────────────────────────
INSERT OVERWRITE dim_item
SELECT
  item_id, valid_from_ms, item_name, category_id, price_cents, brand,
  valid_to_ms, change_time
FROM v_zipper
WHERE dim = 'item';

-- ── ② dim_category ──────────────────────────────────────────────────
INSERT OVERWRITE dim_category
SELECT
  category_id, valid_from_ms, category_name, parent_id, level,
  valid_to_ms, change_time
FROM v_zipper
WHERE dim = 'category';

-- ── ③ dim_user ──────────────────────────────────────────────────────
INSERT OVERWRITE dim_user
SELECT
  user_id, valid_from_ms, user_level, province, register_date,
  valid_to_ms, change_time
FROM v_zipper
WHERE dim = 'user';

-- ── ④ dim_region（静态，无拉链）─────────────────────────────────────
INSERT OVERWRITE dim_region
SELECT province, region, province_code
FROM v_dim
WHERE dim = 'region';

-- ── ⑤ dim_channel（静态，无拉链）────────────────────────────────────
INSERT OVERWRITE dim_channel
SELECT channel, channel_group, is_paid
FROM v_dim
WHERE dim = 'channel';

-- ── 层报告（spec §5.3 可观察性）：layer=<层> rows=… ─────────────────────
-- ★ 五张表**合计**：DIM 是一层五表，逐表打会让 marker 行数漂移，脚本也没有
--   「按表」的概念（spec §5.3 只有 ods/dwd/dim/dws/ads 五个标签）。
-- ★★ 合计**不足以证明拉链真的灌进来了**：三张拉链表全为 0、只剩静态维的
--   12+6 行时，合计仍 = 18 > 0 ⇒ marker 照样出现。故这里额外打一行
--   `dim_zipper_rows=…/…/…`（item/category/user），把「哪张表空了」直接摆在日志上。
--   ⚠️ 这不是断言（断言归 03d）：它只是让空表**可见**，不阻止脚本继续跑。
SELECT CONCAT('layer=dim rows=', CAST(SUM(c) AS STRING)) AS layer_report
FROM (
  SELECT COUNT(*) AS c FROM dim_item
  UNION ALL SELECT COUNT(*) FROM dim_category
  UNION ALL SELECT COUNT(*) FROM dim_user
  UNION ALL SELECT COUNT(*) FROM dim_region
  UNION ALL SELECT COUNT(*) FROM dim_channel
) t;

SELECT CONCAT('dim_zipper_rows=',
  CAST((SELECT COUNT(*) FROM dim_item) AS STRING), '/',
  CAST((SELECT COUNT(*) FROM dim_category) AS STRING), '/',
  CAST((SELECT COUNT(*) FROM dim_user) AS STRING)) AS zipper_report;
