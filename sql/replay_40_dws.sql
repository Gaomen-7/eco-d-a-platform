-- replay_40_dws.sql — DWD × 时态维**区间 join** → dws_subject_window（impl-03c）
--
-- 规格落点：spec §5.2（DWS 行）、§5.3（`replay_40_dws.sql`）、[05] Q14（时态维 join）。
--
-- ★★ 时态 join 的硬形状（票面「时态维 join 的硬形状」，逐字）：
--     ON d.item_id = e.item_id
--     AND e.event_time_ms BETWEEN d.valid_from_ms AND d.valid_to_ms
--   走 **batch 区间 join**，**不是 lookup join**。依据取自本仓已签字口径
--   （spec §5.2 与 [05] Q10，两处逐字同源）：lookup join 是 processing-time 语义，
--   回放必错；Paimon 无 zipper/SCD 官方能力。
--   ⚠️ 这不是我从官方文档里查到的一手引文（本处没有版本号/URL）—— 它是
--   **本仓上下游文档的一致口径**，按 HLD「官方给出 vs 设计值」的分法属**设计值级**
--   约束，别在别处把它当官方保证引用。
--   批区间 join 就是普通等值+范围连接：**不加** temporal / LOOKUP 提示词，
--   **不设** `FOR SYSTEM_TIME AS OF`，**不改** runtime mode。
--
-- ★★ 三类 subject 各走一条区间 join，**三条都必须真的按事件时刻命中版本**：
--   · item     ：事件 × `dim_item`（键 item_id）—— 回填**当时有效**的价格/品类/品牌
--   · category ：事件 →（当时有效的 dim_item 版本）→ `dim_category`（键 category_id）
--                ★ 为什么要两级：商品换过品类后，事件自带的 category_id 是**生成期**
--                  的品类，直接用会把事件算到今天的新品类上 —— 品类 subject 就退化成
--                  「读事件字段」，时态语义消失。两级 join 使品类 subject 也真的按
--                  事件时刻取版本。
--   · user     ：事件 × `dim_user`（键 user_id）—— 用户等级/省份的当时有效版本
--   三条都用 `BETWEEN valid_from_ms AND valid_to_ms`（闭区间，与 DDL 的
--   `valid_to_ms = 下一版 valid_from_ms − 1` 配套：边界那一毫秒归新版本）。
--
-- ★ 为什么**不**把维度列回填进 DWD：DWD 是事件事实表，形状表里没有维度快照列；
--   维度只在 DWS 的 join 里生效。判据「回填值与事件时刻的版本一致」用
--   **同一条 join 的可判查询**取证（见票面 Progress），不靠往 DWD 塞列。
--
-- ★ 窗口 = 1 分钟滚动（gdd §A.1 唯一权威）：
--     window_start = floor(event_time_ms / 60000) × 60000
--   与实时段 `(e.eventTime / JobConfig.WINDOW_SIZE_MS) * WINDOW_SIZE_MS` **逐字同源**
--   （WINDOW_SIZE_MS = 60_000）。dt = 窗口起点的 UTC 日。
--
-- ★ 指标口径（票面 Notes：「由 [03] 推定回填」，**依据不如其它决策硬**）：
--     pv  = 该 (subject, 窗口) 的 **page_view** 事件条数（页面浏览次数；
--           与 DDL 里 `pv BIGINT COMMENT '页面浏览次数'` 逐字一致）
--     uv  = 该 (subject, 窗口) 的**去重 user_id 数**（**全部事件**，不是只 page_view）
--     gmv = 该 (subject, 窗口) 的 purchase `amount` 之和（非 purchase 计 0）
--   ⚠️ 三条都是推定值；已在本票 Progress 登记（spec §8.3：不猜、登记）。
--      与 Doris 实时侧的**同名指标**是否同值由 03f 判，本票只保证键同构 + 同一公式。
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

-- ── 事件 + 窗口起点（窗口口径的唯一落点）────────────────────────────────
-- ★ 直接读 dwd_behavior_event，**不**加落地日过滤，与 ODS 侧相反 —— 差异是真实
--   的、必须显式记住：
--     ODS 是 append + 落地日分区（跨天累积）⇒ 必须按落地日过滤；
--     DWD 是主键表 + `INSERT OVERWRITE` 整表改写 ⇒ 表里**只有本次写入的批次**。
--   反过来做（给 DWD 加落地日过滤）会读空：DWD 的 dt 是**事件日**，没有落地日列。
--   守卫：tests/test_replay_temporal_sql.py::test_dws_does_not_filter_dwd_by_landing_day
CREATE TEMPORARY VIEW v_event_win AS
SELECT
  e.event_id, e.event_time, e.user_id, e.item_id, e.category_id,
  e.event_type, e.amount,
  (e.event_time / 60000) * 60000 AS window_start
FROM dwd_behavior_event e;

-- ── subject = item：事件 × dim_item 的**区间 join** ──────────────────────
CREATE TEMPORARY VIEW v_item_joined AS
SELECT
  w.event_id, w.user_id, w.event_type, w.amount, w.window_start,
  w.item_id AS subject_id
FROM v_event_win w
JOIN dim_item d
  ON d.item_id = w.item_id
 AND w.event_time BETWEEN d.valid_from_ms AND d.valid_to_ms;

-- ── subject = category：事件 →（当时有效的 dim_item 版本）→ dim_category ──
CREATE TEMPORARY VIEW v_category_joined AS
SELECT
  w.event_id, w.user_id, w.event_type, w.amount, w.window_start,
  c.category_id AS subject_id
FROM (
  SELECT
    e.event_id, e.user_id, e.event_type, e.amount, e.window_start, e.event_time,
    di.category_id AS category_id
  FROM v_event_win e
  JOIN dim_item di
    ON di.item_id = e.item_id
   AND e.event_time BETWEEN di.valid_from_ms AND di.valid_to_ms
) w
JOIN dim_category c
  ON c.category_id = w.category_id
 AND w.event_time BETWEEN c.valid_from_ms AND c.valid_to_ms;

-- ── subject = user：事件 × dim_user 的**区间 join** ─────────────────────
CREATE TEMPORARY VIEW v_user_joined AS
SELECT
  w.event_id, w.user_id, w.event_type, w.amount, w.window_start,
  u.user_id AS subject_id
FROM v_event_win w
JOIN dim_user u
  ON u.user_id = w.user_id
 AND w.event_time BETWEEN u.valid_from_ms AND u.valid_to_ms;

-- ── 聚合：三列指标一次算完（pv / uv / gmv）──────────────────────────────
-- ★ 三条 subject 先 UNION ALL 再**一次** GROUP BY：若每条 subject 各写一个
--   GROUP BY 的 UNION ALL，每条支路都独立 shuffle 一整批事件，
--   4 并行下会撞 `Insufficient number of network buffers`（ADS 层已实测到同一错）。
-- `COUNT(DISTINCT user_id)` 在本规模（每窗口 ≤ 数百用户）安全。
CREATE TEMPORARY VIEW v_agg AS
SELECT
  subject_type, subject_id, window_start,
  SUM(CASE WHEN event_type = 'page_view' THEN 1 ELSE 0 END)          AS pv,
  COUNT(DISTINCT user_id)                                            AS uv,
  CAST(COALESCE(SUM(CASE WHEN event_type = 'purchase' THEN amount ELSE 0 END), 0)
       AS DECIMAL(18, 2))                                            AS gmv
FROM (
  SELECT 'item' AS subject_type, subject_id, user_id, event_type, amount, window_start
  FROM v_item_joined
  UNION ALL
  SELECT 'category' AS subject_type, subject_id, user_id, event_type, amount, window_start
  FROM v_category_joined
  UNION ALL
  SELECT 'user' AS subject_type, subject_id, user_id, event_type, amount, window_start
  FROM v_user_joined
) t
GROUP BY subject_type, subject_id, window_start;

-- ── 写入 DWS ─────────────────────────────────────────────────────────
-- ★ `window_end = window_start + 60000`（开区间）——与 window_start 配对，
--   使窗口区间在表内自洽（ADS 侧沿用同一对）。
INSERT OVERWRITE dws_subject_window
SELECT
  DATE_FORMAT(TO_TIMESTAMP_LTZ(window_start, 3), 'yyyy-MM-dd') AS dt,
  subject_type,
  subject_id,
  window_start,
  window_start + 60000                                        AS window_end,
  CAST(pv AS BIGINT)                                          AS pv,
  CAST(uv AS BIGINT)                                          AS uv,
  gmv
FROM v_agg;

-- ── 层报告（spec §5.3 可观察性）：layer=<层> rows=… ─────────────────────
-- ★ 整表计数即可（与 ODS 的落地日过滤不同）：DWS 是主键表且整表 OVERWRITE
--   ⇒ 表里只有本次写入的批次，跨天重跑不会累积。
SELECT CONCAT('layer=dws rows=', CAST(COUNT(*) AS STRING)) AS layer_report
FROM dws_subject_window;
