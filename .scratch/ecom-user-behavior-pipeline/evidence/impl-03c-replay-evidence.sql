-- impl-03c-replay-evidence.sql — impl-03c 的**可复现**验收取证（不是断言；断言归 impl-03d）
--
-- ★★ 为什么在这里而不在 artifacts/（2026-10-23 更正，spec 轴审阅发现）：
--   首版本文件放在 artifacts/，而 .gitignore 第 30 行是 `/artifacts/`（运行产物不入库）
--   ⇒ **取证集与被引用的日志根本没进版本库**，票面上每个 AC 数字在分支上都不可复现。
--   那正是本票声称要修的病（「跑完即删、只留数字」），只是换了层皮。
--   ⇒ 现落在 `.scratch/ecom-user-behavior-pipeline/evidence/`（设计链目录，受版本控制），
--      查询集 `impl-03c-replay-evidence.sql` + 输出 `impl-03c-replay-evidence.out.txt` 均可入库。
--      ⚠️ 输出**不能**叫 .log：.gitignore 有 `*.log`，同样会被吞掉。
--
-- 跑法（从仓库根；容器内 /sql 与 /data 是 :ro，不能 docker cp 进去）：
--   MSYS_NO_PATHCONV=1 docker exec -i eco-flink-jm bash -c 'cat > /tmp/ev.sql' \
--     < .scratch/ecom-user-behavior-pipeline/evidence/impl-03c-replay-evidence.sql
--   MSYS_NO_PATHCONV=1 docker exec -i eco-flink-jm \
--     /opt/flink/bin/sql-client.sh -f /tmp/ev.sql \
--     | sed 's/\x1b\[[0-9;]*m//g' \
--     > .scratch/ecom-user-behavior-pipeline/evidence/impl-03c-replay-evidence.out.txt

SET 'sql-client.execution.result-mode' = 'TABLEAU';
SET 'execution.runtime-mode' = 'batch';
SET 'table.local-time-zone' = 'UTC';
SET 'parallelism.default' = '2';
CREATE CATALOG IF NOT EXISTS paimon WITH ('type'='paimon','warehouse'='file:/warehouse');
USE CATALOG paimon;
USE ecom_lake;

-- ── AC1 五层行数（各层都 > 0）──────────────────────────────────────────
SELECT 'AC1 五层' AS chk,
  (SELECT COUNT(*) FROM ods_behavior_event)   AS ods,
  (SELECT COUNT(*) FROM dwd_behavior_event)   AS dwd,
  (SELECT COUNT(*) FROM dim_item)             AS dim_item,
  (SELECT COUNT(*) FROM dim_category)         AS dim_category,
  (SELECT COUNT(*) FROM dim_user)             AS dim_user,
  (SELECT COUNT(*) FROM dim_region)           AS dim_region,
  (SELECT COUNT(*) FROM dim_channel)          AS dim_channel,
  (SELECT COUNT(*) FROM dws_subject_window)   AS dws,
  (SELECT COUNT(*) FROM ads_window_metric)    AS ads;

-- ── AC2 拉链：同 id 多版本 + 区间首尾相接不重叠 ─────────────────────────
SELECT 'AC2a 多版本商品数' AS chk, COUNT(*) AS n_multi
FROM (SELECT item_id FROM dim_item GROUP BY item_id HAVING COUNT(*) > 1) t;

SELECT 'AC2b 衔接断点(item)' AS chk, COUNT(*) AS n_broken
FROM (
  SELECT valid_to_ms,
         LEAD(valid_from_ms) OVER (PARTITION BY item_id ORDER BY valid_from_ms) AS next_from
  FROM dim_item
) t WHERE next_from IS NOT NULL AND valid_to_ms + 1 <> next_from;

SELECT 'AC2c 区间重叠(item)' AS chk, COUNT(*) AS n_overlap
FROM dim_item a JOIN dim_item b
  ON a.item_id = b.item_id AND a.valid_from_ms < b.valid_from_ms
WHERE a.valid_to_ms >= b.valid_from_ms;

-- ★ 补：`dim_category` / `dim_user` 的**衔接与重叠**（spec 轴审阅指出：首版只查了 item，
--   而票面 AC2 说的是「拉链维表」整体，不是「dim_item 一张」）。同一对判据，表名换一下。
SELECT 'AC2b 衔接断点(category)' AS chk, COUNT(*) AS n_broken
FROM (
  SELECT valid_to_ms,
         LEAD(valid_from_ms) OVER (PARTITION BY category_id ORDER BY valid_from_ms) AS next_from
  FROM dim_category
) t WHERE next_from IS NOT NULL AND valid_to_ms + 1 <> next_from;

SELECT 'AC2c 区间重叠(category)' AS chk, COUNT(*) AS n_overlap
FROM dim_category a JOIN dim_category b
  ON a.category_id = b.category_id AND a.valid_from_ms < b.valid_from_ms
WHERE a.valid_to_ms >= b.valid_from_ms;

SELECT 'AC2b 衔接断点(user)' AS chk, COUNT(*) AS n_broken
FROM (
  SELECT valid_to_ms,
         LEAD(valid_from_ms) OVER (PARTITION BY user_id ORDER BY valid_from_ms) AS next_from
  FROM dim_user
) t WHERE next_from IS NOT NULL AND valid_to_ms + 1 <> next_from;

SELECT 'AC2c 区间重叠(user)' AS chk, COUNT(*) AS n_overlap
FROM dim_user a JOIN dim_user b
  ON a.user_id = b.user_id AND a.valid_from_ms < b.valid_from_ms
WHERE a.valid_to_ms >= b.valid_from_ms;

SELECT 'AC2a 多版本(category/user)' AS chk,
  (SELECT COUNT(*) FROM (SELECT category_id FROM dim_category GROUP BY category_id HAVING COUNT(*) > 1) x) AS cat_multi,
  (SELECT COUNT(*) FROM (SELECT user_id FROM dim_user GROUP BY user_id HAVING COUNT(*) > 1) y)         AS user_multi;
-- ── AC3 时态 join 真按**事件时刻**取版本 ────────────────────────────────
-- (a) item：命中版本的品类 ≠ 该商品**最新**版本的品类
SELECT 'AC3a 命中非最新版(item 品类)' AS chk, COUNT(*) AS n_events
FROM (
  SELECT e.event_id, di.category_id AS joined_cat, l.category_id AS latest_cat
  FROM dwd_behavior_event e
  JOIN dim_item di ON di.item_id = e.item_id
   AND e.event_time BETWEEN di.valid_from_ms AND di.valid_to_ms
  JOIN (SELECT item_id, category_id FROM dim_item WHERE valid_to_ms = 9999999999999) l
    ON l.item_id = e.item_id
) t WHERE joined_cat <> latest_cat;

-- (b) item 的价格：命中版本价格 ≠ 最新版价格（证明回填的**值**按时刻而非最新）
SELECT 'AC3b 命中非最新版(item 价格)' AS chk, COUNT(*) AS n_events
FROM (
  SELECT e.event_id, di.price_cents AS joined_price, l.price_cents AS latest_price
  FROM dwd_behavior_event e
  JOIN dim_item di ON di.item_id = e.item_id
   AND e.event_time BETWEEN di.valid_from_ms AND di.valid_to_ms
  JOIN (SELECT item_id, price_cents FROM dim_item WHERE valid_to_ms = 9999999999999) l
    ON l.item_id = e.item_id
) t WHERE joined_price <> latest_price;

-- (c) user：命中版本的用户等级 ≠ 最新版等级
SELECT 'AC3c 命中非最新版(user 等级)' AS chk, COUNT(*) AS n_events
FROM (
  SELECT e.event_id, du.user_level AS joined_lvl, l.user_level AS latest_lvl
  FROM dwd_behavior_event e
  JOIN dim_user du ON du.user_id = e.user_id
   AND e.event_time BETWEEN du.valid_from_ms AND du.valid_to_ms
  JOIN (SELECT user_id, user_level FROM dim_user WHERE valid_to_ms = 9999999999999) l
    ON l.user_id = e.user_id
) t WHERE joined_lvl <> latest_lvl;

-- (d) 区间 join 未漏行（INNER JOIN 命中率 100%）
SELECT 'AC3d 未命中事件(item)' AS chk, COUNT(*) AS n_missing
FROM dwd_behavior_event e
LEFT JOIN dim_item di ON di.item_id = e.item_id
 AND e.event_time BETWEEN di.valid_from_ms AND di.valid_to_ms
WHERE di.item_id IS NULL;

-- (e) ★ 三条 subject 都是 **1:1 覆盖**（否则 DWS 会因区间重叠而扇出）
--     判据：每条 subject 的 pv 合计 == DWD 的 page_view 数
SELECT 'AC3e subject 1:1 覆盖' AS chk,
  (SELECT SUM(pv) FROM dws_subject_window WHERE subject_type='item')     AS item_pv,
  (SELECT SUM(pv) FROM dws_subject_window WHERE subject_type='category') AS cat_pv,
  (SELECT SUM(pv) FROM dws_subject_window WHERE subject_type='user')     AS user_pv,
  (SELECT COUNT(*) FROM dwd_behavior_event WHERE event_type='page_view') AS dwd_pv;

-- ── AC4 window_start 语义一致 + subject_type 恰三值 ─────────────────────
SELECT 'AC4a subject_type 取值' AS chk, subject_type, COUNT(*) AS n
FROM dws_subject_window GROUP BY subject_type ORDER BY subject_type;

SELECT 'AC4b 未对齐 60s' AS chk, COUNT(*) AS n_unaligned
FROM (SELECT DISTINCT window_start FROM dws_subject_window
      UNION SELECT DISTINCT window_start FROM ads_window_metric) t
WHERE window_start % 60000 <> 0;

SELECT 'AC4c ADS 窗口无对应 DWS' AS chk, COUNT(*) AS n_orphan
FROM (SELECT DISTINCT window_start FROM ads_window_metric) a
LEFT JOIN (SELECT DISTINCT window_start FROM dws_subject_window) d
  ON a.window_start = d.window_start
WHERE d.window_start IS NULL;

SELECT 'AC4d dt 与窗口日不一致' AS chk, COUNT(*) AS n_bad
FROM dws_subject_window
WHERE dt <> DATE_FORMAT(TO_TIMESTAMP_LTZ(window_start, 3), 'yyyy-MM-dd');

-- ── AC5 ADS 键与 Doris 同构（文本判据在 tests/test_replay_temporal_sql.py）──
-- 这里取运行时证据：ADS 的键确实唯一、metric_name 恰三值
SELECT 'AC5a ADS 键重复' AS chk, COUNT(*) AS n_dup
FROM (SELECT dt, metric_name, dim_key, window_start, COUNT(*) AS c
      FROM ads_window_metric GROUP BY dt, metric_name, dim_key, window_start
      HAVING COUNT(*) > 1) t;

SELECT 'AC5b metric_name 取值' AS chk, metric_name, COUNT(*) AS n
FROM ads_window_metric GROUP BY metric_name ORDER BY metric_name;

SELECT 'AC5c dim_key 面' AS chk, dim_key, COUNT(*) AS n
FROM ads_window_metric GROUP BY dim_key ORDER BY dim_key;

-- ── AC6 迟到计数只有计数 + AC8 与 DWD 有对应量 ─────────────────────────
SELECT 'AC6 迟到对账' AS chk,
  (SELECT COUNT(*) FROM dwd_behavior_event WHERE is_late = 1) AS dwd_is_late,
  (SELECT COALESCE(SUM(cnt), 0) FROM dwd_late_event_count)    AS late_sum,
  (SELECT COUNT(*) FROM dwd_late_event_count)                 AS n_buckets;

-- ── AC7 三天跨度 ⇒ 3 个 dt 分区 ────────────────────────────────────────
SELECT 'AC7 dt 分区数' AS chk,
  (SELECT COUNT(DISTINCT dt) FROM dws_subject_window) AS dws,
  (SELECT COUNT(DISTINCT dt) FROM ads_window_metric)  AS ads,
  (SELECT COUNT(DISTINCT dt) FROM dwd_late_event_count) AS late;

-- ── 附加：ADS 与 DWD 的横向对账（自证口径闭合，非 03f 的 Doris 对账）────
SELECT 'X1 ADS ALL pv/gmv vs DWD' AS chk,
  (SELECT SUM(metric_value) FROM ads_window_metric WHERE dim_key='ALL' AND metric_name='pv')  AS ads_pv,
  (SELECT COUNT(*) FROM dwd_behavior_event WHERE event_type='page_view')                      AS dwd_pv,
  (SELECT SUM(metric_value) FROM ads_window_metric WHERE dim_key='ALL' AND metric_name='gmv') AS ads_gmv,
  (SELECT COALESCE(SUM(amount),0) FROM dwd_behavior_event WHERE event_type='purchase')        AS dwd_gmv;

SELECT 'X2 渠道面合计 == 全量面' AS chk,
  (SELECT SUM(metric_value) FROM ads_window_metric WHERE dim_key LIKE 'channel=%' AND metric_name='pv') AS ch_pv,
  (SELECT SUM(metric_value) FROM ads_window_metric WHERE dim_key='ALL' AND metric_name='pv')            AS all_pv;

SELECT 'X3 ADS uv 逐窗口 vs DWD' AS chk, COUNT(*) AS n_mismatch
FROM (
  SELECT a.window_start, a.metric_value AS ads_uv,
         (SELECT COUNT(DISTINCT user_id) FROM dwd_behavior_event d
          WHERE (d.event_time / 60000) * 60000 = a.window_start) AS dwd_uv
  FROM ads_window_metric a
  WHERE a.dim_key = 'ALL' AND a.metric_name = 'uv'
) t WHERE ads_uv <> dwd_uv;