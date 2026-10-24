-- impl-03d 取证集（可复跑）。复跑方式见票面 Progress。
-- ⚠️ 不能 docker cp 进 /sql（只读挂载）；用 stdin 灌进 /tmp。
SET 'sql-client.execution.result-mode' = 'TABLEAU';
SET 'execution.runtime-mode' = 'batch';
SET 'table.local-time-zone' = 'UTC';
-- ★ 并行度 2：本文件是「多条 UNION ALL 各自读一遍 DWD」的形状，4 并行会撞
--   `Insufficient number of network buffers`（impl-03c 已记录同一类事故）。
SET 'parallelism.default' = '2';
CREATE CATALOG IF NOT EXISTS paimon WITH ('type' = 'paimon', 'warehouse' = 'file:/warehouse');
USE CATALOG paimon;
USE ecom_lake;

-- E1 逐表快照数（票面 AC：逐表查 $snapshots，落 [1,20]）
SELECT 'snap_ods' AS k, CAST(COUNT(*) AS STRING) AS v FROM ods_behavior_event$snapshots
UNION ALL SELECT 'snap_dwd', CAST(COUNT(*) AS STRING) FROM dwd_behavior_event$snapshots
UNION ALL SELECT 'snap_reject', CAST(COUNT(*) AS STRING) FROM dwd_behavior_event_reject$snapshots
UNION ALL SELECT 'snap_dim_item', CAST(COUNT(*) AS STRING) FROM dim_item$snapshots
UNION ALL SELECT 'snap_dim_category', CAST(COUNT(*) AS STRING) FROM dim_category$snapshots
UNION ALL SELECT 'snap_dim_user', CAST(COUNT(*) AS STRING) FROM dim_user$snapshots
UNION ALL SELECT 'snap_dim_region', CAST(COUNT(*) AS STRING) FROM dim_region$snapshots
UNION ALL SELECT 'snap_dim_channel', CAST(COUNT(*) AS STRING) FROM dim_channel$snapshots
UNION ALL SELECT 'snap_dws', CAST(COUNT(*) AS STRING) FROM dws_subject_window$snapshots
UNION ALL SELECT 'snap_ads', CAST(COUNT(*) AS STRING) FROM ads_window_metric$snapshots
UNION ALL SELECT 'snap_late', CAST(COUNT(*) AS STRING) FROM dwd_late_event_count$snapshots;

-- E2 A1 三量（本次落地日口径）
SELECT 'a1_ods_distinct' AS k, CAST(COUNT(DISTINCT event_id) AS STRING) AS v
FROM ods_behavior_event WHERE dt = DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd')
UNION ALL SELECT 'a1_dwd_distinct', CAST(COUNT(DISTINCT event_id) AS STRING) FROM dwd_behavior_event
UNION ALL SELECT 'a1_reject_distinct', CAST(COUNT(DISTINCT event_id) AS STRING) FROM dwd_behavior_event_reject
UNION ALL SELECT 'a2_dup_overshoot', CAST(COUNT(*) - COUNT(DISTINCT event_id) AS STRING)
  FROM ods_behavior_event WHERE dt = DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd');

-- E3 A3 两侧总量（逐档相等由 assert_lake.sql 哨兵判；这里只留侧写）
SELECT 'a3_late_rows' AS k, CAST(COUNT(*) AS STRING) AS v FROM dwd_behavior_event WHERE is_late = 1
UNION ALL SELECT 'a3_cnt_sum', CAST(COALESCE(SUM(cnt),0) AS STRING) FROM dwd_late_event_count;

-- E4 幂等指纹
SELECT 'fp_rows' AS k, CAST(COUNT(*) AS STRING) AS v FROM dwd_behavior_event
UNION ALL SELECT 'fp_hash', CAST(SUM(CAST(HASH_CODE(CONCAT_WS('|', event_id,
      CAST(is_late AS STRING), CAST(late_lag_ms AS STRING))) AS BIGINT)) AS STRING) FROM dwd_behavior_event
UNION ALL SELECT 'fp_devent', CAST(COUNT(DISTINCT event_id) AS STRING) FROM dwd_behavior_event
UNION ALL SELECT 'fp_dlag', CAST(COUNT(DISTINCT late_lag_ms) AS STRING) FROM dwd_behavior_event;

-- E5 拉链：三张表的版本数 / 断点数 / 重叠数 / audit_log 变更 id 数
SELECT 'zip_item_versions' AS k, CAST(COUNT(*) AS STRING) AS v FROM dim_item
UNION ALL SELECT 'zip_item_ids', CAST(COUNT(DISTINCT item_id) AS STRING) FROM dim_item
UNION ALL SELECT 'zip_user_versions', CAST(COUNT(*) AS STRING) FROM dim_user
UNION ALL SELECT 'zip_user_ids', CAST(COUNT(DISTINCT user_id) AS STRING) FROM dim_user
UNION ALL SELECT 'zip_cat_versions', CAST(COUNT(*) AS STRING) FROM dim_category
UNION ALL SELECT 'zip_audit_item_changed', CAST(COUNT(*) AS STRING) FROM (
  SELECT item_id FROM dim_item$audit_log GROUP BY item_id HAVING COUNT(*) >= 2) x
UNION ALL SELECT 'zip_audit_user_changed', CAST(COUNT(*) AS STRING) FROM (
  SELECT user_id FROM dim_user$audit_log GROUP BY user_id HAVING COUNT(*) >= 2) y
UNION ALL SELECT 'zip_audit_cat_changed', CAST(COUNT(*) AS STRING) FROM (
  SELECT category_id FROM dim_category$audit_log GROUP BY category_id HAVING COUNT(*) >= 2) z;

-- E6 属性钉住抽查（dim_channel 曾被人为改成 100，用于验证失败路径后可恢复）
SELECT 'opt_dim_channel_min' AS k, `value` AS v FROM dim_channel$options
WHERE `key` = 'snapshot.num-retained.min'
UNION ALL SELECT 'opt_dim_channel_max', `value` FROM dim_channel$options
WHERE `key` = 'snapshot.num-retained.max';
