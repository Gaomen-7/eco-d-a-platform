-- 03e 状态探针：取证当前集群状态 + 采集各反例夹具需要的字面量。
-- 只读，不写任何表。
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

SELECT 'FIXTURE probe BEGIN' AS fx;

SELECT 'probe.now_utc' AS k, CAST(CURRENT_TIMESTAMP AS STRING) AS v;
SELECT 'probe.landing_day' AS k, CAST(MAX(dt) AS STRING) AS v FROM ods_behavior_event;
SELECT 'probe.ods_rows_total' AS k, CAST(COUNT(*) AS STRING) AS v FROM ods_behavior_event;
SELECT 'probe.ods_today_rows' AS k, CAST(COUNT(*) AS STRING) AS v FROM ods_behavior_event
WHERE dt = (SELECT MAX(dt) FROM ods_behavior_event);
SELECT 'probe.ods_today_min_src_seq' AS k, CAST(MIN(src_seq) AS STRING) AS v FROM ods_behavior_event
WHERE dt = (SELECT MAX(dt) FROM ods_behavior_event);
SELECT 'probe.dwd_rows' AS k, CAST(COUNT(*) AS STRING) AS v FROM dwd_behavior_event;
SELECT 'probe.dwd_distinct' AS k, CAST(COUNT(DISTINCT event_id) AS STRING) AS v FROM dwd_behavior_event;
SELECT 'probe.dwd_snapshots' AS k, CAST(COUNT(*) AS STRING) AS v FROM dwd_behavior_event$snapshots;
SELECT 'probe.reject_rows' AS k, CAST(COUNT(*) AS STRING) AS v FROM dwd_behavior_event_reject;
SELECT 'probe.reject_snapshots' AS k, CAST(COUNT(*) AS STRING) AS v FROM dwd_behavior_event_reject$snapshots;
SELECT 'probe.late_count_rows' AS k, CAST(COUNT(*) AS STRING) AS v FROM dwd_late_event_count;

-- DWD 的事件日分布（记录用）
SELECT CONCAT('probe.dwd_dt=', dt) AS k, CAST(COUNT(*) AS STRING) AS v
FROM dwd_behavior_event GROUP BY dt ORDER BY dt;

-- F1 复制源：第一条非迟到行
SELECT 'probe.f1_source' AS k, event_id AS v FROM dwd_behavior_event
WHERE is_late = 0 ORDER BY event_id LIMIT 1;
-- F2 翻转目标：第二条非迟到行（与 F1 源区分）
SELECT 'probe.f2_source' AS k, event_id AS v FROM dwd_behavior_event
WHERE is_late = 0 ORDER BY event_id LIMIT 1 OFFSET 1;
-- F4 改 lag 目标：第一条迟到行
SELECT 'probe.f4_source' AS k, CONCAT(event_id, '/', CAST(late_lag_ms AS STRING)) AS v
FROM dwd_behavior_event WHERE is_late = 1 ORDER BY event_id LIMIT 1;
-- ZIP_interval 破坏目标：某 item 的最后一版
SELECT 'probe.zip_item' AS k, CONCAT(item_id, '/', CAST(valid_from_ms AS STRING)) AS v
FROM dim_item WHERE valid_to_ms = 9999999999999 ORDER BY item_id LIMIT 1;
-- F2b（计数表侧）目标：最大档
SELECT 'probe.late_bucket_top' AS k, CONCAT(dt, '/', CAST(late_bucket AS STRING), '/', CAST(cnt AS STRING)) AS v
FROM dwd_late_event_count ORDER BY cnt DESC LIMIT 1;
