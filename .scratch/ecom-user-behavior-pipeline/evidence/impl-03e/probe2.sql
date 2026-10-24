-- 03e 状态探针 2：取全量 event_id（36 字符 UUID 被 TABLEAU 30 字符列宽截断 ⇒ 拆两半）
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

SELECT 'FIXTURE probe2 BEGIN' AS fx;

SELECT 'probe.f1_a' AS k, SUBSTRING(event_id, 1, 18) AS v FROM dwd_behavior_event
WHERE is_late = 0 ORDER BY event_id LIMIT 1;
SELECT 'probe.f1_b' AS k, SUBSTRING(event_id, 19, 18) AS v FROM dwd_behavior_event
WHERE is_late = 0 ORDER BY event_id LIMIT 1;
SELECT 'probe.f2_a' AS k, SUBSTRING(event_id, 1, 18) AS v FROM dwd_behavior_event
WHERE is_late = 0 ORDER BY event_id LIMIT 1 OFFSET 1;
SELECT 'probe.f2_b' AS k, SUBSTRING(event_id, 19, 18) AS v FROM dwd_behavior_event
WHERE is_late = 0 ORDER BY event_id LIMIT 1 OFFSET 1;
SELECT 'probe.f4_a' AS k, SUBSTRING(event_id, 1, 18) AS v FROM dwd_behavior_event
WHERE is_late = 1 ORDER BY event_id LIMIT 1;
SELECT 'probe.f4_b' AS k, SUBSTRING(event_id, 19, 18) AS v FROM dwd_behavior_event
WHERE is_late = 1 ORDER BY event_id LIMIT 1;
SELECT 'probe.f4_lag' AS k, CAST(late_lag_ms AS STRING) AS v FROM dwd_behavior_event
WHERE is_late = 1 ORDER BY event_id LIMIT 1;
