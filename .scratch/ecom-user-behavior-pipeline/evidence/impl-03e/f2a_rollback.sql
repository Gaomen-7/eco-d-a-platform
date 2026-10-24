-- F2a 回滚：同键 upsert 改回 is_late=0
SET 'sql-client.execution.result-mode' = 'TABLEAU';
SET 'execution.runtime-mode' = 'batch';
SET 'table.local-time-zone' = 'UTC';
SET 'table.dml-sync' = 'true';
CREATE CATALOG IF NOT EXISTS paimon WITH ('type' = 'paimon', 'warehouse' = 'file:/warehouse');
USE CATALOG paimon;
USE ecom_lake;
SELECT 'FIXTURE f2a_rollback BEGIN' AS fx;
INSERT INTO dwd_behavior_event
SELECT dt, event_id, event_time, user_id, item_id, category_id, event_type,
       schema_version, page, source, channel, province, amount, currency,
       quantity, order_id,
       CAST(0 AS TINYINT) AS is_late, late_lag_ms
FROM dwd_behavior_event
WHERE event_id = '00005176-7e4c-4cc4-a865-1bc61718845a';
SELECT CONCAT('fx.f2a.back_to_zero rows=', CAST(COUNT(*) AS STRING)) AS r
FROM dwd_behavior_event
WHERE event_id = '00005176-7e4c-4cc4-a865-1bc61718845a' AND is_late = 0;
