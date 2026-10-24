-- F4 回滚：lag 改回 2941473
SET 'sql-client.execution.result-mode' = 'TABLEAU';
SET 'execution.runtime-mode' = 'batch';
SET 'table.local-time-zone' = 'UTC';
SET 'table.dml-sync' = 'true';
CREATE CATALOG IF NOT EXISTS paimon WITH ('type' = 'paimon', 'warehouse' = 'file:/warehouse');
USE CATALOG paimon;
USE ecom_lake;
SELECT 'FIXTURE f4_rollback BEGIN' AS fx;
INSERT INTO dwd_behavior_event
SELECT dt, event_id, event_time, user_id, item_id, category_id, event_type,
       schema_version, page, source, channel, province, amount, currency,
       quantity, order_id, is_late, CAST(2941473 AS BIGINT)
FROM dwd_behavior_event
WHERE event_id = '000004a7-e232-4b55-a3ff-f5159dc15a87';
SELECT CONCAT('fx.f4.lag=', CAST(late_lag_ms AS STRING)) AS r
FROM dwd_behavior_event WHERE event_id = '000004a7-e232-4b55-a3ff-f5159dc15a87';
