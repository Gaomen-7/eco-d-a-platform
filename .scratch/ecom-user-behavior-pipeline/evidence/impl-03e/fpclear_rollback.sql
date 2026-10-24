-- fp_ready+A3_flag 回滚：INSERT OVERWRITE 从备份还原 → DROP 备份表
SET 'sql-client.execution.result-mode' = 'TABLEAU';
SET 'execution.runtime-mode' = 'batch';
SET 'table.local-time-zone' = 'UTC';
SET 'table.dml-sync' = 'true';
CREATE CATALOG IF NOT EXISTS paimon WITH ('type' = 'paimon', 'warehouse' = 'file:/warehouse');
USE CATALOG paimon;
USE ecom_lake;
SELECT 'FIXTURE fpclear_rollback BEGIN' AS fx;
INSERT OVERWRITE dwd_behavior_event
SELECT dt, event_id, event_time, user_id, item_id, category_id, event_type,
       schema_version, page, source, channel, province, amount, currency,
       quantity, order_id, is_late, late_lag_ms
FROM dwd_behavior_event_bak;
DROP TABLE dwd_behavior_event_bak;
SELECT CONCAT('fx.fc.dwd_rows=', CAST(COUNT(*) AS STRING)) AS r FROM dwd_behavior_event;
