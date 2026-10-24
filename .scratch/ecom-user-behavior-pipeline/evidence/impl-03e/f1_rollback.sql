-- F1 回滚：删除插入的破坏行（主键表 DELETE 按 (dt,event_id)）
SET 'sql-client.execution.result-mode' = 'TABLEAU';
SET 'execution.runtime-mode' = 'batch';
SET 'table.local-time-zone' = 'UTC';
SET 'table.dml-sync' = 'true';
CREATE CATALOG IF NOT EXISTS paimon WITH ('type' = 'paimon', 'warehouse' = 'file:/warehouse');
USE CATALOG paimon;
USE ecom_lake;
SELECT 'FIXTURE f1_rollback BEGIN' AS fx;
DELETE FROM dwd_behavior_event WHERE event_id = '000020fe-0c38-4345-aada-309be337570c_f1_break';
SELECT CONCAT('fx.f1.residual rows=', CAST(COUNT(*) AS STRING)) AS r
FROM dwd_behavior_event WHERE event_id LIKE '%_f1_break';
