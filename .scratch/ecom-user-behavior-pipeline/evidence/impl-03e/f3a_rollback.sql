-- F3a 回滚：max 改回 20
SET 'sql-client.execution.result-mode' = 'TABLEAU';
SET 'execution.runtime-mode' = 'batch';
SET 'table.local-time-zone' = 'UTC';
SET 'table.dml-sync' = 'true';
CREATE CATALOG IF NOT EXISTS paimon WITH ('type' = 'paimon', 'warehouse' = 'file:/warehouse');
USE CATALOG paimon;
USE ecom_lake;
SELECT 'FIXTURE f3a_rollback BEGIN' AS fx;
ALTER TABLE dim_channel SET ('snapshot.num-retained.max' = '20');
SELECT CONCAT('fx.f3a.max=', `value`) AS r FROM dim_channel$options
WHERE `key` = 'snapshot.num-retained.max';
