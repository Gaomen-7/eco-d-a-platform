-- F3b 回滚：max 改回 20 + expire_snapshots 收到 retain_max=20
SET 'sql-client.execution.result-mode' = 'TABLEAU';
SET 'execution.runtime-mode' = 'batch';
SET 'table.local-time-zone' = 'UTC';
SET 'table.dml-sync' = 'true';
CREATE CATALOG IF NOT EXISTS paimon WITH ('type' = 'paimon', 'warehouse' = 'file:/warehouse');
USE CATALOG paimon;
USE ecom_lake;
SELECT 'FIXTURE f3b_rollback BEGIN' AS fx;
ALTER TABLE dwd_behavior_event SET ('snapshot.num-retained.max' = '20');
CALL sys.expire_snapshots(`table` => 'ecom_lake.dwd_behavior_event', retain_max => 20);
SELECT CONCAT('fx.f3b.snaps=', CAST(COUNT(*) AS STRING)) AS r FROM dwd_behavior_event$snapshots;
