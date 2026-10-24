-- F3a: OPT_snapshot_retention —— dim_channel 的 max 改成 100 ⇒ 22 条钉住降为 21
SET 'sql-client.execution.result-mode' = 'TABLEAU';
SET 'execution.runtime-mode' = 'batch';
SET 'table.local-time-zone' = 'UTC';
SET 'table.dml-sync' = 'true';
CREATE CATALOG IF NOT EXISTS paimon WITH ('type' = 'paimon', 'warehouse' = 'file:/warehouse');
USE CATALOG paimon;
USE ecom_lake;
SELECT 'FIXTURE f3a_break BEGIN' AS fx;
ALTER TABLE dim_channel SET ('snapshot.num-retained.max' = '100');
SELECT CONCAT('fx.f3a.max=', `value`) AS r FROM dim_channel$options
WHERE `key` = 'snapshot.num-retained.max';
