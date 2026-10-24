-- ZIP_interval 回滚：删掉插入的版本（主键表 DELETE）
SET 'sql-client.execution.result-mode' = 'TABLEAU';
SET 'execution.runtime-mode' = 'batch';
SET 'table.local-time-zone' = 'UTC';
SET 'table.dml-sync' = 'true';
CREATE CATALOG IF NOT EXISTS paimon WITH ('type' = 'paimon', 'warehouse' = 'file:/warehouse');
USE CATALOG paimon;
USE ecom_lake;
SELECT 'FIXTURE zipint_rollback BEGIN' AS fx;
DELETE FROM dim_item WHERE item_id = 'i_000000' AND valid_from_ms = 1704067294315;
SELECT CONCAT('fx.zi.versions=', CAST(COUNT(*) AS STRING)) AS r
FROM dim_item WHERE item_id = 'i_000000';
