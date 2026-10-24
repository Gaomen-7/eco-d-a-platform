-- F2b 回滚：静态分区 OVERWRITE（源=该分区其余档，非空 ⇒ 真覆盖），剔除 777 行
SET 'sql-client.execution.result-mode' = 'TABLEAU';
SET 'execution.runtime-mode' = 'batch';
SET 'table.local-time-zone' = 'UTC';
SET 'table.dml-sync' = 'true';
CREATE CATALOG IF NOT EXISTS paimon WITH ('type' = 'paimon', 'warehouse' = 'file:/warehouse');
USE CATALOG paimon;
USE ecom_lake;
SELECT 'FIXTURE f2b_rollback BEGIN' AS fx;
INSERT OVERWRITE dwd_late_event_count PARTITION (dt='2024-01-03')
SELECT late_bucket, cnt FROM dwd_late_event_count
WHERE dt='2024-01-03' AND NOT (late_bucket=1704319080000 AND cnt=777);
SELECT CONCAT('fx.f2b.residual=', CAST(COUNT(*) AS STRING)) AS r
FROM dwd_late_event_count
WHERE dt='2024-01-03' AND late_bucket=1704319080000 AND cnt=777;
