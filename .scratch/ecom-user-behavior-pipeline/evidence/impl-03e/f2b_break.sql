-- F2b: A3_late_bucket_split 计数表侧 —— 该档真实 cnt=254，插一行 777 ⇒ l 侧多行与 r 侧不等
SET 'sql-client.execution.result-mode' = 'TABLEAU';
SET 'execution.runtime-mode' = 'batch';
SET 'table.local-time-zone' = 'UTC';
SET 'table.dml-sync' = 'true';
CREATE CATALOG IF NOT EXISTS paimon WITH ('type' = 'paimon', 'warehouse' = 'file:/warehouse');
USE CATALOG paimon;
USE ecom_lake;
SELECT 'FIXTURE f2b_break BEGIN' AS fx;
INSERT INTO dwd_late_event_count VALUES ('2024-01-03', 1704319080000, CAST(777 AS BIGINT));
SELECT CONCAT('fx.f2b.added=', CAST(COUNT(*) AS STRING)) AS r
FROM dwd_late_event_count
WHERE dt='2024-01-03' AND late_bucket=1704319080000;
