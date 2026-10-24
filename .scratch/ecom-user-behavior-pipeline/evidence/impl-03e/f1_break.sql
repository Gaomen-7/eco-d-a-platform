-- F1: A1 分流完备 —— 复制一条非迟到 DWD 行（新 event_id）⇒ dwd_distinct +1 ≠ ods_distinct
SET 'sql-client.execution.result-mode' = 'TABLEAU';
SET 'execution.runtime-mode' = 'batch';
SET 'table.local-time-zone' = 'UTC';
SET 'table.dml-sync' = 'true';
CREATE CATALOG IF NOT EXISTS paimon WITH ('type' = 'paimon', 'warehouse' = 'file:/warehouse');
USE CATALOG paimon;
USE ecom_lake;
SELECT 'FIXTURE f1_break BEGIN' AS fx;
INSERT INTO dwd_behavior_event
SELECT
  dt, '000020fe-0c38-4345-aada-309be337570c_f1_break' AS event_id,
  event_time, user_id, item_id, category_id, event_type, schema_version,
  page, source, channel, province, amount, currency, quantity, order_id,
  is_late, late_lag_ms
FROM dwd_behavior_event
WHERE event_id = '000020fe-0c38-4345-aada-309be337570c';
SELECT CONCAT('fx.f1.inserted rows=', CAST(COUNT(*) AS STRING)) AS r
FROM dwd_behavior_event WHERE event_id LIKE '%_f1_break';
