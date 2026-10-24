-- F4: 幂等 —— 把第一条迟到行（000004a7…，lag=2941473）upsert 成 lag=2941474
-- 行数/桶分布/其余列全不变 ⇒ 9 条哨兵应全绿；但 fp.hash/dlag 变 ⇒ --compare RC=3
SET 'sql-client.execution.result-mode' = 'TABLEAU';
SET 'execution.runtime-mode' = 'batch';
SET 'table.local-time-zone' = 'UTC';
SET 'table.dml-sync' = 'true';
CREATE CATALOG IF NOT EXISTS paimon WITH ('type' = 'paimon', 'warehouse' = 'file:/warehouse');
USE CATALOG paimon;
USE ecom_lake;
SELECT 'FIXTURE f4_break BEGIN' AS fx;
INSERT INTO dwd_behavior_event
SELECT dt, event_id, event_time, user_id, item_id, category_id, event_type,
       schema_version, page, source, channel, province, amount, currency,
       quantity, order_id, is_late, CAST(2941474 AS BIGINT)
FROM dwd_behavior_event
WHERE event_id = '000004a7-e232-4b55-a3ff-f5159dc15a87';
SELECT CONCAT('fx.f4.lag=', CAST(late_lag_ms AS STRING)) AS r
FROM dwd_behavior_event WHERE event_id = '000004a7-e232-4b55-a3ff-f5159dc15a87';
