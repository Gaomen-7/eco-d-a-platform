-- F3b: SNAP_within_upper_bound —— dwd max=100 + 同键 upsert 22 次（内容不变）
-- ⇒ 快照 5+22=27 > 20 ⇒ SNAP_within 红；OPT 因 max 变 100 也降为 21（连带红，票面已预期）
SET 'sql-client.execution.result-mode' = 'TABLEAU';
SET 'execution.runtime-mode' = 'batch';
SET 'table.local-time-zone' = 'UTC';
SET 'table.dml-sync' = 'true';
CREATE CATALOG IF NOT EXISTS paimon WITH ('type' = 'paimon', 'warehouse' = 'file:/warehouse');
USE CATALOG paimon;
USE ecom_lake;
SELECT 'FIXTURE f3b_break BEGIN' AS fx;
ALTER TABLE dwd_behavior_event SET ('snapshot.num-retained.max' = '100');
INSERT INTO dwd_behavior_event
SELECT dt, event_id, event_time, user_id, item_id, category_id, event_type,
       schema_version, page, source, channel, province, amount, currency,
       quantity, order_id, is_late, late_lag_ms
FROM dwd_behavior_event WHERE event_id = '000020fe-0c38-4345-aada-309be337570c';
INSERT INTO dwd_behavior_event
SELECT dt, event_id, event_time, user_id, item_id, category_id, event_type,
       schema_version, page, source, channel, province, amount, currency,
       quantity, order_id, is_late, late_lag_ms
FROM dwd_behavior_event WHERE event_id = '000020fe-0c38-4345-aada-309be337570c';
INSERT INTO dwd_behavior_event
SELECT dt, event_id, event_time, user_id, item_id, category_id, event_type,
       schema_version, page, source, channel, province, amount, currency,
       quantity, order_id, is_late, late_lag_ms
FROM dwd_behavior_event WHERE event_id = '000020fe-0c38-4345-aada-309be337570c';
INSERT INTO dwd_behavior_event
SELECT dt, event_id, event_time, user_id, item_id, category_id, event_type,
       schema_version, page, source, channel, province, amount, currency,
       quantity, order_id, is_late, late_lag_ms
FROM dwd_behavior_event WHERE event_id = '000020fe-0c38-4345-aada-309be337570c';
INSERT INTO dwd_behavior_event
SELECT dt, event_id, event_time, user_id, item_id, category_id, event_type,
       schema_version, page, source, channel, province, amount, currency,
       quantity, order_id, is_late, late_lag_ms
FROM dwd_behavior_event WHERE event_id = '000020fe-0c38-4345-aada-309be337570c';
INSERT INTO dwd_behavior_event
SELECT dt, event_id, event_time, user_id, item_id, category_id, event_type,
       schema_version, page, source, channel, province, amount, currency,
       quantity, order_id, is_late, late_lag_ms
FROM dwd_behavior_event WHERE event_id = '000020fe-0c38-4345-aada-309be337570c';
INSERT INTO dwd_behavior_event
SELECT dt, event_id, event_time, user_id, item_id, category_id, event_type,
       schema_version, page, source, channel, province, amount, currency,
       quantity, order_id, is_late, late_lag_ms
FROM dwd_behavior_event WHERE event_id = '000020fe-0c38-4345-aada-309be337570c';
INSERT INTO dwd_behavior_event
SELECT dt, event_id, event_time, user_id, item_id, category_id, event_type,
       schema_version, page, source, channel, province, amount, currency,
       quantity, order_id, is_late, late_lag_ms
FROM dwd_behavior_event WHERE event_id = '000020fe-0c38-4345-aada-309be337570c';
INSERT INTO dwd_behavior_event
SELECT dt, event_id, event_time, user_id, item_id, category_id, event_type,
       schema_version, page, source, channel, province, amount, currency,
       quantity, order_id, is_late, late_lag_ms
FROM dwd_behavior_event WHERE event_id = '000020fe-0c38-4345-aada-309be337570c';
INSERT INTO dwd_behavior_event
SELECT dt, event_id, event_time, user_id, item_id, category_id, event_type,
       schema_version, page, source, channel, province, amount, currency,
       quantity, order_id, is_late, late_lag_ms
FROM dwd_behavior_event WHERE event_id = '000020fe-0c38-4345-aada-309be337570c';
INSERT INTO dwd_behavior_event
SELECT dt, event_id, event_time, user_id, item_id, category_id, event_type,
       schema_version, page, source, channel, province, amount, currency,
       quantity, order_id, is_late, late_lag_ms
FROM dwd_behavior_event WHERE event_id = '000020fe-0c38-4345-aada-309be337570c';
INSERT INTO dwd_behavior_event
SELECT dt, event_id, event_time, user_id, item_id, category_id, event_type,
       schema_version, page, source, channel, province, amount, currency,
       quantity, order_id, is_late, late_lag_ms
FROM dwd_behavior_event WHERE event_id = '000020fe-0c38-4345-aada-309be337570c';
INSERT INTO dwd_behavior_event
SELECT dt, event_id, event_time, user_id, item_id, category_id, event_type,
       schema_version, page, source, channel, province, amount, currency,
       quantity, order_id, is_late, late_lag_ms
FROM dwd_behavior_event WHERE event_id = '000020fe-0c38-4345-aada-309be337570c';
INSERT INTO dwd_behavior_event
SELECT dt, event_id, event_time, user_id, item_id, category_id, event_type,
       schema_version, page, source, channel, province, amount, currency,
       quantity, order_id, is_late, late_lag_ms
FROM dwd_behavior_event WHERE event_id = '000020fe-0c38-4345-aada-309be337570c';
INSERT INTO dwd_behavior_event
SELECT dt, event_id, event_time, user_id, item_id, category_id, event_type,
       schema_version, page, source, channel, province, amount, currency,
       quantity, order_id, is_late, late_lag_ms
FROM dwd_behavior_event WHERE event_id = '000020fe-0c38-4345-aada-309be337570c';
INSERT INTO dwd_behavior_event
SELECT dt, event_id, event_time, user_id, item_id, category_id, event_type,
       schema_version, page, source, channel, province, amount, currency,
       quantity, order_id, is_late, late_lag_ms
FROM dwd_behavior_event WHERE event_id = '000020fe-0c38-4345-aada-309be337570c';
INSERT INTO dwd_behavior_event
SELECT dt, event_id, event_time, user_id, item_id, category_id, event_type,
       schema_version, page, source, channel, province, amount, currency,
       quantity, order_id, is_late, late_lag_ms
FROM dwd_behavior_event WHERE event_id = '000020fe-0c38-4345-aada-309be337570c';
INSERT INTO dwd_behavior_event
SELECT dt, event_id, event_time, user_id, item_id, category_id, event_type,
       schema_version, page, source, channel, province, amount, currency,
       quantity, order_id, is_late, late_lag_ms
FROM dwd_behavior_event WHERE event_id = '000020fe-0c38-4345-aada-309be337570c';
INSERT INTO dwd_behavior_event
SELECT dt, event_id, event_time, user_id, item_id, category_id, event_type,
       schema_version, page, source, channel, province, amount, currency,
       quantity, order_id, is_late, late_lag_ms
FROM dwd_behavior_event WHERE event_id = '000020fe-0c38-4345-aada-309be337570c';
INSERT INTO dwd_behavior_event
SELECT dt, event_id, event_time, user_id, item_id, category_id, event_type,
       schema_version, page, source, channel, province, amount, currency,
       quantity, order_id, is_late, late_lag_ms
FROM dwd_behavior_event WHERE event_id = '000020fe-0c38-4345-aada-309be337570c';
INSERT INTO dwd_behavior_event
SELECT dt, event_id, event_time, user_id, item_id, category_id, event_type,
       schema_version, page, source, channel, province, amount, currency,
       quantity, order_id, is_late, late_lag_ms
FROM dwd_behavior_event WHERE event_id = '000020fe-0c38-4345-aada-309be337570c';
INSERT INTO dwd_behavior_event
SELECT dt, event_id, event_time, user_id, item_id, category_id, event_type,
       schema_version, page, source, channel, province, amount, currency,
       quantity, order_id, is_late, late_lag_ms
FROM dwd_behavior_event WHERE event_id = '000020fe-0c38-4345-aada-309be337570c';
SELECT CONCAT('fx.f3b.snaps=', CAST(COUNT(*) AS STRING)) AS r FROM dwd_behavior_event$snapshots;
SELECT CONCAT('fx.f3b.rows=', CAST(COUNT(*) AS STRING)) AS r FROM dwd_behavior_event;
