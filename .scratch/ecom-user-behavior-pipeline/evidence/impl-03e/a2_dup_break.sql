-- A2 负向：复制 dt='2026-10-24' 且 src_seq=0 的行（src_seq 全表唯一，恰 1 行）、
-- src_seq 改 4000000000（同 event_id ⇒ ODS 出重复）
-- 预期：lake_assert 仍 RC=0，但 metric.a2_dup_overshoot = 1（只记账、不判定）
SET 'sql-client.execution.result-mode' = 'TABLEAU';
SET 'execution.runtime-mode' = 'batch';
SET 'table.local-time-zone' = 'UTC';
SET 'table.dml-sync' = 'true';
CREATE CATALOG IF NOT EXISTS paimon WITH ('type' = 'paimon', 'warehouse' = 'file:/warehouse');
USE CATALOG paimon;
USE ecom_lake;
SELECT 'FIXTURE a2_dup_break BEGIN' AS fx;
INSERT INTO ods_behavior_event
SELECT event_id, event_time, user_id, item_id, category_id, event_type,
       schema_version, page, source, channel, province, payload,
       CAST(4000000000 AS BIGINT), dt
FROM ods_behavior_event
WHERE dt = '2026-10-24' AND src_seq = 0;
SELECT CONCAT('fx.a2.added=', CAST(COUNT(*) AS STRING)) AS r
FROM ods_behavior_event WHERE src_seq = 4000000000;
