-- A2 回滚：静态分区 OVERWRITE dt='2026-10-24'（源=该分区除破坏行外全部，非空 ⇒ 真覆盖）
SET 'sql-client.execution.result-mode' = 'TABLEAU';
SET 'execution.runtime-mode' = 'batch';
SET 'table.local-time-zone' = 'UTC';
SET 'table.dml-sync' = 'true';
CREATE CATALOG IF NOT EXISTS paimon WITH ('type' = 'paimon', 'warehouse' = 'file:/warehouse');
USE CATALOG paimon;
USE ecom_lake;
SELECT 'FIXTURE a2_dup_rollback BEGIN' AS fx;
INSERT OVERWRITE ods_behavior_event PARTITION (dt='2026-10-24')
SELECT event_id, event_time, user_id, item_id, category_id, event_type,
       schema_version, page, source, channel, province, payload, src_seq
FROM ods_behavior_event
WHERE dt = '2026-10-24' AND src_seq <> 4000000000;
SELECT CONCAT('fx.a2.residual=', CAST(COUNT(*) AS STRING)) AS r
FROM ods_behavior_event WHERE src_seq = 4000000000;
