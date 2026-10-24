-- SNAP_empty 回滚：DROP + 按 replay_01_ddl.sql ③ 段原样重建（回到 0 行 0 快照正例态）
SET 'sql-client.execution.result-mode' = 'TABLEAU';
SET 'execution.runtime-mode' = 'batch';
SET 'table.local-time-zone' = 'UTC';
SET 'table.dml-sync' = 'true';
CREATE CATALOG IF NOT EXISTS paimon WITH ('type' = 'paimon', 'warehouse' = 'file:/warehouse');
USE CATALOG paimon;
USE ecom_lake;
SELECT 'FIXTURE se_rollback BEGIN' AS fx;
DROP TABLE dwd_behavior_event_reject;
CREATE TABLE dwd_behavior_event_reject (
  dt             STRING,
  event_id       STRING,
  event_time     BIGINT,
  user_id        STRING,
  item_id        STRING,
  category_id    STRING,
  event_type     STRING,
  schema_version STRING,
  raw_payload    STRING,
  reject_reason  STRING,
  ingest_time    BIGINT
) PARTITIONED BY (dt)
WITH (
  'bucket' = '-1',
  'snapshot.num-retained.min' = '5',
  'snapshot.num-retained.max' = '20'
);
SELECT CONCAT('fx.se.rows=', CAST(COUNT(*) AS STRING)) AS r FROM dwd_behavior_event_reject;
SELECT CONCAT('fx.se.snaps=', CAST(COUNT(*) AS STRING)) AS r FROM dwd_behavior_event_reject$snapshots;
