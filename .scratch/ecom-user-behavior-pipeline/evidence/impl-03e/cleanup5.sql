-- cleanup5: 最终清理路径 —— DROP TABLE + 按 replay_01_ddl.sql ③ 段原样重建（空表、0 快照）
-- 这正是 SNAP_empty 反例夹具的「回滚」设计；提前用它把 probe 行清掉
SET 'sql-client.execution.result-mode' = 'TABLEAU';
SET 'execution.runtime-mode' = 'batch';
SET 'table.local-time-zone' = 'UTC';
SET 'table.dml-sync' = 'true';
CREATE CATALOG IF NOT EXISTS paimon WITH ('type' = 'paimon', 'warehouse' = 'file:/warehouse');
USE CATALOG paimon;
USE ecom_lake;
SELECT 'FIXTURE cleanup5 BEGIN' AS fx;
DROP TABLE dwd_behavior_event_reject;
CREATE TABLE dwd_behavior_event_reject (
  dt             STRING COMMENT '摄入日 yyyy-MM-dd（处理时间，UTC）—— 不是事件日',
  event_id       STRING COMMENT '原样保留（可能为空——schema_invalid 时正是它坏了）',
  event_time     BIGINT COMMENT '原样保留，**不可信**，不用于分区',
  user_id        STRING,
  item_id        STRING,
  category_id    STRING,
  event_type     STRING,
  schema_version STRING,
  raw_payload    STRING COMMENT '被拒原文（整条 JSON，便于事后复核）',
  reject_reason  STRING COMMENT '封闭三值：schema_invalid / value_out_of_range / time_out_of_range',
  ingest_time    BIGINT COMMENT '摄入时刻 epoch ms（处理时间）'
) PARTITIONED BY (dt)
WITH (
  'bucket' = '-1',
  'snapshot.num-retained.min' = '5',
  'snapshot.num-retained.max' = '20'
);
SELECT CONCAT('fx.residual=', CAST(COUNT(*) AS STRING)) AS r FROM dwd_behavior_event_reject;
SELECT CONCAT('fx.snaps=', CAST(COUNT(*) AS STRING)) AS r FROM dwd_behavior_event_reject$snapshots;
