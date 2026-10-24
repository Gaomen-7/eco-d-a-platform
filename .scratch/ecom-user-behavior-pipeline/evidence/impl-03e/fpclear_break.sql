-- fp_ready+A3_flag: 建备份表（同 DWD DDL 改名）+ 全量拷入 → DELETE 清空 DWD
-- ⇒ 空表：A1（dwd=0≠ods）、A3_bucket（l 有档 r 空 ⇒ FULL JOIN mismatch）、
--         A3_flag（count=0）、fp_ready（rows=0）四红；SNAP 不红（DELETE 也提交快照）
SET 'sql-client.execution.result-mode' = 'TABLEAU';
SET 'execution.runtime-mode' = 'batch';
SET 'table.local-time-zone' = 'UTC';
SET 'table.dml-sync' = 'true';
CREATE CATALOG IF NOT EXISTS paimon WITH ('type' = 'paimon', 'warehouse' = 'file:/warehouse');
USE CATALOG paimon;
USE ecom_lake;
SELECT 'FIXTURE fpclear_break BEGIN' AS fx;
CREATE TABLE IF NOT EXISTS dwd_behavior_event_bak (
  dt             STRING,
  event_id       STRING,
  event_time     BIGINT,
  user_id        STRING,
  item_id        STRING,
  category_id    STRING,
  event_type     STRING,
  schema_version STRING,
  page           STRING,
  source         STRING,
  channel        STRING,
  province       STRING,
  amount         DECIMAL(18, 2),
  currency       STRING,
  quantity       INT,
  order_id       STRING,
  is_late        TINYINT,
  late_lag_ms    BIGINT,
  PRIMARY KEY (dt, event_id) NOT ENFORCED
) PARTITIONED BY (dt)
WITH (
  'bucket' = '1',
  'table.exec.sink.upsert-materialize' = 'NONE',
  'snapshot.num-retained.min' = '5',
  'snapshot.num-retained.max' = '20'
);
INSERT INTO dwd_behavior_event_bak
SELECT dt, event_id, event_time, user_id, item_id, category_id, event_type,
       schema_version, page, source, channel, province, amount, currency,
       quantity, order_id, is_late, late_lag_ms
FROM dwd_behavior_event;
DELETE FROM dwd_behavior_event WHERE dt IS NOT NULL;
SELECT CONCAT('fx.fc.dwd_rows=', CAST(COUNT(*) AS STRING)) AS r FROM dwd_behavior_event;
SELECT CONCAT('fx.fc.bak_rows=', CAST(COUNT(*) AS STRING)) AS r FROM dwd_behavior_event_bak;
