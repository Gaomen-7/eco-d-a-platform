-- ZIP_audit: DROP dim_category → 按 DDL 重建 → 只插 1 行 ⇒ 该表 audit_log 里无「≥2 条」的 id
-- ⇒ COUNT(DISTINCT t)=2 ≠ 3 ⇒ ZIP_audit 红
SET 'sql-client.execution.result-mode' = 'TABLEAU';
SET 'execution.runtime-mode' = 'batch';
SET 'table.local-time-zone' = 'UTC';
SET 'table.dml-sync' = 'true';
CREATE CATALOG IF NOT EXISTS paimon WITH ('type' = 'paimon', 'warehouse' = 'file:/warehouse');
USE CATALOG paimon;
USE ecom_lake;
SELECT 'FIXTURE zipaud_break BEGIN' AS fx;
DROP TABLE dim_category;
CREATE TABLE dim_category (
  category_id   STRING,
  valid_from_ms BIGINT,
  category_name STRING,
  parent_id     STRING,
  level         INT,
  valid_to_ms   BIGINT,
  change_time   BIGINT,
  PRIMARY KEY (category_id, valid_from_ms) NOT ENFORCED
) WITH (
  'bucket' = '1',
  'table.exec.sink.upsert-materialize' = 'NONE',
  'snapshot.num-retained.min' = '5',
  'snapshot.num-retained.max' = '20'
);
INSERT INTO dim_category VALUES
  ('c_f6_probe', 1700000000000, 'f6', 'c_root', 1, 9999999999999, 1700000000000);
SELECT CONCAT('fx.za.rows=', CAST(COUNT(*) AS STRING)) AS r FROM dim_category;
SELECT CONCAT('fx.za.ge2=', CAST(COUNT(*) AS STRING)) AS r FROM (
  SELECT category_id FROM dim_category$audit_log GROUP BY category_id HAVING COUNT(*) >= 2
) a;
