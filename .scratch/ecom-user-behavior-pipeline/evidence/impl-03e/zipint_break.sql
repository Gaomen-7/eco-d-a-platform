-- ZIP_interval: 复制 i_000000 最后一版（vf=1704067294314）为 vf+1、vt=9999999999999
-- ⇒ 原版 vt ≠ next_vf−1 ⇒ ZIP_interval 红；ZIP_audit 因审计多一条不受影响（≥2 即计）
SET 'sql-client.execution.result-mode' = 'TABLEAU';
SET 'execution.runtime-mode' = 'batch';
SET 'table.local-time-zone' = 'UTC';
SET 'table.dml-sync' = 'true';
CREATE CATALOG IF NOT EXISTS paimon WITH ('type' = 'paimon', 'warehouse' = 'file:/warehouse');
USE CATALOG paimon;
USE ecom_lake;
SELECT 'FIXTURE zipint_break BEGIN' AS fx;
INSERT INTO dim_item
SELECT item_id, 1704067294315, item_name, category_id, price_cents, brand,
       9999999999999, change_time
FROM dim_item
WHERE item_id = 'i_000000' AND valid_from_ms = 1704067294314;
SELECT CONCAT('fx.zi.versions=', CAST(COUNT(*) AS STRING)) AS r
FROM dim_item WHERE item_id = 'i_000000';
