-- 探测：append 表（dwd_behavior_event_reject，无主键）的 DELETE 是否可用
SET 'sql-client.execution.result-mode' = 'TABLEAU';
SET 'execution.runtime-mode' = 'batch';
SET 'table.local-time-zone' = 'UTC';
SET 'table.dml-sync' = 'true';
CREATE CATALOG IF NOT EXISTS paimon WITH ('type' = 'paimon', 'warehouse' = 'file:/warehouse');
USE CATALOG paimon;
USE ecom_lake;
SELECT 'FIXTURE probe_delete BEGIN' AS fx;
INSERT INTO dwd_behavior_event_reject VALUES
  ('2026-10-24', 'fx_probe_delete', 0, 'u_fx', 'i_fx', 'c_fx', 'view', 'v1',
   '{"fx":true}', 'value_out_of_range', 0);
SELECT CONCAT('fx.probe.inserted rows=', CAST(COUNT(*) AS STRING)) AS r
FROM dwd_behavior_event_reject WHERE event_id = 'fx_probe_delete';
DELETE FROM dwd_behavior_event_reject WHERE event_id = 'fx_probe_delete';
SELECT CONCAT('fx.probe.residual rows=', CAST(COUNT(*) AS STRING)) AS r
FROM dwd_behavior_event_reject WHERE event_id = 'fx_probe_delete';
