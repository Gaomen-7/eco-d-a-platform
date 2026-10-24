-- SNAP_empty: 空表但有快照 —— 插 1 行（产生快照）再 DROP+按 DDL 重建但保留旧数据？
-- 不行。正解：插行产生快照后，用「重建表 + 拷数据」会重置快照。
-- 唯一纯 SQL 途径制造「0 行 + ≥1 快照」：插行后主键表 DELETE —— reject 是 append 表不可 DELETE。
-- ⇒ 改在【主键表】上制造同构形态：本夹具在 reject 插 1 行 ⇒ 表变非空 ⇒ 走 ⑤c 第二分支
--   (行>0 AND 快照≥1) 仍绿 —— 这测不了。真正的空表+快照形态见 break2（DROP 后建同名表、
--   快照从 0 起，再单独 INSERT 一条会立刻有行）。结论：对 append 表，本形态不可达，
--   见票面 §6 SNAP_empty 行的「前提探测」——实测 append 表 DELETE 不可用 ⇒ 该反例
--   只能以「0 行但 0 快照（正例）」+「有行有快照（正例）」两态覆盖，第三态不可构造。
-- 本夹具保留：插 1 行 ⇒ SNAP_empty 走第二分支仍绿 ⇒ 证明它不是「只认空表」的单向判据。
SET 'sql-client.execution.result-mode' = 'TABLEAU';
SET 'execution.runtime-mode' = 'batch';
SET 'table.local-time-zone' = 'UTC';
SET 'table.dml-sync' = 'true';
CREATE CATALOG IF NOT EXISTS paimon WITH ('type' = 'paimon', 'warehouse' = 'file:/warehouse');
USE CATALOG paimon;
USE ecom_lake;
SELECT 'FIXTURE snapempty_break BEGIN' AS fx;
INSERT INTO dwd_behavior_event_reject VALUES
  ('2026-10-24', 'fx_snap_empty', 0, 'u_fx', 'i_fx', 'c_fx', 'view', 'v1',
   '{"fx":true}', 'value_out_of_range', 0);
SELECT CONCAT('fx.se.rows=', CAST(COUNT(*) AS STRING)) AS r FROM dwd_behavior_event_reject;
SELECT CONCAT('fx.se.snaps=', CAST(COUNT(*) AS STRING)) AS r FROM dwd_behavior_event_reject$snapshots;
