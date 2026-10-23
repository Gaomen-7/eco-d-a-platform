-- replay_00_catalog.sql — Paimon catalog + ecom_lake database（impl-03b）
--
-- 规格落点：spec §5.2 / §5.3；附录 A·M（catalog 与 database 两层分别取名，别混）。
--
-- ★ 两个名字必须分开（2026-10-21 签，附录 A·M）：
--     catalog  = paimon          （存储后端类型）
--     database = ecom_lake       （承载五层的库）
--   原文「单一 catalog（库名 ecom_lake）」混说了两层；本文件按裁决分开取。
--
-- ★ warehouse 路径 file:/warehouse 是 impl-03a 钉下的**同一路径**：
--   该路径以具名卷 paimon-warehouse 逐字挂进 flink-jm 与 flink-tm（compose 两处相同）。
--   本文件**不得**写成别的路径——Paimon 把绝对路径写进 catalog/表元数据，
--   路径一旦不同，两个 Flink 进程就读不到同一份数仓。
--
-- 阻塞：本文件依赖 impl-03a 已验收的部署面（jar 进 /opt/flink/lib + 具名卷同路径）。

-- ★ 必须设 TABLEAU（impl-03b code-review #5，§E7 第 3 条）：sql-client 非交互时
--   任何 SELECT 都报 "Only 'table', 'tableau' and 'changelog' result modes are
--   supported" 之外的结果模式错误。本文件今日无 SELECT ⇒ 今日无害；但**将来**
--   若在此加一条校验 SELECT（例如 SHOW DATABASES 的可判化），不设就会炸在一个
--   与 SQL 语义毫无关系的错上 —— 那是本仓已经踩过的坑，故提前钉住。
SET 'sql-client.execution.result-mode' = 'TABLEAU';

CREATE CATALOG IF NOT EXISTS paimon WITH (
  'type' = 'paimon',
  'warehouse' = 'file:/warehouse'
);

USE CATALOG paimon;
CREATE DATABASE IF NOT EXISTS ecom_lake;
USE ecom_lake;
