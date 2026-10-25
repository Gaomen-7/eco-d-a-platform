#!/usr/bin/env bash
# impl-06b · 第 3 步：RECOVER 回滚路径实机验证（先验证后依赖） + 观察期配置切换
# 产物：stdout 全量进 evidence/impl-06b/03-recover-and-rollback-drill.out.txt
set -euo pipefail

dq() { docker compose run --rm --no-TTY client mysql -h doris-fe -P 9030 -u root --batch "$@"; }

echo "=== [1] RECOVER 前：ecom_rehearsal 在 recycle bin（06a 遗产） ==="
date -u '+UTC: %Y-%m-%d %H:%M:%S'
dq -e "SHOW CATALOG RECYCLE BIN;" | grep -Ei "ecom_rehearsal|Type" || true
echo

echo "=== [2] RECOVER DATABASE ecom_rehearsal（票面缓解③路径的实机验证） ==="
dq -e "RECOVER DATABASE ecom_rehearsal;"
echo

echo "=== [3] 恢复后核对：库回位 + 06a 演练表行数（06a 收尾时为 1 行） ==="
dq -e "SHOW DATABASES;"
dq -e "SHOW TABLES FROM ecom_rehearsal;"
dq -e "SELECT COUNT(*) AS drill_table_rows_after_recover FROM ecom_rehearsal.ads_window_metric_rehearsal;"
echo

echo "=== [4] 回滚演练小表：建表 + 1 行 ==="
dq -e "CREATE TABLE IF NOT EXISTS ecom_rehearsal.reclaim_rollback_check (
  id BIGINT NOT NULL,
  note VARCHAR(64)
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES ('replication_allocation' = 'tag.location.default: 1');"
dq -e "INSERT INTO ecom_rehearsal.reclaim_rollback_check VALUES (1, 'rollback drill row');"
dq -e "SELECT COUNT(*) AS rows_after_insert FROM ecom_rehearsal.reclaim_rollback_check;"
echo

echo "=== [5] DROP TABLE（非 force）→ recycle bin 可见 → RECOVER TABLE → 行数对账 ==="
T_DROP=$(date -u '+%Y-%m-%d %H:%M:%S')
echo "T(drop rollback_check): $T_DROP"
dq -e "DROP TABLE ecom_rehearsal.reclaim_rollback_check;"
dq -e "SHOW CATALOG RECYCLE BIN;" | grep -E "reclaim_rollback_check" || echo "(bin 中未立即可见——记录)"
dq -e "RECOVER TABLE ecom_rehearsal.reclaim_rollback_check;"
dq -e "SELECT COUNT(*) AS rows_after_recover FROM ecom_rehearsal.reclaim_rollback_check;"
echo

echo "=== [6] 观察期配置切换：catalog_trash_expire_second 86400 → 300（缓解方案 §3，收尾恢复） ==="
dq -e "ADMIN SET FRONTEND CONFIG (\"catalog_trash_expire_second\" = \"300\");"
dq -e "SHOW FRONTEND CONFIG LIKE 'catalog_trash_expire_second';"
echo

echo "=== [7] 建实测主对象 reclaim_probe（DUPLICATE KEY，无主键 compaction 干扰） ==="
dq -e "CREATE TABLE ecom_rehearsal.reclaim_probe (
  id BIGINT NOT NULL,
  payload VARCHAR(2048)
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 8
PROPERTIES ('replication_allocation' = 'tag.location.default: 1');"
dq -e "SHOW CREATE TABLE ecom_rehearsal.reclaim_probe\G" || true
