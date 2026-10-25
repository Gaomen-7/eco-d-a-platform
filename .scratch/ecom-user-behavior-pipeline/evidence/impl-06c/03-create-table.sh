#!/usr/bin/env bash
# impl-06c · 第 3 步：建独立库表 + 三张已验证表零接触基线（行数口径修正版）
set -uo pipefail
ROOT="/mnt/f/Anaconda practice project/eco-a-d-platform"
cd "$ROOT"

dq() { docker compose run --rm --no-TTY client mysql -h doris-fe -P 9030 -u root --batch "$@" 2>/dev/null; }

echo "=== [1] 三张已验证 ADS 表基线行数（06c 零接触对账基线） ==="
dq -e "SELECT 'ads_window_metric' AS t, COUNT(*) AS c FROM ecom_ads.ads_window_metric
UNION ALL SELECT 'ads_funnel_step', COUNT(*) FROM ecom_ads.ads_funnel_step
UNION ALL SELECT 'ads_late_expired_count', COUNT(*) FROM ecom_ads.ads_late_expired_count;"

echo
echo "=== [2] 建库 ecom_collision（不存在则建） ==="
dq -e "CREATE DATABASE IF NOT EXISTS ecom_collision;"
dq -e "SHOW DATABASES;"

echo
echo "=== [3] 建 compaction_probe（DUPLICATE KEY，BUCKETS 8，副本 1——形状同 06b reclaim_probe） ==="
dq -e "CREATE TABLE IF NOT EXISTS ecom_collision.compaction_probe (
  id BIGINT NOT NULL,
  payload VARCHAR(2048)
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 8
PROPERTIES ('replication_allocation' = 'tag.location.default: 1');"
dq -e "SHOW CREATE TABLE ecom_collision.compaction_probe\G"

echo
echo "=== [4] 灌数前 du 基线 ==="
docker exec eco-doris-be sh -c "du -sk /opt/apache-doris/be/storage /opt/apache-doris/be/storage/data"
echo "=== CREATE DONE ==="
date -u '+end: %Y-%m-%d %H:%M:%S UTC'
