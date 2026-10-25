#!/usr/bin/env bash
# impl-06a 演练 · 第 5 步：收尾——三张已验证 ADS 表行数对账（验收③后半）+ 演练库清理
# 产物：stdout 全量进 evidence/impl-06a/05-postcheck-cleanup.out.txt
set -euo pipefail

dq() { docker compose run --rm --no-TTY client mysql -h doris-fe -P 9030 -u root --batch "$@"; }

echo "=== [1] 三张已验证 ADS 表 COUNT(*)（与 01 步基线对账；预期逐表相等） ==="
echo "基线（01 步）：ads_funnel_step=2427, ads_window_metric=30132, ads_late_expired_count=9146"
dq -e "SELECT 'ads_window_metric' AS tbl, COUNT(*) AS rows_after FROM ecom_ads.ads_window_metric
UNION ALL SELECT 'ads_funnel_step', COUNT(*) FROM ecom_ads.ads_funnel_step
UNION ALL SELECT 'ads_late_expired_count', COUNT(*) FROM ecom_ads.ads_late_expired_count;"
echo

echo "=== [2] 演练库只删自己：DROP DATABASE ecom_rehearsal（不触碰 ecom_ads） ==="
dq -e "DROP DATABASE IF EXISTS ecom_rehearsal;"
echo "-- DROP DATABASE OK（分区/表进 catalog recycle bin，非 force；BE 侧磁盘异步回收，官方未给出时限）"
echo

echo "=== [3] 清理后状态：ecom_rehearsal 已不在库列表；三张 ADS 表俱在 ==="
dq -e "SHOW DATABASES;"
dq -e "SHOW TABLES FROM ecom_ads;"
echo

echo "=== [4] 收尾时刻（UTC + 本地） ==="
date -u '+UTC: %Y-%m-%d %H:%M:%S'
date '+local: %Y-%m-%d %H:%M:%S (%Z)'
