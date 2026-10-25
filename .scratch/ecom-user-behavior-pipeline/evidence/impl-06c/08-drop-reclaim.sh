#!/usr/bin/env bash
# impl-06c · 第 8 步：清理——非 force DROP + 回盘时间线（联动 impl-06b 口径）+ 配置恢复
# 实验参数：catalog_trash_expire_second 86400→300（观测窗内），结束后恢复并复核
set -uo pipefail
ROOT="/mnt/f/Anaconda practice project/eco-a-d-platform"
cd "$ROOT"

dq() { docker compose run --rm --no-TTY client mysql -h doris-fe -P 9030 -u root --batch "$@" 2>/dev/null; }

echo "=== [0] 三张已验证 ADS 表对账（DROP 前） ==="
dq -e "SELECT 'ads_window_metric' AS t, COUNT(*) AS c FROM ecom_ads.ads_window_metric
UNION ALL SELECT 'ads_funnel_step', COUNT(*) FROM ecom_ads.ads_funnel_step
UNION ALL SELECT 'ads_late_expired_count', COUNT(*) FROM ecom_ads.ads_late_expired_count;"

echo
echo "=== [1] 实验参数切换：catalog_trash_expire_second 86400→300 ==="
dq -e "ADMIN SET FRONTEND CONFIG (\"catalog_trash_expire_second\" = \"300\");"
dq -e "SHOW FRONTEND CONFIG LIKE 'catalog_trash_expire_second';"

echo
echo "=== [2] DROP 前 du 基线 ==="
docker exec eco-doris-be sh -c "du -sk /opt/apache-doris/be/storage /opt/apache-doris/be/storage/data"

echo
echo "=== [3] DROP TABLE（非 force） ==="
date -u '+T0_DROP: %Y-%m-%d %H:%M:%S UTC'
dq -e "DROP TABLE ecom_collision.compaction_probe;"
date -u '+T0_DROP_RETURNED: %Y-%m-%d %H:%M:%S UTC'

echo
echo "=== [4] 回收站可见性（立即查） ==="
dq -e "SHOW RECYCLE BIN;" || true

echo
echo "=== [5] 回盘观测循环：30s tick，直到 du(data) 回落至 ≈灌数前基线（±2%）或 25 min 上限 ==="
# 灌数前基线（01 号凭证）: storage=95540 KB / data=9976 KB（对撞前）
BASELINE_DATA=9976
for i in $(seq 1 50); do
  TS=$(date -u '+%H:%M:%S')
  DU=$(docker exec eco-doris-be sh -c "du -sk /opt/apache-doris/be/storage 2>/dev/null" 2>/dev/null | cut -f1)
  DUD=$(docker exec eco-doris-be sh -c "du -sk /opt/apache-doris/be/storage/data 2>/dev/null" 2>/dev/null | cut -f1)
  BIN=$(dq -e "SHOW RECYCLE BIN;" 2>/dev/null | grep -c compaction_probe || true)
  FE_ERASE=$(docker exec eco-doris-fe sh -c "grep -a 'eraseTable' /opt/apache-doris/fe/log/fe.log 2>/dev/null | tail -1" 2>/dev/null)
  BE_ERASE=$(docker exec eco-doris-be sh -c "grep -ac 'delete tablet dir' /opt/apache-doris/be/log/be.INFO 2>/dev/null" 2>/dev/null || echo NA)
  echo "$TS	storage=${DU}KB	data=${DUD}KB	bin_entries=$BIN	be_delete_dir_total=$BE_ERASE"
  echo "    fe_last_erase: $FE_ERASE"
  # 判定：data 回落到基线 ±2%
  if [ -n "$DUD" ] && [ "$DUD" -gt 0 ] 2>/dev/null; then
    DIFF=$(( DUD - BASELINE_DATA )); ADIFF=${DIFF#-}
    [ "$ADIFF" -le $(( BASELINE_DATA * 2 / 100 )) ] && [ "$DUD" -le $(( BASELINE_DATA * 102 / 100 )) ] && { date -u '+T_RECLAIMED: %Y-%m-%d %H:%M:%S UTC'; echo "reclaimed: data=${DUD}KB ≈ baseline ${BASELINE_DATA}KB"; break; }
  fi
  sleep 30
done

echo
echo "=== [6] FE 日志逐字：dropTable/eraseTable 相关行 ==="
docker exec eco-doris-fe sh -c "grep -a -E 'dropTable|eraseTable|recycleTable' /opt/apache-doris/fe/log/fe.log 2>/dev/null | tail -8"

echo
echo "=== [7] BE 日志逐字：本表 tablet 的 erase 行（tablet 清单本地 grep -F 过滤） ==="
docker exec eco-doris-be sh -c "grep -a -E 'erase|trash|delete tablet dir' /opt/apache-doris/be/log/be.INFO 2>/dev/null | tail -80" > /tmp/impl06c-be-erase-all.log || true
grep -F -f /tmp/impl06c-tablets.txt /tmp/impl06c-be-erase-all.log | tail -12 || echo "(no match)"
echo "--- 全量 erase/trash 行副本: 08-be-erase-all.out.txt"
cp /tmp/impl06c-be-erase-all.log ./08-be-erase-all.out.txt

echo
echo "=== [8] 恢复配置：catalog_trash_expire_second 300→86400 + 复核 ==="
dq -e "ADMIN SET FRONTEND CONFIG (\"catalog_trash_expire_second\" = \"86400\");"
dq -e "SHOW FRONTEND CONFIG LIKE 'catalog_trash_expire_second';"

echo
echo "=== [9] 收尾：DROP DATABASE ecom_collision（非 force，同 06b bin 滞留终态） ==="
dq -e "DROP DATABASE ecom_collision;"
dq -e "SHOW DATABASES;"
dq -e "SHOW RECYCLE BIN;" | tail -5

echo
echo "=== [10] 三张已验证 ADS 表对账（DROP 后） ==="
dq -e "SELECT 'ads_window_metric' AS t, COUNT(*) AS c FROM ecom_ads.ads_window_metric
UNION ALL SELECT 'ads_funnel_step', COUNT(*) FROM ecom_ads.ads_funnel_step
UNION ALL SELECT 'ads_late_expired_count', COUNT(*) FROM ecom_ads.ads_late_expired_count;"
echo "=== DROP-RECLAIM DONE ==="
date -u '+end: %Y-%m-%d %H:%M:%S UTC'
