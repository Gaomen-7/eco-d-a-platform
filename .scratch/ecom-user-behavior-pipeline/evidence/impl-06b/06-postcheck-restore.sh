#!/usr/bin/env bash
# impl-06b · 第 6 步：FE/BE 日志取证 + 配置恢复 + 收尾对账
# 产物：stdout 全量进 evidence/impl-06b/06-postcheck-restore.out.txt
# 参数：$1=T0（UTC 可读） $2=T_END（UTC 可读，缺省=now）——经环境变量传入 awk（ENVIRON），不拼进命令串
set -euo pipefail

T0="${1:?need T0}"
T_END="${2:-$(date -u '+%Y-%m-%d %H:%M:%S')}"
export T0 T_END
dq() { docker compose run --rm --no-TTY client mysql -h doris-fe -P 9030 -u root --batch "$@"; }

echo "=== [1] FE 日志：观测窗内 recycle bin 相关逐字（T0=$T0 → T_END=$T_END） ==="
docker exec -e T0 -e T_END eco-doris-fe sh -c \
  "awk '\$1\" \"\$2 >= ENVIRON[\"T0\"] && \$1\" \"\$2 <= ENVIRON[\"T_END\"]' /opt/apache-doris/fe/log/fe.log | grep -iE 'recycle|expire' | grep -v check_create_table | tail -80"
echo "(空＝窗口内 FE 无 recycle 相关日志行——本身即事实)"
echo

echo "=== [2] BE 日志：erase/trash/garbage sweep 逐字（be.INFO 行首格式与 fe.log 不同，取尾部后按时段目检） ==="
docker exec eco-doris-be sh -c "grep -iE 'erase tablet|trash|garbage sweep' /opt/apache-doris/be/log/be.INFO 2>/dev/null | tail -100"
echo "(空＝BE 无 erase/trash/garbage 日志行——本身即事实)"
echo

echo "=== [3] 配置恢复：catalog_trash_expire_second 300 → 86400 ==="
dq -e "ADMIN SET FRONTEND CONFIG (\"catalog_trash_expire_second\" = \"86400\");"
dq -e "SHOW FRONTEND CONFIG LIKE 'catalog_trash_expire_second';"
echo

echo "=== [4] 收尾：演练库整体退场（非 force，回 bin；默认参数下 24 h 由 FE 清） ==="
dq -e "DROP DATABASE ecom_rehearsal;"
dq -e "SHOW DATABASES;"
echo

echo "=== [5] 三张已验证 ADS 表终态对账（全程零接触的互证） ==="
dq -e "SELECT 'ads_window_metric' AS tbl, COUNT(*) AS rows_after FROM ecom_ads.ads_window_metric
UNION ALL SELECT 'ads_funnel_step', COUNT(*) FROM ecom_ads.ads_funnel_step
UNION ALL SELECT 'ads_late_expired_count', COUNT(*) FROM ecom_ads.ads_late_expired_count;"
echo

echo "=== [6] FE/BE Alive + BE 终态容量 ==="
dq -e "SHOW FRONTENDS;" | cut -c1-160
dq -e "SHOW BACKENDS;" | cut -c1-200
docker exec eco-doris-be sh -c "du -sk /opt/apache-doris/be/storage"
docker exec eco-doris-be sh -c "curl -s http://localhost:8040/metrics | grep -E 'doris_be_disks_(avail_capacity|trash_used_capacity)\{path'"
echo
date -u '+UTC now: %Y-%m-%d %H:%M:%S'
