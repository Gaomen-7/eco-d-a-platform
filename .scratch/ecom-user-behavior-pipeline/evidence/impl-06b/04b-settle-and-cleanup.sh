#!/usr/bin/env bash
# impl-06b · 第 4b 步：04 中止后的收尾采样——du 稳定性 × 3、SHOW DATA（正确形态）、临时文件清理
# 背景：04 号脚本在 [5] 因 `SHOW DATA FROM ecom_rehearsal`（Doris 解析为表名 → No database selected）
#       被 set -e 掐断；灌数本身 Success（见 04 号凭证 [4][5]）。本脚本补齐 [6][7] 缺的采样。
set -uo pipefail

dq() { docker compose run --rm --no-TTY client mysql -h doris-fe -P 9030 -u root -D ecom_rehearsal --batch "$@"; }

echo "=== [1] 行数复核 + SHOW DATA（-D 进库后用正确形态） ==="
date -u '+UTC now: %Y-%m-%d %H:%M:%S'
dq -e "SELECT COUNT(*) AS probe_rows FROM reclaim_probe;"
dq -e "SHOW DATA FROM reclaim_probe;"
echo

echo "=== [2] du 稳定性采样（30 s × 3）——DROP 前 PRE_DROP 基线取末次 ==="
for i in 1 2 3; do
  printf 'sample %d  %s  ' "$i" "$(date -u '+%H:%M:%S')"
  docker exec eco-doris-be sh -c "du -sk /opt/apache-doris/be/storage 2>/dev/null | cut -f1"
  [ "$i" -lt 3 ] && sleep 30
done
echo

echo "=== [3] 临时文件清理（04 [7] 未及执行） ==="
docker exec eco-doris-be sh -c "ls -l /tmp/reclaim_probe.csv 2>/dev/null && rm -f /tmp/reclaim_probe.csv && echo removed-in-be || echo already-absent"
rm -f /tmp/reclaim_probe.csv && echo "removed-host-copy"
echo

echo "=== [4] BE metrics 快照（DROP 前） ==="
docker exec eco-doris-be sh -c "curl -s http://localhost:8040/metrics | grep -E 'doris_be_disks_(avail_capacity|trash_used_capacity)\{path'"
