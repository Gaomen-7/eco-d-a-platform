#!/usr/bin/env bash
# impl-06c · 第 1 步：前置检查 + 全套基线落盘
set -uo pipefail
ROOT="/mnt/f/Anaconda practice project/eco-a-d-platform"
cd "$ROOT"

dq() { docker compose run --rm --no-TTY client mysql -h doris-fe -P 9030 -u root --batch "$@" 2>/dev/null | grep -v '^Container\|^$' ; }

echo "=== [1] 六容器 health ==="
docker ps --format '{{.Names}}\t{{.Status}}' | sort

echo
echo "=== [2] 磁盘余量（WSL ext4，前置条件 ≥ 对撞量×2=40GB） ==="
df -h / | tail -1
docker exec eco-doris-be sh -c 'df -h /opt/apache-doris/be/storage | tail -1'

echo
echo "=== [3] BE du 基线 ==="
docker exec eco-doris-be sh -c "du -sk /opt/apache-doris/be/storage"
docker exec eco-doris-be sh -c "du -sk /opt/apache-doris/be/storage/* 2>/dev/null | sort -rn | head -5"

echo
echo "=== [4] FE 关键配置基线 ==="
dq -e "SHOW FRONTEND CONFIG LIKE 'catalog_trash_expire_second';"
dq -e "SHOW FRONTEND CONFIG LIKE '%compaction%';" | head -30

echo
echo "=== [5] 库名占用确认（ecom_collision 应不存在） ==="
dq -e "SHOW DATABASES;"

echo
echo "=== [6] 三张已验证 ADS 表基线行数 ==="
dq -e "SELECT 'ads_window_metric' t, COUNT(*) c FROM ecom_ads.ads_window_metric UNION ALL SELECT 'ads_funnel_step', COUNT(*) FROM ecom_ads.ads_funnel_step UNION ALL SELECT 'ads_funnel_step_v2', COUNT(*) FROM ecom_ads.ads_funnel_step_v2;"

echo
echo "=== [7] docker stats 基线（空载） ==="
docker stats --no-stream --format '{{.Name}}\t{{.MemUsage}}\t{{.MemPerc}}' | sort

echo
echo "=== [8] BE metrics 基线（memory/compaction/memtable/flush 相关） ==="
docker exec eco-doris-be sh -c "curl -s localhost:8040/metrics | grep -Ei 'mem|compaction|flush|segment' | grep -v '^#' | grep -v ' 0$'" | head -60

echo
echo "=== [9] BE compaction 状态端点探测（存在性确认） ==="
echo "--- /api/compaction/run_status:"
docker exec eco-doris-be sh -c "curl -s -m 5 'localhost:8040/api/compaction/run_status'"
echo
echo "--- /api/show_data (tablet 数):"
docker exec eco-doris-be sh -c "curl -s -m 5 'localhost:8040/api/show_data' | head -c 500"
echo

echo
echo "=== [10] SHOW PROC '/statistic'（compaction score 口径探测） ==="
dq -e "SHOW PROC '/statistic';" | head -10

echo
echo "=== [11] BE 日志基线（最近 compaction 相关行，应为空或历史行） ==="
docker exec eco-doris-be sh -c "grep -E 'compaction' /opt/apache-doris/be/log/be.INFO | tail -5"
echo "=== BASELINE DONE ==="
date -u '+end: %Y-%m-%d %H:%M:%S UTC'
