#!/usr/bin/env bash
# impl-06b · 第 1 步：实施前置 + 基线（DROP 前全量状态快照）
# 产物：stdout 全量进 evidence/impl-06b/01-preconditions-baseline.out.txt
# 执行面：mysql 客户端在 client 容器内（HLD §4.4 两跳约束），一律 docker compose run --rm client。
set -euo pipefail

dq() { docker compose run --rm --no-TTY client mysql -h doris-fe -P 9030 -u root --batch "$@"; }

echo "=== [1] 墙钟与磁盘余量（实施前置） ==="
date -u '+UTC now: %Y-%m-%d %H:%M:%S'
df -h / | sed -n '1,2p'
echo

echo "=== [2] 六服务 healthy（实施前置） ==="
docker compose ps --format '{{.Name}}  {{.Status}}'
echo

echo "=== [3] SHOW FRONTENDS / SHOW BACKENDS 全 Alive（实施前置） ==="
dq -e "SHOW FRONTENDS;" | cut -c1-200
echo
dq -e "SHOW BACKENDS;" | cut -c1-320
echo

echo "=== [4] 三张已验证 ADS 表基线 COUNT(*)（全程不触碰；前后对账口径） ==="
dq -e "SELECT 'ads_window_metric' AS tbl, COUNT(*) AS rows_before FROM ecom_ads.ads_window_metric
UNION ALL SELECT 'ads_funnel_step', COUNT(*) FROM ecom_ads.ads_funnel_step
UNION ALL SELECT 'ads_late_expired_count', COUNT(*) FROM ecom_ads.ads_late_expired_count;"
echo

echo "=== [5] catalog recycle bin 现状快照（DROP 前基线） ==="
dq -e "SHOW CATALOG RECYCLE BIN;"
echo

echo "=== [6] 回收/释放链路相关配置基线 ==="
echo "--- FE: catalog_trash_expire_second（官方文档默认 86400；运行时改动 FE 重启失效） ---"
dq -e "SHOW FRONTEND CONFIG LIKE 'catalog_trash_expire_second';"
echo "--- BE: trash_file_expire_time_sec / min_garbage_sweep_interval（BE HTTP /api/show_config，4.1.5 源码默认 0 / 180） ---"
curl -s "http://127.0.0.1:8040/api/show_config?conf_item=trash_file_expire_time_sec"; echo
curl -s "http://127.0.0.1:8040/api/show_config?conf_item=min_garbage_sweep_interval"; echo
echo

echo "=== [7] BE storage 目录 du 基线（容器内实测字节） ==="
docker exec eco-doris-be sh -c "du -sk /opt/apache-doris/be/storage; du -sk /opt/apache-doris/be/storage/trash 2>/dev/null || echo 'trash dir absent'"
echo

echo "=== [8] BE 磁盘 metrics 基线（/metrics，字节） ==="
curl -s "http://127.0.0.1:8040/metrics" | grep -E "doris_be_disks_(avail|total|trash_used|local_used)_capacity"
echo

echo "=== [9] 已存在的库（确认 ecom_rehearsal 不在——06a 收尾已 DROP 进 bin） ==="
dq -e "SHOW DATABASES;"
echo

echo "=== [10] FE 日志路径可用性（供 purge 时刻取证） ==="
docker exec eco-doris-fe sh -c "ls -la /opt/apache-doris/fe/log/fe.log 2>/dev/null | tail -1"
