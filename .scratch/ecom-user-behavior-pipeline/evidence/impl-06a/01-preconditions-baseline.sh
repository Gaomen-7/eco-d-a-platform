#!/usr/bin/env bash
# impl-06a 演练 · 第 1 步：实施前置 + 基线对账（验收③的前半）
# 产物：stdout 全量进 evidence/impl-06a/01-preconditions-baseline.out.txt
# 执行面：mysql 客户端在 client 容器内（HLD §4.4 两跳约束），一律 docker compose run --rm client。
set -euo pipefail

dq() { docker compose run --rm --no-TTY client mysql -h doris-fe -P 9030 -u root --batch "$@"; }

echo "=== [1] 墙钟与磁盘余量（实施前置） ==="
date -u '+UTC now: %Y-%m-%d %H:%M:%S'
date '+local now: %Y-%m-%d %H:%M:%S (%Z)'
df -h / | sed -n '1,2p'   # Doris named volume 所在的 WSL ext4
echo

echo "=== [2] 六服务 healthy（实施前置） ==="
docker compose ps --format '{{.Name}}  {{.Status}}'
echo

echo "=== [3] SHOW FRONTENDS / SHOW BACKENDS 全 Alive（实施前置） ==="
dq -e "SHOW FRONTENDS;"
echo
dq -e "SHOW BACKENDS;"
echo

echo "=== [4] 三张已验证 ADS 表基线 COUNT(*)（验收③：演练前对账口径） ==="
dq -e "SELECT 'ads_window_metric' AS tbl, COUNT(*) AS rows_before FROM ecom_ads.ads_window_metric
UNION ALL SELECT 'ads_funnel_step', COUNT(*) FROM ecom_ads.ads_funnel_step
UNION ALL SELECT 'ads_late_expired_count', COUNT(*) FROM ecom_ads.ads_late_expired_count;"
echo

echo "=== [5] FE 时区与当前时刻（供分区命名推算） ==="
dq -e "SELECT NOW() AS fe_now, @@session.time_zone AS tz, @@system_time_zone AS sys_tz;"
echo

echo "=== [6] 调度间隔配置（官方给出：600 s）当前生效值 ==="
dq -e "SHOW FRONTEND CONFIG LIKE 'dynamic_partition_check_interval_seconds';"
dq -e "SHOW FRONTEND CONFIG LIKE 'dynamic_partition_enable';"
echo

echo "=== [7] 已存在的库（确认 ecom_rehearsal 尚不存在） ==="
dq -e "SHOW DATABASES;"
