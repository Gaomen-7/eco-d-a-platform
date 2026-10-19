#!/usr/bin/env bash
# 在 compose 网络内执行 Doris 初始化（建库 + 建表 + 打开动态分区总开关）。
#
# ★ 为什么不能在宿主直接跑（HLD §4.4，P0 实测）：
#   Docker Desktop 下**容器地址宿主不可路由**，而 Doris 的 Stream Load / 部分客户端
#   交互会从 FE **重定向到某个 BE**（第二跳是容器地址），宿主侧 follow 不到。
#   官方原话：a stream load from the host cannot follow FE's redirect to a BE;
#            run it from the `client` service instead, which is inside the network.
#   ⇒ 所有 Doris 客户端操作一律经本脚本（在 client 容器内）发起。
#
# 用法（宿主侧执行，但实际命令在容器内跑）：
#   docker compose run --rm client /scripts/doris-init.sh
#
# 依赖：client 服务镜像含 mysql 客户端；init.sql 通过 ./docker 挂载进 /scripts。

set -euo pipefail

FE_HOST="${FE_HOST:-doris-fe}"
FE_PORT="${FE_PORT:-9030}"
FE_USER="${FE_USER:-root}"
SQL_FILE="${SQL_FILE:-/scripts/doris/init.sql}"

echo "[doris-init] FE=${FE_HOST}:${FE_PORT} user=${FE_USER}"
echo "[doris-init] SQL=${SQL_FILE}"

# 等 FE 的 MySQL 协议端口就绪（容器启动有先后）
for i in $(seq 1 60); do
  if mysql -h "$FE_HOST" -P "$FE_PORT" -u "$FE_USER" -e "SELECT 1" >/dev/null 2>&1; then
    echo "[doris-init] FE MySQL 协议就绪（第 ${i} 次探测）"
    break
  fi
  echo "[doris-init] 等待 FE ... ($i/60)"
  sleep 3
done

echo "[doris-init] 执行 init.sql"
mysql -h "$FE_HOST" -P "$FE_PORT" -u "$FE_USER" < "$SQL_FILE"

echo "[doris-init] 自检：动态分区总开关"
mysql -h "$FE_HOST" -P "$FE_PORT" -u "$FE_USER" \
  -e "SHOW FRONTEND CONFIG LIKE 'dynamic_partition_enable';" 2>&1 || true

echo "[doris-init] 完成"
