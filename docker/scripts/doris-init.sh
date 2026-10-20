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
# 依赖：client 服务镜像含 mysql 客户端；init.sql 由 compose 的
#       `./docker/doris:/doris:ro` 挂进容器（**不是** /scripts/doris —— 见下方 Bug C 说明）。

set -euo pipefail

FE_HOST="${FE_HOST:-doris-fe}"
FE_PORT="${FE_PORT:-9030}"
FE_USER="${FE_USER:-root}"
# ★ Bug C 对账（P0 实测，2026-10-20）：默认路径必须与**实际挂载点**一致。
#   历史三处不一致：本变量写 /scripts/doris/init.sql、compose 只挂 ./docker/scripts、
#   而文件真实位置是 docker/doris/init.sql ⇒ 容器内 /scripts/doris 不存在 ⇒ 初始化报错。
#   ⚠️ 修法落点：compose 新增 `./docker/doris:/doris:ro`，**不能**挂 /scripts/doris ——
#      `/scripts` 是只读绑定，runc 无法在其内部建挂载点（实测 runc：
#      `mkdirat ... read-only file system`，容器直接起不来）。故本变量同步为 /doris/init.sql。
SQL_FILE="${SQL_FILE:-/doris/init.sql}"
BE_HOST="${BE_HOST:-doris-be}"
BE_HEARTBEAT_PORT="${BE_HEARTBEAT_PORT:-9050}"
BE_IP=""

echo "[doris-init] FE=${FE_HOST}:${FE_PORT} user=${FE_USER}"
echo "[doris-init] SQL=${SQL_FILE}"

# ★ 探活必须用**元数据查询**，不能用 `SELECT 1`（官方 lib.sh 逐字说明）：
#   `select 1` 走 Nereids，会挑一个 backend 做 scan node，**在 BE 注册之前必然报
#   "No backend available"** —— 那正是我们要等的条件本身，用它探活会自锁。
#   故改用 `SHOW FRONTENDS`（FE 自己就能答，不需要 BE）。
FE_READY=0
for i in $(seq 1 60); do
  if mysql -h "$FE_HOST" -P "$FE_PORT" -u "$FE_USER" -N --batch \
       -e "SHOW FRONTENDS;" >/dev/null 2>&1; then
    echo "[doris-init] FE MySQL 协议就绪（第 ${i} 次探测）"
    FE_READY=1
    break
  fi
  echo "[doris-init] 等待 FE ... ($i/60)"
  sleep 3
done
if [[ "$FE_READY" -ne 1 ]]; then
  echo "[doris-init] ✗ FE MySQL 协议在 180 s 内未就绪，中止" >&2
  exit 1
fi

# ★ Bug D 兜底：BE 注册的**主路径在 be 容器自己的 entrypoint**（起 BE 进程之前，
#   用容器 IP 发 ALTER SYSTEM ADD BACKEND —— 与官方 all-in-one 的 run_be() 同构，
#   见 docker/doris/entrypoint.sh 文件头）。此处仅**幂等兜底**。
#
# ⚠️⚠️ 两个实测踩到的坑（2026-10-20，本会话）：⚠️⚠️
#   ① **绝不能带 `-N`**：`-N` 抑制表头，`col["Alive"]` 就不存在，awk 永不输出
#      ⇒ 探针**恒假**，会把「BE 明明 Alive=true」误判成「未注册」并触发兜底。
#      故此处用 `--batch`（**不带** -N），与官方 lib.sh 的 node_alive 一致。
#   ② **兜底绝不能用主机名**：BE 已用容器 IP 注册过之后，再用 `doris-be:9050`
#      去 ADD，Doris 会把它当成**另一个身份**（Host 字段不同）⇒ 注册出一个
#      **幽灵 backend**（实测 `SHOW BACKENDS` 里同时出现 `172.22.0.7` 与
#      `doris-be` 两行，TabletNum=0、永不承载数据）。
#      ⇒ 兜底也必须用**容器 IP**：从已注册行里学，或由 `${BE_HOST}` 解析。
#        这里改用 `getent hosts` 解析真实地址（无 getent 则回退主机名）。
be_alive() {
  mysql -h "$FE_HOST" -P "$FE_PORT" -u "$FE_USER" --batch -e "SHOW BACKENDS;" 2>/dev/null \
    | awk -F'\t' 'NR==1 { for (i=1;i<=NF;i++) col[$i]=i; next } $col["Alive"]=="true" { n++ } END { exit !(n>0) }'
}

# 取 BE 的真实 IP（IP 语义：fe.conf 无 enable_fqdn_mode 行 ⇒ 默认 false）。
be_ip() {
  local resolved
  resolved="$(getent hosts "$BE_HOST" 2>/dev/null | awk '{print $1; exit}')"
  echo "${resolved:-$BE_HOST}"
}

if be_alive; then
  echo "[doris-init] 已有 Alive=true 的 BE，跳过兜底注册"
else
  BE_IP="$(be_ip)"
  echo "[doris-init] 未发现 Alive 的 BE ⇒ 兜底注册 ${BE_IP}:${BE_HEARTBEAT_PORT}（IP，非主机名）"
  # 幂等：已 ADD 过但尚未 Alive 时 ALTER 会报 "Same backend already exists"，吞掉即可。
  mysql -h "$FE_HOST" -P "$FE_PORT" -u "$FE_USER" \
    -e "ALTER SYSTEM ADD BACKEND \"${BE_IP}:${BE_HEARTBEAT_PORT}\";" 2>&1 || true
  for i in $(seq 1 60); do
    if be_alive; then echo "[doris-init] BE 已 Alive（第 ${i} 次探测）"; break; fi
    echo "[doris-init] 等待 BE 变 Alive ... ($i/60)"
    sleep 3
  done
fi

if ! be_alive; then
  echo "[doris-init] ✗ 仍无 Alive 的 BE —— 建表会因无可用 backend 失败，中止" >&2
  mysql -h "$FE_HOST" -P "$FE_PORT" -u "$FE_USER" -e "SHOW BACKENDS;" 2>&1 || true
  exit 1
fi

echo "[doris-init] 执行 init.sql"
mysql -h "$FE_HOST" -P "$FE_PORT" -u "$FE_USER" < "$SQL_FILE"

echo "[doris-init] 自检：动态分区总开关"
mysql -h "$FE_HOST" -P "$FE_PORT" -u "$FE_USER" \
  -e "SHOW FRONTEND CONFIG LIKE 'dynamic_partition_enable';" 2>&1 || true

echo "[doris-init] 自检：BE 存活（判据＝Alive=true，不是「配置可见」）"
mysql -h "$FE_HOST" -P "$FE_PORT" -u "$FE_USER" -e "SHOW BACKENDS\G" 2>&1 \
  | grep -E 'Host:|HeartbeatPort:|Alive:' || true

echo "[doris-init] 完成"
