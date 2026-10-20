#!/usr/bin/env bash
# Doris 容器入口 —— 按 compose 传入的第一个参数决定起 FE 还是 BE。
#
# 用法（compose command）：
#   command: ["fe"]   -> 起 Frontend
#   command: ["be"]   -> 注册 Backend 后起 Backend
#
# 依据：官方 `start_fe.sh` 要求 JDK **恰为 17**（非 17 打印
#   `ERROR: The jdk_version is N, must be 17.` 后 exit 1），镜像里已装 17。
#
# ★★ Bug D/F 修复（P0 实测，2026-10-20）——**BE 必须先注册才能起** ★★
#   根因（两处，均为上一会话遗漏）：
#   ① 本仓 compose 给 doris-be 设的 `FE_SERVERS` / `BE_ADDR` 是**死配置**：
#      在真 tar 包里对 `fe/bin` 与 `be/bin` **整目录 grep，零命中**——
#      `start_be.sh` 既不读它们、也没有 `--helper` 参数（`getopt` 只认
#      daemon/console/version/benchmark）。真正消费 FE 地址的是**官方自己的
#      entrypoint**（`docker/runtime/all-in-one/4.1/resource/entrypoint.sh` 的
#      `run_be()`）：先 `resolve_master`、再 `register_be`、**最后** `start_be`。
#   ② 没有任何一方发 `ALTER SYSTEM ADD BACKEND`。官方部署文档 Step 3.3 明确：
#      "Before starting a BE node, you need to register it in the FE cluster"。
#
#   ⚠️ **顺序是承重的（load-bearing）**：BE 的 conf 里**没有** FE 地址，BE 靠
#      「FE 在注册后主动来连它」的握手才知道该向谁心跳 ⇒ 必须**先注册、后起进程**。
#      反过来（先起 BE、后补注册）则与官方顺序不符，本会话未验证其可行性。
#
#   ⚠️ **注册用容器 IP，不用主机名**：`fe.conf` 出厂**没有** `enable_fqdn_mode`
#      行 ⇒ 走默认（IP 语义）：官方 FE entrypoint 也是
#      `START_TYPE=$(parse enable_fqdn_mode); MYSELF=$POD_IP`（非 fqdn 时用 IP）。
#      官方 all-in-one 的 `register_be()` 同样用 `my_ip`（`hostname -i`）。
#      ⇒ 上一会话 Bug D 草稿里写 `ADD BACKEND "doris-be:9050"`（主机名）
#        与 IP 语义不符，属**同类错误**（与已证伪的 Bug B 旧修法一样）。
#
#   ⚠️ **判据按列名取值，不按列位置**（官方 `lib.sh:node_alive` 的做法）：
#      `show backends` 的列顺序不保证，必须用表头查 `Host` / `HeartbeatPort` / `Alive`。
#      ⇒ 也**不能用 `SELECT 1` 探活**：该语句走 Nereids，会挑一个 backend 做 scan node，
#        无 backend 时报 "No backend available" —— 恰恰是我们要等的那个条件
#        （官方原话：`select 1` goes through Nereids, which picks a backend as its
#         scan node and fails with "No backend available" until one is registered）。
#        故探活一律走 `SHOW FRONTENDS` / `SHOW BACKENDS` 这类**元数据查询**。
#
#   回滚：`git checkout -- docker/doris/entrypoint.sh`。
set -euo pipefail

ROLE="${1:-}"
DORIS_HOME="${DORIS_HOME:-/opt/apache-doris}"
FE_HOST="${FE_HOST:-doris-fe}"
FE_PORT="${FE_PORT:-9030}"
FE_USER="${FE_USER:-root}"
BE_HEARTBEAT_PORT="${BE_HEARTBEAT_PORT:-9050}"
START_TIMEOUT="${START_TIMEOUT:-300}"

log() { printf '[doris-entrypoint] %s\n' "$*"; }

# 本容器对外被 FE 认知的地址（IP 语义，与官方 my_ip 同构）。
my_ip() { hostname -i | awk '{print $1}'; }

# 元数据查询：root、无密码、batch、无表头。
# ⚠️ `-N`（no header）**只用于** ALTER/DROP 这类不需要读列的语句。
#    凡要**按列名取值**的（node_alive）**绝不能带 `-N`** —— 表头被抑制后
#    `col["Alive"]` 根本不存在，awk 永不输出 ⇒ 探针恒假（本会话实测踩到的坑）。
#    官方 `lib.sh` 的 `node_alive`/`alive_count` 也是 `--batch` **不带** `-N`。
sql() { mysql -h "$FE_HOST" -P "$FE_PORT" -u "$FE_USER" -N --batch \
          --connect-timeout=2 -e "$1" 2>/dev/null; }

# 带表头的批量查询（供按列名取值用）
sql_hdr() { mysql -h "$FE_HOST" -P "$FE_PORT" -u "$FE_USER" --batch \
              --connect-timeout=2 -e "$1" 2>/dev/null; }

# node_alive <frontends|backends> <ip> <port>：按**列名**取 Alive，输出 true/false。
node_alive() {
  local what=$1 ip=$2 port=$3 portcol
  case "$what" in
    frontends) portcol=EditLogPort ;;
    backends)  portcol=HeartbeatPort ;;
  esac
  sql_hdr "show $what" | awk -F'\t' -v ip="$ip" -v port="$port" -v pc="$portcol" '
      NR == 1 { for (i = 1; i <= NF; i++) col[$i] = i; next }
      $col["Host"] == ip && $col[pc] == port { print $col["Alive"]; exit }'
}

# 等 FE 的 MySQL 协议就绪（容器启动有先后；compose 的 depends_on 只保证 8030 healthy）。
wait_fe() {
  local deadline=$((SECONDS + START_TIMEOUT))
  while (( SECONDS < deadline )); do
    [[ "$(sql 'show frontends' | head -1)" != "" ]] && return 0
    sleep 2
  done
  log "等待 FE ${FE_HOST}:${FE_PORT} 超时（${START_TIMEOUT}s）"
  return 1
}

# 注册 BE（幂等：已注册则跳过，避免 "Same backend already exists"）
register_be() {
  local ip; ip="$(my_ip)"
  [[ -n "$ip" ]] || { log "取不到本容器 IP（hostname -i 为空）"; return 1; }
  if [[ "$(node_alive backends "$ip" "$BE_HEARTBEAT_PORT")" == "true" ]]; then
    log "backend ${ip}:${BE_HEARTBEAT_PORT} 已注册，跳过"
    return 0
  fi
  log "注册 backend ${ip}:${BE_HEARTBEAT_PORT} 到 FE ${FE_HOST}:${FE_PORT}"
  # 允许重复执行：若此前已 ADD 过但此刻 Alive 还不是 true，ALTER 会报
  # "Same backend already exists" —— 吞掉即可，真正的判据是后面的 Alive。
  sql "ALTER SYSTEM ADD BACKEND \"${ip}:${BE_HEARTBEAT_PORT}\";" || true
}

# 等 BE 被 FE 认到 Alive=true（否则后续 CREATE TABLE 必失败）
wait_be_alive() {
  local ip; ip="$(my_ip)"
  local deadline=$((SECONDS + START_TIMEOUT))
  while (( SECONDS < deadline )); do
    [[ "$(node_alive backends "$ip" "$BE_HEARTBEAT_PORT")" == "true" ]] && return 0
    sleep 2
  done
  log "等待 backend ${ip}:${BE_HEARTBEAT_PORT} 变 Alive=true 超时（${START_TIMEOUT}s）"
  return 1
}

case "$ROLE" in
  fe)
    # 首次启动需要 --daemon；容器内改用前台模式便于日志与健康检查
    log "starting FE (DORIS_HOME=$DORIS_HOME)"
    exec "$DORIS_HOME/fe/bin/start_fe.sh" --console
    ;;
  be)
    log "starting BE (DORIS_HOME=$DORIS_HOME)"
    # ★ 承重顺序：先注册，再起 BE（见文件头 ①②）
    wait_fe || exit 1
    register_be || exit 1
    log "BE 注册完成，启动进程"
    exec "$DORIS_HOME/be/bin/start_be.sh" --console
    ;;
  *)
    echo "[doris-entrypoint] 用法：doris-entrypoint {fe|be}" >&2
    exit 2
    ;;
esac
