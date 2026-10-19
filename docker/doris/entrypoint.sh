#!/usr/bin/env bash
# Doris 容器入口 —— 按 compose 传入的第一个参数决定起 FE 还是 BE。
#
# 用法（compose command）：
#   command: ["fe"]   -> 起 Frontend
#   command: ["be"]   -> 起 Backend
#
# 依据：官方 `start_fe.sh` 要求 JDK **恰为 17**（非 17 打印
#   `ERROR: The jdk_version is N, must be 17.` 后 exit 1），镜像里已装 17。
# FE/BE 的注册靠环境变量（FE_SERVERS 等，见 compose）。
set -euo pipefail

ROLE="${1:-}"
DORIS_HOME="${DORIS_HOME:-/opt/apache-doris}"

case "$ROLE" in
  fe)
    # 首次启动需要 --daemon；容器内改用前台模式便于日志与健康检查
    echo "[doris-entrypoint] starting FE (DORIS_HOME=$DORIS_HOME)"
    exec "$DORIS_HOME/fe/bin/start_fe.sh" --console
    ;;
  be)
    echo "[doris-entrypoint] starting BE (DORIS_HOME=$DORIS_HOME)"
    exec "$DORIS_HOME/be/bin/start_be.sh" --console
    ;;
  *)
    echo "[doris-entrypoint] 用法：doris-entrypoint {fe|be}" >&2
    exit 2
    ;;
esac
