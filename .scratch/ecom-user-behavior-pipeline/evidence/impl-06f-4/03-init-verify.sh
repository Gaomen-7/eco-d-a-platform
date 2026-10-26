#!/usr/bin/env bash
# impl-06f-4 第三环：make init → make verify 一绿（票面执行序列第 3 步）。
# 逐行带 UTC 时间戳（阶段耗时入票）。verify 全程 ~8 min（五层探针 ~2min + 对账 ~4.5min）。
# bash .scratch/ecom-user-behavior-pipeline/evidence/impl-06f-4/03-init-verify.sh
set -uo pipefail
cd "/mnt/f/Anaconda practice project/eco-a-d-platform" || exit 1

stamp() { date -u +"%H:%M:%S"; }

echo "=== make init 开始 $(stamp) UTC ==="
t0=$(date +%s)
make init 2>&1 | while IFS= read -r l; do printf "[%s] %s\n" "$(stamp)" "$l"; done
init_rc=${PIPESTATUS[0]}
t1=$(date +%s)
echo "=== make init 结束 $(stamp) UTC，EXIT=$init_rc，耗时 $((t1 - t0))s ==="

if [ "$init_rc" -ne 0 ]; then
  echo "FATAL: make init 退出码 $init_rc —— 不再跑 verify"
  exit "$init_rc"
fi

echo
echo "=== make verify 开始 $(stamp) UTC ==="
t2=$(date +%s)
make verify 2>&1 | while IFS= read -r l; do printf "[%s] %s\n" "$(stamp)" "$l"; done
verify_rc=${PIPESTATUS[0]}
t3=$(date +%s)
echo "=== make verify 结束 $(stamp) UTC，EXIT=$verify_rc，耗时 $((t3 - t2))s ==="

echo
echo "=== 链路总耗时（init+verify）：$((t3 - t0))s ==="
echo "INIT_EXIT=$init_rc VERIFY_EXIT=$verify_rc"
exit $verify_rc
