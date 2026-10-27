#!/usr/bin/env bash
# 正常路径**逐字零变化**凭证 —— impl-07a 票面 AC「正常路径的输出与施工前逐字一致」。
#
# 做法：把 git HEAD 上的旧脚本原样取出到临时文件，与工作树的新脚本用**同一份基线**
# 跑同一套真实栈（同一 Kafka offset、同一 Doris），逐字 diff 两边的 stdout/stderr/退出码。
# 旧脚本本来就写标签「基线文件不可解析」到 stderr、正常路径不写 stderr ⇒ 两边都应为空。
set -uo pipefail
# 与 superset-provision.sh / deploy_surface_check.sh 同款：Git-Bash 侧防路径改写。
export MSYS_NO_PATHCONV=1

ROOT="$(cd "$(dirname "$0")/../../../.." && pwd)"
cd "$ROOT" || exit 1

SYNC=docker/scripts/superset-dirty-rate-sync.sh
TMP="$(mktemp -d)"
trap 'rm -rf "${TMP}"' EXIT

git show "HEAD:${SYNC}" > "$TMP/old-sync.sh" || { echo "取不到 HEAD 版本"; exit 1; }
echo "### 对照脚本来源：git HEAD:${SYNC}"
echo "### HEAD 版本与新版本只有错误分支/解析次数不同（见 git diff）"
echo

cp artifacts/verify/dirty-rate-baseline.json "$TMP/baseline.json"

BASELINE_FILE="$TMP/baseline.json" bash "$TMP/old-sync.sh" > "$TMP/old.out" 2> "$TMP/old.err"; old_rc=$?
BASELINE_FILE="$TMP/baseline.json" bash "$SYNC"         > "$TMP/new.out" 2> "$TMP/new.err"; new_rc=$?

echo "── 施工前（HEAD）stdout："
cat "$TMP/old.out"
echo "EXIT=${old_rc}"
echo "── 施工前（HEAD）stderr：$(wc -c < "$TMP/old.err") 字节"
echo
echo "── 施工后（工作树）stdout："
cat "$TMP/new.out"
echo "EXIT=${new_rc}"
echo "── 施工后（工作树）stderr：$(wc -c < "$TMP/new.err") 字节"
echo
echo "══ diff（stdout）══"
if diff -u "$TMP/old.out" "$TMP/new.out"; then echo "逐字一致（PASS）"; else echo "★ 回归：正常路径输出变了"; fi
echo "══ diff（stderr）══"
if diff -u "$TMP/old.err" "$TMP/new.err"; then echo "逐字一致（PASS）"; else echo "★ 回归：正常路径 stderr 变了"; fi
echo "══ 退出码 ══"
[ "$old_rc" -eq "$new_rc" ] && echo "一致：${old_rc}（PASS）" || echo "★ 回归：${old_rc} vs ${new_rc}"

echo
echo "══ 面板今日三行回读（旧脚本写一次、新脚本覆盖写一次，值应相同）══"
dc() { docker compose "$@"; }
dc exec -T doris-fe mysql -h127.0.0.1 -P9030 -uroot ecom_ads -e "SELECT dim_key, metric_value, revision FROM ads_window_metric WHERE metric_name='dlq_total_offset' AND window_start >= CURDATE() ORDER BY dim_key" 2>&1

