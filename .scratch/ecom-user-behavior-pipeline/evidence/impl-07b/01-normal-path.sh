#!/usr/bin/env bash
# impl-07b 正例凭证：**真栈**上正常路径逐字零变化 + dataset id 与施工前同值。
#
# 做法：把 git HEAD 上的旧置备脚本取到临时文件，与新脚本用**同一个导出 zip 落点、
# 同一套真实栈**各跑一遍，逐字 diff stdout/stderr/退出码；另外直接打真接口，
# 用「旧解析形态」与「新解析形态」各取一次 id，比对是否同一个。
#
# ★ SUPERSET_EXPORT_ZIP 指到临时目录：真脚本会刷新 docker/superset/export/ecom-live.zip
#   （**已入库**的交付件），凭证不碰工作树里的入库产物。
set -uo pipefail
export MSYS_NO_PATHCONV=1

ROOT="$(cd "$(dirname "$0")/../../../.." && pwd)"
cd "$ROOT" || exit 1

SH=docker/scripts/superset-provision.sh
# 与脚本本体同一口径：端口/主机可被覆盖时，凭证跟着走（否则端口一漂凭证就静默指错）。
BASE_URL="http://${SUPERSET_HOST:-127.0.0.1}:${SUPERSET_PORT:-8088}"
TMP="$(mktemp -d)"
trap 'rm -rf "${TMP}"' EXIT
export SUPERSET_EXPORT_ZIP="${TMP}/ecom-live.zip"

git show "HEAD:${SH}" > "${TMP}/old-provision.sh" || { echo "取不到 HEAD 版本"; exit 1; }

echo "### impl-07b 正例凭证（$(date -u +%Y-%m-%dT%H:%M:%SZ)）"
echo "### 工作目录：$PWD"
echo "### 脚本：${SH}（对照：git HEAD 版本）"
echo "### 导出 zip 落点：${SUPERSET_EXPORT_ZIP}（临时，不碰入库产物）"
echo

echo "── [1] 真接口上的 dataset 列表（原始响应前 300 字节）"
curl -s -m 20 "${BASE_URL}/api/v1/dataset/?q=(order_columns:table_name,order_direction:asc)" \
  -H "Accept: application/json" | head -c 300
echo
echo

echo "── [2] 旧解析形态取的 id（施工前：管道 + python -c）"
OLD_ID="$(curl -s -m 20 "${BASE_URL}/api/v1/dataset/?q=(order_columns:table_name,order_direction:asc)" \
  -H "Accept: application/json" \
  | "${PYTHON:-python3}" -c 'import json,sys
try:
    d = json.load(sys.stdin)
    rows = d.get("result", [])
    print(next(str(r["id"]) for r in rows if r.get("table_name") == "kpi_current"))
except StopIteration:
    pass')"
echo "旧形态 id=${OLD_ID:-<空>}"
echo

echo "── [3] 真栈：施工前（HEAD）脚本跑一遍"
bash "${TMP}/old-provision.sh" > "${TMP}/old.out" 2> "${TMP}/old.err"
old_rc=$?
cat "${TMP}/old.out"
echo "EXIT=${old_rc}  stderr=$(wc -c < "${TMP}/old.err") 字节"
echo

echo "── [4] 真栈：施工后（工作树）脚本跑一遍"
bash "${SH}" > "${TMP}/new.out" 2> "${TMP}/new.err"
new_rc=$?
cat "${TMP}/new.out"
echo "EXIT=${new_rc}  stderr=$(wc -c < "${TMP}/new.err") 字节"
echo

NEW_ID="$(sed -n 's/^\[superset-provision\] ✓ kpi_current id=//p' "${TMP}/new.out" | tail -1)"
echo "── [5] 判据：同 id / 同退出码 / 输出逐字一致"
echo "旧形态 id=${OLD_ID:-<空>}  新脚本 id=${NEW_ID:-<空>}"
if [ -n "${NEW_ID}" ] && [ "${OLD_ID}" = "${NEW_ID}" ]; then
  echo "id 一致（PASS）：${NEW_ID}"
else
  echo "★ 回归：id 不一致"
fi
echo "══ diff（stdout）══"
if diff -u "${TMP}/old.out" "${TMP}/new.out"; then echo "逐字一致（PASS）"; else echo "★ 回归：正常路径输出变了"; fi

# stderr 全量 diff 里混着**两次运行各自的容器日志时戳**（每行一行也有 DATETIME）⇒ 逐字不可能相等，
# 且与脚本行为无关。判据改成：两边的脚本自有行（[superset-provision] 前缀）逐字比 + 去掉时戳后整份比。
strip_ts() { sed -E 's/^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2},[0-9]{3}://'; }
grep -F '[superset-provision]' "${TMP}/old.err" > "${TMP}/old.err.own" || true
grep -F '[superset-provision]' "${TMP}/new.err" > "${TMP}/new.err.own" || true
strip_ts < "${TMP}/old.err" > "${TMP}/old.err.nots"
strip_ts < "${TMP}/new.err" > "${TMP}/new.err.nots"
echo "── 脚本自有 stderr 行数：old=$(wc -l < "${TMP}/old.err.own") new=$(wc -l < "${TMP}/new.err.own")"
echo "══ diff（stderr·脚本自有行）══"
if diff -u "${TMP}/old.err.own" "${TMP}/new.err.own"; then echo "逐字一致（PASS）"; else echo "★ 回归：正常路径 stderr 的脚本自有行变了"; fi
echo "══ diff（stderr·去掉容器时戳后全量）══"
if diff -u "${TMP}/old.err.nots" "${TMP}/new.err.nots" > "${TMP}/err.diff"; then
  echo "去掉时戳后逐字一致（PASS）"
else
  echo "仍不一致的行数：$(grep -c '^[-+][^-+]' "${TMP}/err.diff")（应只剩时戳造成的差异 ⇒ 见上一条）"
  head -20 "${TMP}/err.diff"
fi
echo "══ 退出码 ══"
[ "${old_rc}" -eq "${new_rc}" ] && echo "一致：${old_rc}（PASS）" || echo "★ 回归：${old_rc} vs ${new_rc}"
echo
echo "── [6] 用这个 id 打 chart_data（后续步骤未变）"
resp="$(curl -s -X POST "${BASE_URL}/api/v1/chart/data" \
  -H "Content-Type: application/json" -H "Accept: application/json" \
  -d "{\"datasource\": {\"id\": ${NEW_ID:-0}, \"type\": \"table\"},\"force\": false,\"queries\": [{\"granularity\": \"window_start\",\"filters\": [{\"col\": \"metric_name\", \"op\": \"==\", \"val\": \"uv\"}],\"metrics\": [{\"expressionType\": \"SIMPLE\", \"column\": {\"column_name\": \"metric_value\"}, \"aggregate\": \"SUM\", \"label\": \"UV\"}],\"row_limit\": 10,\"time_range\": \"No filter\",\"timeseries_limit\": 0}],\"result_format\": \"json\",\"result_type\": \"full\"}")"
echo "${resp:0:200}"
echo "$resp" | grep -q '"status": *"success"' && echo "chart_data status=success（PASS）" || echo "★ chart_data 不出数"
