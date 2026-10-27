#!/usr/bin/env bash
# impl-07b 故障注入：让解析段的每一种真因各出一次**真栈输出**。
#
# 手法：把 curl 前置到 PATH，但桩**只劫持 dataset 列表那一个 URL**，其余调用原样
# 转发给真 curl ⇒ 置备脚本的其余步骤（pymysql 检查 / ORM 置备 / 匿名可达断言 /
# zip 往返 / 脏率同步）全走真栈，注入面只剩解析段，与实机形态一致。
# 另有一条（[9]）根本不换 curl：真接口、合法响应、dataset 确实存在，只把解析器换掉
# ——那正是施工前会被报成「找不到 dataset id」的最恶劣形态。
set -uo pipefail
export MSYS_NO_PATHCONV=1

ROOT="$(cd "$(dirname "$0")/../../../.." && pwd)"
cd "$ROOT" || exit 1

SH=docker/scripts/superset-provision.sh
TMP="$(mktemp -d)"
trap 'rm -rf "${TMP}"' EXIT
export SUPERSET_EXPORT_ZIP="${TMP}/ecom-live.zip"

echo "### impl-07b 故障注入取证件（$(date -u +%Y-%m-%dT%H:%M:%SZ)）"
echo "### 工作目录：$PWD"
echo "### 脚本：${SH}"
echo "### 导出 zip 落点：${SUPERSET_EXPORT_ZIP}（临时，不碰入库产物）"
echo

BIN="${TMP}/bin"
mkdir -p "${BIN}"
REAL_CURL="$(command -v curl)"
# ★ 引号定界符：桩脚本里的 $@ / ${IMPL07B_*} 必须**留到运行时**求值。
cat > "${BIN}/curl" <<'CURLSTUB'
#!/bin/sh
# 只劫持 dataset 列表那一个 URL，且**照真 curl 的契约**办事：有 -o 就把响应体写进
# 那个文件、有 -w 就打印状态码；否则打到 stdout（施工前的管道形态）。
out=""
wfmt=""
prev=""
hit=0
for a in "$@"; do
  case "$a" in *"/api/v1/dataset/"*) hit=1 ;; esac
  if [ "$prev" = "-o" ]; then out="$a"; fi
  if [ "$prev" = "-w" ]; then wfmt="$a"; fi
  prev="$a"
done
if [ "$hit" = 1 ]; then
  if [ -n "$out" ] && [ "$out" != "/dev/null" ]; then
    printf '%s' "${IMPL07B_INJECT_BODY}" > "$out"
  fi
  if [ -n "$wfmt" ]; then
    printf '%s' "${IMPL07B_INJECT_CODE:-200}"
  else
    printf '%s' "${IMPL07B_INJECT_BODY}"
  fi
  exit "${IMPL07B_INJECT_RC:-0}"
fi
exec "${IMPL07B_REAL_CURL}" "$@"
CURLSTUB
chmod +x "${BIN}/curl"

# 跑一遍，摘出脚本自有的 ✓/✗ 行（其余是容器日志噪音），并按**报错措辞**归判。
run_case() {
  local label="$1" body="$2" rc="${3:-0}" code="${4:-200}"
  echo "── ${label}"
  IMPL07B_INJECT_BODY="${body}" IMPL07B_INJECT_RC="${rc}" IMPL07B_INJECT_CODE="${code}" \
    PATH="${BIN}:$PATH" IMPL07B_REAL_CURL="${REAL_CURL}" \
    bash "${SH}" > "${TMP}/c.out" 2> "${TMP}/c.err"
  local got=$?
  echo "   stdout 末尾：$(grep -F '[superset-provision]' "${TMP}/c.out" | tail -1)"
  echo "   stderr：$(grep -F '[superset-provision]' "${TMP}/c.err" | tail -1)"
  echo "   EXIT=${got}"
  if grep -q '找不到 kpi_current 的 dataset id' "${TMP}/c.err"; then
    echo "   → 判定：报「找不到」（仅 NOT_FOUND 这一类允许）"
  elif grep -q '没能从 dataset 列表响应里解析出目标' "${TMP}/c.err"; then
    echo "   → 判定：报「没能解析出目标」（PASS）"
  elif grep -q '没能取回 dataset 列表响应' "${TMP}/c.err"; then
    echo "   → 判定：报「没能取回响应」（PASS）"
  elif grep -q 'dataset 列表接口回了 error message' "${TMP}/c.err"; then
    echo "   → 判定：报「接口在报错」（PASS）"
  elif grep -q '结构变了' "${TMP}/c.err"; then
    echo "   → 判定：报「结构变了」（PASS）"
  elif grep -q 'id 不可用' "${TMP}/c.err"; then
    echo "   → 判定：报「id 不可用」（结构类，PASS）"
  else
    echo "   → 判定：★ 未识别（见上面的 stderr 原文）"
  fi
  echo
}

run_case "[1] 反例① 响应体是 HTML 登录页（不是 JSON）" \
  '<!DOCTYPE html><html><body>Please log in</body></html>'
run_case "[2] 反例② 响应被截断（半截 JSON）" \
  '{"result": [{"id": 42, "table_name": "kpi_cur'
run_case "[3] 反例③ 结构变了（result 不是数组）" \
  '{"result": {"items": []}}'
run_case "[4] 反例④ 目标行没有 id 键" \
  '{"result": [{"table_name": "kpi_current", "uid": "42"}]}'
run_case "[5] 反例⑤ id 是 null" \
  '{"result": [{"id": null, "table_name": "kpi_current"}]}'
run_case "[6] 反例⑥ 接口回 error message" \
  '{"message": "Forbidden"}'
run_case "[7] 反例⑦ 连不上接口（curl 退出码 7，HTTP 000）" '' 7 000
run_case "[8] 真·找不到（合法列表，没有 kpi_current）" \
  '{"result": [{"id": 1, "table_name": "ads_window_metric"}]}'

echo "── [9] 反例⑧ 解析器不可用（真接口 + PYTHON 指向不存在的路径；不换 curl）"
PYTHON="/nonexistent/pyimpl07b/phantom-python3" bash "${SH}" > "${TMP}/n.out" 2> "${TMP}/n.err"
got=$?
echo "   stdout 末尾：$(grep -F '[superset-provision]' "${TMP}/n.out" | tail -1)"
echo "   stderr：$(grep -F '[superset-provision]' "${TMP}/n.err" | tail -1)"
echo "   EXIT=${got}"
echo

echo "── [9b] 反例⑨b 解释器**存在但不可用**（空壳：退 49、零输出、零 stderr；真接口）"
printf '#!/bin/sh\nexit 49\n' > "${TMP}/python3-shell-stub"
chmod +x "${TMP}/python3-shell-stub"
PYTHON="${TMP}/python3-shell-stub" bash "${SH}" > "${TMP}/s.out" 2> "${TMP}/s.err"
got=$?
echo "   stdout 末尾：$(grep -F '[superset-provision]' "${TMP}/s.out" | tail -1)"
echo "   stderr：$(grep -F '[superset-provision]' "${TMP}/s.err" | tail -1)"
echo "   EXIT=${got}"
echo

echo "── [10] 语法自检：bash -n"
bash -n "${SH}" && echo "SYNTAX OK"
echo
echo "── [11] 静态：解析段不得回到「管道 + except StopIteration」（只看非注释行）"
if grep -v '^[[:space:]]*#' "${SH}" | grep -q 'except StopIteration'; then
  echo "★ 回归：解析段又出现 except StopIteration"
else
  echo "零命中（PASS）"
fi
