#!/usr/bin/env bash
# impl-07 收口门 C —— 置备脚本侧：「找不到 dataset id」与「真因不是找不到」必须分开
# （母票 Acceptance criteria 第 4 条）。
#
# 母票这一条只要求「把两类区分开」，比 07b 的六类分流**弱**：本门的判据是
#   ★ 一句话的字面判据：报错里**有没有**出现「找不到 kpi_current 的 dataset id」。
#     有 ⇒ 脚本在说「接口里确实没有这个 dataset」；没有 ⇒ 脚本在说别的原因。
#   于是只有「真·找不到」那一臂允许出现这句话，其余每一臂**必须**不出现。
#
# 为什么这条要单独再跑一遍（07b 已有 13 条守卫测试）：
#   07b 的守卫跑的是**桩**（假 curl / 假 docker，无守护进程）；本门跑**真栈**
#   （真 Superset 接口、真 dataset、真 Doris），只把解析段那一个 URL 的响应换掉。
#   桩上对了、真栈上错了，是这类改动最常见的失败形态（07b 的 code-review 就抓到过
#   「别的行缺 table_name 就报致命」这种桩看不出来的缺陷）。
#
# 手法（照 07b 凭证的先例）：把桩 curl 前置到 PATH，**只劫持 dataset 列表那一个 URL**，
# 其余调用原样转发给真 curl ⇒ 置备的其余步骤全走真栈，注入面只剩解析段。
#
# 执行面：WSL；真栈。导出 zip 指到临时目录，不碰入库产物。
set -uo pipefail
export MSYS_NO_PATHCONV=1

ROOT="$(cd "$(dirname "$0")/../../../.." && pwd)"
cd "$ROOT" || exit 1

PROVISION=docker/scripts/superset-provision.sh
PRE_CONSTRUCTION="fc1445f"
TMP="$(mktemp -d)"
trap 'rm -rf "${TMP}"' EXIT
export SUPERSET_EXPORT_ZIP="${TMP}/ecom-live.zip"

PASS=0
FAIL=0
ok() { PASS=$((PASS + 1)); echo "   ✓ $*"; }
no() { FAIL=$((FAIL + 1)); echo "   ✗ $*"; }

# 退出码与「下一步」是本门每一臂都要判的两条 ⇒ 抽成助手，避免 9 处内联克隆
# （code-review 的 Standards 轴指出过一版有 4 处同形克隆；现在全文件只有这里一处实现）。
# $1=实际退出码  $2=stderr 文件
assert_arm_invariants() {
  local rc="$1" errf="$2"
  if [ "$rc" -ne 0 ]; then ok "退出码非 0（${rc}）"; else no "应退非 0，实得 0"; fi
  if grep -qF '下一步' "$errf"; then ok "带可操作下一步"; else no "报错缺下一步"; fi
}

# 「找不到」这句话是**字面判据**：只有真·找不到那一臂可以出现它。
NOTFOUND_PHRASE="找不到 kpi_current 的 dataset id"

echo "### impl-07 收口门 C：置备脚本「找不到」vs「真因不是找不到」（$(date -u +%Y-%m-%dT%H:%M:%SZ)）"
echo "### 工作目录：$PWD"
echo "### 被测脚本：${PROVISION}"
echo "### 字面判据：stderr 里是否出现「${NOTFOUND_PHRASE}」"
echo "### 真栈：真 Superset / 真 dataset / 真 Doris；只劫持 dataset 列表那一个 URL"
echo

BIN="${TMP}/bin"
mkdir -p "$BIN"
REAL_CURL="$(command -v curl)"
cat > "${BIN}/curl" <<'CURLSTUB'
#!/bin/sh
# 只劫持 dataset 列表那一个 URL，且照真 curl 的契约办事：有 -o 写文件、有 -w 打印状态码。
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
    printf '%s' "${IMPL07_INJECT_BODY}" > "$out"
  fi
  if [ -n "$wfmt" ]; then printf '%s' "${IMPL07_INJECT_CODE:-200}"; else printf '%s' "${IMPL07_INJECT_BODY}"; fi
  exit "${IMPL07_INJECT_RC:-0}"
fi
exec "${IMPL07_REAL_CURL}" "$@"
CURLSTUB
chmod +x "${BIN}/curl"

# 跑一臂，返回：①是否出现「找不到」这句话 ②脚本自有 stderr 末行 ③退出码
run_case() {
  local label="$1" body="$2" rc="${3:-0}" code="${4:-200}"
  IMPL07_INJECT_BODY="$body" IMPL07_INJECT_RC="$rc" IMPL07_INJECT_CODE="$code" \
    IMPL07_REAL_CURL="$REAL_CURL" PATH="${BIN}:$PATH" \
    bash "$PROVISION" > "$TMP/c.out" 2> "$TMP/c.err"
  local got=$?
  local says_notfound=0
  grep -qF "$NOTFOUND_PHRASE" "$TMP/c.err" && says_notfound=1
  echo "── ${label}"
  echo "   stderr 末行：$(grep -F '[superset-provision]' "$TMP/c.err" | tail -1)"
  echo "   EXIT=${got}  出现「找不到」=${says_notfound}"
  CASE_RC=$got
  CASE_NOTFOUND=$says_notfound
}

# ── [1] 门先证明自己能绿：真栈正常路径 ──────────────────────────────
echo "── [1] 正常路径（真栈，不注入）—— 门必须先证明自己能绿"
bash "$PROVISION" > "$TMP/n.out" 2> "$TMP/n.err"
n_rc=$?
new_id=$(sed -n 's/^\[superset-provision\] ✓ kpi_current id=//p' "$TMP/n.out" | tail -1)
if [ "$n_rc" -eq 0 ]; then ok "退出码 0"; else no "正常路径退非 0（${n_rc}）：$(grep -F '[superset-provision]' "$TMP/n.err" | tail -1)"; fi
if [ -n "$new_id" ]; then ok "取到 kpi_current id=${new_id}（真接口、真 dataset）"; else no "没取到 dataset id"; fi
if grep -qF "$NOTFOUND_PHRASE" "$TMP/n.err"; then no "正常路径竟然报「找不到」"; else ok "正常路径不报「找不到」"; fi
if grep -qF '匿名 chart_data 出数' "$TMP/n.out"; then ok "后续步骤（chart_data）未变：仍出数"; else no "chart_data 没出数"; fi
echo

# ── [2] 真·找不到：唯一**允许**报「找不到」的一臂 ──────────────────
echo "── [2] 真·找不到（合法列表里确实没有 kpi_current）—— 唯一允许报「找不到」的一臂"
run_case "[2] 合法列表，没有 kpi_current" \
  '{"result": [{"id": 1, "table_name": "ads_window_metric"}]}'
if [ "$CASE_NOTFOUND" -eq 1 ]; then ok "报出了「找不到」（正确）"; else no "★ 真·找不到被吞掉或被改写成别的说法"; fi
assert_arm_invariants "$CASE_RC" "$TMP/c.err"
echo

# ── [3] 其余每一臂：**必须**不报「找不到」────────────────────────────
echo "── [3] 「真因不是找不到」的各臂：每一臂都**不得**出现「${NOTFOUND_PHRASE}」"
inject_cases() {
  local label="$1" body="$2" rc="${3:-0}" code="${4:-200}"
  run_case "$label" "$body" "$rc" "$code"
  if [ "$CASE_NOTFOUND" -eq 1 ]; then
    no "★ 这一臂报了「找不到」—— 真因不是找不到（排查方向会被引向看板）"
  else
    ok "未报「找不到」（真因已分流到别的类）"
  fi
  assert_arm_invariants "$CASE_RC" "$TMP/c.err"
  echo
}

inject_cases "[3a] 响应是 HTML 登录页（不是 JSON）" \
  '<!DOCTYPE html><html><body>Please log in</body></html>'
inject_cases "[3b] 响应被截断（半截 JSON）" \
  '{"result": [{"id": 42, "table_name": "kpi_cur'
inject_cases "[3c] 结构变了（result 不是数组）" \
  '{"result": {"items": []}}'
inject_cases "[3d] 目标行没有 id 键" \
  '{"result": [{"table_name": "kpi_current", "uid": "42"}]}'
inject_cases "[3e] id 是 null（不可用）" \
  '{"result": [{"id": null, "table_name": "kpi_current"}]}'
inject_cases "[3f] 接口回 error message" \
  '{"message": "Forbidden"}'
inject_cases "[3g] 接口连不上（curl 退 7，HTTP 000）" '' 7 000

# ── [4] 解析器不可用（真接口、真 dataset 存在，只换解释器）──────────
echo "── [4] 解析器不可用：真接口 + 真 dataset 存在，只把解释器换掉"
echo "   ★ 这是母票说的最恶劣形态：施工前会报「找不到」，把排查引向看板"
PYTHON="/nonexistent/pyimpl07/phantom-python3" bash "$PROVISION" > "$TMP/p.out" 2> "$TMP/p.err"
p_rc=$?
p_nf=0; grep -qF "$NOTFOUND_PHRASE" "$TMP/p.err" && p_nf=1
echo "   stderr 末行：$(grep -F '[superset-provision]' "$TMP/p.err" | tail -1)"
echo "   EXIT=${p_rc}  出现「找不到」=${p_nf}"
if [ "$p_nf" -eq 0 ]; then ok "解析器缺席未报「找不到」（正确：真因在执行面）"; else no "★ 解析器缺席被报成「找不到」"; fi
assert_arm_invariants "$p_rc" "$TMP/p.err"
echo

echo "── [4b] 解析器**存在但不可用**（空壳：退 49、零输出、零 stderr；真接口）"
printf '#!/bin/sh\nexit 49\n' > "$TMP/python3-shell-stub"
chmod +x "$TMP/python3-shell-stub"
PYTHON="$TMP/python3-shell-stub" bash "$PROVISION" > "$TMP/s.out" 2> "$TMP/s.err"
s_rc=$?
s_nf=0; grep -qF "$NOTFOUND_PHRASE" "$TMP/s.err" && s_nf=1
echo "   stderr 末行：$(grep -F '[superset-provision]' "$TMP/s.err" | tail -1)"
echo "   EXIT=${s_rc}  出现「找不到」=${s_nf}"
if [ "$s_nf" -eq 0 ]; then ok "空壳解释器未报「找不到」（正确）"; else no "★ 空壳解释器被报成「找不到」"; fi
assert_arm_invariants "$s_rc" "$TMP/s.err"
echo

# ── [5] 施工前对照：同两臂在**旧脚本**上的行为 ──────────────────────
echo "── [5] 施工前（${PRE_CONSTRUCTION}）对照：旧脚本在这两臂上报的是什么"
git show "${PRE_CONSTRUCTION}:${PROVISION}" > "$TMP/old-provision.sh" \
  && ok "取到锚 ${PRE_CONSTRUCTION} 上的旧脚本" \
  || no "取不到旧脚本"
if diff -q "$TMP/old-provision.sh" "$PROVISION" >/dev/null; then
  no "★ 锚与工作树脚本相同 ⇒ 对照会自证"
else
  ok "锚与工作树脚本确实不同（对照有意义）"
fi
# 两臂各跑一次：①解释器缺席 ②解释器存在但不可用（空壳）。
# ★ 不用「kind:interp」拼串再 split —— 路径里可能有空格/冒号，拼串是在给自造 bug 留门
#   （本脚本第一版就踩了：`phantom: /nonexistent/...` 里那个空格把 kind 与路径切错了）。
old_arm() { # $1=标签  $2=解释器路径
  local label="$1" interp="$2"
  PYTHON="$interp" bash "$TMP/old-provision.sh" > "$TMP/o.out" 2> "$TMP/o.err"
  local o_rc=$?
  local o_nf=0
  grep -qF "$NOTFOUND_PHRASE" "$TMP/o.err" && o_nf=1
  echo "   [5-${label}] 旧脚本 EXIT=${o_rc} 出现「找不到」=${o_nf}"
  echo "             末行：$(grep -F '[superset-provision]' "$TMP/o.err" | tail -1)"
  if [ "$o_nf" -eq 1 ]; then
    ok "旧脚本把「${label}」报成了「找不到」（**缺陷复现**：这就是本门要治的误诊）"
  else
    no "旧脚本没报「找不到」（label=${label}）—— 缺陷未复现，对照失去意义"
  fi
}
old_arm "解释器缺席" "/nonexistent/pyimpl07/phantom-python3"
old_arm "空壳解释器" "$TMP/python3-shell-stub"
echo

echo "══ 收口门 C 结果：PASS=${PASS} FAIL=${FAIL} ══"
[ "$FAIL" -eq 0 ] || exit 1
