#!/usr/bin/env bash
# impl-07 收口门 A —— 脏率基线链的**四类故障注入**（母票 Acceptance criteria 第 2 条）。
#
# 母票这一条要求：四类故障逐一确认报错指向**真因**、各出一份可复现的实机输出，
# 「不接受『看起来对』」⇒ 本脚本不只打印：每一臂都**机读断言**，并且断言
# 「别的类不得冒充」（互不冒充）。任何一条不过 ⇒ 退出码 1。
#
# 四类（母票原文）：
#   ①解释器缺席  ②解释器在 PATH 上但不可用（空壳）  ③基线文件是坏 JSON  ④基线文件缺字段
#
# ★ 本脚本与 evidence/impl-07a/01 的分工：那是 07a 的**施工凭证**（打印给人看）；
#   本脚本是母票的**收口门**（断言跑给 CI/下一个人看，红了就退非 0）。两者都保留：
#   母票需要「四类各自不冒充」这一条**母票级**判据，07a 的单票凭证不承担它。
#
# ★ 解释器不可用这一类必须靠**真的执行**抓住：本脚本第 [2b] 段把「存在性探测会放行」
#   这一实测反例逐字打出——impl-05 §5 那条「先 command -v 探测」的建议已被证伪，
#   母票 Answer 留档它，这里留**可复现的反证**。
#
# 执行面：WSL（宿主无 make）。全脚本不连 Kafka/Doris 判据（用固定 offset 的 docker 桩），
# 故可在无守护进程的环境复跑；真栈版见 02/03。
set -uo pipefail
export MSYS_NO_PATHCONV=1

ROOT="$(cd "$(dirname "$0")/../../../.." && pwd)"
cd "$ROOT" || exit 1

SYNC=docker/scripts/superset-dirty-rate-sync.sh
TMP="$(mktemp -d)"
trap 'rm -rf "${TMP}"' EXIT

PASS=0
FAIL=0
ok() { PASS=$((PASS + 1)); echo "   ✓ $*"; }
no() { FAIL=$((FAIL + 1)); echo "   ✗ $*"; }

# 「真因」的分类词表：每一臂断言**自己那一类命中**、其余三类**零命中**。
# 注：`assert_class` 只核措辞，不核「哪一类」——类别由调用方选定的 $mine 决定，
# 故不需要额外的 label 参数（code-review 指出过一版有个从未使用的 label 形参）。
assert_class() {
  local errf="$1" mine="$2"
  if grep -qF "$mine" "$errf"; then ok "报出了真因：${mine}"; else no "未报真因（应含「${mine}」）；实得：$(tr '\n' ' ' < "$errf")"; fi
}
assert_not_classes() {
  local errf="$1"; shift
  local bad=""
  local w
  for w in "$@"; do grep -qF "$w" "$errf" && bad="${bad}「${w}」"; done
  if [ -z "$bad" ]; then ok "未冒充其他类"; else no "冒充了不该出现的类：${bad}"; fi
}
assert_next_step() {
  local errf="$1"
  if grep -qE '下一步|make verify|export PYTHON|apt-get install|删掉' "$errf"; then ok "带可操作下一步"; else no "报错缺下一步动作"; fi
}
assert_exit_nonzero() {
  local rc="$1"
  if [ "$rc" -ne 0 ]; then ok "退出码非 0（${rc}）"; else no "应退非 0，实得 0"; fi
}

# 固定 offset 的 docker 桩：本门只判「报错形态」，不判集群 ⇒ 让脚本能跑到尾即可。
# ★ 但**不改**脚本自己的 docker 调用形态（那条链由 02 在真栈上验）。
mkdir -p "$TMP/bin"
cat > "$TMP/bin/docker" <<'DOCKERSTUB'
#!/bin/sh
# 输入 topic 名里**也含** dlq 这个名字的公共前缀 ⇒ dlq 那一臂必须写在前面。
# 输出格式照真 kafka-get-offsets.sh：「topic:分区:offset」（脚本的 awk 按 NF==3 取 $3）。
case "$*" in
  *kafka-get-offsets.sh*topic\ ecom.behavior.event.v1.dlq*) echo "ecom.behavior.event.v1.dlq:0:14911" ;;
  *kafka-get-offsets.sh*topic\ ecom.behavior.event.v1*)    echo "ecom.behavior.event.v1:0:77736" ;;
esac
exit 0
DOCKERSTUB
chmod +x "$TMP/bin/docker"

echo "### impl-07 收口门 A：脏率基线链四类故障注入（$(date -u +%Y-%m-%dT%H:%M:%SZ)）"
echo "### 工作目录：$PWD"
echo "### 被测脚本：${SYNC}"
echo "### 判据：每一类报自己的真因 + 不冒充其他三类 + 带下一步 + 退非 0；任一不过即退 1"
echo

GOOD="$TMP/good.json"
cp artifacts/verify/dirty-rate-baseline.json "$GOOD" || { echo "取不到真实基线文件"; exit 1; }
echo "### 基线样本（真实基线副本）：$(printf '%s' "$(cat "$GOOD")" | tr -d '\n')"
echo

run_sync() { # $1=输出前缀  $2=额外环境（VAR=VAL）
  local prefix="$1" ; shift
  env PATH="$TMP/bin:$PATH" "$@" BASELINE_FILE="$BASELINE_FOR_CASE" \
    bash "$SYNC" > "$TMP/${prefix}.out" 2> "$TMP/${prefix}.err"
  echo $?
}

# ── [1] 正常路径：门自身必须先证明「能绿」────────────────────────────
echo "── [1] 正常路径（基线完好）—— 门必须先证明自己能绿"
BASELINE_FOR_CASE="$GOOD"
rc=$(run_sync normal)
if [ "$rc" -eq 0 ]; then ok "退出码 0"; else no "正常路径退非 0（${rc}）"; fi
if grep -qF '完成：Δ=0' "$TMP/normal.out"; then ok "走完 Δ 与面板写入"; else no "正常路径没走到尾"; fi
if [ -s "$TMP/normal.err" ]; then no "正常路径 stderr 非空：$(tr '\n' ' ' < "$TMP/normal.err")"; else ok "正常路径 stderr 0 字节"; fi
echo

# ── [2] 故障① 解释器缺席 ─────────────────────────────────────────────
echo "── [2] 故障① 解释器缺席（PYTHON 指向不存在路径）"
BASELINE_FOR_CASE="$GOOD"
rc=$(run_sync f1 PYTHON="$TMP/no-such-python3")
assert_exit_nonzero "$rc"
assert_class "$TMP/f1.err" "解释器缺席"
assert_not_classes "$TMP/f1.err" "解释器不可用" "不是合法 JSON" "缺必需字段"
assert_next_step "$TMP/f1.err"
echo "   stderr：$(cat "$TMP/f1.err")"
echo

# ── [3] 故障② 解释器存在但不可用（空壳）—— 本门的关键反例 ─────────────
echo "── [3] 故障② 解释器在 PATH 上但不可用（空壳：退 49、零输出、零 stderr）"
printf '#!/bin/sh\nexit 49\n' > "$TMP/python3-shell-stub"
chmod +x "$TMP/python3-shell-stub"
BASELINE_FOR_CASE="$GOOD"
rc=$(run_sync f2 PYTHON="$TMP/python3-shell-stub")
assert_exit_nonzero "$rc"
assert_class "$TMP/f2.err" "解释器不可用"
assert_not_classes "$TMP/f2.err" "解释器缺席" "不是合法 JSON" "缺必需字段"
assert_next_step "$TMP/f2.err"
echo "   stderr：$(cat "$TMP/f2.err")"
echo

# ── [2b] 留档反证：存在性探测会放行这个空壳 ──────────────────────────
echo "── [2b] ★ 留档反证：impl-05 §5 建议的「先 command -v 探测」为何被证伪"
if command -v "$TMP/python3-shell-stub" >/dev/null 2>&1; then
  ok "存在性探测：**通过**（放行了一个不可用的解释器 —— 所以它不能当判据）"
else
  no "存在性探测未通过（样本构造有问题）"
fi
"$TMP/python3-shell-stub" -c "print(1)" > "$TMP/probe.out" 2> "$TMP/probe.err"
probe_rc=$?
probe_err_bytes=$(wc -c < "$TMP/probe.err" | tr -d ' ')
probe_out_bytes=$(wc -c < "$TMP/probe.out" | tr -d ' ')
if [ "$probe_rc" -eq 49 ] && [ "$probe_err_bytes" -eq 0 ] && [ "$probe_out_bytes" -eq 0 ]; then
  ok "真跑一次：rc=49、stdout=${probe_out_bytes} 字节、stderr=${probe_err_bytes} 字节（静默失败 ⇒ 只有真的执行才分辨得出）"
else
  no "真跑一次：rc=${probe_rc}、stdout=${probe_out_bytes} B、stderr=${probe_err_bytes} B（期望 rc=49、0/0）"
fi
echo

# ── [4] 故障③ 基线文件是坏 JSON ──────────────────────────────────────
echo "── [4] 故障③ 基线文件是坏 JSON"
printf '{"dlq_offset": 14911,' > "$TMP/broken.json"
BASELINE_FOR_CASE="$TMP/broken.json"
rc=$(run_sync f3)
assert_exit_nonzero "$rc"
assert_class "$TMP/f3.err" "不是合法 JSON"
assert_not_classes "$TMP/f3.err" "解释器缺席" "解释器不可用" "缺必需字段"
assert_next_step "$TMP/f3.err"
echo "   stderr：$(cat "$TMP/f3.err")"
echo

# ── [5] 故障④ 基线文件缺字段 ────────────────────────────────────────
echo "── [5] 故障④ 基线文件缺必需字段（dlq_offset）"
printf '{"input_events": 77700}' > "$TMP/missing.json"
BASELINE_FOR_CASE="$TMP/missing.json"
rc=$(run_sync f4)
assert_exit_nonzero "$rc"
assert_class "$TMP/f4.err" "缺必需字段：dlq_offset"
assert_not_classes "$TMP/f4.err" "解释器缺席" "解释器不可用" "不是合法 JSON"
assert_next_step "$TMP/f4.err"
echo "   stderr：$(cat "$TMP/f4.err")"
echo

# ── [6] 四类**互不冒充**的矩阵自检 ───────────────────────────────────
echo "── [6] 四类真因矩阵自检（每一类只能命中自己）"
labels=("①解释器缺席" "②解释器不可用" "③坏JSON" "④缺字段")
errfiles=("$TMP/f1.err" "$TMP/f2.err" "$TMP/f3.err" "$TMP/f4.err")
words=("解释器缺席" "解释器不可用" "不是合法 JSON" "缺必需字段")
for i in 0 1 2 3; do
  hits=0
  for w in "${words[@]}"; do grep -qF "$w" "${errfiles[$i]}" && hits=$((hits + 1)); done
  if [ "$hits" -eq 1 ]; then ok "${labels[$i]}：只命中自己（矩阵对角）"; else no "${labels[$i]}：命中 ${hits} 类（应为 1）"; fi
done
echo

# ── [7] 静态形态：不得回退到吞证据/存在性探测 ────────────────────────
echo "── [7] 静态形态守卫（回退即红）"
if grep -q "2>/dev/null" "$SYNC"; then no "又出现裸 2>/dev/null"; else ok "全脚本零 2>/dev/null"; fi
if grep -q "command -v" "$SYNC"; then no "又用 command -v 做解释器判据（已被证伪）"; else ok "无 command -v 判据"; fi
if bash -n "$SYNC"; then ok "bash -n SYNTAX OK"; else no "语法错"; fi
echo

echo "══ 收口门 A 结果：PASS=${PASS} FAIL=${FAIL} ══"
[ "$FAIL" -eq 0 ] || exit 1
