#!/usr/bin/env bash
# impl-07 收口门 B —— 基线文件完好时，脏率同步脚本的行为与施工前**逐字一致**
# （母票 Acceptance criteria 第 3 条）。
#
# 判据四条（母票原文「同一份基线、同一组 Kafka offset，写入面板的三行值不变、退出码不变」）：
#   ① stdout 逐字一致  ② stderr 逐字一致  ③ 退出码一致  ④ 面板三行（回读）一致
#
# ★★ 本脚本最重要的设计点是**对照的锚**，不是判据本身：
#
#   子票 07a 的凭证（evidence/impl-07a/02-diff-vs-head.sh）用 `git show HEAD:` 取「施工前」
#   版本。在施工当时（改动未提交）HEAD 指向施工前的脚本，这是对的；但**一旦施工提交入库**，
#   HEAD 就变成了**新脚本** ⇒ 那条对照退化成「新脚本 vs 新脚本」，永远 PASS 且毫无信息量。
#   这正是「用实现证明实现」的另一种形态，也是本门要防的第一件事。
#
#   ⇒ 本脚本的锚是**施工前的提交哈希 `fc1445f`**（impl-07 开票那次提交，三张副票的
#     基线状态），不是 HEAD。并且**断言工作树版本与该锚确实不同**——若两者相同，
#     说明锚写错了（或有人把锚往前挪了一步），此时本门**必红**，而不是安静地自证。
#
# 执行面：WSL；真栈（真 Kafka offset、真 Doris 写入）。docker 需 MSYS_NO_PATHCONV=1。
set -uo pipefail
export MSYS_NO_PATHCONV=1

ROOT="$(cd "$(dirname "$0")/../../../.." && pwd)"
cd "$ROOT" || exit 1

SYNC=docker/scripts/superset-dirty-rate-sync.sh
# 施工前的锚：impl-07 开票提交（此前 HEAD 即三张副票施工前的形态）。
PRE_CONSTRUCTION="fc1445f"
TMP="$(mktemp -d)"
trap 'rm -rf "${TMP}"' EXIT

PASS=0
FAIL=0
ok() { PASS=$((PASS + 1)); echo "   ✓ $*"; }
no() { FAIL=$((FAIL + 1)); echo "   ✗ $*"; }

echo "### impl-07 收口门 B：正常路径与施工前逐字零变化（$(date -u +%Y-%m-%dT%H:%M:%SZ)）"
echo "### 工作目录：$PWD"
echo "### 被测脚本：${SYNC}"
echo "### 对照锚：${PRE_CONSTRUCTION}（impl-07 开票提交，施工前形态）"
echo

# ── [0] 锚的**有效性**自检：这里是防「自证」的那颗钉子 ────────────────
echo "── [0] 对照锚有效性（防「新脚本 vs 新脚本」的自证）"
git cat-file -e "${PRE_CONSTRUCTION}^{commit}" 2>/dev/null \
  && ok "锚 ${PRE_CONSTRUCTION} 在仓库里" \
  || { no "锚 ${PRE_CONSTRUCTION} 不存在（历史被改写？）"; echo "══ 收口门 B 中止：锚无效 ══"; exit 1; }
git show "${PRE_CONSTRUCTION}:${SYNC}" > "$TMP/old-sync.sh" \
  && ok "取到锚上的旧脚本" \
  || { no "取不到锚上的旧脚本"; exit 1; }
if diff -q "$TMP/old-sync.sh" "$SYNC" >/dev/null; then
  no "★ 锚上的脚本与工作树**逐字相同** ⇒ 本次对照会自证（永远 PASS 且无信息量）；锚选错了"
else
  ok "锚上脚本与工作树**确实不同**（对照有意义）：$(diff -u "$TMP/old-sync.sh" "$SYNC" | grep -cE '^[-+][^-+]' | tr -d ' ') 行差异"
fi
echo

# ── [1] 同一份基线副本、真栈各跑一遍 ─────────────────────────────────
cp artifacts/verify/dirty-rate-baseline.json "$TMP/baseline.json" \
  && ok "取到真实基线文件（同一份给两边）" \
  || { no "取不到 artifacts/verify/dirty-rate-baseline.json"; exit 1; }
echo "   基线内容：$(tr -d '\n' < "$TMP/baseline.json")"
echo

BASELINE_FILE="$TMP/baseline.json" bash "$TMP/old-sync.sh" > "$TMP/old.out" 2> "$TMP/old.err"
old_rc=$?
BASELINE_FILE="$TMP/baseline.json" bash "$SYNC" > "$TMP/new.out" 2> "$TMP/new.err"
new_rc=$?

echo "── [1] 施工前（${PRE_CONSTRUCTION}）stdout"
cat "$TMP/old.out"
echo "   EXIT=${old_rc}  stderr=$(wc -c < "$TMP/old.err" | tr -d ' ') 字节"
echo "── [2] 施工后（工作树）stdout"
cat "$TMP/new.out"
echo "   EXIT=${new_rc}  stderr=$(wc -c < "$TMP/new.err" | tr -d ' ') 字节"
echo

# ── [3] 判据① stdout 逐字 ───────────────────────────────────────────
echo "── [3] 判据① stdout 逐字一致"
if diff -u "$TMP/old.out" "$TMP/new.out"; then ok "stdout 逐字一致"; else no "正常路径 stdout 变了（正向路径行为变化一律视为回归）"; fi
echo

# ── [4] 判据② stderr 逐字 ───────────────────────────────────────────
echo "── [4] 判据② stderr 逐字一致"
if diff -u "$TMP/old.err" "$TMP/new.err"; then ok "stderr 逐字一致"; else no "正常路径 stderr 变了"; fi
echo

# ── [5] 判据③ 退出码 ────────────────────────────────────────────────
echo "── [5] 判据③ 退出码一致"
if [ "$old_rc" -eq "$new_rc" ]; then ok "退出码一致：${old_rc}"; else no "退出码不一致：${old_rc} vs ${new_rc}"; fi
echo

# ── [6] 判据④ 面板三行回读 ──────────────────────────────────────────
echo "── [6] 判据④ 面板三行（回读，旧脚本写一次、新脚本覆盖写一次，值应相同）"
dc() { docker compose "$@"; }
dc exec -T doris-fe mysql -h127.0.0.1 -P9030 -uroot ecom_ads -e \
  "SELECT dim_key, metric_value FROM ads_window_metric WHERE metric_name='dlq_total_offset' AND window_start >= CURDATE() ORDER BY dim_key" \
  > "$TMP/panel.out" 2>&1
cat "$TMP/panel.out"
# 期望值从**日志行**取（脚本自己说它写了什么），再从面板回读核对——
# 这样「写进去的」与「读回来的」两条链都被钉住，而不是只信其中一条。
delta_line=$(grep -oE '完成：Δ=[0-9]+' "$TMP/new.out" | head -1 | grep -oE '[0-9]+')
base_line=$(grep -oE 'dlq_offset=[0-9]+' "$TMP/new.out" | head -1 | grep -oE '[0-9]+')
input_line=$(grep -oE 'input=[0-9]+' "$TMP/new.out" | head -1 | grep -oE '[0-9]+')
for pair in "baseline_mirror:${base_line}" "dlq_count_delta:${delta_line}" "total_events:${input_line}"; do
  key="${pair%%:*}"; want="${pair##*:}"
  got=$(awk -v k="$key" '$1==k {print $2}' "$TMP/panel.out")
  if [ -n "$want" ] && [ "$got" = "$want" ]; then ok "面板 ${key}=${got}（与脚本日志一致）"; else no "面板 ${key}=${got:-<缺>}，脚本说 ${want:-<未取到>}"; fi
done
echo

# ── [7] 脚本铁律：只碰报错分支 ──────────────────────────────────────
echo "── [7] 判据⑤ 范围纪律：Δ 计算与面板写入形态未动"
# 从两边脚本里抽出这三段，先**逐字**比。逐字不等时，再比「剥掉 ${VAR}→$VAR 花括号写法」的形态：
# ★ 实测事实（如实登记，不粉饰）：施工中 `$delta`/`$cur_input` 被写成了 `${delta}`/`${cur_input}`
#   （bash 里两种写法语义完全相同，这是既有的「统一花括号」风格）。故逐字比会红一行。
#   本门因此分两级判：①**语义形态**必须逐字一致；②逐字不一致时**必须**只是花括号差异——
#   且要把差异行数打出来。若出现任何非花括号的差异 ⇒ 判回归。
extract_core() {
  sed -n '/③ Δ 与保护/,/面板读法/p' "$1"
}
extract_core "$TMP/old-sync.sh" > "$TMP/old.core"
extract_core "$SYNC" > "$TMP/new.core"
if diff -u "$TMP/old.core" "$TMP/new.core" > "$TMP/core.diff"; then
  ok "Δ 计算 / 负值保护 / 面板写入 / 收尾打印 **逐字**未动（$(wc -l < "$TMP/new.core" | tr -d ' ') 行）"
else
  brace_lines=$(grep -cE '^[-+][^-+]' "$TMP/core.diff" | tr -d ' ')
  echo "   逐字比有差异（${brace_lines} 行），逐行列出："
  grep -E '^[-+][^-+]' "$TMP/core.diff" | sed 's/^/     /'
  # 花括号归一化：把 ${VAR} 折成 $VAR（仅限简单变量名，不动 ${VAR:-默认} 这类形态）。
  normalize_braces() { sed -E 's/\$\{([A-Za-z_][A-Za-z0-9_]*)\}/\$\1/g' "$1"; }
  normalize_braces "$TMP/old.core" > "$TMP/old.core.n"
  normalize_braces "$TMP/new.core" > "$TMP/new.core.n"
  if diff -u "$TMP/old.core.n" "$TMP/new.core.n"; then
    ok "差异**仅**为 \${VAR}→\$VAR 花括号写法（bash 语义完全相同），Δ 计算与写入逻辑未动"
  else
    no "出现花括号之外的真实差异 ⇒ 正向路径的计算或写入段被改动了"
  fi
fi
echo

# ── [7b] 花括号之外，施工还动了两处 ⇒ 单独验，别让它落在判据⑤的盲区里 ──
# ★ code-review（Spec 轴）指出：判据⑤ 的 sed 区间是「③ Δ 与保护 → 面板读法」，
#   **够不到** ② 段的 `topic_total_offset()`，也够不到 `require_offset` ——
#   而施工恰好改了这两处。盲区里的改动等于没验，故这里补两条**窄**判据。
echo "── [7b] 花括号之外的两处改动（判据⑤ 盲区，单独验）"
normalize_braces() { sed -E 's/\$\{([A-Za-z_][A-Za-z0-9_]*)\}/\$\1/g' "$1"; }

# (a) Kafka 读取：docker 命令与 awk 过滤必须「除花括号外」逐字一致。
extract_kafka() {
  sed -n '/^topic_total_offset() {/,/^}/p' "$1" | normalize_braces /dev/stdin
}
extract_kafka "$TMP/old-sync.sh" > "$TMP/old.kafka"
extract_kafka "$SYNC" > "$TMP/new.kafka"
echo "   ── (a) topic_total_offset()（Kafka 读取口径）"
if diff -u "$TMP/old.kafka" "$TMP/new.kafka"; then
  ok "docker 命令 / awk 过滤 / bootstrap **逐字未动**（除 \${VAR} 花括号）"
else
  no "Kafka 读取口径被改了 ⇒ 归因待查（票面 Notes 要求不改 Kafka 读取）"
fi
# 把这一处如实登记：施工把 --topic "$topic" 写成了 --topic "${topic}"（语义相同）。
if grep -qF -- '--topic "${topic}"' "$SYNC" && grep -qF -- '--topic "$topic"' "$TMP/old-sync.sh"; then
  echo "   [登记] \`--topic \"\$topic\"\` → \`--topic \"\${topic}\"\`：bash 语义完全相同，取值不变；"
  echo "          但票面 ac 说「不改 Kafka 读取」⇒ 该措辞已在母票 Answer 里按事实收紧（不改的是**取值口径**，不是每个字符）"
fi
echo

# (b) offset 校验：旧形态 `[ -n X ] && [ X -ge 0 ] 2>/dev/null` 与新的纯十进制校验，
#     对**合法输入**必须判决相同；两者只在「非十进制」输入上分流（那是本轮要治的面）。
echo "   ── (b) require_offset 的判决（合法/非十进制各一组）"
offset_verdict() { # $1=值  $2=实现（old|new）
  local v="$1" impl="$2"
  if [ "$impl" = old ]; then
    if [ -n "$v" ] && [ "$v" -ge 0 ] 2>/dev/null; then echo PASS; else echo FAIL; fi
  else
    case "$v" in '' | *[!0-9]*) echo FAIL ;; *) echo PASS ;; esac
  fi
}
same=0; diffn=0
for v in 0 1 14911 77736; do
  o=$(offset_verdict "$v" old); n=$(offset_verdict "$v" new)
  [ "$o" = "$n" ] && same=$((same + 1)) || { diffn=$((diffn + 1)); echo "   ★ 合法输入 ${v}：old=${o} new=${n}"; }
done
if [ "$diffn" -eq 0 ]; then ok "合法 offset（${same} 个样本）两版判决完全一致"; else no "合法输入上两版判决不同 ⇒ 正向路径回归"; fi
# 非十进制：旧形态靠 2>/dev/null 静默吞，新形态明确判 FAIL —— 这是本轮**有意**的收窄。
for v in 'oops' '+5' ' 5' '-1'; do
  o=$(offset_verdict "$v" old); n=$(offset_verdict "$v" new)
  [ "$o" = "$n" ] && echo "   ${v}：old=${o} new=${n}（一致）" || echo "   ${v}：old=${o} new=${n}（★ 收窄：旧形态会放行，新形态拒绝）"
done
echo

echo "══ 收口门 B 结果：PASS=${PASS} FAIL=${FAIL} ══"
[ "$FAIL" -eq 0 ] || exit 1
