#!/usr/bin/env bash
# 四类故障注入 + 正常路径 —— impl-07a 实机凭证（一次跑全，输出由调用方 tee 到 .out.txt）
#
# 判据（票面 AC）：①解释器缺席 ②解释器在 PATH 上但不可用（空壳）③坏 JSON ④缺字段
# 各自报**真因**；正常路径与施工前**逐字一致**。
#
# 执行面：WSL（宿主无 make）。docker 命令需 MSYS_NO_PATHCONV=1（本机 Git-Bash 侧）。
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/../../../.." && pwd)"
cd "$ROOT" || exit 1

SYNC=docker/scripts/superset-dirty-rate-sync.sh
TMP="$(mktemp -d)"
trap 'rm -rf "${TMP}"' EXIT

echo "### impl-07a 故障注入取证件（$(date -u +%Y-%m-%dT%H:%M:%SZ)）"
echo "### 工作目录：$PWD"
echo "### 脚本：$SYNC"
echo

# ── 正例：基线文件完好（真实基线副本）────────────────────────────────
cp artifacts/verify/dirty-rate-baseline.json "$TMP/good.json"
echo "── [1] 正常路径（基线完好；真实基线文件副本）"
BASELINE_FILE="$TMP/good.json" bash "$SYNC" 2>&1
echo "EXIT=$?"
echo

# ── 故障①：解释器缺席 ────────────────────────────────────────────────
echo "── [2] 故障① 解释器缺席（PYTHON 指向不存在的路径）"
BASELINE_FILE="$TMP/good.json" PYTHON="$TMP/no-such-python3" bash "$SYNC" 2>&1
echo "EXIT=$?"
echo

# ── 故障②：解释器在 PATH 上但不可用（空壳）──────────────────────────
# ★ 这一条是本票的关键反例：存在性探测会通过，只有真的执行才知道不能用。
printf '#!/bin/sh\nexit 49\n' > "$TMP/python3-shell-stub"
chmod +x "$TMP/python3-shell-stub"
echo "── [2b] 空壳解释器的**存在性探测**会放行（这正是 impl-05 §5 建议被证伪之处）"
command -v "$TMP/python3-shell-stub" >/dev/null && echo "存在性探测：通过（危险——所以不能用它当判据）"
"$TMP/python3-shell-stub" -c "print(1)" >/dev/null 2>"$TMP/stub.err"; echo "真跑一次：rc=$? stderr=$(wc -c < "$TMP/stub.err") 字节（静默失败）"
echo
echo "── [3] 故障② 解释器不可用（存在、可 invoke、退 49 且不吐 stderr）"
BASELINE_FILE="$TMP/good.json" PYTHON="$TMP/python3-shell-stub" bash "$SYNC" 2>&1
echo "EXIT=$?"
echo

# ── 故障③：坏 JSON ──────────────────────────────────────────────────
printf '{"dlq_offset": 14911,' > "$TMP/broken.json"
echo "── [4] 故障③ 基线文件是坏 JSON"
BASELINE_FILE="$TMP/broken.json" bash "$SYNC" 2>&1
echo "EXIT=$?"
echo

# ── 故障④：缺字段 ──────────────────────────────────────────────────
printf '{"input_events": 77700}' > "$TMP/missing.json"
echo "── [5] 故障④ 基线文件缺 dlq_offset 字段"
BASELINE_FILE="$TMP/missing.json" bash "$SYNC" 2>&1
echo "EXIT=$?"
echo

# ── 附带：input_events 缺失不再致命（仅日志参考）────────────────────
printf '{"dlq_offset": 14911}' > "$TMP/logonly.json"
echo "── [6] input_events 缺失（应成功；该键仅日志参考）"
BASELINE_FILE="$TMP/logonly.json" bash "$SYNC" 2>&1
echo "EXIT=$?"
echo

# ── 静态形态：不再有裸 2>/dev/null ───────────────────────────────────
echo "── [7] 静态：grep 2>/dev/null（应零命中）"
grep -n "2>/dev/null" "$SYNC" && echo "★ 回归：仍有吞 stderr 的重定向" || echo "零命中（PASS）"
echo "── [8] 语法自检：bash -n"
bash -n "$SYNC" && echo "SYNTAX OK"

# ── 附录 A：code-review 抓到的两个假真因（已修，钉子在同名 pytest）────────
echo "── [9] 可用但启动吐 stderr 噪音的解释器（不得报「不可用」）"
printf '#!/bin/sh\necho "warning: sitecustomize noise" >&2\nexec "%s" "$@"\n' "$(command -v python3)" > "$TMP/python3-noisy"
chmod +x "$TMP/python3-noisy"
# 不进 head 管道：SIGPIPE 会把退出码变成 141，与脚本行为无关。
BASELINE_FILE="$TMP/good.json" PYTHON="$TMP/python3-noisy" bash "$SYNC" > "$TMP/noisy.out" 2> "$TMP/noisy.err"
noisy_rc=$?
cat "$TMP/noisy.out"
echo "stderr：$(wc -c < "$TMP/noisy.err") 字节（应为 0，等于正常路径；噪音只留在自检内部）"
echo "EXIT=$noisy_rc（应为 0 —— 可用解释器不得被报成不可用）"
echo
echo "── [10] dlq_offset 不是整数（{\"dlq_offset\": \"oops\"}）"
printf '{"dlq_offset": "oops"}' > "$TMP/type-bad.json"
BASELINE_FILE="$TMP/type-bad.json" bash "$SYNC" 2>&1
echo "EXIT=$?"
echo
echo "── [11] dlq_offset 为负（{\"dlq_offset\": -5}）"
printf '{"dlq_offset": -5}' > "$TMP/negative.json"
BASELINE_FILE="$TMP/negative.json" bash "$SYNC" 2>&1
echo "EXIT=$?"
