#!/usr/bin/env bash
# 反例凭证（真进程真信号）：写入中途 kill ⇒ 目标文件仍是**完整的旧版**，
# 且下一次读它能正常解析（impl-07c AC 3）。
#
# 对照两臂：
#   [A] 施工前写法（git HEAD，先截断再写）⇒ kill 时刻目标**是半截 JSON**、不可解析。
#   [B] 施工后写法（临时文件 + os.replace）⇒ 同一 kill 时刻目标**是完整旧版**、可解析。
#
# 手法：子进程在各自的中断窗口内 SIGSTOP 自停，父进程 waitpid 确认到达该点后 SIGKILL。
# 这是真进程、真信号、真文件系统——不是进程内造假（进程内造假见 pytest 守卫）。
set -uo pipefail
cd "$(dirname "$0")/../../../.." || exit 1   # 仓库根

EVIDENCE=".scratch/ecom-user-behavior-pipeline/evidence/impl-07c"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

OLD_BASELINE='{
  "dlq_offset": 14911,
  "input_events": 77700
}'

run_arm() {
  # $1 = 臂名（old|new），$2 = 目标目录
  local impl="$1" dir="$2"
  mkdir -p "$dir"
  printf '%s\n' "$OLD_BASELINE" > "$dir/dirty-rate-baseline.json"     # 先立一个完整旧版

  # ★ 直接调 .venv 的解释器（不经 `uv run`：它会再 fork 一层，被 kill 的不是我们等的 pid）。
  .venv/bin/python "$EVIDENCE/evidence_killer.py" "$impl" "$dir" >/dev/null 2>"$dir/arm.err" &
  local pid=$!
  # 等子进程自停（到达中断窗口）。SIGSTOP 后 stat 首字符是 T。
  local state="" i=""
  for i in $(seq 1 600); do
    state="$(ps -o stat= -p "$pid" 2>/dev/null | tr -d ' ')"
    [ -n "$state" ] && [ "${state:0:1}" = "T" ] && break
    sleep 0.05
  done
  if [ "${state:0:1}" != "T" ]; then
    echo "✗ [$impl] 子进程没到达中断窗口（stat=${state:-<空>}）"; cat "$dir/arm.err"; return 1
  fi
  kill -9 "$pid" 2>/dev/null
  wait "$pid" 2>/dev/null
  echo "── [$impl] 已在中断窗口内被 SIGKILL（stat=${state}）"
}

echo "▶ 臂 A：施工前写法（HEAD，先截断再写）"
run_arm old "$TMP/old" || exit 1
echo "  目标文件内容：$(printf '%s' "$(cat "$TMP/old/dirty-rate-baseline.json")")"
echo "  目标文件字节数：$(wc -c < "$TMP/old/dirty-rate-baseline.json")（完整应为 51）"

echo
echo "▶ 臂 B：施工后写法（临时文件 + os.replace）"
run_arm new "$TMP/new" || exit 1
echo "  目标文件内容：$(printf '%s' "$(cat "$TMP/new/dirty-rate-baseline.json")")"
echo "  目标文件字节数：$(wc -c < "$TMP/new/dirty-rate-baseline.json")"
echo "  目录内容（含硬杀必然留下的临时文件——SIGKILL 跑不了 finally）："
echo "    $(ls -A "$TMP/new" | tr '\n' ' ')"

echo
echo "▶ 判定：读侧（未改动的 load_dirty_rate_baseline）在两条臂上各自看到什么"
uv run python - "$TMP/old" "$TMP/new" <<'PY'
import sys
from pathlib import Path

repo = Path.cwd()
sys.path.insert(0, str(repo / "src"))
from ecomgen.verify_runner import DIRTY_RATE_BASELINE, load_dirty_rate_baseline  # noqa: E402

old_dir, new_dir = Path(sys.argv[1]), Path(sys.argv[2])
old_raw = (old_dir / DIRTY_RATE_BASELINE).read_text(encoding="utf-8")
new_raw = (new_dir / DIRTY_RATE_BASELINE).read_text(encoding="utf-8")

old_loaded = load_dirty_rate_baseline(str(old_dir))
new_loaded = load_dirty_rate_baseline(str(new_dir))

print(f"[A 施工前] 原始内容 {old_raw!r}")
print(f"[A 施工前] 读侧解析 ⇒ {old_loaded}   ← 期望 None（半截 JSON，不可解析）")
print(f"[B 施工后] 原始内容 {new_raw!r}")
print(f"[B 施工后] 读侧解析 ⇒ {new_loaded}   ← 期望 14911/77700（完整的旧版）")

ok = True
if old_loaded is not None:
    print("✗ [A] 施工前写法本该留下半截文件，却没复现缺陷——本凭证无效")
    ok = False
if new_loaded is None:
    print("✗ [B] 施工后写法下目标文件不可解析——原子替换没生效")
    ok = False
if new_loaded is not None and new_raw.strip() != new_raw.strip().rstrip("\n") + "\n":
    pass  # 仅记录，不判
# 完整旧版逐字未变
if new_raw != '{\n  "dlq_offset": 14911,\n  "input_events": 77700\n}\n':
    print(f"✗ [B] 目标文件不是逐字未变的旧版：{new_raw!r}")
    ok = False

if ok:
    print()
    print("✓ PASS：施工前写法在中断点留下半截（读侧 None），施工后写法目标是逐字未变的完整旧版")
    print("        ⇒ 读者永不看到半截文件；被写坏的只有临时文件")
raise SystemExit(0 if ok else 1)
PY

# ── 硬杀残骸的收尾（放在读侧判定之后：清扫会重写目标文件，不能抢在前面对照）──
echo
echo "▶ 硬杀残骸收尾：超龄（mtime 拨到 90 s 前）后，下一次落盘前的清扫应把它带走"
uv run python - "$TMP/new" <<'PY'
import os
import sys
import time
from pathlib import Path

repo = Path.cwd()
sys.path.insert(0, str(repo / "src"))
from ecomgen.verify_runner import (  # noqa: E402
    DIRTY_RATE_BASELINE,
    STALE_BASELINE_TEMP_GRACE_S,
    DirtyRateSample,
    save_dirty_rate_baseline,
)

d = Path(sys.argv[1])
temps = sorted(p.name for p in d.glob(f".{DIRTY_RATE_BASELINE}.*.tmp"))
print(f"  清扫前临时文件：{temps}")
if not temps:
    print("✗ 硬杀本该留下临时文件（SIGKILL 跑不了 finally）——本臂无效")
    raise SystemExit(1)
old = time.time() - STALE_BASELINE_TEMP_GRACE_S - 30
for p in d.glob(f".{DIRTY_RATE_BASELINE}.*.tmp"):
    os.utime(p, (old, old))

save_dirty_rate_baseline(DirtyRateSample(dlq_offset=16000, input_events=81000), str(d))
left = sorted(p.name for p in d.iterdir() if p.name != "arm.err")  # arm.err 是本凭证的诊断文件
print(f"  清扫后目录内容（不计 arm.err）：{left}")
if left != [DIRTY_RATE_BASELINE]:
    print(f"✗ 硬杀残骸没被清掉：{left}")
    raise SystemExit(1)
print("  ✓ 硬杀残留的临时文件在下一次落盘前被清扫")
print("    ⇒ 『正常 / 异常 / 硬杀』三种死法都不残留，且目标始终是完整 JSON")
PY
