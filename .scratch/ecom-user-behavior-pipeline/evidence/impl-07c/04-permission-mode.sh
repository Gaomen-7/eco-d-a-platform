#!/usr/bin/env bash
# 权限位凭证：`os.replace` 会把临时文件的 0600 一起搬到目标上（impl-07c code-review 实测缺陷）。
# 本脚本对**真文件**量三种情况的 mode：
#   [1] 施工前（HEAD）首次落盘   [2] 施工后首次落盘   [3] 施工后覆盖落盘（旧文件 0644）
# 要求 [1] == [2] == [3]，否则「读者侧不改也得能读」这条验收就不成立。
set -uo pipefail
cd "$(dirname "$0")/../../../.." || exit 1

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
git show HEAD:src/ecomgen/verify_runner.py > "$TMP/old_verify_runner.py" || exit 1

uv run python - "$TMP" <<'PY'
import importlib.util
import os
import sys
from pathlib import Path

root = Path(sys.argv[1])
sys.path.insert(0, str(Path.cwd() / "src"))

spec = importlib.util.spec_from_file_location("old_verify_runner", root / "old_verify_runner.py")
old = importlib.util.module_from_spec(spec)
sys.modules["old_verify_runner"] = old
spec.loader.exec_module(old)

from ecomgen import verify_runner as new  # noqa: E402

umask = os.umask(0); os.umask(umask)
print(f"当前 umask={oct(umask)} ⇒ open('w') 的产物应为 {oct(0o666 & ~umask)}")

old_dir, new_dir = root / "o", root / "n"
old.save_dirty_rate_baseline(old.DirtyRateSample(dlq_offset=14911, input_events=77700), str(old_dir))
new.save_dirty_rate_baseline(new.DirtyRateSample(dlq_offset=14911, input_events=77700), str(new_dir))

# ★ 先量**首次落盘**的权限位，再做覆盖实验（顺序颠倒会量到覆盖后的结果）。
new_t = new_dir / new.DIRTY_RATE_BASELINE
m_old = (old_dir / new.DIRTY_RATE_BASELINE).stat().st_mode & 0o777
m_first = new_t.stat().st_mode & 0o777

# 覆盖落盘：把目标设成 0640（一个非默认但仍可读的旧文件），再落一次。
os.chmod(new_t, 0o640)
new.save_dirty_rate_baseline(new.DirtyRateSample(dlq_offset=15555, input_events=80000), str(new_dir))
m_over = new_t.stat().st_mode & 0o777

print(f"[1] 施工前首次落盘：{oct(m_old)}")
print(f"[2] 施工后首次落盘：{oct(m_first)}")
print(f"[3] 施工后覆盖落盘（旧文件 0640）：{oct(m_over)}  ← 应沿用 0640")
ok = True
if m_old != m_first:
    print("✗ 首次落盘权限位与施工前不一致：读者会丢掉原本有的读权限")
    ok = False
if m_over != 0o640:
    print(f"✗ 覆盖落盘没沿用旧文件的权限位（得到 {oct(m_over)}，期望 0o640）")
    ok = False
print()
print("✓ PASS" if ok else "✗ FAIL")
raise SystemExit(0 if ok else 1)
PY
