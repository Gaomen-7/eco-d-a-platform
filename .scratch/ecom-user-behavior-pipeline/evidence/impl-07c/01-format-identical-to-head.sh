#!/usr/bin/env bash
# 正例凭证：正常写入的字节与**施工前（git HEAD）的非原子写法**逐字一致（impl-07c AC 4）。
#
# 做法是**真对照**，不是自证：从 `git HEAD:` 取出施工前的 `verify_runner.py`，
# 单独 import 它的 `save_dirty_rate_baseline`，与新实现写进各自目录，再 `cmp` 字节。
#
# 执行面：WSL（uv run python）。
set -uo pipefail
cd "$(dirname "$0")/../../../.." || exit 1   # 仓库根

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

git show HEAD:src/ecomgen/verify_runner.py > "$TMP/old_verify_runner.py" || {
  echo "✗ 取不到施工前的 verify_runner.py"; exit 1; }

echo "▶ 取施工前（HEAD）的写盘函数作对照组：$TMP/old_verify_runner.py"

uv run python - "$TMP" <<'PY'
import importlib.util
import shutil
import sys
from pathlib import Path

root = Path(sys.argv[1])
repo = Path.cwd()

# ── 对照组：HEAD 版本的 save_dirty_rate_baseline（真从 git 取出来的那份）──
spec = importlib.util.spec_from_file_location("old_verify_runner", root / "old_verify_runner.py")
old = importlib.util.module_from_spec(spec)
assert spec.loader is not None
sys.path.insert(0, str(repo / "src"))
sys.modules["old_verify_runner"] = old  # dataclass 要按模块名回查 __dict__
spec.loader.exec_module(old)

# ── 实验组：工作树里的新实现 ──
sys.path.insert(0, str(repo / "src"))
from ecomgen import verify_runner as new  # noqa: E402

sample = new.DirtyRateSample(dlq_offset=14911, input_events=77700)

old_dir = root / "old"
new_dir = root / "new"
old.save_dirty_rate_baseline(old.DirtyRateSample(dlq_offset=14911, input_events=77700), str(old_dir))
new.save_dirty_rate_baseline(sample, str(new_dir))

old_bytes = (old_dir / "dirty-rate-baseline.json").read_bytes()
new_bytes = (new_dir / "dirty-rate-baseline.json").read_bytes()

print(f"施工前（HEAD）落盘字节：{old_bytes!r}")
print(f"施工后（原子）落盘字节：{new_bytes!r}")
print(f"字节数：old={len(old_bytes)} new={len(new_bytes)}")
if old_bytes != new_bytes:
    print("✗ 字节不一致——本票不得改变基线文件的格式")
    raise SystemExit(1)

# 首次落盘（目标不存在）与覆盖落盘（目标已存在）两条路径都要验。
new2 = root / "new_first"
new.save_dirty_rate_baseline(sample, str(new2))
first_bytes = (new2 / "dirty-rate-baseline.json").read_bytes()
new.save_dirty_rate_baseline(sample, str(new2))          # 覆盖
overwrite_bytes = (new2 / "dirty-rate-baseline.json").read_bytes()
print(f"首次落盘 = 覆盖落盘 = HEAD 逐字：{first_bytes == overwrite_bytes == old_bytes}")

# 残留检查：两条路径跑完后目录里只有基线文件本体
for d in (new_dir, new2):
    entries = sorted(p.name for p in d.iterdir())
    print(f"{d.name}/ 目录内容：{entries}")
    assert entries == ["dirty-rate-baseline.json"], f"临时文件残留：{entries}"

# 读者侧零改动可用
loaded = new.load_dirty_rate_baseline(str(new2))
print(f"读侧解析：{loaded}")
assert loaded == sample
print("✓ PASS：落盘方式变了，落盘内容与两条路径的产物都与 HEAD 逐字一致，且读侧零改动可用")
PY
