#!/usr/bin/env bash
# 守卫强度凭证：code-review 的 Spec 轴用**三处变异**打穿了第一版「读源码黑名单」守卫
# （impl-07c 施工版）。本脚本对每一处变异重跑守卫测试，要求**必红**。
#
#   变异 ① 把落盘写成 `with path.open("w")`（旧黑名单只查 `open(path`，漏掉这种写法）
#   变异 ② 把「先截断」挪进一个被调用的辅助函数（函数体内不再出现该形态）
#   变异 ③ 把 `os.replace` 包进 `except OSError: pass`（静默吞掉票面禁止吞的 OSError）
#
# 另附一条**不变异**的对照：确认守卫在原实现上是绿的（否则「必红」毫无意义）。
set -uo pipefail
cd "$(dirname "$0")/../../../.." || exit 1   # 仓库根

SRC="src/ecomgen/verify_runner.py"
BAK="$(mktemp)"
cp "$SRC" "$BAK"
trap 'cp "$BAK" "$SRC"; rm -f "$BAK"' EXIT

TEST="tests/test_verify_baseline_atomic.py"
run_guard() {
  uv run pytest "$TEST" -q 2>&1 | tail -3
}

echo "▶ 对照：未变异（守卫应为绿）"
if run_guard | grep -q "failed"; then
  echo "✗ 对照臂红了——守卫本身有问题，后面的『必红』不成立"; exit 1
fi
echo "  ✓ 未变异时守卫为绿"

mutate() {
  local label="$1" py="$2"
  cp "$BAK" "$SRC"
  uv run python - "$SRC" <<PY
import pathlib, sys
p = pathlib.Path(sys.argv[1]); s = p.read_text(encoding="utf-8")
$py
p.write_text(s, encoding="utf-8")
PY
  echo
  echo "▶ 变异 $label"
  local out; out="$(run_guard)"
  echo "$out" | sed 's/^/  /'
  if echo "$out" | grep -q "failed"; then
    echo "  ✓ 守卫抓住了变异（必红成立）"
  else
    echo "  ✗ 守卫没抓到变异——这条守卫不够敏感"
    cp "$BAK" "$SRC"; return 1
  fi
  cp "$BAK" "$SRC"
}

RC=0
mutate "① 用 with path.open(\"w\") 落盘" '
old = """        os.replace(temp_path, path)"""
new = """        # 变异①：先截断再写目标文件
        with path.open("w", encoding="utf-8") as fh:
            fh.write(text)
        os.replace(temp_path, path)"""
assert old in s, "变异①锚点找不到"
s = s.replace(old, new, 1)
' || RC=1

mutate "② 把截断挪进辅助函数" '
helper = """

def _truncating_write(path, text):
    with path.open("w", encoding="utf-8") as fh:
        fh.write(text)
    return path
"""
anchor = "def save_dirty_rate_baseline("
assert anchor in s, "变异②锚点找不到"
s = s.replace(anchor, helper.lstrip() + "\n" + anchor, 1)
s = s.replace("        os.replace(temp_path, path)", "        _truncating_write(path, text)\n        os.replace(temp_path, path)", 1)
' || RC=1

mutate "③ os.replace 包进 except OSError: pass" '
old = """        os.replace(temp_path, path)"""
new = """        try:
            os.replace(temp_path, path)
        except OSError:
            pass  # 变异③：静默吞掉票面禁止吞的 OSError"""
assert old in s, "变异③锚点找不到"
s = s.replace(old, new, 1)
' || RC=1

echo
if [ "$RC" -eq 0 ]; then
  echo "✓ PASS：三处变异全部被守卫抓住，原实现对守卫为绿"
else
  echo "✗ FAIL：至少一处变异逃过守卫"
fi
exit "$RC"
