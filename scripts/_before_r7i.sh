#!/usr/bin/env bash
# 取证（BEFORE）：用**未修复语义**的 shim 跑 R7iFalsification，证明两案反例可机读复现。
# 本脚本退出码 0 = 「反例成立」这件取证完成（与 java 的退出码取反）。
#
# shim 顶替的正是本次新建的 SourceTimePolicy（生产侧改两行语义，判据不变）：
#   startingOffsets()  = OffsetsInitializer.earliest()         ← 旧 GateSegment.java:232
#   watermarkGenerator = forBoundedOutOfOrderness(...) 裸用   ← 旧 withTimestampAssigner
# 故 harness 一字不改地跑在旧语义上 ⇒ 红，跑在新语义上 ⇒ 绿。
#
# ★ 2026-10-27 去硬编码（两处），与 scripts/r7i-falsify.sh 同款：
#   ① 仓库根从脚本自身位置推导（原写死 /mnt/f/…）；
#   ② JDK 从 PATH / $HOME/p0-toolchain 逐级回落（原写死 /home/gyx/p0-toolchain/…）；
#   ③ classpath 缓存改为与 r7i-falsify.sh **同一个** $TMPDIR 下的派生路径
#      （原两边共用写死的 /tmp/_cp.txt）。
#   ⚠️ ③ 必须与 r7i-falsify.sh 保持同一算法，否则本脚本会读到别的缓存或读不到。
set -uo pipefail

# 仓库根：脚本放在 scripts/ 下，故取脚本自身目录的上一级。
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT" || { echo "FATAL: 无法进入仓库根 $REPO_ROOT" >&2; exit 1; }

# ── JDK 解析：与 r7i-falsify.sh / build-flink.sh 同一判据 ──────────────────
#   按主版本 17 选，不按位置选（宿主 PATH 里可能是 Windows 侧 JDK 1.8）。
P0="$HOME/p0-toolchain"
java_major() { "$1" -version 2>&1 | head -1 | grep -oE '"[0-9]+' | tr -d '"'; }

if [ -x "$P0/usr/lib/jvm/java-17-openjdk-amd64/bin/javac" ]; then
  JDK_BIN="$P0/usr/lib/jvm/java-17-openjdk-amd64/bin"
elif command -v javac >/dev/null 2>&1 && [ "$(java_major "$(command -v java)")" = "17" ]; then
  JDK_BIN="$(dirname "$(command -v javac)")"
else
  echo "FATAL: 没有可用的 JDK 17。本判据需要 JDK 17（Flink 2.2 锚点）。" >&2
  if command -v javac >/dev/null 2>&1; then
    echo "  检测到 PATH 上有 javac，但其 java 主版本 = $(java_major "$(command -v java)" 2>/dev/null || echo '?')。" >&2
  fi
  echo "  有 sudo：  sudo apt-get install -y openjdk-17-jdk-headless" >&2
  echo "  无 sudo：  bash scripts/p0-wsl-install-nodeb.sh && bash scripts/p0-wsl-fix-symlinks.sh" >&2
  exit 1
fi
JA="$JDK_BIN/javac"
JAVA="$JDK_BIN/java"

# ── classpath 缓存：算法必须与 r7i-falsify.sh 逐字一致 ─────────────────────
M2="${MAVEN_REPO:-$HOME/.m2/repository}"
CP_CACHE="${TMPDIR:-/tmp}/_r7i_cp_$(printf '%s' "$M2" | cksum | cut -d' ' -f1).txt"
if [ ! -s "$CP_CACHE" ]; then
  echo "FATAL: classpath 缓存不存在：$CP_CACHE" >&2
  echo "  本脚本设计为在 scripts/r7i-falsify.sh 之后跑（由它构造 classpath）。" >&2
  echo "  先跑：bash scripts/r7i-falsify.sh" >&2
  exit 1
fi
CP=$(cat "$CP_CACHE")

D=$(mktemp -d)
mkdir -p "$D/shim/com/ecom/flink" "$D/out"
cp scripts/_before_shim/SourceTimePolicy_old.java "$D/shim/com/ecom/flink/SourceTimePolicy.java"

# ★ 生产源码里**排除**新的 SourceTimePolicy（用 shim 顶替），其余原样编译
SRC=$(find flink/src/main/java -name "*.java" ! -name "SourceTimePolicy.java" | tr '\n' ' ')
if ! $JA -nowarn -d "$D/out" -cp "$CP" $SRC \
      "$D/shim/com/ecom/flink/SourceTimePolicy.java" \
      flink/src/test/java/com/ecom/flink/R7iFalsification.java; then
  echo "[BEFORE] javac 失败"
  exit 1
fi

set +e
$JAVA -cp "$D/out:$CP" com.ecom.flink.R7iFalsification 2>&1 | grep -v '^SLF4J' | tail -20
rc_java=${PIPESTATUS[0]}
set -e
echo "java rc=$rc_java（期望非 0：旧语义下反例成立）"
if [ "$rc_java" -eq 0 ]; then
  echo "[BEFORE] ✗ 旧语义居然全绿 —— 反例打不到旧缺陷，判据不成立"
  exit 1
fi
echo "[BEFORE] ✓ 旧语义按预期红（两案反例可机读复现）"
exit 0
