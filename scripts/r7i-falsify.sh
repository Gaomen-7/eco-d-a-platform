#!/usr/bin/env bash
# R7-i 三案的**机读判据**（impl-06d）——退出码即判据：0=全绿，非 0=有反例未转绿。
#
# 三案各自的判据落在不同语言侧（判据不能跨语言，但必须能证伪旧行为）：
#   案 1（live 跨度超界）  → tests/test_cli.py（pytest）
#                          test_counterexample_start_aligned_shift_violates_tolerance
#                          test_live_shift_keeps_tail_within_future_tolerance
#   案 2（冷启全量重放）  → R7iFalsification#checkCommittedOffsetsSemantics
#   案 3（时间戳先于质量门）→ R7iFalsification#checkRejectedEventDoesNotAdvanceWatermark
#                          + #checkDirtyNonTimeReasonDoesNotAdvanceWatermark
#
# 案 2/3 的**反例取证**用 scripts/_before_r7i.sh（同一 harness 跑在旧语义 shim 上 ⇒ 必红）。
#
# 为什么不用 JUnit：离线环境没有可跑的 JUnit（见 R7iFalsification 类注释），
# 引入新依赖需要联网，与「离线可复现」冲突 ⇒ javac + java 断言程序。
#
# ★ 2026-10-27 去硬编码（两处）：
#   ① 仓库根：改为**从脚本自身位置推导**（原写死 /mnt/f/Anaconda practice project/…）
#      ⇒ 克隆到任何路径、搬到任何机器都成立；
#   ② JDK / Maven 缓存：原写死 /home/gyx/p0-toolchain/…（gyx 甚至不是通用用户名）
#      ⇒ 改为「PATH → $HOME/p0-toolchain → $HOME/.m2」逐级回落，最后报真因。
set -uo pipefail

# 仓库根：脚本放在 scripts/ 下，故取脚本自身目录的上一级。
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT" || { echo "FATAL: 无法进入仓库根 $REPO_ROOT" >&2; exit 1; }

# ── JDK 解析：与 build-flink.sh 同一判据（**按主版本 17 选，不按位置选**）────
#   ⚠️ 不能「PATH 上有 javac 就用」：本仓宿主 PATH 里是 Windows 侧 JDK 1.8
#      （/e/space/09-tools/Jdk1.8），编译 Flink 2.2.1 必然失败，且报错指向代码
#      而不是工具链。⇒ 优先 p0-toolchain，其次 PATH 但要求主版本 == 17。
P0="$HOME/p0-toolchain"
java_major() { "$1" -version 2>&1 | head -1 | grep -oE '"[0-9]+' | tr -d '"'; }

if [ -x "$P0/usr/lib/jvm/java-17-openjdk-amd64/bin/javac" ]; then
  JDK_BIN="$P0/usr/lib/jvm/java-17-openjdk-amd64/bin"
  echo "[r7i-falsify] JDK：p0-toolchain（17）"
elif command -v javac >/dev/null 2>&1 && [ "$(java_major "$(command -v java)")" = "17" ]; then
  JDK_BIN="$(dirname "$(command -v javac)")"
  echo "[r7i-falsify] JDK：PATH 上的系统 JDK（主版本 17）"
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

# Maven 本地缓存：尊重 MAVEN_REPO（可覆盖），否则 $HOME/.m2/repository。
M2="${MAVEN_REPO:-$HOME/.m2/repository}"
if [ ! -d "$M2" ]; then
  echo "FATAL: 找不到 Maven 本地仓库 $M2（本判据靠它构造离线 classpath）。" >&2
  echo "  先构建过作业即可生成（bash scripts/build-flink.sh -DskipTests package），" >&2
  echo "  或用 MAVEN_REPO=/path/to/.m2/repository 指定。" >&2
  exit 1
fi

# classpath 由本地 Maven 缓存构造（离线；不联网、不依赖 jar 是否已 package）
# ★ 缓存文件放 $TMPDIR（不是写死 /tmp/_cp.txt）：多用户/沙箱环境下 /tmp 未必可写，
#   且写死单一路径会让不同仓库实例互相串味。key 里带 M2 路径的短哈希以区分缓存源。
CP_CACHE="${TMPDIR:-/tmp}/_r7i_cp_$(printf '%s' "$M2" | cksum | cut -d' ' -f1).txt"
if [ ! -s "$CP_CACHE" ]; then
  CP=$(find "$M2/org/apache/flink" -name "*.jar" ! -name "*tests*" ! -name "*sources*" 2>/dev/null | tr '\n' ':')
  CP="$CP"$(find "$M2/org/apache/kafka" -name "*.jar" ! -name "*tests*" ! -name "*sources*" 2>/dev/null | tr '\n' ':')
  CP="$CP"$(find "$M2/com/google/code/gson" "$M2/org/slf4j" "$M2/com/esotericsoftware" \
    -name "*.jar" 2>/dev/null | tr '\n' ':')
  if [ -z "${CP//:/}" ]; then
    echo "FATAL: 在 $M2 下没找到 Flink/Kafka 依赖 jar，classpath 为空。" >&2
    echo "  先跑一次构建（bash scripts/build-flink.sh -DskipTests package）把依赖拉下来。" >&2
    exit 1
  fi
  echo "$CP" > "$CP_CACHE"
fi
CP=$(cat "$CP_CACHE")

D=$(mktemp -d)
mkdir -p "$D/out"
SRC=$(find flink/src/main/java -name "*.java" | tr '\n' ' ')
if ! $JA -nowarn -d "$D/out" -cp "$CP" $SRC \
      flink/src/test/java/com/ecom/flink/R7iFalsification.java; then
  echo "[r7i-falsify] ✗ javac 失败"
  exit 1
fi
$JAVA -cp "$D/out:$CP" com.ecom.flink.R7iFalsification 2>&1 | grep -v '^SLF4J'
exit "${PIPESTATUS[0]}"
