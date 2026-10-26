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
set -uo pipefail
REPO="/mnt/f/Anaconda practice project/eco-a-d-platform"
cd "$REPO" || exit 1

JA=/home/gyx/p0-toolchain/usr/lib/jvm/java-17-openjdk-amd64/bin/javac
JAVA=/home/gyx/p0-toolchain/usr/lib/jvm/java-17-openjdk-amd64/bin/java
M2=/home/gyx/.m2/repository

# classpath 由本地 Maven 缓存构造（离线；不联网、不依赖 jar 是否已 package）
if [ ! -s /tmp/_cp.txt ]; then
  CP=$(find "$M2/org/apache/flink" -name "*.jar" ! -name "*tests*" ! -name "*sources*" | tr '\n' ':')
  CP="$CP"$(find "$M2/org/apache/kafka" -name "*.jar" ! -name "*tests*" ! -name "*sources*" | tr '\n' ':')
  CP="$CP"$(find "$M2/com/google/code/gson" "$M2/org/slf4j" "$M2/com/esotericsoftware" \
    -name "*.jar" 2>/dev/null | tr '\n' ':')
  echo "$CP" > /tmp/_cp.txt
fi
CP=$(cat /tmp/_cp.txt)

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
