#!/usr/bin/env bash
# 取证（BEFORE）：用**未修复语义**的 shim 跑 R7iFalsification，证明两案反例可机读复现。
# 本脚本退出码 0 = 「反例成立」这件取证完成（与 java 的退出码取反）。
#
# shim 顶替的正是本次新建的 SourceTimePolicy（生产侧改两行语义，判据不变）：
#   startingOffsets()  = OffsetsInitializer.earliest()         ← 旧 GateSegment.java:232
#   watermarkGenerator = forBoundedOutOfOrderness(...) 裸用   ← 旧 withTimestampAssigner
# 故 harness 一字不改地跑在旧语义上 ⇒ 红，跑在新语义上 ⇒ 绿。
# ⚠️ 工具链硬编码：JDK/Maven 走 P0 解包的固定用户路径（Makefile 同源，见
#   Makefile 头注「工具链自举」），与「运行宿主只有一台、路径钉死」的 P0 前提一致；
#   换宿主时随 ~/.p0-toolchain-env.mk 一起改。
set -uo pipefail
REPO="/mnt/f/Anaconda practice project/eco-a-d-platform"
cd "$REPO" || exit 1
CP=$(cat /tmp/_cp.txt)
D=$(mktemp -d)
mkdir -p "$D/shim/com/ecom/flink" "$D/out"
cp scripts/_before_shim/SourceTimePolicy_old.java "$D/shim/com/ecom/flink/SourceTimePolicy.java"

JA=/home/gyx/p0-toolchain/usr/lib/jvm/java-17-openjdk-amd64/bin/javac
JAVA=/home/gyx/p0-toolchain/usr/lib/jvm/java-17-openjdk-amd64/bin/java

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
