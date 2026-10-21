#!/usr/bin/env bash
# 一次性构建：解析依赖 + package（IPv4 优先，见 build-flink.sh 注释）。
set -uo pipefail
cd "/mnt/f/Anaconda practice project/eco-a-d-platform/flink" || exit 1
export PATH="$HOME/p0-toolchain/usr/share/maven/bin:$HOME/p0-toolchain/usr/lib/jvm/java-17-openjdk-amd64/bin:$PATH"
export MAVEN_OPTS="-Djava.net.preferIPv4Stack=true"
mvn package -DskipTests 2>&1 | tail -45
echo "mvn rc=${PIPESTATUS[0]}"
