#!/usr/bin/env bash
# 在 WSL 内构建 Flink 作业。
#
# ★ 为什么要 `-Djava.net.preferIPv4Stack=true`（2026-10-21 实测）：
#   WSL 的 resolv.conf 指向 172.17.32.1，`repo.maven.apache.org` 只解析出 IPv6，
#   而本机 IPv6 不通 ⇒ Maven 报 "Temporary failure in name resolution"。
#   强制 IPv4 优先即可正常下载（无需改全局 DNS）。
set -uo pipefail
cd "/mnt/f/Anaconda practice project/eco-a-d-platform/flink" || exit 1
export PATH="$HOME/p0-toolchain/usr/share/maven/bin:$HOME/p0-toolchain/usr/lib/jvm/java-17-openjdk-amd64/bin:$PATH"
export MAVEN_OPTS="-Djava.net.preferIPv4Stack=true"
mvn "$@" 2>&1 | tail -60
echo "mvn rc=${PIPESTATUS[0]}"
