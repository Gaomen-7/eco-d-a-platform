#!/usr/bin/env bash
# 在 WSL 内构建 Flink 作业。
#
# ★ 为什么要 `-Djava.net.preferIPv4Stack=true`（2026-10-21 实测）：
#   WSL 的 resolv.conf 指向 172.17.32.1，`repo.maven.apache.org` 只解析出 IPv6，
#   而本机 IPv6 不通 ⇒ Maven 报 "Temporary failure in name resolution"。
#   强制 IPv4 优先即可正常下载（无需改全局 DNS）。
#
# ★ 仓库根**从脚本自身位置推导**（2026-10-27 改，原为硬编码
#   `/mnt/f/Anaconda practice project/eco-a-d-platform/flink`）：
#   硬编码使「克隆到别的路径」或「整目录搬到另一台机器」后本脚本必然
#   cd 失败。与 scripts/fetch-paimon-jar.sh、deploy_surface_check.sh 同款写法，
#   不依赖调用者的 cwd，也不依赖宿主路径形态（WSL / 原生 Linux 都成立）。
#
# ★ JDK/Maven 的查找顺序（2026-10-27 改，原为硬编码 $HOME/p0-toolchain）：
#   ① 环境已有可用 mvn ⇒ 直接用（apt 装的、或 ~/.p0-toolchain-env.sh 已 source）；
#   ② 否则回落到 $HOME/p0-toolchain（scripts/p0-wsl-install-nodeb.sh 产出）；
#   ③ 都没有 ⇒ 报真因并给出两条修法，而不是让 mvn 以 "command not found" 失败。
#   这样同一份脚本在「apt 装过」与「无 sudo 解包」两种机器上都能用。
set -uo pipefail

# 仓库根：脚本放在 scripts/ 下，故取脚本自身目录的上一级。
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT/flink" || { echo "FATAL: 进不去 $REPO_ROOT/flink" >&2; exit 1; }

# ── 工具链解析 ────────────────────────────────────────────────────────────
P0="$HOME/p0-toolchain"

# ★★ 选择顺序（2026-10-27 定稿，别改回「PATH 优先」）★★
#   **按 JDK 是否是 17 来选，不按位置选。**
#   为什么不能「PATH 上有 mvn 就直接用」（第一版改法踩到的回归）：
#     本仓宿主 PATH 里有 **Windows 侧的 Maven 3.6 + JDK 1.8**
#     （实测 /e/space/09-tools/Maven3.6/bin/mvn、/e/space/09-tools/Jdk1.8）。
#     Flink 2.2.1 需要 JDK 17 ⇒ 用它构建必然失败，而失败信息是 Maven 内部的
#     编译器报错，指向「代码有问题」而不是「工具链选错了」——正是
#     p0-wsl-install.wizard.sh:269-270 早就警告过的坑：
#       「另请确认 mvn 不再借用 Windows 侧的 Maven 3.6」
#   ⇒ 判据是「这套 mvn 背后的 java 是不是 17」，命中才用；否则继续找。
#
#   ① $HOME/p0-toolchain（无 sudo 解包产物，JDK 恰为 17）
#   ② PATH 上的 mvn —— 仅当其 java 主版本 == 17 时采用（apt 安装属这一支）
#   ③ 都没有 ⇒ 报真因 + 两条修法
java_major() { "$1" -version 2>&1 | head -1 | grep -oE '"[0-9]+' | tr -d '"'; }

MVN_BIN=""
if [ -x "$P0/usr/share/maven/bin/mvn" ] && [ -x "$P0/usr/lib/jvm/java-17-openjdk-amd64/bin/java" ]; then
  export JAVA_HOME="$P0/usr/lib/jvm/java-17-openjdk-amd64"
  export PATH="$P0/usr/share/maven/bin:$JAVA_HOME/bin:$PATH"
  MVN_BIN="$P0/usr/share/maven/bin/mvn"
  echo "[build-flink] 工具链：p0-toolchain（JDK 17 + Maven，无 sudo 解包）"
elif command -v mvn >/dev/null 2>&1 && [ "$(java_major "$(command -v java)")" = "17" ]; then
  MVN_BIN="$(command -v mvn)"
  echo "[build-flink] 工具链：PATH 上的系统 Maven（java 主版本 17）"
else
  echo "FATAL: 没有可用的 JDK 17 + Maven 组合。构建 Flink 作业需要 JDK 17 + Maven ≥3.8.6。" >&2
  if command -v mvn >/dev/null 2>&1; then
    echo "  检测到 PATH 上有 mvn，但其 java 主版本 = $(java_major "$(command -v java)" 2>/dev/null || echo '?')（本项目锚点是 17）。" >&2
    echo "  ⚠️ 本仓宿主常见形态：PATH 里是 Windows 侧 Maven 3.6 + JDK 1.8，用它构建必失败。" >&2
  fi
  echo "  有 sudo：  sudo apt-get install -y openjdk-17-jdk-headless maven" >&2
  echo "  无 sudo：  bash scripts/p0-wsl-install-nodeb.sh && bash scripts/p0-wsl-fix-symlinks.sh" >&2
  exit 1
fi

export MAVEN_OPTS="-Djava.net.preferIPv4Stack=true"
"$MVN_BIN" "$@" 2>&1 | tail -60
echo "mvn rc=${PIPESTATUS[0]}"
