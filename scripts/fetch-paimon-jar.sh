#!/usr/bin/env bash
# 预取 Paimon bundled connector jar 到 docker/flink/lib/（构建上下文内）。
#
# 为什么有这支脚本（与 docker/flink/Dockerfile 的注释互为一对）：
#   Dockerfile **不在构建期联网下载** jar —— 构建期联网不可复现，且离线重跑必然失败。
#   于是下载这一步被**前置**到这里：先跑本脚本把 jar 落进构建上下文，再 docker build。
#   构建期只剩一次静默的 COPY ⇒ 镜像内容由**上下文里的字节**决定，可离线重建。
#
# 幂等：已存在且 size 与 sha256 **都**匹配 ⇒ 打印 SKIP 并退出 0（不重复下载 57 MB）。
#       任一不匹配 ⇒ 视为损坏/被替换，重新下载覆盖。
# 失败即非 0：下载不完整、或校验不过，都会报错退出（**绝不**留下半个 jar 冒充有效产物）。
#
# ★ 为什么是 repo1.maven.org，而不是 repo.maven.apache.org（2026-10-21 实测）：
#   WSL 的 resolv.conf 指向 172.17.32.1，`repo.maven.apache.org` **只解析出 IPv6**，
#   而本机 IPv6 不通 ⇒ 直接 "Temporary failure in name resolution"（不是 404，是解析失败）。
#   同一个 Maven Central 的 `repo1.maven.org` 走 IPv4 正常。
#   见 scripts/build-flink.sh 顶部同款注释（那里是 mvn 侧，用 -Djava.net.preferIPv4Stack=true）。
#   本脚本额外加 `-4`（curl 强制 IPv4）：即便某天 repo1 也返回 AAAA 记录，也不会踩同一个坑。
#
# ★ 为什么先下 .part 再 mv：
#   curl 中途断线/被杀会留下**截断的 jar**。若直接写目标名，下次运行的幂等判断会把它
#   当"已存在的 jar"去校验——校验能挡住（size/sha256 不符），但若有人跳过本脚本直接用
#   该文件构建，坏 jar 就进了镜像。用 .part 后缀 + 只在**校验通过后**才 mv，可以让
#   "目标路径存在 ⇒ 它一定是完整且校验过的"成为一条不变式。
#   .part 文件若因异常残留，下次运行会被覆盖（curl -o 直接截断重写），无需手工清理。
#
# 期望值（本仓钉死的**设计值**，非官方保证）：
#   size   = 57697871 B        （实测 2026-10-22；Paimon 官方未公布该 artifact 的校验值）
#   sha256 = 43a6416a241ab736fb6ed2b6061a37fc5cca5ba8737d77eac1b977f859107dd1（实测）
#   ⚠️ 这两个值一旦要改，必须**同时**改 docker/flink/Dockerfile 里的 LABEL 与 RUN 自检，
#      否则构建期自检会以 sha256 不符失败（这是刻意的双处钉死）。
#
# 用法：bash scripts/fetch-paimon-jar.sh
# 覆盖下载：先 rm docker/flink/lib/paimon-flink-2.2-2.1.0.jar 再跑本脚本。

set -uo pipefail

# 仓库根：脚本放在 scripts/ 下，故取脚本自身目录的上一级。
# 不依赖调用者的 cwd（从 WSL、Git Bash 或仓库根之外的任何位置调用都成立）。
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

JAR_NAME="paimon-flink-2.2-2.1.0.jar"
DEST_DIR="$REPO_ROOT/docker/flink/lib"
DEST="$DEST_DIR/$JAR_NAME"
URL="https://repo1.maven.org/maven2/org/apache/paimon/paimon-flink-2.2/2.1.0/$JAR_NAME"

EXPECTED_SIZE=57697871
EXPECTED_SHA256="43a6416a241ab736fb6ed2b6061a37fc5cca5ba8737d77eac1b977f859107dd1"

# sha256sum 在 Git Bash / WSL 都有（coreutils）。没有就直接失败，不要静默降级成"只查大小"。
if ! command -v sha256sum >/dev/null 2>&1; then
  echo "ERROR: sha256sum 不可用；本脚本的校验不可省略（不接受无校验下载）" >&2
  exit 1
fi

actual_size() { wc -c < "$1" | tr -d ' \r'; }
actual_sha256() { sha256sum "$1" | cut -d' ' -f1; }

# ── 幂等快路径：已存在且 size + sha256 都匹配 ⇒ SKIP ─────────────────────────
if [ -f "$DEST" ]; then
  size="$(actual_size "$DEST")"
  sha="$(actual_sha256 "$DEST")"
  if [ "$size" = "$EXPECTED_SIZE" ] && [ "$sha" = "$EXPECTED_SHA256" ]; then
    echo "SKIP: $DEST 已存在且校验通过"
    echo "  size   = $size  (期望 $EXPECTED_SIZE)"
    echo "  sha256 = $sha"
    exit 0
  fi
  echo "WARN: $DEST 已存在但校验不符，将重新下载" >&2
  echo "  size   = $size  (期望 $EXPECTED_SIZE)" >&2
  echo "  sha256 = $sha" >&2
fi

# ── 下载到 .part，校验通过后才 mv 到目标名 ──────────────────────────────────
mkdir -p "$DEST_DIR"
PART="$DEST.part"
# 上一次异常退出可能留下 .part；显式删掉，避免把旧字节续进新文件。
rm -f "$PART"

echo "GET $URL"
echo " -> $PART"
# -f：HTTP 4xx/5xx 直接失败（否则会把错误页写成 jar）
# -L：跟随重定向（Maven Central 有时重定向到 CDN）
# -S：失败时打印错误
# -s：静默进度条
# -4：强制 IPv4（理由见文件头注释）
# --retry：网络抖动重试（repo1 偶发 5xx），5 次 + 指数退避
if ! curl -fsSL -4 --retry 5 --retry-delay 3 --retry-all-errors -o "$PART" "$URL"; then
  echo "ERROR: 下载失败：$URL" >&2
  rm -f "$PART"
  exit 1
fi

size="$(actual_size "$PART")"
sha="$(actual_sha256 "$PART")"

if [ "$size" != "$EXPECTED_SIZE" ]; then
  echo "ERROR: size 不符 —— 期望 $EXPECTED_SIZE，实际 $size" >&2
  rm -f "$PART"
  exit 1
fi
if [ "$sha" != "$EXPECTED_SHA256" ]; then
  echo "ERROR: sha256 不符 —— 期望 $EXPECTED_SHA256，实际 $sha" >&2
  rm -f "$PART"
  exit 1
fi

mv -f "$PART" "$DEST"

echo "OK: 已获取并校验 $DEST"
echo "  size   = $size  (期望 $EXPECTED_SIZE)"
echo "  sha256 = $sha"
