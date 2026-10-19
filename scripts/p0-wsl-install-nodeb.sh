#!/usr/bin/env bash
# 无 sudo 安装 make / JDK17 / Maven：下 .deb 后用 dpkg-deb -x 解到用户目录。
# 背景：WSL 侧 sudo 需交互密码，agent 无法代劳；但 .deb 可以用户级解包使用。
set -uo pipefail
WORK="$HOME/p0-debs"
PREFIX="$HOME/p0-toolchain"
mkdir -p "$WORK" "$PREFIX"

echo "== 1. 解析 .deb URL 清单 =="
apt-get install --print-uris -y make openjdk-17-jdk-headless maven 2>/dev/null \
  | grep -oE "http[^']+\.deb" | sort -u > "$WORK/urls.txt"
echo "   deb 数量: $(wc -l < "$WORK/urls.txt")"

echo "== 2. 下载（IPv4 强制 + 重试）=="
cd "$WORK"
while read -r url; do
  f=$(basename "$url")
  if [[ -s "$f" ]]; then echo "   已有 $f"; continue; fi
  printf '   下载 %s ... ' "$f"
  if curl -4 -LsSf --retry 3 --retry-delay 2 -o "$f" "$url"; then
    echo "OK ($(stat -c%s "$f") B)"
  else
    echo "FAIL"
  fi
done < "$WORK/urls.txt"

echo "== 3. 解包到 $PREFIX =="
for f in "$WORK"/*.deb; do
  [[ -e "$f" ]] || continue
  dpkg-deb -x "$f" "$PREFIX" 2>/dev/null && echo "   解包 $(basename "$f")"
done

echo "== 4. 核对产物 =="
for p in usr/bin/make usr/lib/jvm/java-17-openjdk-amd64/bin/java usr/share/maven/bin/mvn; do
  if [[ -e "$PREFIX/$p" ]]; then echo "   OK  $p"; else echo "   MISSING  $p"; fi
done

echo "== 5. 环境变量脚本 =="
cat > "$HOME/.p0-toolchain-env.sh" <<'EOF'
# 由 scripts/p0-wsl-install-nodeb.sh 生成：把用户级解包的工具放进 PATH
export P0_TOOLCHAIN="$HOME/p0-toolchain"
export JAVA_HOME="$P0_TOOLCHAIN/usr/lib/jvm/java-17-openjdk-amd64"
export M2_HOME="$P0_TOOLCHAIN/usr/share/maven"
export PATH="$P0_TOOLCHAIN/usr/bin:$JAVA_HOME/bin:$M2_HOME/bin:$PATH"
EOF
echo "   已写 $HOME/.p0-toolchain-env.sh"
cat "$HOME/.p0-toolchain-env.sh"
