#!/usr/bin/env bash
# 修好用户级解包工具链的「绝对符号链接」问题（第二版：直接替换悬空链接）。
#
# 背景：.deb 的目录布局假设自己被装到 /（如 JDK conf/security/java.security →
# /etc/java-17-openjdk/security/java.security；Maven bin/m2.conf → /etc/maven/m2.conf）。
# 用户级 dpkg-deb -x 到 ~/p0-toolchain 后，这些**绝对**链接全部悬空。
# 处置：把悬空链接删掉，重指到 $PREFIX 内的实体文件。
set -uo pipefail
P="$HOME/p0-toolchain"
J="$P/usr/lib/jvm/java-17-openjdk-amd64"
MV="$P/usr/share/maven"

retarget() {  # retarget <link> <real-file>
  local link="$1" real="$2"
  if [[ -e "$real" ]]; then
    rm -f "$link"
    ln -s "$real" "$link"
    echo "   ✓ $(echo "$link" | sed "s|$P/||")  ->  $(echo "$real" | sed "s|$P/||")"
  else
    echo "   ⚠ 实体缺失，跳过：$real"
  fi
}

echo "== 1. JDK：conf 下所有悬空绝对链接 =="
# 链接目标已先被上一轮脚本改写过 ⇒ 只处理**仍然悬空**且指向 /etc 的。
# 注意：tgt 若已是 "$P/etc/..." 形（含前缀），不能再拼一次 $P（否则变双前缀）。
while IFS= read -r link; do
  [[ -e "$link" ]] && continue            # 已修好，跳过
  tgt=$(readlink "$link")
  [[ "$tgt" == /* ]] || continue
  real="$P$tgt"
  [[ -e "$real" ]] || real="$tgt"         # 目标本就带前缀时直接用
  retarget "$link" "$real"
done < <(find "$J/conf" -type l 2>/dev/null)

echo "== 2. Maven：bin/m2.conf =="
retarget "$MV/bin/m2.conf" "$P/etc/maven/m2.conf"

echo "== 3. Maven：conf/logging（mvn 读 $MV/conf/logging，须为实体）=="
if [[ -d "$P/etc/maven/logging" ]]; then
  rm -rf "$MV/conf/logging" 2>/dev/null
  cp -r "$P/etc/maven/logging" "$MV/conf/logging"
  echo "   ✓ conf/logging 已复制为实体"
fi

echo
echo "== 复验 =="
# shellcheck disable=SC1090
source "$HOME/.p0-toolchain-env.sh"
printf "   java  : "; java -version 2>&1 | head -1
printf "   javac : "; javac -version 2>&1 | head -1
printf "   mvn   : "; mvn -version 2>&1 | head -1
printf "   make  : "; make --version 2>&1 | head -1
