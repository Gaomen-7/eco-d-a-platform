#!/usr/bin/env bash
# impl-06i 证据 03（反例凭证 B，seg2 腿）：向 scripts/ 注入一条「宿主路径 jar 引用」
# （不含 ADD JAR 字样）⇒ 判据 ⑤ 的附加扫描段必须抓到、⑤ 必 FAIL。
# 必要性：本票把 seg2 也切到了白名单（旧口径下 seg2 被 .mimosa/ 18 行基线副本打成
# 恒假，seg1 先 FAIL 把它挡住了——票面未记，本票复盘新发现），故 seg2 也要反例。
# bash .scratch/ecom-user-behavior-pipeline/evidence/impl-06i/03-red-sh-hostpath.sh
set -uo pipefail
cd "/mnt/f/Anaconda practice project/eco-a-d-platform" || exit 1

unset COMPOSE_EXTRA_FILES
echo "COMPOSE_EXTRA_FILES=[${COMPOSE_EXTRA_FILES:-}]（期望为空：不设 override）"
echo "START_UTC=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
start=$(date +%s)

echo
echo "== 0. 预置 usrlib 作业 jar（上一轮 ③ 的 down/up 重建容器会清掉可写层；④ 需要它）=="
for c in eco-flink-jm eco-flink-tm; do
  docker exec "$c" mkdir -p /opt/flink/usrlib
  docker cp flink/target/ecom-flink-0.1.0-SNAPSHOT.jar "$c":/opt/flink/usrlib/ecom-flink.jar
  docker exec "$c" ls -l /opt/flink/usrlib/
done

echo
echo "== 1. 注入反例探针：scripts/zz_impl06i_red.sh（宿主路径 jar 引用，无 ADD JAR）=="
PROBE="scripts/zz_impl06i_red.sh"
printf 'HOST_JAR=/mnt/f/zz/impl06i-red.jar\n' > "$PROBE"
echo "探针内容："
cat "$PROBE"

echo
echo "== 2. 全门实跑（期望：⑤ FAIL 且只有 ⑤ FAIL，且 FAIL 文案是 seg2 的「有文件引用宿主路径的 jar」；退出码 1）=="
bash scripts/deploy_surface_check.sh
rc=$?

echo
echo "== 3. 还原：删除探针 =="
rm -f "$PROBE"
if [ -e "$PROBE" ]; then echo "✗ 探针删除失败"; else echo "探针已删除"; fi

end=$(date +%s)
echo
echo "EXIT_CODE=$rc"
echo "ELAPSED_SECONDS=$((end - start))"
echo "END_UTC=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
exit 0
