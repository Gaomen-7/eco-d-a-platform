#!/usr/bin/env bash
# impl-06i 证据 02（反例凭证 A）：向 sql/ 注入一条真 ADD JAR '<宿主路径>' ⇒ 判据 ⑤ 必 FAIL。
# 同时证明「收窄白名单没有把真缺陷放走」；其余 10 条应照绿（不设 override）。
# 执行面：WSL。副作用：条目 ③ 会 down（不带 -v）+ up -d，全部容器重启。
# bash .scratch/ecom-user-behavior-pipeline/evidence/impl-06i/02-red-sql-addjar.sh
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
echo "== 1. 注入反例探针：sql/zz_impl06i_red.sql（一条真 ADD JAR 宿主路径语句）=="
PROBE="sql/zz_impl06i_red.sql"
printf "ADD JAR '/mnt/f/Anaconda practice project/eco-a-d-platform/flink/target/ecom-flink-0.1.0-SNAPSHOT.jar';\n" > "$PROBE"
echo "探针内容："
cat "$PROBE"

echo
echo "== 2. 全门实跑（期望：⑤ FAIL 且只有 ⑤ FAIL；退出码 1）=="
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
# 证据脚本自身恒退 0：门的 rc 是预期 FAIL，判读看 EXIT_CODE 行
exit 0
