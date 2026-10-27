#!/usr/bin/env bash
# impl-06i 证据 04（改后必绿）：干净树全门实跑 —— 不设 override，期望 11/11 PASS、退 0。
# 票面验收：「实施后不设 override 复跑 deploy_surface_check.sh：⑤ PASS 且其余 10 条不回归」。
# bash .scratch/ecom-user-behavior-pipeline/evidence/impl-06i/04-clean-full-gate.sh
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
echo "== 1. 全门实跑（期望：11/11 PASS，退出码 0）=="
bash scripts/deploy_surface_check.sh
rc=$?

end=$(date +%s)
echo
echo "EXIT_CODE=$rc"
echo "ELAPSED_SECONDS=$((end - start))"
echo "END_UTC=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
exit $rc
