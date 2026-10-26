#!/usr/bin/env bash
# impl-06f-4 第二环：恢复 Flink 作业（check ③ 的 down/up 重建了容器 ⇒ usrlib 的
# 作业 jar 随可写层丢失，须重拷重提；票面验收「跑完后把 Flink 作业恢复到 RUNNING」）。
# 提交面与 .uv-cache/deploy-job.sh 同源：两端 usrlib + `flink run -d`。
# 不做 docker restart（③ 刚重建容器，JVM 是新的，无缓存类风险）。
# bash .scratch/ecom-user-behavior-pipeline/evidence/impl-06f-4/02-restore-flink-job.sh
set -uo pipefail
cd "/mnt/f/Anaconda practice project/eco-a-d-platform" || exit 1
JAR=flink/target/ecom-flink-0.1.0-SNAPSHOT.jar

echo "START_UTC=$(date -u +%Y-%m-%dT%H:%M:%SZ)"

echo
echo "== 1. ③ 重启后的 usrlib 现状（预期：空——可写层不随 down 保留）=="
for c in eco-flink-jm eco-flink-tm; do
  echo "-- $c --"
  docker exec "$c" ls -l /opt/flink/usrlib/ 2>&1
done

echo
echo "== 2. 重拷作业 jar 到两端 usrlib =="
for c in eco-flink-jm eco-flink-tm; do
  docker exec "$c" mkdir -p /opt/flink/usrlib
  docker cp "$JAR" "$c":/opt/flink/usrlib/ecom-flink.jar
done
for c in eco-flink-jm eco-flink-tm; do
  echo "-- $c --"
  docker exec "$c" ls -l /opt/flink/usrlib/ecom-flink.jar 2>&1
done

echo
echo "== 3. 提交作业（flink run -d，主类 P1RealtimeJob）=="
docker exec eco-flink-jm /opt/flink/bin/flink run -d \
  -c com.ecom.flink.P1RealtimeJob /opt/flink/usrlib/ecom-flink.jar 2>&1 \
  | grep -vE "^WARNING|Unknown module" | tail -8

echo
echo "== 4. 等作业 RUNNING（最多 200s）=="
ok=0
for i in $(seq 1 40); do
  out=$(curl -sf -m 3 http://127.0.0.1:8081/jobs/overview 2>/dev/null)
  case "$out" in *'"state":"RUNNING"'*) echo "RUNNING after $((i * 5))s: $out"; ok=1; break;; esac
  sleep 5
done
if [ "$ok" -ne 1 ]; then echo "TIMEOUT 未达 RUNNING：${out:-<REST 不可达>}"; exit 1; fi

echo
echo "== 5. 等消费组追平（六分区 lag=0，最多 300s；06d 案 2：续读 committed offsets）=="
for i in $(seq 1 60); do
  lag_total=$(docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 --describe --group ecom-p1-consumer 2>/dev/null \
    | awk "NR>1 && \$6 ~ /^[0-9]+$/ {s+=\$6} END {print s+0}")
  echo "[$((i * 5))s] lag_total=${lag_total:-?}"
  if [ "${lag_total:-999}" = "0" ]; then echo "LAG=0 已追平"; break; fi
  sleep 5
done

echo
echo "== 6. 源算子水位（R7-i：不得被钉在未来）=="
sleep 10
curl -sf -m 5 "http://127.0.0.1:8081/jobs/overview" 2>/dev/null | head -c 300; echo
echo "END_UTC=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
