#!/usr/bin/env bash
# impl-06i 证据 05（环境归位）：最后一轮全门的 ③ 重建了容器 ⇒ usrlib 作业 jar 随
# 可写层丢失，须重拷；并按 06f-4 收尾门的稳态口径把作业恢复到 RUNNING。
# 与 .scratch/ecom-user-behavior-pipeline/evidence/impl-06f-4/02-restore-flink-job.sh 同源。
# bash .scratch/ecom-user-behavior-pipeline/evidence/impl-06i/05-restore-job.sh
set -uo pipefail
cd "/mnt/f/Anaconda practice project/eco-a-d-platform" || exit 1
JAR=flink/target/ecom-flink-0.1.0-SNAPSHOT.jar

echo "START_UTC=$(date -u +%Y-%m-%dT%H:%M:%SZ)"

echo
echo "== 1. 重拷作业 jar 到两端 usrlib =="
for c in eco-flink-jm eco-flink-tm; do
  docker exec "$c" mkdir -p /opt/flink/usrlib
  docker cp "$JAR" "$c":/opt/flink/usrlib/ecom-flink.jar
  docker exec "$c" ls -l /opt/flink/usrlib/ecom-flink.jar
done

echo
echo "== 2. 提交作业（flink run -d，主类 P1RealtimeJob）=="
docker exec eco-flink-jm /opt/flink/bin/flink run -d \
  -c com.ecom.flink.P1RealtimeJob /opt/flink/usrlib/ecom-flink.jar 2>&1 \
  | grep -vE "^WARNING|Unknown module" | tail -8

echo
echo "== 3. 等作业 RUNNING（最多 200s）=="
ok=0
for i in $(seq 1 40); do
  out=$(curl -sf -m 3 http://127.0.0.1:8081/jobs/overview 2>/dev/null)
  case "$out" in *'"state":"RUNNING"'*) echo "RUNNING after $((i * 5))s"; ok=1; break;; esac
  sleep 5
done
if [ "$ok" -ne 1 ]; then echo "TIMEOUT 未达 RUNNING：${out:-<REST 不可达>}"; exit 1; fi

echo
echo "== 4. 等消费组追平（六分区 lag=0，最多 300s）=="
for i in $(seq 1 60); do
  lag_total=$(docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 --describe --group ecom-p1-consumer 2>/dev/null \
    | awk "NR>1 && \$6 ~ /^[0-9]+$/ {s+=\$6} END {print s+0}")
  echo "[$((i * 5))s] lag_total=${lag_total:-?}"
  if [ "${lag_total:-999}" = "0" ]; then echo "LAG=0 已追平"; break; fi
  sleep 5
done

echo "END_UTC=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
