#!/usr/bin/env bash
# impl-06f-4 前置状态取证（跑判据链之前的环境快照；非破坏性）。
# 执行面：WSL。bash .scratch/ecom-user-behavior-pipeline/evidence/impl-06f-4/00-preconditions.sh
set -uo pipefail
cd "/mnt/f/Anaconda practice project/eco-a-d-platform" || exit 1

echo "== 0. 时间与 COMPOSE_EXTRA_FILES =="
date -u +"NOW_UTC=%Y-%m-%dT%H:%M:%SZ"
echo "COMPOSE_EXTRA_FILES=[${COMPOSE_EXTRA_FILES:-}]（票面要求：本票全程不设）"

echo
echo "== 1. compose ps（--profile doris，含一次性服务）=="
MSYS_NO_PATHCONV=1 docker compose --profile doris ps -a 2>&1

echo
echo "== 2. Flink 作业状态（跑前记录；check ③ 重启会杀 RUNNING 作业）=="
echo "-- /jobs/overview --"
curl -sf -m 5 http://127.0.0.1:8081/jobs/overview 2>&1 || echo "(REST 不可达)"
echo

echo
echo "== 3. 作业 jar 在 usrlib（两端）=="
for c in eco-flink-jm eco-flink-tm; do
  echo "-- $c --"
  docker exec "$c" ls -l /opt/flink/usrlib/ 2>&1
done

echo
echo "== 4. host 侧构建产物（重提作业用）=="
ls -l flink/target/ecom-flink-0.1.0-SNAPSHOT.jar 2>&1

echo
echo "== 5. topic 端点 offset + 最晚 event_time（impl-05 §9.1 时钟约束）=="
total_in=0; total_dlq=0; maxet=0
for p in 0 1 2 3 4 5; do
  end=$(docker compose exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server localhost:9092 --topic ecom.behavior.event.v1 --partitions $p 2>/dev/null | cut -d: -f3)
  total_in=$((total_in + end))
  start=$((end > 2 ? end - 2 : 0))
  et=$(docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 --topic ecom.behavior.event.v1 --partition $p \
    --offset "$start" --max-messages 2 --timeout-ms 8000 2>/dev/null \
    | grep -oE '"event_time": [0-9]+' | grep -oE '[0-9]+' | sort -n | tail -1)
  [ -n "$et" ] && [ "$et" -gt "$maxet" ] && maxet=$et
done
for p in 0 1 2 3 4 5; do
  end=$(docker compose exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server localhost:9092 --topic ecom.behavior.event.v1.dlq --partitions $p 2>/dev/null | cut -d: -f3)
  total_dlq=$((total_dlq + end))
done
echo "INPUT_TOTAL_OFFSET=$total_in"
echo "DLQ_TOTAL_OFFSET=$total_dlq"
echo "MAX_EVENT_TIME=$maxet"
echo "MAX_ET_UTC=$(date -u -d @$((maxet / 1000)) +%Y-%m-%dT%H:%M:%SZ)"
echo "WALLCLOCK_MINUS_MAX_ET_SECONDS=$(( $(date -u +%s) - maxet / 1000 ))（须 > 120s 才可安全重提作业）"

echo
echo "== 6. 脏率基线（verify 将与之比较）=="
cat artifacts/verify/dirty-rate-baseline.json 2>&1
echo

echo "== 7. 冻结端口连通性预探（TCP 层；对照 README 已知环境限制）=="
for port in 8030 8040 8081 8088 9010 9030 9060 9092; do
  if timeout 3 bash -c "echo > /dev/tcp/127.0.0.1/$port" 2>/dev/null; then
    echo "port $port: TCP 可达"
  else
    echo "port $port: TCP 不可达"
  fi
done
