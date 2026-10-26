#!/usr/bin/env bash
# impl-06f-4 第五环：判据链跑完后的收尾快照（作业 RUNNING + 全栈健康 + usrlib 终态）。
# bash .scratch/ecom-user-behavior-pipeline/evidence/impl-06f-4/05-final-state.sh
set -uo pipefail
cd "/mnt/f/Anaconda practice project/eco-a-d-platform" || exit 1

echo "FINAL_UTC=$(date -u +%Y-%m-%dT%H:%M:%SZ)"

echo
echo "== 1. Flink 作业终态（票面：不留半死状态）=="
curl -sf -m 5 http://127.0.0.1:8081/jobs/overview 2>&1 | python3 -c '
import json, sys
d = json.load(sys.stdin)
for j in d.get("jobs", []):
    print("jid=%s state=%s tasks=%s" % (j["jid"], j["state"], j.get("tasks")))
' 2>&1 || curl -sf -m 5 http://127.0.0.1:8081/jobs/overview 2>&1

echo
echo "== 2. 消费组 lag 终态 =="
docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group ecom-p1-consumer 2>&1

echo
echo "== 3. 全栈健康 =="
MSYS_NO_PATHCONV=1 docker compose --profile doris ps 2>&1

echo
echo "== 4. usrlib 终态（判据 ④ 口径的延续面）=="
for c in eco-flink-jm eco-flink-tm; do
  echo "-- $c --"
  docker exec "$c" ls -l /opt/flink/usrlib/ 2>&1
done

echo
echo "== 5. 判据 ③ 的探针持久性复核（down/up 后卷内探针仍在）=="
ls -la /tmp/eco-deploy-surface-check/ 2>&1 | tail -5
