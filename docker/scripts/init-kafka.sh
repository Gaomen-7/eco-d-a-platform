#!/usr/bin/env bash
# 建两个 topic 并施加配置 —— REQ-F-MQ-01..04
# 分区数与 retention 见 gdd §A.2；本脚本是「配置落地」的唯一位置。
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:9092}"
TOPIC="${TOPIC:-ecom.behavior.event.v1}"
DLQ="${DLQ:-ecom.behavior.event.v1.dlq}"
PARTITIONS="${PARTITIONS:-6}"
RETENTION_MS="${RETENTION_MS:-604800000}"     # 7 天
RETENTION_BYTES="${RETENTION_BYTES:-2147483648}"  # 2 GB（须显式设，默认 -1 无上限）

KT="/opt/kafka/bin/kafka-topics.sh"
KC="/opt/kafka/bin/kafka-configs.sh"

echo "[init-kafka] bootstrap=$BOOTSTRAP partitions=$PARTITIONS"
for i in $(seq 1 30); do
  if "$KT" --bootstrap-server "$BOOTSTRAP" --list >/dev/null 2>&1; then break; fi
  echo "[init-kafka] waiting for broker... ($i/30)"; sleep 2
done

create_topic() {
  local name="$1"
  if "$KT" --bootstrap-server "$BOOTSTRAP" --list | grep -qx "$name"; then
    echo "[init-kafka] topic exists: $name"
  else
    "$KT" --bootstrap-server "$BOOTSTRAP" --create \
      --topic "$name" --partitions "$PARTITIONS" --replication-factor 1
    echo "[init-kafka] created: $name"
  fi
  # ★ P0 实测修正：Kafka 4.3.1 的 kafka-topics.sh **不再支持 --alter 改配置**
  #   （报错原文：「Option combination "[[bootstrap-server], [config]]" can't be used
  #   with option "[alter]" (To alter topic configurations, the kafka-configs tool
  #   can be used.)」）⇒ 必须改用 **kafka-configs.sh --alter**。
  #   retention：官方语义「任一条件先满足即删」（gdd §A.2 / D3）
  "$KC" --bootstrap-server "$BOOTSTRAP" --alter --entity-type topics --entity-name "$name" \
    --add-config "retention.ms=$RETENTION_MS,retention.bytes=$RETENTION_BYTES"
  echo "[init-kafka] retention applied: $name (ms=$RETENTION_MS bytes=$RETENTION_BYTES)"
}

create_topic "$TOPIC"
create_topic "$DLQ"

echo "[init-kafka] final state:"
"$KT" --bootstrap-server "$BOOTSTRAP" --describe --topic "$TOPIC"
"$KT" --bootstrap-server "$BOOTSTRAP" --describe --topic "$DLQ"
