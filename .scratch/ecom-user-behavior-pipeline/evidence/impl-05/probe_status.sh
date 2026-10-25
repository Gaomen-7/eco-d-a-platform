#!/usr/bin/env bash
# impl-05 取证辅助：DLQ/输入总 offset + 源算子水位（供终验反复调用）。
set -uo pipefail
JID="237d04c4b419b36344db0d25e0514f07"
SRC="cbc357ccb763df2852fee8c4fc7d55f2"

sum_offsets() {
  docker exec eco-kafka /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server localhost:9092 --topic "$1" \
    | awk -F: 'NF==3 && $3 ~ /^[0-9]+$/ { s += $3 } END { print s+0 }'
}

echo "dlq_total=$(sum_offsets ecom.behavior.event.v1.dlq) input_total=$(sum_offsets ecom.behavior.event.v1)"
echo "now_utc_s=$(date +%s)"
curl -s "http://127.0.0.1:8081/jobs/${JID}/vertices/${SRC}/metrics?get=0.Source__gate-source.currentOutputWatermark,1.Source__gate-source.currentOutputWatermark,2.Source__gate-source.currentOutputWatermark,3.Source__gate-source.currentOutputWatermark"
echo
