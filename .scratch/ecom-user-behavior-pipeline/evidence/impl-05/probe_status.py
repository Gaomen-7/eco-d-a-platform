"""impl-05 取证辅助：读 DLQ/输入总 offset 与源算子水位（一行摘要）。

用法（WSL，仓库根）：uv run python .scratch/.../evidence/impl-05/probe_status.py
"""

import json
import sys
import urllib.request

sys.path.insert(0, "src")

from confluent_kafka import Consumer

JM = "http://127.0.0.1:8081"
JID = "237d04c4b419b36344db0d25e0514f07"
SRC = "cbc357ccb763df2852fee8c4fc7d55f2"


def topic_total(consumer: Consumer, topic: str) -> int:
    md = consumer.list_topics(topic, timeout=10)
    total = 0
    for pid in md.topics[topic].partitions:
        _lo, hi = consumer.get_watermark_offsets(topic, pid, timeout=10)
        total += hi
    return total


def main() -> int:
    consumer = Consumer(
        {
            "bootstrap.servers": "127.0.0.1:29092",
            "group.id": "probe-status",
            "client.id": "probe-status",
        }
    )
    dlq = topic_total(consumer, "ecom.behavior.event.v1.dlq")
    inp = topic_total(consumer, "ecom.behavior.event.v1")
    consumer.close()
    try:
        with urllib.request.urlopen(
            f"{JM}/jobs/{JID}/vertices/{SRC}/metrics"
            "?get=0.Source__gate-source.currentOutputWatermark,"
            "1.Source__gate-source.currentOutputWatermark,"
            "2.Source__gate-source.currentOutputWatermark,"
            "3.Source__gate-source.currentOutputWatermark",
            timeout=10,
        ) as resp:
            wms = [int(m["value"]) for m in json.load(resp)]
    except Exception as exc:  # noqa: BLE001
        wms = []
        print("watermark query failed:", exc)
    import time

    now_ms = int(time.time() * 1000)
    print(f"dlq_total={dlq} input_total={inp} now={now_ms}")
    for i, wm in enumerate(wms):
        print(f"wm[{i}]={wm} ahead_of_now_ms={wm - now_ms}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
