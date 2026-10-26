"""取证脚本：往主 topic 投一条 et = now + 2 h 的**质量门脏事件**（R7-i 案 3 诱因）。

复现 impl-05 §4 的钉死条件：topic 里存在未陈化的未来事件。
本脚本只在取证时用，不进产品路径。
"""

from __future__ import annotations

import json
import sys
import time

from confluent_kafka import Producer


def main() -> int:
    bootstrap = sys.argv[1] if len(sys.argv) > 1 else "kafka:9092"
    ahead_ms = int(sys.argv[2]) if len(sys.argv) > 2 else 2 * 3_600_000
    now_ms = int(time.time() * 1000)
    et = now_ms + ahead_ms
    record = {
        "schema_version": "1.0",
        "event_id": f"r7i-future-{now_ms}",
        "event_time": et,
        "user_id": "u_000001",
        "item_id": "i_00001",
        "category_id": "c_00001",
        "event_type": "page_view",
        "payload": "{}",
    }
    producer = Producer({"bootstrap.servers": bootstrap, "client.id": "r7i-inject"})
    producer.produce(
        "ecom.behavior.event.v1",
        key=str(record["user_id"]).encode(),
        value=json.dumps(
            record, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode(),
    )
    producer.flush(20)
    print(
        f"injected future event: event_time={et} (now+{ahead_ms} ms) "
        f"iso={time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(et / 1000))}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
