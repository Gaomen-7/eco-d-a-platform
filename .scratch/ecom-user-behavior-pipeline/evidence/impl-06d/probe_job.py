#!/usr/bin/env python3
"""impl-06d 取证探针：读 Flink REST 的作业读入进度，判「冷启是否重放」。

判据（机读，见 impl-06d 票面）：
  - 作业刚 RUNNING 时，源算子 read-records / numRecordsIn 是否 ≈ topic 全量；
  - 消费组 committed offsets 与队列尾（LOG-END-OFFSET）之差，即"应消费增量".

用法：
  python3 probe_job.py <mode>      mode = summary | sources
"""

from __future__ import annotations

import json
import subprocess
import sys
import urllib.request

FLINK_UI = "http://127.0.0.1:8081"
GROUP = "ecom-p1-consumer"
TOPIC = "ecom.behavior.event.v1"


def _get(url: str) -> dict:
    with urllib.request.urlopen(url, timeout=20) as resp:
        return json.loads(resp.read().decode())


def topic_offsets() -> dict[int, tuple[int, int]]:
    """返回 {partition: (committed, log_end)}；committed 缺失记为 None。"""
    out = subprocess.run(
        [
            "docker",
            "exec",
            "eco-kafka",
            "/opt/kafka/bin/kafka-consumer-groups.sh",
            "--bootstrap-server",
            "localhost:9092",
            "--describe",
            "--group",
            GROUP,
        ],
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    parts: dict[int, tuple[int, int]] = {}
    for line in out.splitlines():
        cols = line.split()
        if len(cols) >= 6 and cols[0] == GROUP:
            part = int(cols[2])
            committed = int(cols[3]) if cols[3] not in ("-", "") else 0
            log_end = int(cols[4])
            parts[part] = (committed, log_end)
    return parts


def topic_totals() -> dict[str, int]:
    parts = topic_offsets()
    return {
        "committed": sum(c for c, _ in parts.values()),
        "log_end": sum(e for _, e in parts.values()),
        "partitions": len(parts),
    }


def summary() -> dict:
    overview = _get(f"{FLINK_UI}/jobs/overview")
    jobs = overview.get("jobs", [])
    running = [j for j in jobs if j.get("state") == "RUNNING"]
    report: dict = {"running_jobs": [j["jid"] for j in running]}
    if running:
        jid = running[0]["jid"]
        detail = _get(f"{FLINK_UI}/jobs/{jid}")
        vertices = detail.get("vertices", [])
        # ★ 读 job detail 里**内嵌**的 vertex metrics（实测 2.2.1 可靠），
        #   而不是 /metrics?get=read-records —— 后者在 vertex 层返回空数组。
        sources = []
        for v in vertices:
            if "gate-source" not in (v.get("name") or ""):
                continue
            # ★ 不用 job detail 内嵌的 metrics 块：实测 2.2.1 里它对
            #   read-records 恒返回 0（不随消费刷新）。逐 subtask 查真值再求和。
            per_sub = [
                _metric(jid, v["id"], "Source__gate-source.numRecordsIn", sub)
                for sub in range(int(v.get("parallelism") or 0))
            ]
            known = [x for x in per_sub if x is not None]
            sources.append(
                {
                    "name": v.get("name"),
                    "parallelism": v.get("parallelism"),
                    "read_records": sum(known) if known else None,
                    "per_subtask": per_sub,
                }
            )
        report["source"] = sources
        report["job_id"] = jid
    report["topic"] = topic_totals()
    return report


def watermarks() -> dict:
    """读 source 算子的**当前水位**（Flink REST /vertices/{vid}/watermarks），
    并给出「相对墙钟的未来偏移」——案 3 的机读终判面。

    判据：被拒的 future 事件**不得**把水位抬到墙钟之后超过裕度（5 min）。
    旧实现下该值 ≈ +2 h（impl-05 §4 实测），修复后应 ≈ −outOfOrderness。
    """
    import time as _time

    now_ms = int(_time.time() * 1000)
    overview = _get(f"{FLINK_UI}/jobs/overview")
    running = [j for j in overview.get("jobs", []) if j.get("state") == "RUNNING"]
    if not running:
        return {"running_jobs": [], "now_ms": now_ms}
    jid = running[0]["jid"]
    detail = _get(f"{FLINK_UI}/jobs/{jid}")
    report: dict = {"job_id": jid, "now_ms": now_ms, "sources": []}
    for v in detail.get("vertices", []):
        name = v.get("name") or ""
        if "gate-source" not in name:
            continue
        # ★ /vertices/{id}/watermarks 在 2.2.1 实测返回 []（空数组），
        #   能取到的水位在 **subtask 的 metric**：{op}.currentOutputWatermark。
        #   名字随算子链路而定（此处实测 parse-event.currentOutputWatermark），
        #   故按候选名逐个试，取第一个非空。
        values: list[int] = []
        errors: list[str] = []
        for sub in range(int(v.get("parallelism") or 0)):
            for metric in (
                "parse-event.currentOutputWatermark",
                "currentOutputWatermark",
            ):
                try:
                    data = _get(
                        f"{FLINK_UI}/jobs/{jid}/vertices/{v['id']}"
                        f"/subtasks/{sub}/metrics?get={metric}"
                    )
                except (OSError, ValueError) as exc:
                    errors.append(f"sub{sub}:{exc}")
                    continue
                got = [
                    int(float(item["value"]))
                    for item in data
                    if item.get("id") == metric and item.get("value") not in (None, "")
                ]
                if got:
                    values.extend(got)
                    break
        entry = {
            "name": name,
            "subtasks": len(values),
            "min": min(values) if values else None,
            "max": max(values) if values else None,
        }
        if values:
            entry["ahead_of_now_ms_at_min"] = min(values) - now_ms
            entry["ahead_of_now_ms_at_max"] = max(values) - now_ms
        report["sources"].append(entry)
    return report


def _metric(jid: str, vid: str, name: str, sub: int | None = None) -> int | None:
    """读一个 metric。★ 实测 2.2.1：**vertex 层**的 `?get=` 常返回空数组，
    真值在 **subtask 层**（`/subtasks/{n}/metrics?get=`）⇒ 必须给 sub。"""
    suffix = "" if sub is None else f"/subtasks/{sub}"
    try:
        data = _get(f"{FLINK_UI}/jobs/{jid}/vertices/{vid}{suffix}/metrics?get={name}")
    except (OSError, ValueError):
        return None
    for item in data:
        # ★ 响应里 id 带 subtask 前缀（如 "3.Source__gate-source.numRecordsIn"），
        #   故按**后缀**匹配，不要求逐字全等。
        if str(item.get("id")).split(".")[-1] == name.split(".")[-1]:
            try:
                return int(float(item["value"]))
            except (TypeError, ValueError):
                return None
    return None


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "summary"
    if mode == "summary":
        result = summary()
    elif mode == "topic":
        result = topic_totals()
    elif mode == "watermarks":
        result = watermarks()
    else:
        raise SystemExit(f"未知 mode: {mode}")
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
