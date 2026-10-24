"""受控探针对账：Paimon↔Doris「两套物化、一个口径」的严格判据面。

**为什么需要它**（2026-10-24 实测归因，见 impl-03f §2.4）：
直接全量对账（Kafka 全 topic 复算 vs Doris 全表）差 5218 行——根因是
实时作业**无 HA**、历史多轮重启后按 earliest 重放，live 多轮重产的同
event_id 事件在 topic 里**交错**，重放时同窗事件的「到达序」与原始发生序
不同 ⇒ 部分事件被 `sideOutputLateData` 判超期旁路（只进
`ads_late_expired_count`，不进 `ads_window_metric`）。旁路表只有
`(window, lag)` 两维且 lag 去重合并，**无法**反推出每个
`(window, metric, dim)` 键被旁路了多少 ⇒ 全量逐值对账在历史数据上
**原理性不可达**（不是实现技巧问题）。

**本模块的判据形态**（口径裁决 C 的落地补充：不修历史、建对账专用切面）：
1. **探针批**：产 N 条事件，`event_time` 取**当下**近窗（近到 watermark 干净），
   `event_id` 用 uuid ⇒ 与历史零重叠；`category_id` 用保留前缀 `c_recon`
   ⇒ 探针键集与历史键集天然隔离；
2. 投**同一条 Kafka 主 topic**（与实时作业消费的同一链路，不是旁路注入）；
   批时 + 等待 200 s 后各投一轮「逐分区」踢水位（`send_watermark_ticks`×2）:
   watermark = max(event_time)−120 s 且 operator 水位取各 subtask 的 **MIN**
   ⇒ 单分区 tick 会被其余分区卡住，必须逐分区推（2026-10-24 三轮实测定案，
   见 `send_watermark_ticks` 头注）；
3. 窗口关闭 + allowedLateness（120 s）+ sink flush ⇒ Doris 出 `is_settled=1` 行；
4. 期望侧：同一批字节走 `expected_rows_from_events`（validate_event 同源闸门）；
5. 对账**限定探针键集 + 本批窗口范围**：逐值严格相等，任一方向缺席即 FAIL
   （范围过滤同为实测教训：无 HA 重启重放**上一轮**探针批的残留行与本批
   同词表不同窗口，不过滤会以「期望=None」污染本批判定）；
6. 历史键集不进本判据（全量不可达的归因已登记票面 §2.4 / spec §8.3）。

**is_settled=0 不判**（票面 AC 第 3 条）：采集器只取 `is_settled=1` 行。
"""

from __future__ import annotations

import json
import subprocess
import time
import uuid
from typing import Any

from ecomgen.reconcile import (
    DORIS_QUERY_TIMEOUT_S,
    EVENT_METRIC_PREFIX,
    epoch_ms_to_utc_string,
    parse_doris_rows,
)
from ecomgen.verify import CheckResult, Status

# 探针批参数（全部**设计值**）
PROBE_N_EVENTS = 24
PROBE_CATEGORY = "c_recon"
PROBE_EVENT_TYPES = ("page_view", "add_to_cart", "view_item")
# 投递后等窗口关闭(≤60s) + allowedLateness(120s) + sink 缓冲，取 200 s
PROBE_SETTLE_WAIT_S = 200
# ★ 第二条踢水位的后置等待（设计值）：watermark = max(event_time) − 120 s
#   （forBoundedOutOfOrderness，GateSegment.mainSource），窗口 [ws,ws+60s) 的
#   关闭条件是 watermark ≥ ws+60s ⇒ 需要某事件 event_time ≥ ws+180s。
#   批后即时 tick 的 event_time≈投递时刻，只能把 watermark 推到「此刻−120s」，
#   **永远追不上当前分钟窗的关闭线**（实测 2026-10-24：批+即时 tick ⇒ 200 s 后
#   窗口仍开、只有 provisional 行）。故流程为：批 → 等 200 s（此时钟已走过
#   ws+180s）→ 投第二条 tick（event_time=当下 ⇒ watermark ≥ ws+60s ⇒ 窗关、
#   聚合发射 is_settled=1）→ 再等 60 s（发射+sink flush 1s）→ 采集。
PROBE_POST_TICK_WAIT_S = 60
# Doris 查询只取探针键集（category 前缀 + 落定）+ **本批窗口范围**。
# ★ 范围过滤是首跑实测教训（2026-10-24）：无 HA 重启会把**上一轮探针批**
#   重放落定，残留行与本批键集同词表（c_recon）不同窗口——不过滤时它们
#   以「期望=None 实测=N」污染本批判定。窗口范围从探针批自身事件推出，
#   每轮必新（近窗 ⇒ 不会与历史批重叠）。
PROBE_DORIS_SQL_TEMPLATE = (
    "SELECT window_start, metric_name, dim_key, metric_value "
    "FROM ecom_ads.ads_window_metric "
    "WHERE is_settled = 1 AND dim_key = '{cat}' "
    "AND window_start >= '{ws_min}' AND window_start <= '{ws_max}'"
)

KAFKA_BOOTSTRAP_HOST = "127.0.0.1:29092"  # fault.py 的 FEED_BOOTSTRAP 同源
KAFKA_TOPIC = "ecom.behavior.event.v1"

# ── 内联超时数字：一律设计值，非官方保证（与 verify_runner 同纪律）──────
KAFKA_METADATA_TIMEOUT_S = 10  # 设计值：list_topics 元数据拉取
KAFKA_FLUSH_TIMEOUT_S = 30  # 设计值：produce 后的 flush 上限


def doris_batch_query(sql: str) -> tuple[int, str]:
    """经 compose 网内 `client` 跑一条 mysql 查询，返回 (rc, 输出)。

    与 verify_runner._doris_scalar_runner 同一执行面（HLD §4.4：Doris 只能
    网内查）；三处采集器（本函数 / _doris_scalar_runner / 探针采集）共用
    形状，超时 120 s＝reconcile.DORIS_QUERY_TIMEOUT_S 同源设计值。
    """
    cmd = [
        "docker",
        "compose",
        "run",
        "--rm",
        "-T",
        "client",
        "mysql",
        "-h",
        "doris-fe",
        "-P",
        "9030",
        "-u",
        "root",
        "-N",
        "--batch",
        "-e",
        sql,
    ]
    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=DORIS_QUERY_TIMEOUT_S,
            check=False,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired) as exc:
        return 2, str(exc)
    return proc.returncode, (proc.stdout or "") + (proc.stderr or "")


def build_probe_events(
    n_events: int = PROBE_N_EVENTS,
    now_ms: int | None = None,
) -> list[dict[str, Any]]:
    """造探针事件：全部合法、uuid 唯一 id、近窗 event_time、保留 category。

    时间布局：从 `now - 30 s` 起，每条 +300 ms ⇒ 全部落在同一分钟窗
    （或最多跨两个窗），且都早于 now ⇒ 闸门时间界内（非未来）。
    """
    base = (now_ms if now_ms is not None else int(time.time() * 1000)) - 30_000
    events: list[dict[str, Any]] = []
    for i in range(n_events):
        events.append(
            {
                "event_id": str(uuid.uuid4()),
                "event_time": base + i * 300,
                "user_id": f"recon-user-{i}",
                "item_id": f"recon-item-{i}",
                "category_id": PROBE_CATEGORY,
                "event_type": PROBE_EVENT_TYPES[i % len(PROBE_EVENT_TYPES)],
                "schema_version": "1.0",
                "page": "home",
                "source": "web",
                "channel": "organic",
                "province": "上海市",
                "payload": {"session_id": f"recon-user-{i}"},
                # add_to_cart 需要 quantity（SRS §3.3 必填子集）
                **(
                    {"quantity": 2}
                    if PROBE_EVENT_TYPES[i % len(PROBE_EVENT_TYPES)] == "add_to_cart"
                    else {}
                ),
            }
        )
    return events


def build_watermark_tick(now_ms: int | None = None) -> dict[str, Any]:
    """踢水位事件：**当下时刻**的合法事件（c_00000，非保留段）。

    ★ 为什么必须：探针批的 event_time 全部早于投递时刻（近窗 ⇒ watermark 干净），
    若批后无任何新事件，watermark 永远停在批内最大 event_time 附近 ⇒ 探针窗
    不 close、allowedLateness 不计时、Doris 永不落定（首跑实测形态：
    期望全缺、实测全空）。批后投一条 now 事件把 watermark 推过窗口末端，
    三个计时全部启动。它与探针批不同 category，不进探针键集。
    """
    return {
        "event_id": str(uuid.uuid4()),
        "event_time": now_ms if now_ms is not None else int(time.time() * 1000),
        "user_id": "wm-tick",
        "item_id": "wm-tick",
        "category_id": "c_00000",
        "event_type": "page_view",
        "schema_version": "1.0",
        "page": "home",
        "source": "web",
        "channel": "organic",
        "province": "上海市",
        "payload": {"session_id": "wm-tick"},
    }


def probe_window_bounds_ms(events: list[dict[str, Any]]) -> tuple[int, int]:
    """探针批覆盖的窗口范围 [min_start, max_start]（epoch ms，1 min 对齐）。"""
    starts = [e["event_time"] // 60_000 * 60_000 for e in events]
    return min(starts), max(starts)


def send_watermark_ticks(now_ms: int | None = None) -> int:
    """**每个分区**各投一条踢水位事件。返回成功条数（=分区数），-1=失败。

    ★★ 为什么必须逐分区（2026-10-24 实测定案）：operator watermark = 各
    source subtask 水位的 **MIN**（并行 source 的水位对齐语义）。一条 tick
    只落一个分区 ⇒ 其余 subtask 水位不动 ⇒ 全局水位被卡。实测：批事件散落
    4/6 分区、tick 只进 2 个分区 ⇒ 水位钉在「批内最小 et −120s」，窗口
    [ws,ws+60s) 迟迟不关（当轮 FAIL；待**下一轮**批事件把其余分区推上去后
    上一轮窗口才落定）。逐分区各投一条 et=now ⇒ 所有 subtask 水位 =
    now−120s ⇒ now ≥ ws+180s 时窗口必关。
    """
    try:
        from confluent_kafka import Producer
    except ImportError:
        return -1
    producer = Producer(
        {"bootstrap.servers": KAFKA_BOOTSTRAP_HOST, "client.id": "recon-probe-tick"}
    )
    try:
        md = producer.list_topics(topic=KAFKA_TOPIC, timeout=KAFKA_METADATA_TIMEOUT_S)
        partition_ids = sorted(md.topics[KAFKA_TOPIC].partitions)
    except Exception:  # noqa: BLE001 —— 元数据失败按投递失败处理
        return -1
    if not partition_ids:
        return -1
    sent = 0
    for pid in partition_ids:
        ev = build_watermark_tick(now_ms=now_ms)
        producer.produce(
            topic=KAFKA_TOPIC,
            partition=pid,
            key=ev["event_id"].encode(),
            value=json.dumps(ev, ensure_ascii=False, sort_keys=True).encode("utf-8"),
        )
        producer.poll(0)
        sent += 1
    producer.flush(KAFKA_FLUSH_TIMEOUT_S)
    return sent


def send_probe_events(events: list[dict[str, Any]]) -> int:
    """把探针批投进主 topic（与实时作业同一入口）。返回成功条数，-1=失败。"""
    try:
        from confluent_kafka import Producer
    except ImportError:
        return -1
    producer = Producer(
        {"bootstrap.servers": KAFKA_BOOTSTRAP_HOST, "client.id": "recon-probe"}
    )
    sent = 0
    for event in events:
        producer.produce(
            topic=KAFKA_TOPIC,
            key=event["event_id"].encode(),
            value=json.dumps(event, ensure_ascii=False, sort_keys=True).encode("utf-8"),
        )
        producer.poll(0)
        sent += 1
    producer.flush(KAFKA_FLUSH_TIMEOUT_S)
    return sent


def collect_doris_probe_rows(
    window_bounds_ms: tuple[int, int],
) -> dict[tuple[str, str, str], int] | None:
    """读 Doris 探针键集的落定行（限定本批窗口范围）。失败 ⇒ None。

    无 HA 重启会重放**上一轮**探针批，残留行与本批同词表不同窗口，
    不过滤会污染判定（模块头注实测教训）——所以窗口范围是**必填**。
    """
    sql = PROBE_DORIS_SQL_TEMPLATE.format(
        cat=PROBE_CATEGORY,
        ws_min=epoch_ms_to_utc_string(window_bounds_ms[0]),
        ws_max=epoch_ms_to_utc_string(window_bounds_ms[1]),
    )
    rc, out = doris_batch_query(sql)
    if rc != 0:
        return None
    return parse_doris_rows(out)


def check_probe_reconciliation(
    expected: dict[tuple[int, str, str], int],
    doris: dict[tuple[str, str, str], int],
) -> CheckResult:
    """探针对账判定（纯函数，可单测）：探针键集逐值严格相等。

    **词表双保险**（票面 AC 第 3 条的「is_settled=0 不判」在判定面同样成立）：
    只比 `event_*` 指标——采集 SQL 过滤 `is_settled=1` 是第一道，这里词表
    过滤是第二道（provisional_emitted/uv 等实时侧自有指标即使混进采集
    结果也不判）。check 名 `recon-offline-mirror-doris`：**离线公式镜像**
    （与 AggregateWindow 同源复算）vs Doris 落定行——不虚称读到了 Paimon
    （全量 Paimon 对账已归因不可达，票面 §2.4；本切面是裁决 C 精神下
    「离线侧同源复算」的落点）。
    """
    name = "recon-offline-mirror-doris"
    expected_norm = {
        (epoch_ms_to_utc_string(ws), metric, dim): value
        for (ws, metric, dim), value in expected.items()
        if metric.startswith(EVENT_METRIC_PREFIX)
    }
    doris_norm = {
        key: value
        for key, value in doris.items()
        if key[1].startswith(EVENT_METRIC_PREFIX)
    }
    mismatches: list[str] = []
    for key in sorted(set(expected_norm) | set(doris_norm)):
        want = expected_norm.get(key)
        got = doris_norm.get(key)
        if want != got:
            mismatches.append(f"{key[1]}@{key[0]}: 期望={want} 实测={got}")
    if not mismatches:
        return CheckResult(
            Status.PASS,
            name,
            f"rows={len(expected_norm)} 探针键集逐值全等",
            "探针批 is_settled=1 逐值严格相等（键集隔离）",
        )
    detail = "; ".join(mismatches[:5])
    actual = f"差异 {len(mismatches)} 行: {detail}"
    return CheckResult(
        Status.FAIL,
        name,
        actual,
        "探针批 is_settled=1 逐值严格相等（键集隔离）",
        actual,
    )


def run_probe_reconciliation(
    wait_s: int = PROBE_SETTLE_WAIT_S,
    post_tick_wait_s: int = PROBE_POST_TICK_WAIT_S,
) -> CheckResult:
    """探针对账入口：造批 → 投批+tick1 → 等 → tick2 → 等 → 复算 → 比对。

    水位数学（PROBE_POST_TICK_WAIT_S 头注）：tick1（批时）保证事件被消费；
    tick2（等待 200 s 后）把 watermark 推过窗口末端 ⇒ 窗关 ⇒ is_settled=1。
    对账查询按本批窗口范围过滤，历史探针批的重放残留行不进判据。
    """
    # 局部导入避免测试环境拉起采集器依赖
    from ecomgen.reconcile import expected_rows_from_events

    events = build_probe_events()
    window_bounds = probe_window_bounds_ms(events)
    sent = send_probe_events(events)
    ticks1 = send_watermark_ticks()
    if sent != len(events) or ticks1 < 1:
        return CheckResult(
            Status.FAIL,
            "recon-offline-mirror-doris",
            f"探针投递失败（sent={sent} ticks={ticks1}）",
            "探针批 is_settled=1 逐值严格相等（键集隔离）",
        )
    time.sleep(wait_s)
    # 第二轮逐分区 tick：此刻 et=now ⇒ 所有 subtask 水位 = now−120s ≥ ws+60s
    # （等待 200 s 已保证 now ≥ ws+180s）⇒ 窗口关闭、落定行发射。
    ticks2 = send_watermark_ticks()
    if ticks2 < 1:
        return CheckResult(
            Status.FAIL,
            "recon-offline-mirror-doris",
            f"第二轮踢水位投递失败（{ticks2}）",
            "探针批 is_settled=1 逐值严格相等（键集隔离）",
        )
    time.sleep(post_tick_wait_s)
    doris = collect_doris_probe_rows(window_bounds_ms=window_bounds)
    if doris is None:
        return CheckResult(
            Status.FAIL,
            "recon-offline-mirror-doris",
            "Doris 读取失败",
            "探针批 is_settled=1 逐值严格相等（键集隔离）",
        )
    expected = expected_rows_from_events(
        json.dumps(e, ensure_ascii=False, sort_keys=True) for e in events
    )
    return check_probe_reconciliation(expected, doris)
