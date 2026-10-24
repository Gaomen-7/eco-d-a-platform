"""Paimon↔Doris 对账的**期望复算面**（impl-03f 交付物 1，口径裁决 C）。

**本模块的现状定位**（2026-10-24 code-review 后收窄）：只保留**探针对账**
（`reconcile_probe`）依赖的复算与解析纯函数。曾有的「全量对账采集器」
（Kafka 全 topic 导出 → 复算 → 全表比对）已在实机归因中证明**原理性不可达**
（无 HA 重放乱序 ⇒ 超期旁路无维度反推，票面 §2.4 全链证据），死代码移除；
归因结论保留在本头注与 spec §8.3，防止后人重走。

**口径裁决 C**（用户 2026-10-24 裁定，票面 §0.2 的 A/B/C 三选一）：
两侧 `ads_window_metric` 现状**四层不相交**（窗口区间 / metric_name 词表 /
dim_key 词表 / 单位）——
  Paimon replay：pv/uv/gmv × {ALL, channel=…} × 2024-01（DECIMAL 元）
  Doris 实时：  event_* × {c_000xx, ''} × 2026-10（BIGINT 个）
裁决 C 的含义：**不动** replay 的 pv/uv/gmv 既有口径、不动 Java 作业、不动
Doris DDL，由对账面从「同一批事件字节」复算**与实时侧同名同键同窗口**的
期望行，与 Doris 落定行逐值比对。

**期望行的来源**（同源性逐条锚定）：
1. 输入＝受控探针批的事件字节（reconcile_probe 投进 main topic 的同一批）；
2. 闸门判定＝`ecomgen.events.validate_event`（与 `EventQuality.reason` 逐条同源，
   events.py 文件头注就是两侧共源的契约）：脏事件进 DLQ、不进 ADS 计数；
3. 去重＝同 `event_id` 只计一次（`GateSegment.ValidateAndDedup` 的 1 h TTL 在
   「同批一次性投递」下等价于「首次出现」；重复走 duplicate 通道不进计数）；
4. 窗口/公式＝`WindowSegment.AggregateWindow` 的镜像：
   `window_start = floor(event_time/60000)*60000`（1 min 滚动、epoch 对齐、UTC）、
   `metric_name = 'event_' || event_type`、`dim_key = category_id`、
   `metric_value = COUNT(*)`（个，与 Doris BIGINT 同量纲）；
   Flink 侧按 keyBy(category) 分组、窗口内再按 event_type 分桶 ⇒
   期望行的聚合粒度＝(window, event_type, category)，与按 event 分组再聚合同值。
5. 比对范围＝Doris `is_settled=1` 且 `metric_name LIKE 'event_%'` 的行
   （spec §4.2 对账条文「只比 is_settled=1`；uv/duplicate_absorbed/
   provisional_emitted 是实时侧自有指标，**不在**本切面公式面内，已登记缺口）。

**is_settled=0 不判**（票面 AC 第 3 条）：暂定行的差异不得导致失败——
采集 SQL 里就过滤 `is_settled=1`，判定面只见落定行。

**为什么比对键用字符串时间**：Doris `window_start` 是 DATETIME，mysql 客户端
渲染成 `YYYY-MM-DD HH:MM:SS`；期望侧 epoch ms 按 UTC 折算成同一格式——
两侧都在 UTC 口径（spec §4.2「UTC epoch 对齐」），折算是纯函数可单测。
"""

from __future__ import annotations

import datetime as dt
import json
from collections.abc import Iterable
from typing import Any

from ecomgen.events import validate_event
from ecomgen.verify import CheckResult, Status

# 本切面比对的指标前缀（实时侧事件计数的词表面）
EVENT_METRIC_PREFIX = "event_"

# ── 超时数字：一律设计值，非官方保证（与 verify_runner 同纪律）──────────
DORIS_QUERY_TIMEOUT_S = 120  # 设计值（与 verify_runner 的 _doris_scalar_runner 同源）


def expected_rows_from_events(
    lines: Iterable[str],
) -> dict[tuple[int, str, str], int]:
    """从事件字节流复算期望行：键 (window_start_ms, metric_name, dim_key) → 计数。

    与实时侧的四点同源见模块头注（闸门 / 去重 / 窗口公式 / 聚合粒度）。
    坏 JSON 行跳过——它们进不了任何一侧的 ADS。
    """
    counts: dict[tuple[int, str, str], int] = {}
    seen: set[str] = set()
    batch_min_ms: int | None = None
    parsed: list[tuple[str, dict[str, Any]]] = []
    for line in lines:
        token = line.strip()
        if not token:
            continue
        try:
            event = json.loads(token)
        except json.JSONDecodeError:
            continue
        if not isinstance(event, dict):
            continue
        parsed.append((token, event))
        et = event.get("event_time")
        if isinstance(et, int) and (batch_min_ms is None or et < batch_min_ms):
            batch_min_ms = et

    for _, event in parsed:
        # 闸门：脏事件（Java 侧 EventQuality.reason 同源判定）不进 ADS 计数
        if validate_event(event, batch_min_ms=batch_min_ms):
            continue
        event_id = event.get("event_id")
        if not isinstance(event_id, str):
            continue
        # 去重：同 event_id 只计首次（1 h TTL 在单批投递下等价首次出现）
        if event_id in seen:
            continue
        seen.add(event_id)
        et = event.get("event_time")
        etype = event.get("event_type")
        category = event.get("category_id")
        if not isinstance(et, int) or not isinstance(etype, str):
            continue
        dim = category if isinstance(category, str) else ""
        window_start = (et // 60_000) * 60_000
        key = (window_start, f"event_{etype}", dim)
        counts[key] = counts.get(key, 0) + 1
    return counts


def parse_doris_rows(out: str) -> dict[tuple[str, str, str], int]:
    """解析 mysql `--batch -N` 的四列输出：键 (ws, metric, dim) → value。

    只认恰好 4 列、第 4 列是整数的行（警告/表头天然不齐列被跳过）。
    """
    rows: dict[tuple[str, str, str], int] = {}
    for line in out.splitlines():
        parts = line.split("\t")
        if len(parts) != 4:
            continue
        value = parts[3].strip()
        if not value.lstrip("-").isdigit():
            continue
        rows[(parts[0].strip(), parts[1].strip(), parts[2].strip())] = int(value)
    return rows


def epoch_ms_to_utc_string(epoch_ms: int) -> str:
    """epoch ms → 'YYYY-MM-DD HH:MM:SS'（UTC）——与 Doris DATETIME 渲染同形。

    （原 `_epoch_ms_to_utc_string`；探针面与测试共用，转正为公共函数。）
    """
    return dt.datetime.fromtimestamp(epoch_ms / 1000, tz=dt.UTC).strftime(
        "%Y-%m-%d %H:%M:%S"
    )


def check_reconciliation_from_rows(
    expected: dict[tuple[int, str, str], int],
    doris: dict[tuple[str, str, str], int],
) -> CheckResult:
    """对账判定（纯函数，可单测）：期望 vs Doris 落定行逐值严格相等。

    - 只比 `event_*` 指标（本切面公式面；其它指标词表缺口已登记）；
    - 全外连接语义：任一侧缺席 ⇒ 差异（缺席不是相等）；
    - 全等 ⇒ PASS，`actual` 带 rows=N 供报告。
    """
    name = "recon-paimon-doris"
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
            mismatches.append(f"{key[1]}/{key[2]}@{key[0]}: 期望={want} 实测={got}")

    metrics = sorted({key[1] for key in expected_norm} | {k[1] for k in doris_norm})
    if not mismatches:
        actual = f"rows={len(expected_norm)} 全等 metrics={len(metrics)}"
        return CheckResult(Status.PASS, name, actual, "is_settled=1 行逐值严格相等")
    detail = "; ".join(mismatches[:5])
    actual = f"差异 {len(mismatches)} 行（rows 期望 {len(expected_norm)} / 实测 {len(doris_norm)}）: {detail}"
    return CheckResult(Status.FAIL, name, actual, "is_settled=1 行逐值严格相等", actual)
