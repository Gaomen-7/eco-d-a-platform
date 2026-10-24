"""探针对账判据面测试（impl-03f 交付物 1，口径裁决 C 的受控批形态）。

**为什么是探针批**（reconcile_probe.py 模块头注 + 票面 §2.4 归因）：
历史数据全量对账原理性不可达（无 HA 重放乱序 ⇒ 超期旁路无维度反推）。
受控探针批：新分钟窗 + 保留 category `c_recon` ⇒ 键集与历史隔离 ⇒
逐值严格相等可判。

**测试策略**：纯函数注入（build_probe_events / check_probe_reconciliation /
build_watermark_tick / probe_window_bounds_ms），真实路径（投 Kafka、等落定）
不在单测内触碰。
"""

from __future__ import annotations

import time

from ecomgen.reconcile import epoch_ms_to_utc_string
from ecomgen.reconcile_probe import (
    PROBE_CATEGORY,
    PROBE_DORIS_SQL_TEMPLATE,
    build_probe_events,
    build_watermark_tick,
    check_probe_reconciliation,
    probe_window_bounds_ms,
)
from ecomgen.verify import Status


def _mirror_counts(events: list[dict]) -> dict[tuple[int, str, str], int]:
    """镜像 AggregateWindow 公式复算期望（与 reconcile.expected_rows 同口径，
    测试内联一份以便独立于被测实现）。"""
    counts: dict[tuple[int, str, str], int] = {}
    for e in events:
        ws = e["event_time"] // 60000 * 60000
        key = (ws, f"event_{e['event_type']}", e["category_id"])
        counts[key] = counts.get(key, 0) + 1
    return counts


def _as_doris_keys(counts: dict[tuple[int, str, str], int]) -> dict:
    return {(epoch_ms_to_utc_string(ws), m, d): v for (ws, m, d), v in counts.items()}


def test_probe_events_all_valid_and_unique() -> None:
    """探针事件：uuid 唯一、近窗时间、quantity 补齐 add_to_cart。"""
    now = 1792844285768
    events = build_probe_events(12, now_ms=now)
    ids = [e["event_id"] for e in events]
    assert len(set(ids)) == 12
    assert all(e["category_id"] == PROBE_CATEGORY for e in events)
    # 时间全在过去（< now）且跨度小（12*300ms）
    assert all(now - 60_000 <= e["event_time"] < now for e in events)
    # add_to_cart 必带 quantity
    for e in events:
        if e["event_type"] == "add_to_cart":
            assert e.get("quantity") is not None


def test_watermark_tick_is_current_and_not_probe_category() -> None:
    """踢水位事件：event_time=当下、category 非保留段（不进探针键集）。"""
    now = 1792844285768
    tick = build_watermark_tick(now_ms=now)
    assert tick["event_time"] == now
    assert tick["category_id"] != PROBE_CATEGORY
    assert tick["event_id"] != ""


def test_probe_window_bounds_aligned_and_covers_batch() -> None:
    """窗口范围：1 min 对齐、[min,max] 覆盖批内全部窗口起点。"""
    now = int(time.time() * 1000)
    events = build_probe_events(24, now_ms=now)
    lo, hi = probe_window_bounds_ms(events)
    assert lo % 60_000 == 0 and hi % 60_000 == 0
    for e in events:
        ws = e["event_time"] // 60_000 * 60_000
        assert lo <= ws <= hi


def test_probe_doris_sql_filters_window_range() -> None:
    """SQL 模板必须带窗口范围（首跑教训：旧批重放残留行会污染判定）。"""
    sql = PROBE_DORIS_SQL_TEMPLATE.format(
        cat=PROBE_CATEGORY, ws_min="2026-10-24 12:00:00", ws_max="2026-10-24 12:01:00"
    )
    assert "is_settled = 1" in sql
    assert f"dim_key = '{PROBE_CATEGORY}'" in sql
    assert "window_start >= '2026-10-24 12:00:00'" in sql
    assert "window_start <= '2026-10-24 12:01:00'" in sql


def test_probe_recon_pass_when_identical() -> None:
    """期望与 Doris 在探针键集上逐值相等 ⇒ PASS。"""
    events = build_probe_events(6, now_ms=1792844285768)
    counts = _mirror_counts(events)
    result = check_probe_reconciliation(counts, _as_doris_keys(counts))
    assert result.status is Status.PASS
    assert result.name == "recon-offline-mirror-doris"


def test_probe_recon_fail_on_value_mismatch() -> None:
    events = build_probe_events(6, now_ms=1792844285768)
    counts = _mirror_counts(events)
    doris = {k: v + 1 for k, v in _as_doris_keys(counts).items()}
    result = check_probe_reconciliation(counts, doris)
    assert result.status is Status.FAIL


def test_probe_recon_fail_on_missing_doris_rows() -> None:
    """Doris 侧探针键集空（作业没消费/没落定）⇒ FAIL，不是 SKIP。"""
    events = build_probe_events(6, now_ms=1792844285768)
    counts = _mirror_counts(events)
    result = check_probe_reconciliation(counts, {})
    assert result.status is Status.FAIL


def test_probe_recon_fails_when_foreign_category_leaks_into_collection() -> None:
    """历史 category 的行混进采集结果 ⇒ 红——采集器必须按 dim_key=c_recon
    过滤的契约固化（全外连接：doris 多键=多写 ⇒ FAIL，这是设计而非意外）。"""
    events = build_probe_events(6, now_ms=1792844285768)
    counts = _mirror_counts(events)
    doris = _as_doris_keys(counts)
    leaked = {**doris, ("2026-10-21 11:47:00", "event_page_view", "c_00001"): 5}
    result = check_probe_reconciliation(counts, leaked)
    assert result.status is Status.FAIL  # 混入历史键 ⇒ 红（采集器过滤契约）


def test_probe_recon_fails_when_stale_probe_window_leaks_into_collection() -> None:
    """上一轮探针批的残留行（同 c_recon、**不同窗口**）混进采集 ⇒ 红——
    采集器按本批窗口范围过滤的契约固化（首跑实测教训：无 HA 重启重放旧批）。"""
    events = build_probe_events(6, now_ms=1792844285768)
    counts = _mirror_counts(events)
    doris = _as_doris_keys(counts)
    stale = {**doris, ("2026-10-24 12:18:00", "event_page_view", PROBE_CATEGORY): 2}
    result = check_probe_reconciliation(counts, stale)
    assert result.status is Status.FAIL  # 旧窗口残留 ⇒ 红（窗口范围过滤契约）


def test_probe_recon_ignores_unsettled_provisional_rows() -> None:
    """is_settled=0 的暂定行混进采集结果 ⇒ **不判**（词表双保险）：
    provisional_emitted 不在 event_* 词表，即使采集层过滤失效也不进比对
    （票面 AC 第 3 条的判定面负例）。"""
    events = build_probe_events(6, now_ms=1792844285768)
    counts = _mirror_counts(events)
    doris = _as_doris_keys(counts)
    contaminated = {
        **doris,
        ("2026-10-24 13:11:00", "provisional_emitted", PROBE_CATEGORY): 24,
        ("2026-10-24 13:02:00", "uv", ""): 99,
    }
    result = check_probe_reconciliation(counts, contaminated)
    assert result.status is Status.PASS  # 暂定/自有指标差异不导致失败


def test_send_watermark_ticks_targets_every_partition(monkeypatch) -> None:
    """逐分区踢水位：每个分区恰好一条、et 同值（MIN(subtask) 水位语义的修法）。"""
    import types

    import ecomgen.reconcile_probe as rp

    produced: list[tuple[int, dict]] = []

    class FakePartitionMeta:
        """镜像 confluent_kafka 的 partitions 元数据：可迭代且可 len。"""

        def __init__(self, ids: list[int]) -> None:
            self._ids = ids

        def __iter__(self):
            return iter(self._ids)

        def __len__(self) -> int:
            return len(self._ids)

    class FakeTopicMeta:
        def __init__(self, ids: list[int]) -> None:
            self.partitions = FakePartitionMeta(ids)

    class FakeClusterMetadata:
        def __init__(self, ids: list[int]) -> None:
            self.topics = {rp.KAFKA_TOPIC: FakeTopicMeta(ids)}

    class FakeProducer:
        def __init__(self, *_a, **_k) -> None:
            pass

        def list_topics(self, topic=None, timeout=-1):
            return FakeClusterMetadata([0, 1, 2, 3, 4, 5])

        def produce(self, *, topic, partition, key, value):
            import json as _json

            produced.append((partition, _json.loads(value.decode())))

        def poll(self, _t: float = 0) -> int:
            return 0

        def flush(self, _t: float = 0) -> int:
            return 0

    fake_mod = types.ModuleType("confluent_kafka")
    fake_mod.Producer = FakeProducer  # type: ignore[attr-defined]
    monkeypatch.setitem(__import__("sys").modules, "confluent_kafka", fake_mod)
    now = 1792844285768
    sent = rp.send_watermark_ticks(now_ms=now)
    assert sent == 6
    assert sorted(p for p, _ in produced) == [0, 1, 2, 3, 4, 5]
    assert all(ev["event_time"] == now for _, ev in produced)
