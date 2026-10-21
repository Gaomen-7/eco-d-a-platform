"""事件生产流水线的测试（TDD 第六片：把维度、漏斗、注入器装配成事件流）。

判据来源：spec.md §4.7（注入器判据）、gdd.md §1.1（判据 3/4）、票面验收第 3 条。
接缝 = `ecomgen.producer`：给定 seed 与参数，产出一条**可复现的事件序列**，
每条事件都带「它属于哪个通道」的标记（正常/可修正迟到/超期迟到）。
"""

from __future__ import annotations

from ecomgen.producer import EventBatch, ProducerParams, produce_batch


def _batch(**overrides: object) -> EventBatch:
    params = ProducerParams(seed=42, n_sessions=400, **overrides)  # type: ignore[arg-type]
    return produce_batch(params)


def test_batch_is_reproducible_for_same_seed() -> None:
    """同种子同参数 ⇒ 事件序列逐字节相同（REQ-F-GEN-06）。"""
    a = _batch()
    b = _batch()
    assert [e.to_json() for e in a.events] == [e.to_json() for e in b.events]


def test_different_seed_yields_different_events() -> None:
    a = produce_batch(ProducerParams(seed=1, n_sessions=200))
    b = produce_batch(ProducerParams(seed=2, n_sessions=200))
    assert [e.to_json() for e in a.events] != [e.to_json() for e in b.events]


def test_all_produced_events_are_schema_valid() -> None:
    """生成器自己的产出必须通过自己的校验（否则判据自相矛盾）。"""
    batch = _batch()
    for event in batch.events:
        assert event.violations == [], (
            f"{event.record.get('event_id')} 违规: {event.violations}"
        )


def test_late_correctable_count_matches_ratio() -> None:
    """`--late-correctable 0.03` ⇒ 约 3% 的事件走可修正迟到通道。"""
    batch = _batch(late_correctable=0.03, late_expired=0.0)
    total = len(batch.events)
    n_correctable = batch.count_channel("late_correctable")
    assert total > 0
    assert 0.01 <= n_correctable / total <= 0.06, (
        f"实测比例 {n_correctable / total:.3f}"
    )


def test_late_expired_count_matches_ratio() -> None:
    """`--late-expired 0.02` ⇒ 约 2% 走超期通道（票面验收第 3 条的一半）。"""
    batch = _batch(late_correctable=0.0, late_expired=0.02)
    total = len(batch.events)
    n_expired = batch.count_channel("late_expired")
    assert 0.005 <= n_expired / total <= 0.045, f"实测比例 {n_expired / total:.3f}"


def test_channels_are_mutually_exclusive() -> None:
    """三个注入器语义互不混用（gdd Q21）：一条事件只属一个迟到通道。"""
    batch = _batch(late_correctable=0.05, late_expired=0.05)
    for event in batch.events:
        assert not (event.is_late_correctable and event.is_late_expired)


def test_late_offsets_are_relative_to_watermark() -> None:
    """偏移相对水印：迟到事件的 event_time 严格小于它自己的水印基准。"""
    batch = _batch(late_correctable=0.05, late_expired=0.02)
    late = [e for e in batch.events if e.is_late_correctable or e.is_late_expired]
    assert late, "样本未产生迟到事件"
    for event in late:
        assert event.watermark_ms is not None
        assert event.record["event_time"] < event.watermark_ms


def test_purchase_always_present_with_preceding_steps() -> None:
    """默认 chaos=0：所有 purchase 都有完整前置步（gdd §1.1 判据）。"""
    batch = _batch(chaos_ratio=0.0)
    for event in batch.events:
        if event.record["event_type"] == "purchase":
            steps = batch.session_steps(event.session_id)
            assert steps == ["view_item", "add_to_cart", "begin_checkout", "purchase"]


def test_event_ids_are_unique() -> None:
    """`event_id` 全局唯一（SRS 规则⑤；重复是另一条通道，生成侧不应制造重复）。"""
    batch = _batch()
    ids = [e.record["event_id"] for e in batch.events]
    assert len(ids) == len(set(ids))


def test_late_expired_events_are_actually_expired_under_flink_watermark() -> None:
    """超期事件必须在 **Flink 的水位语义**下真的超期，而不只是「偏移大」。

    ★ 这条是 P1 实机证伪后补的（2026-10-21）：旧测试只断言「比例对、偏移为负」，
    但实机 `ads_late_expired_count` 恒为 **0 行**。根因与偏移无关，是**发送序与
    水位推进速率**的关系：

    - 生成器按 `_WATERMARK_STEP_MS` 逐事件推进水印（步进 5 ms/条）；
    - Flink 的窗口只有在 `watermark > windowEnd + allowedLateness` 时才清理，
      被清理窗口里的事件才走 `sideOutputLateData`；
    - 一条超期事件的 `event_time = 当前水印 − 130..300 s`，若本批总跨度只有
      271 s，则**排在该事件之后的事件不足以把水位推到它的窗口之后** ⇒
      它压根不会被判超期，而是被当普通迟到元素喂给已关闭窗口（或被静默丢弃）。

    ⇒ 判据必须模拟 Flink 语义：对每条超期事件，检查**它之后的发送序**是否把水位
      推过了「它所属窗口 + 允许延迟」。这才是「超期段确有迟到计数条目」的前提。
    """
    from ecomgen.injectors import ALLOWED_LATENESS_MS

    window_ms = 60_000
    batch = _batch(late_correctable=0.03, late_expired=0.02)

    expired_events = [e for e in batch.events if e.is_late_expired]
    assert expired_events, "样本未产生超期事件"

    # 重放发送序，用「至今最大 event_time − 允许延迟」作为水位（与 GateSegment 的
    # forBoundedOutOfOrderness 同构），逐条判定它自己的窗口是否已被清理。
    sent_max = 0
    provably_expired = 0
    for event in batch.events:
        t = int(event.record["event_time"])
        watermark = sent_max - ALLOWED_LATENESS_MS
        if event.is_late_expired:
            window_end = (t // window_ms) * window_ms + window_ms
            # 窗口已清理 ⇒ Flink 会把这条走 sideOutputLateData（= 真的超期）
            if watermark > window_end + ALLOWED_LATENESS_MS:
                provably_expired += 1
        sent_max = max(sent_max, t)

    assert provably_expired > 0, (
        "超期通道在 Flink 水位语义下**不可达**："
        f"共 {len(expired_events)} 条超期标记事件，但没有一条落在已清理窗口之后。"
        "这说明生成器的水位推进速率与注入偏移不匹配 —— 实机会表现为 "
        "ads_late_expired_count 恒 0 行。"
    )


def test_late_expired_reachable_within_single_kafka_partition() -> None:
    """超期必须在 **单个 Kafka 分区内**可达 —— 分区级才是 Flink 的真实语义。

    ★ P1 实机证伪（2026-10-21）：`ads_late_expired_count` 恒 0 行，DLQ 也恒 0。

    上一版判据只看**全局**发送序，看似可达；但 Flink 的 `KafkaSource` 是
    **每分区一个 subtask、各自独立算水位**（`WatermarkStrategy` 是 per-subtask 的）。
    生成器按 `user_id` 分区键打散，同一分区内的事件被抽稀，**分区内乱序幅度
    远小于全局** ⇒ 分区水位推不过超期事件的「窗口 + 允许延迟」⇒ 该事件不被判超期。

    实测证据（`--seed 7 --sessions 200`，6 分区）：
      全局重放 expired>0，但逐分区重放 **expired=0 for all 6 partitions**。

    ⇒ 判据必须按「同一 user_id 的发送序」重放，因为那才是同一分区的顺序。
    """
    from ecomgen.injectors import ALLOWED_LATENESS_MS

    window_ms = 60_000
    batch = _batch(late_correctable=0.03, late_expired=0.02)

    # 同 user_id ⇒ 同分区键 ⇒ 同 Flink subtask ⇒ 同一水位推进序列
    by_user: dict[str, list[tuple[int, bool]]] = {}
    for event in batch.events:
        by_user.setdefault(event.session_id, []).append(
            (int(event.record["event_time"]), event.is_late_expired)
        )

    provably_expired = 0
    for events in by_user.values():
        sent_max = 0
        for t, is_expired in events:
            watermark = sent_max - ALLOWED_LATENESS_MS
            if is_expired:
                window_end = (t // window_ms) * window_ms + window_ms
                if watermark > window_end + ALLOWED_LATENESS_MS:
                    provably_expired += 1
            sent_max = max(sent_max, t)

    assert provably_expired > 0, (
        "超期通道在**单分区**内不可达 ⇒ 实机 ads_late_expired_count 必然恒 0 行。"
        f"本批超期标记 {batch.count_channel('late_expired')} 条，无一落入已清理窗口。"
    )


def test_batch_params_validate_ratios() -> None:
    """参数越界必须尽早报错，而不是产出畸形批次。"""
    import pytest

    with pytest.raises(ValueError):
        ProducerParams(seed=1, n_sessions=10, late_correctable=1.5)
    with pytest.raises(ValueError):
        ProducerParams(seed=1, n_sessions=10, late_expired=-0.1)
