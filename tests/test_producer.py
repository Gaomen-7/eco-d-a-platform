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


def test_batch_params_validate_ratios() -> None:
    """参数越界必须尽早报错，而不是产出畸形批次。"""
    import pytest

    with pytest.raises(ValueError):
        ProducerParams(seed=1, n_sessions=10, late_correctable=1.5)
    with pytest.raises(ValueError):
        ProducerParams(seed=1, n_sessions=10, late_expired=-0.1)
