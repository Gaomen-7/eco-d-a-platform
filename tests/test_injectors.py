"""三个注入器的完整判据（TDD 第二片：超期 + 混沌 + 语义互不混用）。

判据来源：gdd.md §1.1 流程 3、spec.md §4.7、票面验收第 3 条。
"""

from __future__ import annotations

import random

from ecomgen.injectors import (
    ALLOWED_LATENESS_MS,
    chaos_extra_step,
    correctable_offset_ms,
    expired_offset_ms,
)


def test_expired_offset_exceeds_allowed_lateness() -> None:
    """超期迟到的偏移必须大于 120 s 允许延迟：130 s ~ 300 s。"""
    rng = random.Random(42)
    offsets = [expired_offset_ms(rng) for _ in range(500)]

    assert all(130_000 <= o <= 300_000 for o in offsets), "超期偏移越界"
    assert all(o > ALLOWED_LATENESS_MS for o in offsets), "超期迟到必须超允许延迟"


def test_correctable_and_expired_ranges_do_not_overlap() -> None:
    """语义互不混用：两个注入器的偏移区间必须不相交（gdd Q21）。"""
    rng = random.Random(1)
    correctable = [correctable_offset_ms(rng) for _ in range(300)]
    expired = [expired_offset_ms(rng) for _ in range(300)]

    assert max(correctable) < min(expired), "可修正与超期区间重叠"


def test_chaos_extra_step_is_off_at_zero_ratio() -> None:
    """chaos-ratio 默认 0 ⇒ 恒为 False（漏斗 100% 连贯，gdd §1.1 判据）。"""
    rng = random.Random(3)
    assert all(chaos_extra_step(rng, 0.0) is False for _ in range(200))


def test_chaos_extra_step_triggers_at_full_ratio() -> None:
    """chaos-ratio 1.0 ⇒ 恒为 True（跳步）。"""
    rng = random.Random(3)
    assert all(chaos_extra_step(rng, 1.0) is True for _ in range(50))


def test_chaos_extra_step_rate_matches_ratio() -> None:
    """跳步频率应约等于 chaos-ratio（宽松容差，测的是分布而非精确值）。"""
    rng = random.Random(11)
    hits = sum(chaos_extra_step(rng, 0.3) for _ in range(2000))
    assert 480 <= hits <= 720, f"实测跳步率 {hits / 2000:.3f} 偏离 0.3 过多"
