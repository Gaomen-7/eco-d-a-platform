"""漏斗与会话轨迹的测试（TDD 第四片）。

判据来源：gdd.md §1.1（会话轨迹：view_item → add_to_cart → begin_checkout →
purchase 按步进概率前行；purchase 必有前置步）+ SRS `REQ-F-GEN-05`（逐级递减）。
接缝 = `ecomgen.funnel` 的纯函数：给定 rng 与 chaos 设置，产出一条会话的步序列。
"""

from __future__ import annotations

import random

from ecomgen.funnel import FunnelParams, generate_session_steps


def test_steps_are_a_prefix_of_the_funnel_without_chaos() -> None:
    """chaos=0 时会话轨迹必须是漏斗的前缀：沿固定顺序推进，不跳不倒序。"""
    rng = random.Random(5)
    params = FunnelParams(chaos_ratio=0.0)
    funnel = ["view_item", "add_to_cart", "begin_checkout", "purchase"]

    for _ in range(300):
        steps = generate_session_steps(rng, params)
        assert steps == funnel[: len(steps)], f"轨迹非法: {steps}"


def test_non_empty_session_starts_with_view_item() -> None:
    """漏斗第 1 步是入口：**非空**会话必以 `view_item` 起始。

    空会话合法——用户可能进入站点但从未点开任何商品详情。这类会话只贡献
    `page_view`，不进入漏斗计数。
    """
    rng = random.Random(6)
    params = FunnelParams(chaos_ratio=0.0)
    non_empty = 0
    for _ in range(500):
        steps = generate_session_steps(rng, params)
        if steps:
            non_empty += 1
            assert steps[0] == "view_item"
    assert non_empty > 0


def test_purchase_always_has_its_preceding_steps_without_chaos() -> None:
    """chaos=0 时 purchase 100% 有前置步（gdd §1.1 判据）。"""
    rng = random.Random(8)
    params = FunnelParams(chaos_ratio=0.0)
    funnel = ["view_item", "add_to_cart", "begin_checkout", "purchase"]

    purchases = 0
    for _ in range(2000):
        steps = generate_session_steps(rng, params)
        if "purchase" in steps:
            purchases += 1
            assert steps == funnel, f"purchase 缺前置步: {steps}"
    assert purchases > 0, "样本里没有 purchase，判据无法生效"


def test_funnel_is_monotone_decreasing_over_many_sessions() -> None:
    """逐级递减（REQ-F-GEN-05）：各步会话数沿 1→4 非增。"""
    rng = random.Random(9)
    params = FunnelParams()
    funnel = ["view_item", "add_to_cart", "begin_checkout", "purchase"]
    counts = dict.fromkeys(funnel, 0)

    sessions = 5000
    for _ in range(sessions):
        for step in generate_session_steps(rng, params):
            counts[step] += 1

    assert (
        counts["view_item"]
        >= counts["add_to_cart"]
        >= counts["begin_checkout"]
        >= counts["purchase"]
    )
    # 递减必须**严格**：四级全部出现且逐级变少，否则「递减」是空话
    assert counts["purchase"] < counts["view_item"]


def test_chaos_can_skip_intermediate_steps() -> None:
    """chaos>0 时允许跳步：出现 purchase 但缺中间步的会话。"""
    rng = random.Random(12)
    params = FunnelParams(chaos_ratio=0.5)
    funnel = ["view_item", "add_to_cart", "begin_checkout", "purchase"]

    skipped = 0
    for _ in range(1000):
        steps = generate_session_steps(rng, params)
        if "purchase" in steps and steps != funnel:
            skipped += 1
    assert skipped > 0, "chaos=0.5 下应出现跳步会话"
