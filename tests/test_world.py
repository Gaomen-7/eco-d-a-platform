"""维度数据与事件生成的测试（TDD 第五片）。

判据来源：gdd.md §1.1 流程 2（先产出维度数据：商品/品类/用户/地域/渠道）
+ SRS §3.3（维度字段的取值来源）。
接缝 = `ecomgen.world`：一次性建出可复现的维度世界，事件生成从其取属性。
"""

from __future__ import annotations

import random

from ecomgen.world import World, build_world


def test_world_is_deterministic_for_same_seed() -> None:
    """同种子同世界（维度也是产出一部分，必须可复现）。"""
    a = build_world(seed=99)
    b = build_world(seed=99)
    assert a.items == b.items
    assert a.categories == b.categories
    assert a.users == b.users
    assert a.provinces == b.provinces
    assert a.channels == b.channels


def test_world_differs_for_different_seed() -> None:
    assert build_world(seed=1).items != build_world(seed=2).items


def test_world_sizes_are_configured() -> None:
    """用户池默认 10 万（gdd 【已确认 Q18】），但测试用小值以免慢。"""
    world = build_world(seed=1, n_users=50, n_items=20)
    assert len(world.users) == 50
    assert len(world.items) == 20


def test_every_item_belongs_to_a_known_category() -> None:
    """商品→品类引用完整性：生成器不能产出悬空引用。"""
    world = build_world(seed=3, n_items=40)
    for item in world.items:
        assert item.category_id in world.categories, f"{item.item_id} 品类悬空"


def test_provinces_and_channels_are_non_empty_strings() -> None:
    world = build_world(seed=4)
    assert all(isinstance(p, str) and p for p in world.provinces)
    assert all(isinstance(c, str) and c for c in world.channels)


def test_world_is_immutable_snapshot() -> None:
    """World 是只读快照：事件生成不得改写维度（避免跨事件串味）。"""
    world: World = build_world(seed=5, n_items=5)
    try:
        world.items.append(world.items[0])  # type: ignore[union-attr]
    except (AttributeError, TypeError):
        return
    raise AssertionError("World.items 应不可变")


def test_rng_is_derived_from_seed_not_shared() -> None:
    """两条独立会话流在同种子下产出相同序列，互不干扰对方的 rng。"""
    world = build_world(seed=7, n_items=10)
    r1 = world.rng_for("stream-a")
    r2 = world.rng_for("stream-a")
    r3 = world.rng_for("stream-b")
    a = [r1.random() for _ in range(5)]
    b = [r2.random() for _ in range(5)]
    c = [r3.random() for _ in range(5)]
    assert a == b
    assert a != c


def test_world_rng_for_returns_random_instance() -> None:
    world = build_world(seed=8)
    assert isinstance(world.rng_for("x"), random.Random)
