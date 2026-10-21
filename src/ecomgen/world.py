"""维度世界（商品/品类/用户/地域/渠道）—— gdd.md §1.1 流程 2。

**为什么把维度独立成物**：生成器要维护商品/品类/地域/渠道维度数据，且事件里的
维度属性必须**引用完整**（不能出现悬空品类）。把维度建成一次性、不可变的快照，
事件生成只读不写，就能同时满足三件事：

1. 引用完整性由构造保证（商品建时就挂在既有品类下）；
2. `--seed` 可复现（世界由种子派生，不含墙上时钟）；
3. 事件流之间的随机数**互不串味**（`rng_for` 派生独立流，而不是共用一个全局 rng）。

维度是**设计值**，不承诺真实电商分布 —— 承诺的是可复现与引用完整。
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import Final

# 省级行政区（地域维度，SRS §3.3 `province`）。取覆盖面广的常见集合。
PROVINCES: Final[tuple[str, ...]] = (
    "北京市",
    "上海市",
    "广东省",
    "浙江省",
    "江苏省",
    "四川省",
    "山东省",
    "河南省",
    "湖北省",
    "湖南省",
    "福建省",
    "陕西省",
)

# 渠道枚举（SRS §3.3 `channel`）
CHANNELS: Final[tuple[str, ...]] = (
    "organic",
    "paid_search",
    "social",
    "email",
    "direct",
    "affiliate",
)

# 来源端（SRS §3.3 `source`）
SOURCES: Final[tuple[str, ...]] = ("web", "ios", "android", "mini_program")

# 页面标识（SRS §3.3 `page`）
PAGES: Final[tuple[str, ...]] = (
    "home",
    "category",
    "item_detail",
    "cart",
    "checkout",
    "payment_result",
)

_N_CATEGORIES: Final[int] = 12


@dataclass(frozen=True)
class Item:
    """商品维度。`category_id` 必须指向既有品类（引用完整性）。"""

    item_id: str
    category_id: str
    price_cents: int


@dataclass(frozen=True)
class World:
    """只读维度快照。

    `items` / `categories` 等用 tuple 而非 list：快照一旦建立就不该被改写，
    否则事件之间会互相串味（不可复现）。
    """

    seed: int
    categories: tuple[str, ...]
    items: tuple[Item, ...]
    users: tuple[str, ...]
    provinces: tuple[str, ...]
    channels: tuple[str, ...]
    sources: tuple[str, ...] = field(default_factory=lambda: SOURCES)
    pages: tuple[str, ...] = field(default_factory=lambda: PAGES)

    def rng_for(self, stream: str) -> random.Random:
        """从主种子派生一条**独立**随机流。

        为什么需要它：漏斗推进、注入器、金额抽样各自消费随机数，若共用一个
        rng，任何一处改动都会平移后面所有产出 ⇒ `--seed` 复现判据变得脆弱。
        以「种子 + 流名」派生，各流互不干扰，且同种子同流恒定。
        """
        return random.Random(f"{self.seed}:{stream}")


def build_world(
    seed: int,
    *,
    n_users: int = 100_000,
    n_items: int = 2_000,
    n_categories: int = _N_CATEGORIES,
) -> World:
    """构建维度世界。同 `seed` + 同规模 ⇒ 完全相同（REQ-F-GEN-06）。"""
    if n_users < 1 or n_items < 1 or n_categories < 1:
        raise ValueError("维度规模必须为正")

    rng = random.Random(seed)

    categories = tuple(f"c_{i:05d}" for i in range(n_categories))

    items: list[Item] = []
    for i in range(n_items):
        items.append(
            Item(
                item_id=f"i_{i:06d}",
                category_id=categories[rng.randrange(n_categories)],
                # 价格区间 5 元 ~ 2000 元（分），设计值
                price_cents=rng.randrange(500, 200_001),
            )
        )

    users = tuple(f"u_{i:06d}" for i in range(n_users))

    return World(
        seed=seed,
        categories=categories,
        items=tuple(items),
        users=users,
        provinces=PROVINCES,
        channels=CHANNELS,
    )
