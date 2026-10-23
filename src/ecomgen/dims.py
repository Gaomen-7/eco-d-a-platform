"""维度变更流（gdd §1.1 流程 2 / §D6.1；spec §5.2 拉链）。

**为什么维度变更流要单独成物**：DIM 层的三张拉链表 `dim_item`/`dim_category`/
`dim_user` 的拉链时间源是**变更事件的 `change_time`**（事件时间语义，满足
canonical-truth §3.2 D-3）。没有变更流，拉链表就只有一个「初始版本」，
`valid_to_ms` 永远等于哨兵值，时态 join 退化成「永远命中最新版」——
那正是 `REQ-F-LAKE-03` 想排除的假时态。

**为什么与事件流分开落盘**：事件流的字节由 `--seed` 决定且已被判据锁定
（同 seed 同哈希）。把维度变更**混进事件文件**会改动事件字节、把 03b 的
门槛批判据一起推翻。故维度变更流写**另一个文件**，由 `--dim-out` 指定。

**可复现**：全部取值来自 `world.rng_for("dim")` 这条**派生流**——与 funnel /
late / attr 各流互不串味（同 seed 同产出，改事件生成不会平移维度）。

**区间形状**（[05] Q10 签字口径）：
- 每个对象至少一个「初始版本」，`change_time` = `start_ms`（批次最早事件时刻）；
  ⇒ 批次内**每一条**事件都落在某个版本区间里，时态 join 不会漏行。
- 变更时间严格落在 `(start_ms, end_ms)` 内 ⇒ 跨日多版本自然出现（3 天跨度）。
- 区间的 `valid_from_ms`/`valid_to_ms` **不在这里算**：它们是**同一 id 的版本按
  `change_time` 排序后的相邻关系**，由 `sql/replay_30_dim.sql` 用窗口函数
  `LEAD` 一次算出（一处实现，别在两处各算一遍）。
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Final

from ecomgen.jsonl import write_jsonl
from ecomgen.world import PRICE_CENTS_MAX, PRICE_CENTS_MIN, World

# ── 维度种类标记（`dim` 字段取值；SQL 侧按它分流到五张表）─────────────────
DIM_ITEM: Final[str] = "item"
DIM_CATEGORY: Final[str] = "category"
DIM_USER: Final[str] = "user"
DIM_REGION: Final[str] = "region"
DIM_CHANNEL: Final[str] = "channel"

DIM_KINDS: Final[tuple[str, ...]] = (
    DIM_ITEM,
    DIM_CATEGORY,
    DIM_USER,
    DIM_REGION,
    DIM_CHANNEL,
)

# ── 变更比例（**设计值**，不是官方承诺）──────────────────────────────────
# 只要求「跨日多版本真实存在且可抽查」，不承诺真实电商的维度变更率。
ITEM_CHANGE_RATIO: Final[float] = 0.20
ITEM_SECOND_CHANGE_RATIO: Final[float] = 0.05
# 变更里改品类的比例：品类 subject 的时态 join 靠它才有可观测差异
# （若品类从不变，`dws_subject_window` 的 category 行与「直接取事件字段」无法区分）。
ITEM_CATEGORY_CHANGE_RATIO: Final[float] = 0.30
USER_CHANGE_RATIO: Final[float] = 0.03
CATEGORY_CHANGE_RATIO: Final[float] = 0.25

# ── 静态维（无拉链）的取值域（**设计值**）────────────────────────────────
# ★ 省份映射必须与 `world.PROVINCES` **逐项对齐**，不靠「缺了就填未知」兜底：
#   少一个映射 ⇒ 该省份在 dim_region 里落成 ("未知","")，而 dim_region 的主键就是
#   省份 ⇒ 表上看不出错，只有「未知」占比异常。故下面取值用 `[...]` 而不是 `.get`：
#   缺即抛（暴露在 `ecomgen dump` 第一步），并由
#   tests/test_dims.py::test_static_dimensions_cover_the_world_enums 钉住键集合对齐。
_PROVINCE_REGION: Final[dict[str, tuple[str, str]]] = {
    "北京市": ("华北", "110000"),
    "上海市": ("华东", "310000"),
    "广东省": ("华南", "440000"),
    "浙江省": ("华东", "330000"),
    "江苏省": ("华东", "320000"),
    "四川省": ("西南", "510000"),
    "山东省": ("华东", "370000"),
    "河南省": ("华中", "410000"),
    "湖北省": ("华中", "420000"),
    "湖南省": ("华中", "430000"),
    "福建省": ("华东", "350000"),
    "陕西省": ("西北", "610000"),
}

_CHANNEL_GROUP: Final[dict[str, tuple[str, int]]] = {
    "organic": ("自然", 0),
    "direct": ("自然", 0),
    "email": ("自然", 0),
    "social": ("付费", 1),
    "paid_search": ("付费", 1),
    "affiliate": ("付费", 1),
}

_BRANDS: Final[tuple[str, ...]] = ("Nova", "Orbit", "Pine", "Quartz", "Rivet", "Solace")
_USER_LEVELS: Final[tuple[str, ...]] = ("普通", "白银", "黄金", "白金")


def _item_row(
    *,
    item_id: str,
    item_name: str,
    category_id: str,
    price_cents: int,
    brand: str,
    change_time: int,
) -> dict[str, Any]:
    return {
        "dim": DIM_ITEM,
        "item_id": item_id,
        "item_name": item_name,
        "category_id": category_id,
        "price_cents": price_cents,
        "brand": brand,
        "change_time": change_time,
    }


def build_dimension_stream(
    world: World, *, start_ms: int, end_ms: int
) -> list[dict[str, Any]]:
    """产出维度变更流（一行一个**版本**，顺序稳定）。

    `start_ms` = 批次最早事件时刻（初始版本的 `change_time`）；
    `end_ms` = 批次事件时间的开区间上界（变更时刻严格小于它）——
    由调用方从**已产出的事件批次**里取，保证「事件落在区间内」由构造成立。
    """
    if end_ms <= start_ms + 1:
        raise ValueError(f"时间跨度太小：start_ms={start_ms} end_ms={end_ms}")

    rng = world.rng_for("dim")
    rows: list[dict[str, Any]] = []

    # ── 静态维：无拉链，一次给全 ────────────────────────────────────────
    for province in world.provinces:
        # 缺映射**不是**可容忍的降级：静默产出 ("未知","") 会让 dim_region 少一个
        # 真实大区，而表上完全正常。缺即抛，暴露在 `ecomgen dump` 的第一步。
        region, code = _PROVINCE_REGION[province]
        rows.append(
            {
                "dim": DIM_REGION,
                "province": province,
                "region": region,
                "province_code": code,
            }
        )
    for channel in world.channels:
        # 与省份同理：渠道缺映射会让 dim_channel 少一个真实渠道组，缺即抛。
        group, is_paid = _CHANNEL_GROUP[channel]
        rows.append(
            {
                "dim": DIM_CHANNEL,
                "channel": channel,
                "channel_group": group,
                "is_paid": is_paid,
            }
        )

    # ── 品类：初始版本 + 少量改名 ──────────────────────────────────────
    for idx, category_id in enumerate(world.categories):
        name = f"品类{idx:02d}"
        rows.append(
            {
                "dim": DIM_CATEGORY,
                "category_id": category_id,
                "category_name": name,
                "parent_id": None,
                "level": 1,
                "change_time": start_ms,
            }
        )
        if rng.random() < CATEGORY_CHANGE_RATIO:
            rows.append(
                {
                    "dim": DIM_CATEGORY,
                    "category_id": category_id,
                    "category_name": f"{name}-新版",
                    "parent_id": None,
                    "level": 1,
                    "change_time": rng.randrange(start_ms + 1, end_ms),
                }
            )

    # ── 商品：初始版本 + 价格/品牌/（部分）品类变更 ────────────────────
    for idx, item in enumerate(world.items):
        name = f"商品{idx:04d}"
        brand = rng.choice(_BRANDS)
        rows.append(
            _item_row(
                item_id=item.item_id,
                item_name=name,
                category_id=item.category_id,
                price_cents=item.price_cents,
                brand=brand,
                change_time=start_ms,
            )
        )
        change_time = None
        if rng.random() < ITEM_CHANGE_RATIO:
            change_time = rng.randrange(start_ms + 1, end_ms)
            new_category = item.category_id
            if rng.random() < ITEM_CATEGORY_CHANGE_RATIO:
                # 换到**另一个**品类：保证品类 subject 的时态差异真实存在
                others = [c for c in world.categories if c != item.category_id]
                new_category = others[rng.randrange(len(others))]
            rows.append(
                _item_row(
                    item_id=item.item_id,
                    item_name=name,
                    category_id=new_category,
                    price_cents=rng.randrange(PRICE_CENTS_MIN, PRICE_CENTS_MAX),
                    brand=rng.choice(_BRANDS),
                    change_time=change_time,
                )
            )
        # 第二版变更：只有「第一版也变过」的对象才可能有，保证版本序单调
        if change_time is not None and rng.random() < ITEM_SECOND_CHANGE_RATIO:
            second = rng.randrange(change_time + 1, end_ms)
            rows.append(
                _item_row(
                    item_id=item.item_id,
                    item_name=name,
                    category_id=item.category_id,
                    price_cents=rng.randrange(PRICE_CENTS_MIN, PRICE_CENTS_MAX),
                    brand=rng.choice(_BRANDS),
                    change_time=second,
                )
            )

    # ── 用户：初始版本 + 少量等级变更 ──────────────────────────────────
    for user_id in world.users:
        level = rng.choice(_USER_LEVELS)
        province = rng.choice(world.provinces)
        register_date = f"2023-{rng.randint(1, 12):02d}-{rng.randint(1, 28):02d}"
        rows.append(
            {
                "dim": DIM_USER,
                "user_id": user_id,
                "user_level": level,
                "province": province,
                "register_date": register_date,
                "change_time": start_ms,
            }
        )
        if rng.random() < USER_CHANGE_RATIO:
            rows.append(
                {
                    "dim": DIM_USER,
                    "user_id": user_id,
                    "user_level": rng.choice(_USER_LEVELS),
                    "province": province,
                    "register_date": register_date,
                    "change_time": rng.randrange(start_ms + 1, end_ms),
                }
            )

    return rows


def write_dimension_stream(path: Path, rows: list[dict[str, Any]]) -> None:
    """写 JSONL —— 共用 `ecomgen.jsonl.write_jsonl`（与事件流同一份实现）。

    ★ 实现**不在这里**：事件流与维度流必须同字节约定（键序固定 / 无空格 / LF /
    末尾换行）。两侧各写一遍 json.dumps 时，任一侧漏掉 sort_keys 就按文件类型分叉，
    而两份文件各自的单测都还是绿的（各自和自己比当然相等）。
    """
    write_jsonl(path, rows)


def count_by_kind(rows: list[dict[str, Any]]) -> dict[str, int]:
    """按维度种类计数——CLI 的完成行与测试都用它，别处不重数。"""
    counts: dict[str, int] = {}
    for row in rows:
        kind = str(row["dim"])
        counts[kind] = counts.get(kind, 0) + 1
    return counts
