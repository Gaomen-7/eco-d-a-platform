"""`ecomgen.dims` 维度变更流的单测（impl-03c）。

**这一片守什么**：维度变更流是 DIM 拉链的**唯一**输入。它一旦不满足下面任一条，
拉链在表上看起来完全正常（行数、区间、主键都对），错的只是**语义**：

1. **批次内每条事件都落在某个版本区间里** —— 初始版本的 `change_time` 必须等于
   批次最早事件时刻 `start_ms`。少一个对象、或把初始版本推到 `start_ms` 之后，
   该对象在 `[start_ms, 第一版)` 之间的事件就会**漏命中**（时态 join 丢行）。
2. **单 id 内 change_time 严格单调** —— 拉链的 `valid_to_ms` 靠相邻版本排序算出；
   同 id 两版同一 `change_time` 会撞主键 `(id, valid_from_ms)`，或算出空/负区间。
3. **商品换品类必须换到*另一个*品类** —— 品类 subject 的时态差异全靠它；
   换到同一个品类等于没换，而 `dws_subject_window` 的 category 行会退化成
   「直接读事件字段」，时态 join 白做但仍能跑出数。
4. **同 seed 同字节** —— 与 `test_threshold_batch` 的可复现判据同一把尺子。
"""

from __future__ import annotations

import itertools
import json
from pathlib import Path

import pytest

from ecomgen.dims import (
    DIM_CATEGORY,
    DIM_CHANNEL,
    DIM_ITEM,
    DIM_KINDS,
    DIM_REGION,
    DIM_USER,
    build_dimension_stream,
    count_by_kind,
    write_dimension_stream,
)
from ecomgen.world import build_world

# 小世界即可：判据与规模无关（2 000 商品 / 12 品类 / 20 用户足够暴露全部形状）
_WORLD = build_world(20241022, n_users=20, n_items=200)
_START = 1_704_067_200_000  # 2024-01-01T00:00:00Z
_END = _START + 3 * 86_400_000  # 3 天后


@pytest.fixture(scope="module")
def rows() -> list[dict]:
    return build_dimension_stream(_WORLD, start_ms=_START, end_ms=_END)


def _by_kind(rows: list[dict], kind: str) -> list[dict]:
    return [r for r in rows if r["dim"] == kind]


def _id_col(kind: str) -> str:
    return {
        DIM_ITEM: "item_id",
        DIM_CATEGORY: "category_id",
        DIM_USER: "user_id",
        DIM_REGION: "province",
        DIM_CHANNEL: "channel",
    }[kind]


# ── 1. 覆盖：每个对象都有初始版本，且初始版本恰在 start_ms ─────────────────
@pytest.mark.parametrize("kind", [DIM_ITEM, DIM_CATEGORY, DIM_USER])
def test_every_zipped_object_has_an_initial_version_at_start_ms(rows, kind) -> None:
    """拉链表里的每个对象都必须有一个 `change_time == start_ms` 的初始版本。

    ★ 这是「批次内每条事件都能命中某版本」的**唯一保证**。若某对象缺初始版本、
    或初始版本被推到 `start_ms` 之后，该对象在 `[start_ms, 第一版)` 区间内的
    事件就会在 `BETWEEN valid_from_ms AND valid_to_ms` 里**两个版本都不命中**
    —— 区间 join 是 INNER JOIN，这些事件直接消失，且表上完全看不出。
    """
    kind_rows = _by_kind(rows, kind)
    id_col = _id_col(kind)
    # 每个对象恰有一个初始版本
    initial = [r for r in kind_rows if r["change_time"] == _START]
    assert len(initial) == len({r[id_col] for r in kind_rows}), (
        f"{kind}: 初始版本数 {len(initial)} ≠ 对象数 —— 有对象缺初始版本"
    )
    assert len({r[id_col] for r in initial}) == len(initial), (
        f"{kind}: 同一对象出现多个 start_ms 版本 ⇒ 会撞主键 (id, valid_from_ms)"
    )


@pytest.mark.parametrize("kind", [DIM_ITEM, DIM_CATEGORY, DIM_USER])
def test_version_change_times_are_strictly_increasing_per_object(rows, kind) -> None:
    """同一对象的 `change_time` 必须**严格单调递增**（含初始版本）。

    拉链的 `valid_to_ms = 下一版 change_time − 1` 由相邻排序算出：
    两版同一 `change_time` ⇒ 主键 `(id, valid_from_ms)` 冲突；
    顺序颠倒 ⇒ 算出**负区间**（`valid_to_ms < valid_from_ms`），
    该版本永不命中而表上只是多一行看不见的垃圾。
    """
    id_col = _id_col(kind)
    by_id: dict[str, list[int]] = {}
    for r in _by_kind(rows, kind):
        by_id.setdefault(r[id_col], []).append(int(r["change_time"]))
    for obj, times in by_id.items():
        assert times == sorted(times), f"{kind}/{obj}: change_time 未按序产出：{times}"
        assert len(set(times)) == len(times), (
            f"{kind}/{obj}: 出现相同 change_time ⇒ 撞主键或空区间：{times}"
        )


@pytest.mark.parametrize("kind", [DIM_ITEM, DIM_CATEGORY, DIM_USER])
def test_change_times_stay_inside_the_batch_span(rows, kind) -> None:
    """除初始版本外，`change_time` 必须严格落在 `(start_ms, end_ms)` 内。

    越上界 ⇒ 该版本在批次内永不生效（区间 join 命不中），却仍占一行；
    越下界 ⇒ 与初始版本重叠。
    """
    id_col = _id_col(kind)
    for r in _by_kind(rows, kind):
        t = int(r["change_time"])
        if t == _START:
            continue
        assert _START < t < _END, (
            f"{kind}/{r[id_col]}: change_time={t} 不在 ({_START}, {_END}) 内"
        )


# ── 2. 跨日多版本真实存在（票面验收第 1 条的前提）──────────────────────
def test_items_have_multiple_versions_across_days(rows) -> None:
    """必须有商品跨日多版本 —— 这是 DIM 拉链验收的**输入前提**。

    若全是单版本，`valid_to_ms` 永远等于哨兵值，时态 join 退化成
    「永远命中最新版」= 假时态。判据同时要求「多版本对象存在」与
    「版本时间跨日」（不止在小时级抖动）。
    """
    by_id: dict[str, list[int]] = {}
    for r in _by_kind(rows, DIM_ITEM):
        by_id.setdefault(r["item_id"], []).append(int(r["change_time"]))
    multi = {k: v for k, v in by_id.items() if len(v) > 1}
    assert multi, "没有任何商品有第二个版本 ⇒ 拉链退化成单版本，时态 join 是假的"
    # 至少有一个对象的版本跨了 UTC 日边界
    crosses_day = any(
        min(times) // 86_400_000 != max(times) // 86_400_000 for times in multi.values()
    )
    assert crosses_day, "多版本都落在同一天内 ⇒ 不构成跨日拉链"


def test_item_category_changes_always_switch_to_a_different_category(rows) -> None:
    """商品换品类时必须换到**另一个**品类（否则品类 subject 的时态差异是假的）。

    ★ `dws_subject_window` 的 category 行若「换品类 = 换到同一个」，
    两级区间 join 与「直接读事件字段」产出**完全相同**的数字，
    时态语义当场消失，而所有行数/区间判据仍然绿。
    """
    by_item: dict[str, list[str]] = {}
    for r in _by_kind(rows, DIM_ITEM):
        by_item.setdefault(r["item_id"], []).append(r["category_id"])
    # 版本内品类变化 = 真换品类（价格/品牌变更而品类不变是允许的另一回事）
    switches = sum(
        1
        for cats in by_item.values()
        for prev, nxt in itertools.pairwise(cats)
        if prev != nxt
    )
    assert switches > 0, "没有任何商品真的换了品类 ⇒ 品类 subject 的时态 join 无从检验"
    # 每个商品的每一版品类都必须落在 world.categories 里
    for item_id, cats in by_item.items():
        for cat in cats:
            assert cat in _WORLD.categories, f"{item_id}: 品类 {cat!r} 不在 world 里"


# ── 3. 静态维：一次给全、无 change_time ─────────────────────────────────
def test_static_dimensions_have_no_change_time(rows) -> None:
    """`dim_region` / `dim_channel` 无拉链 ⇒ 行里**不该**有 `change_time`。

    带上它，SQL 的 `v_dim` 投影会把它当拉链维处理，静态维就被塞进
    `v_zipper` 的窗口里（每个省份多一份带哨兵值的假版本）。
    """
    for kind in (DIM_REGION, DIM_CHANNEL):
        for r in _by_kind(rows, kind):
            assert "change_time" not in r, f"{kind} 行里出现了 change_time：{r}"


def test_static_dimensions_cover_the_world_enums(rows) -> None:
    """静态维必须**逐一覆盖** `world.provinces` / `world.channels`。

    少一个省份 ⇒ 该省份的事件在 `dim_region` 上区间 join 落空（或退化成 CASE），
    而省份是用户维的**属性**、不是 join 键，缺失只表现为「其他」占比异常。
    """
    provinces = {r["province"] for r in _by_kind(rows, DIM_REGION)}
    channels = {r["channel"] for r in _by_kind(rows, DIM_CHANNEL)}
    assert provinces == set(_WORLD.provinces), (
        f"省份覆盖不全：{provinces ^ set(_WORLD.provinces)}"
    )
    assert channels == set(_WORLD.channels), (
        f"渠道覆盖不全：{channels ^ set(_WORLD.channels)}"
    )
    # 主键唯一（dim_region/dim_channel 的 PK 就是这一列）
    assert len(provinces) == len(_by_kind(rows, DIM_REGION)), "dim_region 有重复省份"
    assert len(channels) == len(_by_kind(rows, DIM_CHANNEL)), "dim_channel 有重复渠道"


# ── 4. 可复现：同 seed 同字节 ─────────────────────────────────────────
def test_same_seed_produces_identical_rows() -> None:
    """同 seed 的两次构建必须产出**完全相同**的行序列（判据与事件流同一把尺子）。

    维度流用了 `rng_for("dim")` 这条派生流：它与 funnel/late/attr 互不串味，
    但**仍然要求**同 seed 同产出 —— 否则 `make replay` 无法复跑对账。
    """
    a = build_dimension_stream(_WORLD, start_ms=_START, end_ms=_END)
    b = build_dimension_stream(_WORLD, start_ms=_START, end_ms=_END)
    assert a == b, "同 seed 两次构建产出不同 ⇒ 维度流不可复现"


def test_different_seed_changes_the_stream() -> None:
    """换 seed 必须改变维度流 —— 否则「按 seed 可复现」是空话。"""
    other = build_world(999, n_users=20, n_items=200)
    a = build_dimension_stream(_WORLD, start_ms=_START, end_ms=_END)
    b = build_dimension_stream(other, start_ms=_START, end_ms=_END)
    assert a != b, "换 seed 后维度流完全相同 ⇒ seed 没接进维度流"


def test_written_bytes_are_stable_and_parse_back(tmp_path: Path) -> None:
    """落盘可逐行解析回来、两次写字节相同。

    ★ 字节约定的本体（键序固定 / 无空格 / LF / 末尾换行）由
    `tests/test_jsonl.py` 钉在**共用写手** `ecomgen.jsonl.write_jsonl` 上 ——
    事件流与维度流走同一份实现，故「按文件类型分叉」已经不可构造，
    这里只保留维度流这一侧的端到端往返验证。
    """
    rows = build_dimension_stream(_WORLD, start_ms=_START, end_ms=_END)
    p1, p2 = tmp_path / "a.jsonl", tmp_path / "b.jsonl"
    write_dimension_stream(p1, rows)
    write_dimension_stream(p2, rows)
    assert p1.read_bytes() == p2.read_bytes(), "两次落盘字节不同"

    text = p1.read_text(encoding="utf-8")
    assert text.endswith("\n"), "文件末尾缺换行"
    assert "\r" not in text, "出现 CRLF（约定是纯 \n）"
    lines = text.splitlines()
    assert len(lines) == len(rows), f"行数不符：{len(lines)} != {len(rows)}"
    for line, original in zip(lines, rows):
        assert json.loads(line) == original
        # 键序固定：序列化结果与 sort_keys 一致
        assert line == json.dumps(
            original, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        )


# ── 5. 计数助手与种类域 ─────────────────────────────────────────────────
def test_count_by_kind_covers_every_kind(rows) -> None:
    """`count_by_kind` 必须覆盖全部五种维度种类，且合计等于总行数。

    CLI 的完成行用它打印、测试用它断言 —— 若它悄悄漏一类，
    「维度变更流 N 行」的数字与落盘内容会对不上，而没人会发现。
    """
    counts = count_by_kind(rows)
    assert set(counts) == set(DIM_KINDS), f"种类不全：{set(counts) ^ set(DIM_KINDS)}"
    assert sum(counts.values()) == len(rows)
    for kind in DIM_KINDS:
        assert counts[kind] == len(_by_kind(rows, kind))


def test_too_short_span_is_rejected() -> None:
    """跨度不足两个毫秒必须**显式报错**，不能产出空/非法区间。

    跨度太小意味着「初始版本 = 唯一版本」且 `randrange` 会取到空区间；
    静默产出会让下游得到一个看似正常、实则无变更的维度流。
    """
    with pytest.raises(ValueError):
        build_dimension_stream(_WORLD, start_ms=_START, end_ms=_START + 1)
