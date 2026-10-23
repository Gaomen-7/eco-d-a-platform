"""事件生产流水线：把维度世界、漏斗轨迹、注入器装配成事件批次。

**为什么这一层独立**：`world`（维度）、`funnel`（轨迹）、`injectors`（偏移）
各自是纯函数，但「一条真实事件长什么样」需要把它们串起来，且必须保证三件事：
1. 每条事件带**通道标记**（正常 / 可修正迟到 / 超期迟到），供 verify 断言计数；
2. 迟到偏移相对**水印**（不允许读墙上时钟），从而任何速率下迟到程度都是设计值；
3. 会话轨迹可回溯（`session_steps`），使「purchase 必有前置步」可证。

水印在这里的语义是「本批次已经推进到的最大事件时间」——事件按水印推进生成，
迟到事件只是把 event_time 往回拨，制造确定性的乱序。
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Final

from ecomgen.events import validate_event
from ecomgen.funnel import FunnelParams, generate_session_steps
from ecomgen.injectors import correctable_offset_ms, expired_offset_ms
from ecomgen.world import World, build_world

# 每条事件在批次里的通道标记（互斥）
CHANNEL_NORMAL: Final[str] = "normal"
CHANNEL_LATE_CORRECTABLE: Final[str] = "late_correctable"
CHANNEL_LATE_EXPIRED: Final[str] = "late_expired"

# 批次内的模拟时间推进步长：每条事件推进 3 ms 水印 ⇒ 200 ev/s 约等于
# 「1 事件 / 5 ms」的密度。这是**设计值**，只影响水印推进速度与迟到幅度，
# 不承诺真实墙钟速率（F3 裁决：内容确定、节奏不确定）。
#
# ★★ P1 实机证伪后改正（2026-10-21）：旧值 **5 ms** 使整批时间跨度只有
#   `事件数 × 5 ms`（434 条 ⇒ 2.2 s … 实测 271 s 是因为上游时钟另有推进）。
#   而两条迟到通道的**可达性下界**是第一性算出来的：
#
#     可达条件的总量下界 = 窗口长度 + 允许延迟 + 最大迟到偏移
#                        = 60 s + 120 s + 300 s = **480 s**
#
#   批内跨度低于这个下界时，无论怎么排发送序，**同分区水印都不可能既越过某窗口
#   末尾、又落在其允许延迟之内**（可修正），更不可能越过允许延迟（超期）。
#   实测证据（旧值）：6 个分区 `expired=0 / correctable=0`，Doris
#   `ads_late_expired_count` 恒 0 行、`revision` 只由 UV/重复段偶然推高。
#
#   ⇒ 取 **1500 ms/事件**：`--sessions 200`（约 434 条）⇒ 跨度 ≈ 650 s，
#     覆盖 480 s 下界且留余量；`--sessions 1000` 时跨度更大，同样成立。
#   ⚠️ 这不是「调大凑数」：480 s 是上面那条和式的直接结果；步长只是把它摊到
#     每条事件上。
_WATERMARK_STEP_MS: Final[int] = 1_500

# 批次起点的固定基座（UTC epoch 毫秒）——**不读墙上时钟**，否则不可复现。
# 取 2024-01-01T00:00:00Z；F7 裁决允许 live 模式在发送前整体平移到本轮起点。
_EPOCH_BASE_MS: Final[int] = 1_704_067_200_000

SCHEMA_VERSION: Final[str] = "1.0"


def event_dt(event_time_ms: int) -> str:
    """事件时间的 UTC 日期（`yyyy-MM-dd`）—— **dt 口径的唯一权威**。

    ★ 为什么收在这里（impl-03b code-review）：这个换算此前在 `cli._dt_of` 与
      `tests/test_threshold_batch.py._dt_of` 各写一份，而它正是 DDL 里
      `DATE_FORMAT(TO_TIMESTAMP_LTZ(event_time, 3), 'yyyy-MM-dd')` 的 Python 侧对应物。
      两份若漂移（例如一份改成 UTC+8），测试会用自己的错误口径去验自己的产出，
      两边一起错、仍然全绿。dt=事件日 UTC 是形状表的硬约定（§5.2）。
    """
    return datetime.fromtimestamp(event_time_ms / 1000, tz=UTC).strftime("%Y-%m-%d")


@dataclass(frozen=True)
class ProducerParams:
    """生成器参数（与 CLI 一一对应）。"""

    seed: int
    n_sessions: int = 1_000
    rate_per_sec: int = 200
    late_correctable: float = 0.03
    late_expired: float = 0.02
    chaos_ratio: float = 0.0
    n_users: int = 100_000
    n_items: int = 2_000
    # 每条事件推进的水印步长（毫秒）。None ⇒ 用 _WATERMARK_STEP_MS（1500，P1 口径）。
    #
    # ★ 为什么做成参数（impl-03b）：门槛批要求「1 048 576 条铺满 **3 天**」。
    #   固定 1500 ms/条时跨度 = 事件数 × 1500 ms ≈ 1 048 576 × 1.5 s ≈ **18.2 天**
    #   ⇒ dt 会出 19 个分区，与票面「3 天跨度让 dt 有 3 个分区」正面冲突。
    #   跨度与条数必须**解耦**：步长 = 目标跨度 / 条数。
    #   ⚠️ 默认值保持 None（不改 P1 行为）：P1 的迟到可达性下界 480 s 是按
    #      1500 ms/条 + sessions 规模推得的，动默认值会推翻 impl-02e 的实机结论。
    watermark_step_ms: int | None = None
    # 产够这么多条就**停止生成**（None ⇒ 按 n_sessions 跑满）。
    #
    # ★ 为什么不是「产完再裁」（impl-03b 探针证伪，2026-10-22）：
    #   (a) 裁最后那个列表会把超期通道清空（迟到事件被搬到尾部，取前 N 全是正常事件）；
    #   (b) 「请求条数 ≠ 实际产出条数」（实测 n_sessions=10000 实产 22289 条）⇒
    #       按请求条数算的步长使跨度 ×2.2。
    #   而「生成到 N 条即停」使水印下标**连续为 1..N**，步长 = 目标跨度/N 精确成立；
    #   多出的那条会话的最后几条从**尾部**削掉，水印仍连续。
    max_events: int | None = None
    # 批次起点（UTC epoch 毫秒）。None ⇒ _EPOCH_BASE_MS（2024-01-01T00:00:00Z）。
    #
    # ★ 为什么门槛批要挪基座（impl-03b 探针证伪，2026-10-22）：
    #   迟到事件把 event_time 往回拨，超期档最多拨 300 s（injectors._EXPIRED_MAX_MS）。
    #   基座正好压在 00:00:00Z 时，最早那批事件的 event_time 会落到**前一天**，
    #   末尾又恰好顶上第 4 天 ⇒ dt 出 5 个分区（实测 2023-12-31/2024-01-04 各 1 条），
    #   与票面「3 天跨度 ⇒ 3 个分区」不符。
    #   ⇒ 起点前移 300 s、跨度扣掉这 300 s，使 event_time 严格落在 [base, base+3d) 内。
    epoch_base_ms: int | None = None

    def __post_init__(self) -> None:
        if self.n_sessions < 0:
            raise ValueError(f"n_sessions 必须非负，实得 {self.n_sessions}")
        if self.rate_per_sec <= 0:
            raise ValueError(f"rate_per_sec 必须为正，实得 {self.rate_per_sec}")
        for name in ("late_correctable", "late_expired", "chaos_ratio"):
            value = getattr(self, name)
            if not 0.0 <= value <= 1.0:
                raise ValueError(f"{name} 必须在 [0,1]，实得 {value}")
        if self.watermark_step_ms is not None and self.watermark_step_ms < 1:
            raise ValueError(
                f"watermark_step_ms 必须 ≥ 1，实得 {self.watermark_step_ms}"
            )
        if self.max_events is not None and self.max_events < 0:
            raise ValueError(f"max_events 必须非负，实得 {self.max_events}")
        if self.epoch_base_ms is not None and self.epoch_base_ms < 0:
            raise ValueError(f"epoch_base_ms 必须非负，实得 {self.epoch_base_ms}")

    @classmethod
    def from_plan(
        cls,
        plan: ThresholdBatchPlan,
        *,
        seed: int = 0,
        rate_per_sec: int = 200,
        late_correctable: float = 0.03,
        late_expired: float = 0.02,
        chaos_ratio: float = 0.0,
        n_users: int = 100_000,
        n_items: int = 2_000,
    ) -> ProducerParams:
        """按门槛批 plan 装配参数（**唯一**的 plan → params 映射）。

        ★ 为什么收在这里（impl-03b code-review）：`watermark_step_ms` / `max_events` /
          `epoch_base_ms` 三个值总是一起出现，且此前在 `cli._params` 与
          `tests/test_threshold_batch.py` 两处**各装一遍**——那正是 Data Clumps，
          也正是 plan_threshold_batch「推导集中、别处不重抄」要消灭的东西。
          抄两遍的症状：一处改了字段名/新加一个 plan 字段，另一处静默产错批次，
          而单测因为抄的是同一份装配而仍然通过。
        """
        return cls(
            seed=seed,
            n_sessions=plan.carrier_sessions,
            max_events=plan.n_events,
            watermark_step_ms=plan.watermark_step_ms,
            epoch_base_ms=plan.epoch_base_ms,
            rate_per_sec=rate_per_sec,
            late_correctable=late_correctable,
            late_expired=late_expired,
            chaos_ratio=chaos_ratio,
            n_users=n_users,
            n_items=n_items,
        )


@dataclass(frozen=True)
class Event:
    """一条已定型的事件：记录本体 + 通道标记 + 校验结论。"""

    record: dict[str, Any]
    channel: str
    watermark_ms: int | None
    violations: list[str] = field(default_factory=list)

    @property
    def session_id(self) -> str:
        """会话标识 —— 与 `user_id` 同值（每用户一个会话序列，见 gdd Q12 分区键）。"""
        return str(self.record.get("user_id", ""))

    @property
    def is_late_correctable(self) -> bool:
        return self.channel == CHANNEL_LATE_CORRECTABLE

    @property
    def is_late_expired(self) -> bool:
        return self.channel == CHANNEL_LATE_EXPIRED

    def to_json(self) -> str:
        """稳定序列化：键序固定、无空格，使 `--seed` 的字节级比较成立。"""
        return json.dumps(
            self.record, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        )


@dataclass
class EventBatch:
    """一次产出的事件集合 + 会话轨迹索引（供判据回溯）。"""

    events: list[Event] = field(default_factory=list)
    _session_steps: dict[str, list[str]] = field(default_factory=dict)

    def count_channel(self, channel: str) -> int:
        return sum(1 for e in self.events if e.channel == channel)

    def session_steps(self, session_id: str) -> list[str]:
        return list(self._session_steps.get(session_id, []))

    def session_ids_in_order(self) -> list[str]:
        return list(self._session_steps)


def _uuid4_from(rng: Any) -> str:
    """确定性 UUID：从派生 rng 取 16 字节，而不是调 `uuid.uuid4()`（后者不可复现）。"""
    return str(uuid.UUID(bytes=bytes(rng.getrandbits(8) for _ in range(16)), version=4))


def _build_event(
    *,
    event_id: str,
    event_time: int,
    world: World,
    rng: Any,
    event_type: str,
    session_id: str,
    item: Any,
    order_id: str | None,
    quantity: int | None,
) -> dict[str, Any]:
    """按 SRS §3.3 的字段表组装一条事件。"""
    record: dict[str, Any] = {
        "event_id": event_id,
        "event_time": event_time,
        "user_id": session_id,
        "item_id": item.item_id,
        "category_id": item.category_id,
        "event_type": event_type,
        "schema_version": SCHEMA_VERSION,
        "page": event_type if event_type != "page_view" else "home",
        "source": rng.choice(world.sources),
        "channel": rng.choice(world.channels),
        "province": rng.choice(world.provinces),
        "payload": {"session_id": session_id},
    }
    if quantity is not None:
        record["quantity"] = quantity
    if order_id is not None:
        record["order_id"] = order_id
    if event_type == "purchase":
        record["amount"] = "%.2f" % (item.price_cents / 100 * (quantity or 1))
        record["currency"] = "CNY"
    return record


def produce_batch(params: ProducerParams) -> EventBatch:
    """产出一批事件。同 `params` ⇒ 逐字节相同。"""
    world = build_world(params.seed, n_users=params.n_users, n_items=params.n_items)
    funnel_rng = world.rng_for("funnel")
    late_rng = world.rng_for("late")
    dup_rng = world.rng_for("uuid")
    attr_rng = world.rng_for("attr")

    batch = EventBatch()
    base_ms = _EPOCH_BASE_MS if params.epoch_base_ms is None else params.epoch_base_ms
    watermark = base_ms
    order_seq = 0
    batch_min_ms = base_ms
    step_ms = (
        _WATERMARK_STEP_MS
        if params.watermark_step_ms is None
        else params.watermark_step_ms
    )
    emitted = 0

    def emit(
        session_id: str,
        event_type: str,
        item: Any,
        order_id: str | None,
        quantity: int | None,
    ) -> bool:
        """发一条事件：注入器在此**统一**施加（含 page_view，保持比例口径真实）。

        返回 False 表示已达 max_events 上限、调用方应停止生成。
        """
        nonlocal watermark, batch_min_ms, emitted

        if params.max_events is not None and emitted >= params.max_events:
            return False

        watermark += step_ms

        # 三个注入器语义互不混用：一条事件只归一个通道
        channel = CHANNEL_NORMAL
        event_time = watermark
        roll = late_rng.random()
        if roll < params.late_expired:
            channel = CHANNEL_LATE_EXPIRED
            event_time = watermark - expired_offset_ms(late_rng)
        elif roll < params.late_expired + params.late_correctable:
            channel = CHANNEL_LATE_CORRECTABLE
            event_time = watermark - correctable_offset_ms(late_rng)

        record = _build_event(
            event_id=_uuid4_from(dup_rng),
            event_time=event_time,
            world=world,
            rng=attr_rng,
            event_type=event_type,
            session_id=session_id,
            item=item,
            order_id=order_id,
            quantity=quantity,
        )
        batch_min_ms = min(batch_min_ms, event_time)
        violations = list(validate_event(record, batch_min_ms=batch_min_ms))
        batch.events.append(
            Event(
                record,
                channel,
                watermark if channel != CHANNEL_NORMAL else None,
                violations,
            )
        )
        emitted += 1
        return True

    for session_idx in range(params.n_sessions):
        session_id = world.users[session_idx % len(world.users)]
        steps = generate_session_steps(funnel_rng, _funnel_params(params))

        # 每次会话先来一条 page_view（浏览页，漏斗之外、占比最高）
        if not emit(
            session_id,
            "page_view",
            world.items[attr_rng.randrange(len(world.items))],
            None,
            None,
        ):
            break
        # ★ 记**实际发出**的步，不是 generate_session_steps 的完整步进表
        #   （impl-03b code-review #3）：max_events 截停会砍掉会话尾部，
        #   旧版把完整步进表记进来 ⇒ session_steps() 报出根本没有对应事件的步，
        #   破坏本模块头注「purchase 必有前置步可证」这条判据的可证性。
        emitted_steps: list[str] = []
        if not steps:
            batch._session_steps[session_id] = emitted_steps
            continue  # 空会话：只贡献 page_view

        quantity: int | None = None
        order_id: str | None = None
        for step in steps:
            if step == "add_to_cart":
                quantity = attr_rng.randint(1, 3)
            if step == "begin_checkout":
                order_seq += 1
                order_id = f"o_{order_seq:08d}"
            if not emit(
                session_id,
                step,
                world.items[attr_rng.randrange(len(world.items))],
                order_id if step in ("begin_checkout", "purchase") else None,
                quantity
                if step in ("add_to_cart", "begin_checkout", "purchase")
                else None,
            ):
                break
            emitted_steps.append(step)
        batch._session_steps[session_id] = emitted_steps

        # ★ 只有一个停机判据：`max_events` 到顶（impl-03b code-review 指出旧版用
        #   `stop` 局部标志 + 外层 `if stop: break` 两套机制表达同一条件）。
        #   `emit` 返回 False 即表示到顶，故在内层 break 后于**一处**统一判断。
        if params.max_events is not None and emitted >= params.max_events:
            break

    return _apply_late_delivery_order(batch)


# 迟到事件在发送序里**后移**的格数（设计值）。
#
# ★★ P1 实机证伪后补（2026-10-21）：旧实现直接在产出位处发送迟到事件，
#   实机 `ads_late_expired_count` 恒 **0 行**。根因不是偏移幅度，而是**发送序**：
#   Flink 的 `KafkaSource` 每分区一个 subtask、各自独立算水印；一条超期事件的
#   `event_time = 水印 − 130..300 s`，若它**紧接**在产出位发送，该分区后面
#   再没有足量事件把水印推过「它所属窗口 + 允许延迟」⇒ 窗口从未被清理 ⇒
#   `sideOutputLateData` 永不触发。实测：全局重放 expired>0，逐分区重放全 0。
#
# 修法：把迟到事件 **后移到发送序靠后的位置**（仍保持 `--seed` 确定性）。
# 这样它到达时，同分区水印早已越过它的窗口 + 允许延迟，Flink 才会判它超期。
# 语义不变——「偏移相对水印」的定义就是「相对该分区已推进到的水印」，而水印
# 只由发送序决定；后移正是让这条定义真正成立。
#
# 后移**按通道分档**（两条通道要落进 Flink 的不同分支）：
# - 可修正：窗口已关闭、但**尚未**超过 allowedLateness ⇒ 走重触发 → 修正已落定行；
# - 超期：已超过 allowedLateness ⇒ 窗口已清理 → `sideOutputLateData` 只计数。
#
# 档位是「在正常事件序列里的**插入比例**」，不是绝对条数——不同的
# `--sessions`/`--rate` 下事件总数差一个数量级，绝对条数会让分档失效。
_CORRECTABLE_INSERT_FRACTION: Final[float] = 0.92
# 超期排在最后 5% 的位置：确保它之后仍有事件推进水印，越过窗口 + 允许延迟。
_EXPIRED_INSERT_FRACTION: Final[float] = 0.99


def _apply_late_delivery_order(batch: EventBatch) -> EventBatch:
    """把迟到事件后移到发送序靠后，使它们在水印语义下真的「迟到」。

    见上方常量注释：两条通道按**插入比例**分档，保证
    - 可修正 ⇒ 窗口已关、未过允许延迟 ⇒ `allowedLateness` 重触发（修正落定行）；
    - 超期 ⇒ 已过允许延迟 ⇒ 窗口已清理 ⇒ `sideOutputLateData`（只计数）。
    """
    normal: list[Event] = []
    correctable: list[Event] = []
    expired: list[Event] = []

    for event in batch.events:
        if event.channel == CHANNEL_LATE_CORRECTABLE:
            correctable.append(event)
        elif event.channel == CHANNEL_LATE_EXPIRED:
            expired.append(event)
        else:
            normal.append(event)

    if not correctable and not expired:
        return batch

    def _cut(fraction: float) -> int:
        return min(len(normal), max(0, int(len(normal) * fraction)))

    # 各通道内部保持原始相对顺序 ⇒ 同 seed 仍逐字节可复现。
    cor_at = _cut(_CORRECTABLE_INSERT_FRACTION)
    exp_at = _cut(_EXPIRED_INSERT_FRACTION)
    # 超期的插入点更靠后；若两者倒挂则夹正，避免顺序依赖浮点。
    exp_at = max(exp_at, cor_at)

    reordered = (
        normal[:cor_at]
        + correctable
        + normal[cor_at:exp_at]
        + expired
        + normal[exp_at:]
    )

    batch.events = reordered
    return batch


def _funnel_params(params: ProducerParams) -> FunnelParams:
    return FunnelParams(chaos_ratio=params.chaos_ratio)


# ── 门槛批参数推导（impl-03b）───────────────────────────────────────────
# 门槛批形状是**可判的**三个数：恰好 N 条、跨度 3 天、dt 恰 3 个分区。
# 这三个数不是独立的——上面两节（max_events / epoch_base_ms）各自解掉了一个坑，
# 这里把推导集中成纯函数，供 CLI 与单测共用，避免两处各写一份口径。
#
# 各量的关系（DAY = 86 400 000 ms，base = 2024-01-01T00:00:00Z）：
#
#   base_shifted = base + MAX_LATE            # 起点前移，给最早的迟到事件留出回拨余量
#   span_used    = span - MAX_LATE - MARGIN   # 跨度扣掉前移量，保证末条 < base + span
#   step_ms      = span_used // N             # 水印步长按**实际条数** N 算
#
# ⇒ event_time 严格落在 [base, base + span) 内，dt 恰为 3 个日期。
_THRESHOLD_BASE_MS = _EPOCH_BASE_MS
# 末尾余量 1 s：避免最后一条恰好压在次日 00:00:00.000 上（边界归属不稳）。
_THRESHOLD_TAIL_MARGIN_MS = 1_000
# 超期档最大回拨量，与 injectors._EXPIRED_MAX_MS 同源（300 s）。此处冗余写死
# 并在单测里断言两者相等，避免 producer 反向 import injectors 的内部常量。
_THRESHOLD_MAX_LATE_MS = 300_000


@dataclass(frozen=True)
class ThresholdBatchPlan:
    """门槛批的推导结果（纯值，无副作用）。"""

    n_events: int
    watermark_step_ms: int
    epoch_base_ms: int
    expected_dt_count: int
    min_dt: str
    max_dt: str
    # 载体会话数：会话只负责「造够条数」，条数由 max_events 精确截断。
    #
    # ★ 为什么收进 plan（impl-03b code-review #4）：这个公式此前在 cli._params 与
    #   测试 `_batch_from_plan` / `test_three_dt_partitions_hold_across_seeds`
    #   里各抄一份，恰好违背 plan_threshold_batch 自己的存在理由（「推导集中与此，
    #   CLI 只负责传参」）。三处若各自漂移，症状是「产不够 ⇒ 实际条数 < 请求条数」，
    #   而单测因为抄的是同一份公式而仍然通过。
    carrier_sessions: int


# 每会话平均产出条数（设计值）。实测口径：--sessions 10000 ⇒ 22 289 条（★2.227）。
# 取 2.0 作分母 + 1.2 余量 ⇒ 载体会话数 ≈ 1.2/2.0 × N = 0.6N，即约 20% 富余。
# ⚠️ 富余量是**单向**的：少了会造不满 N 条（max_events 截停前会话已用光），
#    多了只是多跑几步空转。故这里宁可偏多。
_EVENTS_PER_SESSION: Final[float] = 2.0
_CARRIER_MARGIN: Final[float] = 1.2


def _carrier_sessions(n_events: int) -> int:
    """造够 `n_events` 条所需的**载体会话数**（条数由 max_events 精确截断）。

    唯一权威定义（impl-03b code-review #4）：CLI 与测试都必须读 `plan.carrier_sessions`，
    不得再各写一份公式 —— 漂移的症状是「实际条数 < 请求条数」，而抄同一份公式的
    测试会跟着一起漂、抓不到。
    """
    return max(1, int(n_events / _EVENTS_PER_SESSION * _CARRIER_MARGIN))


def plan_threshold_batch(n_events: int, span_days: int = 3) -> ThresholdBatchPlan:
    """由「条数 + 跨度天数」推导门槛批的三个参数。

    为什么不让调用方自己写 step/base：这两个数一旦算错，症状是「链路看着通、
    dt 分区数不对」，极难从日志看出来（impl-03b 探针正是这样撞了两次）。
    推导集中在此，CLI 只负责传 n_events 与 span_days。
    """
    if n_events <= 0:
        raise ValueError(f"n_events 必须为正，实得 {n_events}")
    if span_days <= 0:
        raise ValueError(f"span_days 必须为正，实得 {span_days}")
    day_ms = 86_400_000
    span_ms = span_days * day_ms
    base_shifted = _THRESHOLD_BASE_MS + _THRESHOLD_MAX_LATE_MS
    span_used = span_ms - _THRESHOLD_MAX_LATE_MS - _THRESHOLD_TAIL_MARGIN_MS
    if span_used <= 0:
        raise ValueError(f"跨度太小：{span_days} 天扣掉迟到余量后无可用跨度")
    step_ms = span_used // n_events
    if step_ms < 1:
        raise ValueError(
            f"{n_events} 条铺不进 {span_days} 天：步长会 < 1 ms，请减少条数或加大跨度"
        )
    # dt 取值域：从 base 起算的 span_days 个 UTC 日
    first = datetime.fromtimestamp(_THRESHOLD_BASE_MS / 1000, tz=UTC)
    min_dt = first.strftime("%Y-%m-%d")
    last = datetime.fromtimestamp(
        (_THRESHOLD_BASE_MS + (span_days - 1) * day_ms) / 1000, tz=UTC
    )
    return ThresholdBatchPlan(
        n_events=n_events,
        watermark_step_ms=step_ms,
        epoch_base_ms=base_shifted,
        expected_dt_count=span_days,
        min_dt=min_dt,
        max_dt=last.strftime("%Y-%m-%d"),
        carrier_sessions=_carrier_sessions(n_events),
    )
