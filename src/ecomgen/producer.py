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
_WATERMARK_STEP_MS: Final[int] = 5

# 批次起点的固定基座（UTC epoch 毫秒）——**不读墙上时钟**，否则不可复现。
# 取 2024-01-01T00:00:00Z；F7 裁决允许 live 模式在发送前整体平移到本轮起点。
_EPOCH_BASE_MS: Final[int] = 1_704_067_200_000

SCHEMA_VERSION: Final[str] = "1.0"


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

    def __post_init__(self) -> None:
        if self.n_sessions < 0:
            raise ValueError(f"n_sessions 必须非负，实得 {self.n_sessions}")
        if self.rate_per_sec <= 0:
            raise ValueError(f"rate_per_sec 必须为正，实得 {self.rate_per_sec}")
        for name in ("late_correctable", "late_expired", "chaos_ratio"):
            value = getattr(self, name)
            if not 0.0 <= value <= 1.0:
                raise ValueError(f"{name} 必须在 [0,1]，实得 {value}")


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
    watermark = _EPOCH_BASE_MS
    order_seq = 0
    batch_min_ms = _EPOCH_BASE_MS

    def emit(
        session_id: str,
        event_type: str,
        item: Any,
        order_id: str | None,
        quantity: int | None,
    ) -> None:
        """发一条事件：注入器在此**统一**施加（含 page_view，保持比例口径真实）。"""
        nonlocal watermark, batch_min_ms

        watermark += _WATERMARK_STEP_MS

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

    for session_idx in range(params.n_sessions):
        session_id = world.users[session_idx % len(world.users)]
        steps = generate_session_steps(funnel_rng, _funnel_params(params))
        batch._session_steps[session_id] = steps

        # 每次会话先来一条 page_view（浏览页，漏斗之外、占比最高）
        emit(
            session_id,
            "page_view",
            world.items[attr_rng.randrange(len(world.items))],
            None,
            None,
        )
        if not steps:
            continue  # 空会话：只贡献 page_view

        quantity: int | None = None
        order_id: str | None = None
        for step in steps:
            if step == "add_to_cart":
                quantity = attr_rng.randint(1, 3)
            if step == "begin_checkout":
                order_seq += 1
                order_id = f"o_{order_seq:08d}"
            emit(
                session_id,
                step,
                world.items[attr_rng.randrange(len(world.items))],
                order_id if step in ("begin_checkout", "purchase") else None,
                quantity
                if step in ("add_to_cart", "begin_checkout", "purchase")
                else None,
            )

    return batch


def _funnel_params(params: ProducerParams) -> FunnelParams:
    return FunnelParams(chaos_ratio=params.chaos_ratio)
