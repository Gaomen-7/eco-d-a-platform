"""三个迟到/混沌注入器的纯函数面（spec.md §4.7、gdd.md §1.1 流程 3）。

**为什么是纯函数**：偏移一律相对**水印**计算，与墙上时钟无关 ⇒ 任何速率下迟到
程度都是设计值，且 `--seed` 下可复现（`REQ-F-GEN-06`）。纯函数让这条判据可单测，
不必起 Kafka 或等真实时间流逝。

三个注入器语义**互不混用**（gdd Q21）：可修正 / 超期 / 混沌各走各的通道。
"""

from __future__ import annotations

import random

# 允许延迟（实时侧）= 2 分钟。可修正迟到的偏移必须严格落在其内。
ALLOWED_LATENESS_MS = 120_000

# 可修正迟到的偏移区间（含端点）：5 s ~ 115 s。
_CORRECTABLE_MIN_MS = 5_000
_CORRECTABLE_MAX_MS = 115_000

# 超期迟到的偏移区间（含端点）：130 s ~ 300 s。下界刻意越过 120 s，
# 与可修正区间**不相交** ⇒ 两条通道永不混淆（gdd Q21）。
_EXPIRED_MIN_MS = 130_000
_EXPIRED_MAX_MS = 300_000


def correctable_offset_ms(rng: random.Random) -> int:
    """可修正迟到的偏移量（毫秒）。

    落在「允许延迟内」：`5 s ≤ 偏移 ≤ 115 s < 120 s`。
    上限刻意留 5 s 余量，使事件**确实**落入已落定窗口（而非边界处恰好不迟到）。
    """
    return rng.randint(_CORRECTABLE_MIN_MS, _CORRECTABLE_MAX_MS)


def expired_offset_ms(rng: random.Random) -> int:
    """超期迟到的偏移量（毫秒）：> 允许延迟，只能计数、绝不修正。"""
    return rng.randint(_EXPIRED_MIN_MS, _EXPIRED_MAX_MS)


def chaos_extra_step(rng: random.Random, chaos_ratio: float) -> bool:
    """本次是否触发混沌跳步。

    `chaos_ratio == 0` ⇒ 恒 False（漏斗 100% 连贯，这是默认行为的判据）；
    `1.0` ⇒ 恒 True。中间值即概率。
    """
    if chaos_ratio <= 0.0:
        return False
    if chaos_ratio >= 1.0:
        return True
    return rng.random() < chaos_ratio
