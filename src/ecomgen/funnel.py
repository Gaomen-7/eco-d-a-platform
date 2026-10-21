"""会话轨迹与步进漏斗（gdd.md §1.1 流程 / 【已确认 Q20】）。

**为什么以会话为单位**：漏斗的语义是「一个用户在一次会话里走到第几步」，
不是「独立事件按概率抽样」。前者才能保证 `purchase` 必有前置步，也才能让
`cnt_inferred` 的单调断言有意义（spec §4.2）。

步进概率按真实电商形态取值（`page_view` 占绝对多数，逐级递减到 `purchase`），
是**设计值**而非官方保证 —— 只承诺递减关系，不承诺具体比例。
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from typing import Final

# 漏斗四步（顺序冻结）。注意 `page_view` 不在漏斗里：它是「浏览量，占比最高」
# 的独立事件（SRS §3.3），与商品详情浏览 `view_item` 是两回事。
FUNNEL_STEPS: Final[tuple[str, ...]] = (
    "view_item",
    "add_to_cart",
    "begin_checkout",
    "purchase",
)


@dataclass(frozen=True)
class FunnelParams:
    """漏斗步进概率与混沌设置。

    `p*` 是**条件**概率：走到上一步后，是否继续走到这一步。
    逐级递减由 `p_view_item > p_add_to_cart > p_begin_checkout > p_purchase`
    的取值保证（默认值即满足）。
    """

    p_view_item: float = 0.72  # 进入会话后确实浏览了商品详情
    p_add_to_cart: float = 0.45  # 浏览后加购
    p_begin_checkout: float = 0.38  # 加购后下单
    p_purchase: float = 0.52  # 下单后支付
    chaos_ratio: float = 0.0  # 故意跳步的比例（--chaos-ratio）

    def __post_init__(self) -> None:
        for name in ("p_view_item", "p_add_to_cart", "p_begin_checkout", "p_purchase"):
            value = getattr(self, name)
            if not 0.0 <= value <= 1.0:
                raise ValueError(f"{name} 必须在 [0,1]，实得 {value}")
        if not 0.0 <= self.chaos_ratio <= 1.0:
            raise ValueError(f"chaos_ratio 必须在 [0,1]，实得 {self.chaos_ratio}")


def generate_session_steps(rng: random.Random, params: FunnelParams) -> list[str]:
    """推进一条会话，返回它实际走过的步（始终是漏斗的前缀）。

    chaos 的语义是**跳步**：一旦某步被跳过，后面所有步都不再走（轨迹仍是前缀）。
    这保证 `cnt_observed` 可以非单调（真实跳步合法，spec §4.2 明确不押它），
    而 `cnt_inferred` 的单调性由补记逻辑负责，与此处无关。
    """
    steps: list[str] = []
    probabilities = (
        params.p_view_item,
        params.p_add_to_cart,
        params.p_begin_checkout,
        params.p_purchase,
    )

    for step, probability in zip(FUNNEL_STEPS, probabilities):
        advanced = rng.random() < probability
        if not advanced:
            break  # 步进概率决定会话在此终止：后续步不再走（前缀性质）
        # 混沌：**跳过本步但继续推进**（不是终止会话）。这正是「跳步」的语义——
        # 它让 `cnt_observed` 出现中途缺步的情形，从而可证观测值确实允许非单调。
        if params.chaos_ratio > 0.0 and rng.random() < params.chaos_ratio:
            continue
        steps.append(step)

    return steps
