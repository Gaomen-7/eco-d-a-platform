"""事件 schema 与质量规则（SRS.md §3.3 —— 唯一权威）。

**为什么校验长在这里**：SRS §3.3 的三条质量规则（①时间范围 ②amount ③枚举
④非空 ⑤唯一性）既是生成器的产出约束，也是 Flink 侧分流 DLQ 的判据。把规则
写成**纯函数**，两侧共用同一份口径，不出现「生成器认为合法、作业认为脏」的错位。

DLQ 只摘真脏三原因码（票面 ②）：`schema_invalid` / `value_out_of_range` /
`time_out_of_range`。**重复**（规则⑤）不在此列 —— 它走独立通道计数，
不进 DLQ、不计脏率（gdd §D D2）。
"""

from __future__ import annotations

from typing import Any, Final

# 五类事件枚举（SRS §3.1 `REQ-F-GEN-02`，冻结）
EVENT_TYPES: Final[tuple[str, ...]] = (
    "page_view",
    "view_item",
    "add_to_cart",
    "begin_checkout",
    "purchase",
)

# 公共必填字段（SRS §3.3 公共字段表）
_REQUIRED_COMMON: Final[tuple[str, ...]] = (
    "event_id",
    "event_time",
    "user_id",
    "item_id",
    "category_id",
    "event_type",
    "schema_version",
    "payload",
)

# 五类事件的**额外**必填子集（SRS §3.3 「五类事件的必填子集」）
_REQUIRED_BY_TYPE: Final[dict[str, tuple[str, ...]]] = {
    "page_view": (),
    "view_item": (),
    "add_to_cart": ("quantity",),
    "begin_checkout": ("order_id",),
    "purchase": ("amount", "currency", "order_id"),
}

# 规则①上界：不晚于「当前 + 5 分钟」（设计裕度，非官方保证）
#
# ★ 公开名（impl-06d）：live 的平移语义要按**同一个**裕度判「尾部是否超界」，
#   否则两侧各写一个 5 min，改一侧另一侧就成了假判据。
#   Flink 侧对偶是 EventQuality.FUTURE_TOLERANCE_MS（两处同源、改则同步）。
FUTURE_TOLERANCE_MS: Final[int] = 5 * 60_000
# 兼容旧私有名（同源，勿另赋值）。新代码一律用公开名；存量 test_replay_sql.py
# 仍 import 私名，待该测试面下次改动时一并收敛。
_FUTURE_TOLERANCE_MS: Final[int] = FUTURE_TOLERANCE_MS

# 规则①下界的允许乱序幅度 = 实时侧允许延迟，默认 2 分钟
DEFAULT_ALLOWED_LATENESS_MS: Final[int] = 120_000

SCHEMA_INVALID: Final[str] = "schema_invalid"
VALUE_OUT_OF_RANGE: Final[str] = "value_out_of_range"
TIME_OUT_OF_RANGE: Final[str] = "time_out_of_range"


def validate_event(
    event: dict[str, Any],
    *,
    now_ms: int | None = None,
    batch_min_ms: int | None = None,
    allowed_lateness_ms: int = DEFAULT_ALLOWED_LATENESS_MS,
) -> list[str]:
    """校验单个事件，返回**去重后**的违规原因码列表；空列表＝合法。

    参数是**可选**的时钟基准，因为规则①是**区间**判据而非绝对判据：
    - `now_ms`：上界基准（不晚于 now + 5 min）。缺省时跳过上界判定。
    - `batch_min_ms`：本批最早事件时间，下界＝该值 − `allowed_lateness_ms`。
      这是 SRS 强调的**相对**下界 —— 对生成器产出与公开数据集回放同时成立。
    """
    violations: list[str] = []

    # ── 规则④ + 必填子集：结构与取值 ──────────────────────────────
    for field in _REQUIRED_COMMON:
        if field not in event or event[field] in (None, ""):
            violations.append(SCHEMA_INVALID)
            break

    for field in ("event_id", "user_id", "item_id", "category_id"):
        value = event.get(field)
        if isinstance(value, str) and not value.strip():
            violations.append(SCHEMA_INVALID)
            break

    event_type = event.get("event_type")
    if event_type is not None and event_type not in EVENT_TYPES:
        # 枚举之外 ⇒ 取值越界（不是结构缺失）
        violations.append(VALUE_OUT_OF_RANGE)
    elif isinstance(event_type, str):
        for field in _REQUIRED_BY_TYPE.get(event_type, ()):
            if event.get(field) in (None, ""):
                violations.append(SCHEMA_INVALID)
                break

    # ── 规则②：amount ≥ 0 ────────────────────────────────────────
    amount = event.get("amount")
    if amount is not None:
        try:
            if float(amount) < 0:
                violations.append(VALUE_OUT_OF_RANGE)
        except (TypeError, ValueError):
            violations.append(VALUE_OUT_OF_RANGE)

    # ── 规则①：时间范围（区间判据，两端都可缺省）──────────────────
    event_time = event.get("event_time")
    if isinstance(event_time, int):
        if now_ms is not None and event_time > now_ms + _FUTURE_TOLERANCE_MS:
            violations.append(TIME_OUT_OF_RANGE)
        if batch_min_ms is not None and event_time < batch_min_ms - allowed_lateness_ms:
            violations.append(TIME_OUT_OF_RANGE)

    # 去重但保序：同一原因码只报一次，顺序稳定便于断言
    return list(dict.fromkeys(violations))
