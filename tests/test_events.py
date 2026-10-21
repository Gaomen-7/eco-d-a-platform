"""事件 schema 与质量规则的测试（TDD 第三片）。

判据来源：SRS.md §3.3（公共字段表 + 五类事件必填子集 + 数据质量规则 ①~⑤）。
接缝 = `ecomgen.events` 的公开校验函数：给定一个事件 dict，返回违规原因码列表。
"""

from __future__ import annotations

from ecomgen.events import (
    EVENT_TYPES,
    validate_event,
)


def _valid_event(**overrides: object) -> dict[str, object]:
    """一个通过全部规则的基准事件（page_view，无额外必填）。"""
    base: dict[str, object] = {
        "event_id": "3f2a1b4c-5d6e-4f70-8a9b-0c1d2e3f4a5b",
        "event_time": 1_700_000_000_000,
        "user_id": "u_000123",
        "item_id": "i_004567",
        "category_id": "c_00089",
        "event_type": "page_view",
        "schema_version": "1.0",
        "payload": {"raw": True},
    }
    base.update(overrides)
    return base


def test_valid_event_has_no_violations() -> None:
    assert validate_event(_valid_event()) == []


def test_five_event_types_are_the_frozen_enum() -> None:
    """五类事件枚举冻结（SRS §3.1 REQ-F-GEN-02）。"""
    assert EVENT_TYPES == (
        "page_view",
        "view_item",
        "add_to_cart",
        "begin_checkout",
        "purchase",
    )


def test_unknown_event_type_is_out_of_range() -> None:
    assert "value_out_of_range" in validate_event(_valid_event(event_type="refund"))


def test_missing_required_field_is_schema_invalid() -> None:
    ev = _valid_event()
    del ev["event_id"]
    assert "schema_invalid" in validate_event(ev)


def test_empty_required_string_is_schema_invalid() -> None:
    """规则④：user_id / item_id / event_id 非空。"""
    assert "schema_invalid" in validate_event(_valid_event(user_id=""))


def test_amount_negative_is_out_of_range() -> None:
    """规则②：amount ≥ 0。"""
    ev = _valid_event(event_type="purchase", amount=-1, currency="CNY", order_id="o_1")
    assert "value_out_of_range" in validate_event(ev)


def test_purchase_requires_amount_currency_order_id() -> None:
    """五类事件必填子集：purchase 额外必填 amount/currency/order_id。"""
    assert "schema_invalid" in validate_event(_valid_event(event_type="purchase"))


def test_add_to_cart_requires_quantity() -> None:
    assert "schema_invalid" in validate_event(_valid_event(event_type="add_to_cart"))


def test_begin_checkout_requires_order_id() -> None:
    assert "schema_invalid" in validate_event(_valid_event(event_type="begin_checkout"))


def test_event_time_too_far_in_future_is_time_out_of_range() -> None:
    """规则①上界：不晚于「当前 + 5 分钟」。"""
    ev = _valid_event(event_time=1_700_000_000_000 + 5 * 60_000 + 1)
    assert "time_out_of_range" in validate_event(ev, now_ms=1_700_000_000_000)


def test_event_time_below_relative_lower_bound_is_time_out_of_range() -> None:
    """规则①下界是**相对**下界：不早于「批次最早 event_time − 允许乱序幅度」。

    这个下界对生成器产出与公开数据集回放**同时**成立，故不能用绝对时钟。
    """
    batch_min = 1_700_000_000_000
    ev = _valid_event(event_time=batch_min - 120_000 - 1)
    assert "time_out_of_range" in validate_event(ev, batch_min_ms=batch_min)


def test_event_time_at_lower_bound_is_legal() -> None:
    """边界：恰好等于下界（批次最早 − 120 s）合法，不早退。"""
    batch_min = 1_700_000_000_000
    ev = _valid_event(event_time=batch_min - 120_000)
    assert validate_event(ev, batch_min_ms=batch_min) == []
