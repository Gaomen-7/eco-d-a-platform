"""Paimon↔Doris 对账判据面测试（impl-03f 交付物 1，口径裁决 C）。

**口径裁决 C**（用户 2026-10-24）：两侧现状四层不相交（窗口/词表/dim_key/单位，
票面 §0.2 实测），对账基准取「同一批事件字节过两条链路」的期望复算——
离线侧从 Kafka main topic 导出**同批字节**，用与 Java 闸门同源的
`events.validate_event` + 与 `WindowSegment.AggregateWindow` 同源的公式
复算期望行，与 Doris `is_settled=1` 的 `event_*` 行逐值比对。

**判据形态**（spec §4.2 对账条文 + 票面 AC）：
- 逐 `metric_name`、只比 `is_settled=1`、逐值严格相等；
- `is_settled=0`（暂定）行的差异**不得**导致失败（票面 AC 第 3 条）；
- 任一侧缺席 ⇒ 不等（缺席不是相等）；
- 全外连接语义：期望多于 Doris（漏写）与 Doris 多于期望（多写）都算差异。

**测试策略**（与 test_verify_paimon_layers.py 同构）：
- 判定逻辑抽纯函数 `check_reconciliation_from_rows`，注入假行集；
- 期望行复算抽纯函数 `expected_rows_from_events`，注入假事件字节流；
- mysql `--batch -N` 输出解析抽纯函数 `parse_doris_rows`；
- 真实路径（连 Kafka/Doris 的采集器）不在单测内触碰。
"""

from __future__ import annotations

import json

from ecomgen.reconcile import (
    check_reconciliation_from_rows,
    expected_rows_from_events,
    parse_doris_rows,
)
from ecomgen.verify import Status

# ══════════════════════════════════════════════════════════════════════
# 期望行复算：与实时侧公式的同源性
# ══════════════════════════════════════════════════════════════════════


def _ev(
    event_id: str,
    event_time: int,
    event_type: str = "page_view",
    category: str = "c_00001",
) -> str:
    """造一条合法事件字节（字段齐、枚举内、时间在界内）。"""
    return json.dumps(
        {
            "event_id": event_id,
            "event_time": event_time,
            "user_id": "u_1",
            "item_id": "i_1",
            "category_id": category,
            "event_type": event_type,
            "schema_version": "1.0",
            "page": "home",
            "source": "web",
            "channel": "organic",
            "province": "上海市",
            "payload": {"session_id": "u_1"},
        },
        ensure_ascii=False,
        sort_keys=True,
    )


def test_expected_rows_counts_per_window_type_category() -> None:
    """同窗同键计数、跨窗分桶、metric_name 前缀 event_、epoch 对齐分钟。"""
    # 两条同窗同键 + 一条同窗异类 + 一条邻窗 ⇒ 期望三行三个值
    # （1700000000000 // 60000 * 60000 = 1699999980000：epoch 分钟对齐，
    #   不是十进制去掉末四位——手算期望值必须按 floor 语义来）
    rows = expected_rows_from_events(
        [
            _ev("e1", 1700000000_000, "page_view", "c_1"),
            _ev("e2", 1700000000_030, "page_view", "c_1"),  # 同窗（同分钟）
            _ev("e3", 1700000000_050, "view_item", "c_1"),  # 同窗异类
            _ev("e4", 1700000060_000, "page_view", "c_1"),  # 下一个分钟窗
        ]
    )
    assert rows == {
        (1699999980_000, "event_page_view", "c_1"): 2,
        (1699999980_000, "event_view_item", "c_1"): 1,
        (1700000040_000, "event_page_view", "c_1"): 1,
    }


def test_expected_rows_floor_aligns_epoch_minutes() -> None:
    """窗口起点 = floor(event_time/60000)*60000（非整分时间落进所属分钟窗）。"""
    rows = expected_rows_from_events([_ev("e1", 1700000042_999)])
    assert list(rows) == [(1700000040_000, "event_page_view", "c_00001")]


def test_expected_rows_excludes_dirty_events() -> None:
    """脏事件（与 Java 闸门同源判定）不进期望 —— 它们进了 DLQ，不进 ADS。"""
    dirty_missing_type = json.dumps(
        {"event_id": "d1", "event_time": 1700000000_000, "user_id": "u"}
    )
    dirty_bad_enum = _ev("d2", 1700000000_000)
    dirty_bad_enum = dirty_bad_enum.replace("page_view", "nonexistent_type")
    rows = expected_rows_from_events([dirty_missing_type, dirty_bad_enum])
    assert rows == {}


def test_expected_rows_excludes_duplicates() -> None:
    """同 event_id 二次到达 ⇒ 重复通道，不进期望计数（gdd §D D2 同源）。"""
    line = _ev("dup-1", 1700000000_000)
    rows = expected_rows_from_events([line, line])
    assert rows == {(1699999980_000, "event_page_view", "c_00001"): 1}


def test_expected_rows_tolerates_unparsable_lines() -> None:
    """坏 JSON 行跳过不炸（导出面可能有残行；计数不进来就是了）。"""
    rows = expected_rows_from_events(["not json", "", _ev("e1", 1700000000_000)])
    assert rows == {(1699999980_000, "event_page_view", "c_00001"): 1}


# ══════════════════════════════════════════════════════════════════════
# Doris 行解析：mysql --batch -N 输出
# ══════════════════════════════════════════════════════════════════════


def test_parse_doris_rows_basic() -> None:
    out = (
        "2026-10-21 11:47:00\tevent_page_view\tc_00001\t3\n"
        "2026-10-21 11:47:00\tevent_page_view\tc_00002\t7\n"
    )
    rows = parse_doris_rows(out)
    assert rows == {
        ("2026-10-21 11:47:00", "event_page_view", "c_00001"): 3,
        ("2026-10-21 11:47:00", "event_page_view", "c_00002"): 7,
    }


def test_parse_doris_rows_skips_non_data_lines() -> None:
    out = "warning: some client note\nevent_id\tmetric\n2026-10-21 11:47:00\tuv\t\t5\n"
    rows = parse_doris_rows(out)
    # uv 行 dim_key 为空串 ⇒ 键含空串，值 5；警告行四列不齐被跳过
    assert rows == {("2026-10-21 11:47:00", "uv", ""): 5}


# ══════════════════════════════════════════════════════════════════════
# 判定面：全外连接逐值比对
# ══════════════════════════════════════════════════════════════════════


def test_recon_pass_when_identical() -> None:
    # 1699999980000 ms = 2023-11-14 22:13:00 UTC（floor 后的窗口起点）
    expected = {(1699999980_000, "event_page_view", "c_1"): 2}
    doris = {("2023-11-14 22:13:00", "event_page_view", "c_1"): 2}
    # 键折算：epoch ms → 'YYYY-MM-DD HH:MM:SS'（UTC），由被测函数内部完成
    result = check_reconciliation_from_rows(expected, doris)
    assert result.status is Status.PASS
    assert "rows=1" in result.actual


def test_recon_fail_on_value_mismatch() -> None:
    expected = {(1700000000_000, "event_page_view", "c_1"): 2}
    doris = {("2026-10-21 11:47:00", "event_page_view", "c_1"): 3}
    result = check_reconciliation_from_rows(expected, doris)
    assert result.status is Status.FAIL
    assert "event_page_view" in result.actual


def test_recon_fail_on_either_side_missing() -> None:
    """全外连接语义：漏写（期望有 Doris 无）与多写（Doris 有期望无）都红。"""
    base = {("2026-10-21 11:47:00", "event_page_view", "c_1"): 2}
    # 期望多一行 ⇒ Doris 漏写
    expected = {
        (1700000000_000, "event_page_view", "c_1"): 2,
        (1700000000_000, "event_view_item", "c_1"): 1,
    }
    result_missing = check_reconciliation_from_rows(expected, base)
    assert result_missing.status is Status.FAIL
    # Doris 多一行 ⇒ 多写
    doris_extra = dict(base)
    doris_extra[("2026-10-21 11:47:00", "event_view_item", "c_1")] = 1
    result_extra = check_reconciliation_from_rows(
        {(1700000000_000, "event_page_view", "c_1"): 2}, doris_extra
    )
    assert result_extra.status is Status.FAIL


def test_recon_skips_non_event_metrics() -> None:
    """uv/duplicate_absorbed/provisional_emitted 是实时侧自有指标，
    不在本对账切面的公式面内（票面 §0.2 缺口 1：uv 两层口径不同）——
    它们出现在 Doris 行集里**不判差异**，由 `is_settled=0 不判` 与
    词表登记共同兜住。"""
    expected = {(1699999980_000, "event_page_view", "c_1"): 2}
    doris = {
        ("2023-11-14 22:13:00", "event_page_view", "c_1"): 2,
        ("2026-10-22 06:00:00", "uv", ""): 99,  # 非本切面指标 ⇒ 忽略
    }
    result = check_reconciliation_from_rows(expected, doris)
    assert result.status is Status.PASS


def test_recon_fail_when_doris_event_metric_absent_from_expected() -> None:
    """event_* 指标**在**本切面公式面内 ⇒ Doris 有而期望无 = 多写，红。"""
    expected = {}
    doris = {("2026-10-21 11:47:00", "event_page_view", "c_1"): 2}
    result = check_reconciliation_from_rows(expected, doris)
    assert result.status is Status.FAIL


def test_recon_epoch_to_utc_string_key_conversion() -> None:
    """1700000000_000 ms = 2023-11-14 22:13:20 UTC —— 键折算必须 UTC。"""
    expected = {(1700000000_000, "event_page_view", "c_1"): 1}
    doris = {("2023-11-14 22:13:20", "event_page_view", "c_1"): 1}
    result = check_reconciliation_from_rows(expected, doris)
    assert result.status is Status.PASS
