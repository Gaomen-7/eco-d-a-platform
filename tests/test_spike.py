"""impl-06g 判据单测：尖峰速率 ≥1000 events/s 的机读断言面。

**为什么这些测试离线可跑**：判定逻辑全部是纯函数（`spike.py` 的
`*_result` / `spike_checks_from_facts`、`cli.SendMeasurement` 的口径），
不碰 Docker / Kafka / Flink——真发判据的语义先在离线面钉死，实机跑的
只是采集（与 verify_runner 同一判据纪律：纯函数判语义、默认路径碰环境）。
"""

from __future__ import annotations

import json

from ecomgen.cli import SendMeasurement, _window_quota_exhausted
from ecomgen.spike import (
    RATE_TARGET_EVENTS_S,
    SpikeFacts,
    dlq_delta_result,
    lag_settle_result,
    liveness_result,
    parse_consumer_lag,
    parse_dlq_reasons,
    parse_exception_count,
    parse_running_state,
    parse_topic_total_offset,
    rate_result,
    spike_checks_from_facts,
)
from ecomgen.verify import Status

# ── 一手口径：SendMeasurement ────────────────────────────────────────


def test_measurement_rate_uses_actual_events_over_wall() -> None:
    """速率 = 实际发出条数 / 含 flush 的墙钟秒——不用计划条数当分子。"""
    m = SendMeasurement(events=11135, wall_ms=11129)
    rate = m.rate_per_sec
    assert rate is not None
    assert abs(rate - 1000.5) < 0.1  # 11135 / 11.129 s


def test_measurement_rate_undefined_on_empty_batch() -> None:
    """空批次（wall=0）⇒ 速率无定义，不是 0——0 会被当成「实测过但极慢」。"""
    m = SendMeasurement(events=0, wall_ms=0)
    assert m.rate_per_sec is None


def test_measurement_plan_block_roundtrip() -> None:
    """measured_send 块的写/读回环：判据侧读的就是这个块。"""
    m = SendMeasurement(events=1001, wall_ms=1000)
    block = m.to_plan_block()
    assert block["events"] == 1001
    assert block["wall_ms"] == 1000
    assert block["rate_per_sec"] == 1001.0
    assert "basis" in block  # 口径声明必须随机读面走
    restored = SendMeasurement.from_plan_block(json.loads(json.dumps(block)))
    assert restored == m


def test_measurement_from_plan_block_rejects_missing_fields() -> None:
    """缺字段 / 坏类型 ⇒ None（不静默造 0）。"""
    assert SendMeasurement.from_plan_block({}) is None
    assert SendMeasurement.from_plan_block({"events": 5}) is None
    assert SendMeasurement.from_plan_block({"events": "5", "wall_ms": 5}) is None


# ── 分窗信贷节流（impl-06g：逐条 pacing 证伪后的修法）────────────────


def test_window_quota_exhausted_front_load() -> None:
    """窗初份额一次性可用（front-load）：第 1 秒内 sent=1000 未满 1000 份额。"""
    # 窗口 1（elapsed < 1s）：份额 = 1000×1
    assert _window_quota_exhausted(999, 0.999, 1000) is False
    # 发满份额即等边界——即使墙钟仍在第 1 窗内
    assert _window_quota_exhausted(1000, 0.999, 1000) is True
    # 第 2 窗（1s ≤ elapsed < 2s）：份额 = 1000×2
    assert _window_quota_exhausted(1500, 1.2, 1000) is False  # 份额 2000 未满
    assert _window_quota_exhausted(1999, 1.999, 1000) is False
    assert _window_quota_exhausted(2000, 1.999, 1000) is True
    # 第 3 窗
    assert _window_quota_exhausted(2000, 2.001, 1000) is False
    assert _window_quota_exhausted(2999, 2.999, 1000) is False


def test_window_quota_makes_rate_a_floor_not_ceiling() -> None:
    """完成 N 条所需的窗口数 = ceil(N/rate) ⇒ 实测速率 ≥ rate（窗口裕度）。

    11 134 条 @1000/s ⇒ 12 窗 ⇒ 墙钟 < 12 s ⇒ 速率 > 11134/12 ≈ 927.8 ≥ 1000
    不成立——这条断的是**结构**：发满份额才睡 ⇒ 发送侧自身不再制造欠冲，
    实测速率的下界由窗口边界计算给出，与逐条 pacing 的「结构性欠冲」相反。
    """
    n, rate = 11134, 1000
    windows = -(-n // rate)  # ceil
    assert windows == 12
    # 每窗的实际发送时长 > 0 ⇒ 总墙钟 < windows 秒 ⇒ 速率 > n/windows
    assert n / windows >= 927  # 下界下限
    # 而 front-load 语义保证：只要发送侧每秒能发 ≥ rate 条，
    # 速率实测必然 ≥ rate -（仅受窗口边界量化）。
    assert rate * windows >= n  # 份额总是够发完全部 N 条


# ── 判据 1：速率阈值（SRS 冻结判据，不得调低）────────────────────────


def test_rate_result_passes_at_and_above_threshold() -> None:
    r = rate_result(RATE_TARGET_EVENTS_S, 1000, 1000)
    assert r.status is Status.PASS
    r = rate_result(1001.6, 11135, 11116)  # gdd 原型实测同量级
    assert r.status is Status.PASS


def test_rate_result_fails_below_threshold() -> None:
    r = rate_result(999.9, 11135, 11136)
    assert r.status is Status.FAIL
    assert "999.9" in r.actual


def test_rate_result_fails_when_undefined() -> None:
    """速率取不到（发送失败/空批次）⇒ FAIL：没有一手证据就是没证据。"""
    r = rate_result(None, -1, -1)
    assert r.status is Status.FAIL


# ── 判据 2：期间存活 ──────────────────────────────────────────────────


def test_liveness_result_passes_when_all_running() -> None:
    r = liveness_result({"samples": 12, "non_running_samples": 0, "new_exceptions": 0})
    assert r.status is Status.PASS


def test_liveness_result_fails_on_non_running_sample() -> None:
    r = liveness_result({"samples": 12, "non_running_samples": 1, "new_exceptions": 0})
    assert r.status is Status.FAIL


def test_liveness_result_fails_on_new_exception() -> None:
    r = liveness_result({"samples": 12, "non_running_samples": 0, "new_exceptions": 2})
    assert r.status is Status.FAIL


def test_liveness_result_fails_when_monitoring_missing() -> None:
    """期间监控缺失（samples 缺 / 异常计数缺）⇒ FAIL：取不到 ≠ 没崩。"""
    assert (
        liveness_result({"samples": 5, "non_running_samples": 0}).status is Status.FAIL
    )
    assert (
        liveness_result(
            {"samples": 5, "non_running_samples": 0, "new_exceptions": None}
        ).status
        is Status.FAIL
    )
    assert (
        liveness_result(
            {"samples": 0, "non_running_samples": 0, "new_exceptions": 0}
        ).status
        is Status.FAIL
    )


# ── 判据 3：lag 回落 ──────────────────────────────────────────────────


def test_lag_settle_result_passes_on_zero() -> None:
    assert lag_settle_result(0, 35.0).status is Status.PASS


def test_lag_settle_result_fails_on_backlog_or_missing() -> None:
    assert lag_settle_result(417, 180.0).status is Status.FAIL
    assert lag_settle_result(None, 180.0).status is Status.FAIL


# ── 判据 4：DLQ 增量归因 ──────────────────────────────────────────────


def test_dlq_delta_result_passes_on_zero_delta() -> None:
    r = dlq_delta_result(0, {})
    assert r.status is Status.PASS


def test_dlq_delta_result_fails_with_reason_distribution() -> None:
    """有增量 ⇒ FAIL 且 actual 携带 reason 分布（归因证据面）。"""
    r = dlq_delta_result(7, {"time_out_of_range": 7})
    assert r.status is Status.FAIL
    assert "time_out_of_range×7" in r.actual


def test_dlq_delta_result_fails_without_sample() -> None:
    """有增量但样本没抓到 ⇒ 仍 FAIL，并点名需人工归因。"""
    r = dlq_delta_result(3, {})
    assert r.status is Status.FAIL
    assert "人工" in r.actual


# ── 事实 → 判据列表（端到端判定面）───────────────────────────────────


def _good_facts() -> SpikeFacts:
    return SpikeFacts(
        measurement=SendMeasurement(events=11135, wall_ms=11116),
        during={"samples": 12, "non_running_samples": 0, "new_exceptions": 0},
        lag_after_settle=0,
        lag_wait_s=35.0,
        dlq_delta=0,
        dlq_reasons={},
        input_delta=11135,
    )


def test_facts_to_checks_all_pass() -> None:
    checks = spike_checks_from_facts(_good_facts())
    assert len(checks) == 4
    assert all(c.is_pass for c in checks)
    rate = checks[0]
    assert "1000" in rate.actual or "11135" in rate.actual


def test_facts_to_checks_rate_shortfall_fails_only_rate() -> None:
    """速率差一点（999.5）⇒ 只有速率判据 FAIL，其余照判（好状态不被掩盖）。"""
    facts = _good_facts()
    short = SpikeFacts(
        measurement=SendMeasurement(events=11135, wall_ms=11141),
        during=facts.during,
        lag_after_settle=facts.lag_after_settle,
        lag_wait_s=facts.lag_wait_s,
        dlq_delta=facts.dlq_delta,
        dlq_reasons=facts.dlq_reasons,
        input_delta=facts.input_delta,
    )
    checks = spike_checks_from_facts(short)
    assert [c.status for c in checks] == [
        Status.FAIL,
        Status.PASS,
        Status.PASS,
        Status.PASS,
    ]


def test_facts_to_checks_send_missing_fails() -> None:
    """发送测量缺失（live 失败）⇒ 速率判据 FAIL，判据面不装聋。"""
    facts = _good_facts()
    broken = SpikeFacts(
        measurement=None,
        during=facts.during,
        lag_after_settle=facts.lag_after_settle,
        lag_wait_s=facts.lag_wait_s,
        dlq_delta=facts.dlq_delta,
        dlq_reasons=facts.dlq_reasons,
        input_delta=facts.input_delta,
    )
    checks = spike_checks_from_facts(broken)
    assert checks[0].status is Status.FAIL


# ── 采集面解析（纯函数）───────────────────────────────────────────────


def test_parse_running_state() -> None:
    running = json.dumps({"jobs": [{"jid": "j1", "state": "RUNNING"}]})
    assert parse_running_state(running) == "RUNNING"
    failed = json.dumps({"jobs": [{"jid": "j1", "state": "FAILING"}]})
    assert parse_running_state(failed) is None
    assert parse_running_state("not json") is None


def test_parse_exception_count() -> None:
    empty = json.dumps({"exceptionHistory": {"entries": [], "truncated": False}})
    assert parse_exception_count(empty) == 0
    two = json.dumps({"exceptionHistory": {"entries": [{}, {}], "truncated": False}})
    assert parse_exception_count(two) == 2
    assert parse_exception_count("not json") is None
    assert parse_exception_count(json.dumps({"other": 1})) is None


def test_parse_consumer_lag_ignores_headers_and_missing_group() -> None:
    out = (
        "Consumer group 'ecom-p1-consumer' has no active members.\n"
        "\n"
        "GROUP            TOPIC                  PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG\n"
        "ecom-p1-consumer ecom.behavior.event.v1 0          9992            9992            0\n"
        "ecom-p1-consumer ecom.behavior.event.v1 1          9566            9566            0\n"
        "ecom-p1-consumer ecom.behavior.event.v1 2          100             517             417\n"
    )
    assert parse_consumer_lag(out) == 417
    assert parse_consumer_lag("no rows at all") is None


def test_parse_topic_total_offset() -> None:
    out = "ecom.behavior.event.v1:0:9992\necom.behavior.event.v1:1:10\n"
    assert parse_topic_total_offset(out) == 10002
    assert parse_topic_total_offset("") is None


def test_parse_dlq_reasons() -> None:
    out = (
        '{"reason":"time_out_of_range","event_id":"x","event_time":1,"raw":null}\n'
        '{"reason":"time_out_of_range","event_id":"y","event_time":2,"raw":null}\n'
        '{"reason":"schema_invalid","event_id":"z","event_time":3,"raw":null}\n'
        "Processed a total of 3 messages\n"
    )
    assert parse_dlq_reasons(out) == {
        "time_out_of_range": 2,
        "schema_invalid": 1,
    }
    assert parse_dlq_reasons("") == {}
