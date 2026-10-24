"""verify 判据逻辑的测试（TDD 第九片：判据是否真的在断言）。

**这一片测的是「判据不会恒真」**——票面验收第 7 条要求「故意改坏一个输入时
必须退出非 0」。所以这里用假采样器注入各种畸形输入，确认判据会 FAIL。

如果这些测试全部通过，而实测又确实能证伪，那「断言在断言」才算成立。
"""

from __future__ import annotations

import json
from pathlib import Path

from ecomgen.verify import Status, VerifyReport
from ecomgen.verify_runner import (
    TOPIC_RETENTION_BYTES,
    TOPIC_RETENTION_MS,
    check_doris_ads_rows_from_counts,
    check_flink_job_state_from_json,
    check_funnel_monotone_result_from_rows,
    check_kafka_retention_from_configs,
    check_superset_health_from_body,
    parse_dynamic_topic_configs,
)


def test_flink_running_passes_on_running_state() -> None:
    payload = json.dumps({"jobs": [{"state": "RUNNING", "jid": "abc"}]})
    assert check_flink_job_state_from_json(payload).status is Status.PASS


def test_flink_fails_on_empty_jobs_array() -> None:
    """`/jobs/overview` 无作业时是空数组（P0 实测）——这不是通过。"""
    assert check_flink_job_state_from_json('{"jobs": []}').status is Status.FAIL


def test_flink_fails_on_non_running_state() -> None:
    payload = json.dumps({"jobs": [{"state": "RESTARTING"}]})
    assert check_flink_job_state_from_json(payload).status is Status.FAIL


def test_flink_fails_on_malformed_json() -> None:
    assert check_flink_job_state_from_json("not json").status is Status.FAIL


def test_superset_health_passes_on_plain_ok() -> None:
    """响应体是纯文本 `OK`，不是 JSON（P0 实测）。"""
    assert check_superset_health_from_body("OK").status is Status.PASS


def test_superset_health_fails_on_json_body() -> None:
    """若有人把判据写成「能解析 JSON」就会在这里暴露。"""
    assert check_superset_health_from_body('{"status":"ok"}').status is Status.FAIL


def test_funnel_monotone_passes_on_decreasing_rows() -> None:
    rows = [(1, 100), (2, 60), (3, 20), (4, 5)]
    assert check_funnel_monotone_result_from_rows(rows).status is Status.PASS


def test_funnel_monotone_passes_on_equal_rows() -> None:
    """非增包含相等（等号合法）。"""
    rows = [(1, 50), (2, 50), (3, 10)]
    assert check_funnel_monotone_result_from_rows(rows).status is Status.PASS


def test_funnel_monotone_fails_when_increase_appears() -> None:
    """违反即 fail —— 这是「判据真的在断言」的直接证据。"""
    rows = [(1, 10), (2, 90)]
    assert check_funnel_monotone_result_from_rows(rows).status is Status.FAIL


def test_funnel_monotone_skips_when_no_rows() -> None:
    """无漏斗行是「场景未激活」⇒ SKIP，不是 fail（spec §4.3 场景前置）。"""
    assert check_funnel_monotone_result_from_rows([]).status is Status.SKIP


def test_p1_report_shape_is_five_real_checks(tmp_path: Path) -> None:
    """分层形态固化（impl-03f 起）：五项**真断言**，第三项不再 SKIP。

    P1 时期第三项是显式 SKIP（spec §4.3）；P2 转真后本测试钉住新形态：
    第三项必须走采集器（不退回打印一行 SKIP 的旧形态）。这里 monkeypatch
    采集函数注入「五层全 7 行」⇒ 第三项 PASS；SKIP 形态会被
    test_verify_paimon_layers.py 的同名契约测试拦住。
    """
    import ecomgen.verify_runner as vr

    assert len(vr.P1_CHECKS) == 5
    assert vr.P1_CHECKS[2].__name__ == "check_paimon_layers"

    original = vr.collect_paimon_layer_counts
    vr.collect_paimon_layer_counts = lambda: dict.fromkeys(  # type: ignore[assignment]
        vr.PAIMON_LAYER_TABLES, 7
    )
    try:
        third = vr.P1_CHECKS[2]()
    finally:
        vr.collect_paimon_layer_counts = original  # type: ignore[assignment]

    assert third.status is Status.PASS
    assert "paimon" in third.name

    report = VerifyReport(
        checks=[c() for c in vr.P1_CHECKS if c is not vr.P1_CHECKS[2]]
    )
    report.checks.insert(2, third)
    # 第三项已注入「五层全 7 行」⇒ 它本身必须 PASS 且不为 SKIP
    # （旧实现曾以 SKIP 形态占位——恒真断言 `... or True` 已在 code-review 中剔除）。
    assert report.checks[2].status is Status.PASS
    assert report.checks[2].name == third.name


def test_recon_probe_is_wired_as_extra_check() -> None:
    """探针对账接线形态固化（impl-03f §2.6-4）：附加断言末位，
    不动五项面（P1_CHECKS 恒 5）；调用即走 reconcile_probe 入口。"""
    import ecomgen.verify_runner as vr

    assert vr.P1_CHECKS == (
        vr.check_kafka_topic,
        vr.check_flink_job_running,
        vr.check_paimon_layers,
        vr.check_doris_ads_rows,
        vr.check_superset_health,
    )
    assert vr.P1_EXTRA_CHECKS[-1].__name__ == "check_recon_probe"

    import ecomgen.reconcile_probe as rprobe

    captured: dict[str, str] = {}

    def _fake_entry(**_kw):
        from ecomgen.reconcile_probe import check_probe_reconciliation

        captured["called"] = "yes"
        return check_probe_reconciliation({}, {})

    original = rprobe.run_probe_reconciliation
    rprobe.run_probe_reconciliation = _fake_entry  # type: ignore[assignment]
    try:
        result = vr.check_recon_probe()
    finally:
        rprobe.run_probe_reconciliation = original  # type: ignore[assignment]
    assert captured["called"] == "yes"
    assert result.name == "recon-offline-mirror-doris"


# ── kafka-topic-retention：读法与施加方同源（impl-02c）─────────────────
# 实测输出格式（Kafka 4.3.1，`kafka-configs.sh --describe --entity-type topics`）

_KAFKA_DESCRIBE_OK = (
    "Dynamic configs for topic ecom.behavior.event.v1 are:\n"
    f"  retention.bytes={TOPIC_RETENTION_BYTES} sensitive=false "
    "synonyms={DYNAMIC_TOPIC_CONFIG:retention.bytes=2147483648, "
    "DEFAULT_CONFIG:log.retention.bytes=-1}\n"
    f"  retention.ms={TOPIC_RETENTION_MS} sensitive=false "
    "synonyms={DYNAMIC_TOPIC_CONFIG:retention.ms=604800000}\n"
)


def test_parse_dynamic_topic_configs_reads_expected_keys() -> None:
    configs = parse_dynamic_topic_configs(_KAFKA_DESCRIBE_OK)
    assert configs is not None
    assert configs["retention.ms"] == TOPIC_RETENTION_MS
    assert configs["retention.bytes"] == TOPIC_RETENTION_BYTES


def test_parse_dynamic_topic_configs_returns_none_without_header() -> None:
    """没有 `Dynamic configs ... are:` 头 ⇒ 取不到，**不**静默当空配置。"""
    assert parse_dynamic_topic_configs("") is None
    assert (
        parse_dynamic_topic_configs("Topic: x Configs: min.insync.replicas=1") is None
    )


def test_kafka_retention_passes_when_both_keys_match() -> None:
    """正例：施加方写的两个键都能在**同一配置面**读到。"""
    result = check_kafka_retention_from_configs(
        parse_dynamic_topic_configs(_KAFKA_DESCRIBE_OK)
    )
    assert result.status is Status.PASS


def test_kafka_retention_fails_when_ms_is_wrong() -> None:
    """反例 1：retention.ms 被改错 ⇒ FAIL（判据真的在断言）。"""
    result = check_kafka_retention_from_configs(
        {"retention.ms": "60000", "retention.bytes": TOPIC_RETENTION_BYTES}
    )
    assert result.status is Status.FAIL
    assert "60000" in result.actual


def test_kafka_retention_fails_when_bytes_missing() -> None:
    """反例 2：只施加了 retention.ms、漏了 bytes ⇒ FAIL（不是「有一项对就算过」）。"""
    result = check_kafka_retention_from_configs({"retention.ms": TOPIC_RETENTION_MS})
    assert result.status is Status.FAIL
    assert "缺失" in result.actual


def test_kafka_retention_fails_when_configs_unavailable() -> None:
    """反例 3：命令失败/取不到 ⇒ FAIL，**不是** SKIP（否则又变恒真假通过）。"""
    result = check_kafka_retention_from_configs(None, raw="boom")
    assert result.status is Status.FAIL
    assert not result.is_pass


def test_kafka_retention_fails_on_empty_dynamic_block() -> None:
    """反例 4：topic 存在但没有任何动态配置（retention 未施加）⇒ FAIL。"""
    result = check_kafka_retention_from_configs(
        parse_dynamic_topic_configs(
            "Dynamic configs for topic ecom.behavior.event.v1 are:\n"
        )
    )
    assert result.status is Status.FAIL


# ── doris-ads-rows：逐表独立落状态（impl-02c）─────────────────────────


def test_doris_ads_rows_passes_when_all_tables_have_rows() -> None:
    counts = {
        "ads_window_metric": 757,
        "ads_funnel_step": 89,
        "ads_late_expired_count": 157,
    }
    result = check_doris_ads_rows_from_counts(counts)
    assert result.status is Status.PASS
    assert "ads_late_expired_count=157 PASS" in result.actual


def test_doris_ads_rows_fails_naming_only_the_empty_table() -> None:
    """超期表为空 ⇒ FAIL，但另两张表的行数仍是 PASS 并可见（不再被掩盖）。"""
    counts = {
        "ads_window_metric": 757,
        "ads_funnel_step": 89,
        "ads_late_expired_count": 0,
    }
    result = check_doris_ads_rows_from_counts(counts)
    assert result.status is Status.FAIL
    assert "ads_window_metric=757 PASS" in result.actual
    assert "ads_funnel_step=89 PASS" in result.actual
    assert "ads_late_expired_count=0 FAIL" in result.actual


def test_doris_ads_rows_fails_when_query_fails() -> None:
    result = check_doris_ads_rows_from_counts(None, raw="connect refused")
    assert result.status is Status.FAIL
    assert result.detail == "connect refused"
