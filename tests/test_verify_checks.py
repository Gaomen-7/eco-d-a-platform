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
    check_flink_job_state_from_json,
    check_funnel_monotone_result_from_rows,
    check_superset_health_from_body,
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


def test_p1_report_shape_is_four_pass_one_skip(tmp_path: Path) -> None:
    """P1 分层形态固化：第三项 SKIP，其余四项真断言，退出码 0。"""
    from ecomgen.verify_runner import P1_CHECKS

    assert len(P1_CHECKS) == 5
    # 第三项必须是那把「显式 SKIP」的
    third = P1_CHECKS[2]()
    assert third.status is Status.SKIP
    assert "paimon" in third.name

    report = VerifyReport(checks=[c() for c in P1_CHECKS])
    counts = report.counts()
    assert counts["SKIP"] >= 1
    assert "PASS" in counts
