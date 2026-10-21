"""补上「死判据未接线」的缺口测试（is_settled 两态 / F6 降级阶梯 / 重复通道口径）。

**这一片测的是「新接进来的判据真的会 FAIL」**——票面验收要求每条新判据都必须
给出反例证据，否则「接上了」只是接了行代码，不证明它还能证伪。

覆盖：
1. `is_settled` 两态：只有落定 ⇒ FAIL、从未落定 ⇒ FAIL、两态都有 ⇒ PASS、
   取不到 ⇒ SKIP；
2. F6 降级阶梯：OOM 计数达阈值 ⇒ FAIL、低于阈值 ⇒ PASS、容器取不到 ⇒ SKIP，
   且 `degradation` 字典里留下**实测的 oom 计数与阈值**；
3. `run_verify()` 真的把 `degradation` 填进报告、把降级判据追加进 checks
   （用 monkeypatch 隔离全部碰容器的采集器）；
4. `fault.DUPLICATE_COUNT_SQL` 取的是 `duplicate_absorbed` 的 metric_value，
   **不是** `SUM(revision)`（防回归）。
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from ecomgen import verify_runner
from ecomgen.fault import DUPLICATE_COUNT_SQL
from ecomgen.verify import CheckResult, Status
from ecomgen.verify_runner import (
    DEGRADATION_OOM_THRESHOLD,
    ContainerOomState,
    SettledCounts,
    check_degradation_ladder_result,
    check_is_settled_two_states_from_counts,
)


def _container(
    name: str, *, oom_killed: bool = False, exit_code: int = 0
) -> ContainerOomState:
    return ContainerOomState(name=name, oom_killed=oom_killed, exit_code=exit_code)


# ── 1. `is_settled` 两态判据 ──────────────────────────────────────────


def test_settled_two_states_fails_when_only_settled_rows() -> None:
    """只有落定行 ⇒ FAIL：观察不到「暂定→落定」迁移，断言跑得太晚或暂定从未写。"""
    result = check_is_settled_two_states_from_counts(SettledCounts(0, 5))
    assert result.status is Status.FAIL
    assert result.is_failure


def test_settled_two_states_fails_when_window_never_closed() -> None:
    """只有暂定行 ⇒ FAIL：窗口从未关闭。"""
    result = check_is_settled_two_states_from_counts(SettledCounts(5, 0))
    assert result.status is Status.FAIL
    assert result.is_failure


def test_settled_two_states_passes_when_both_states_visible() -> None:
    """两态都 >0 ⇒ PASS（暂定与落定同表可见，迁移留过痕）。"""
    result = check_is_settled_two_states_from_counts(SettledCounts(3, 4))
    assert result.status is Status.PASS
    assert not result.is_failure


def test_settled_two_states_skips_when_counts_unavailable() -> None:
    """取不到行数 ⇒ SKIP，**不是** PASS（否则又变成恒真假通过）。"""
    result = check_is_settled_two_states_from_counts(None)
    assert result.status is Status.SKIP
    assert not result.is_pass


# ── 2. F6 降级阶梯判据 ───────────────────────────────────────────────


def test_degradation_fails_when_oom_count_reaches_threshold() -> None:
    """OOM 杀 ≥ 阈值（设计值 2）⇒ FAIL，且降级字典里写实测计数与阈值。"""
    states = [
        _container("flink-jm", oom_killed=False, exit_code=0),
        _container("flink-tm", exit_code=137),
        _container("kafka", oom_killed=True),
    ]
    result, degradation = check_degradation_ladder_result(states)

    assert result.status is Status.FAIL
    assert degradation is not None
    assert degradation["oom_killed_count"] == 2
    assert degradation["threshold"] == DEGRADATION_OOM_THRESHOLD
    assert degradation["degraded"] is True
    assert "设计值" in degradation["threshold_basis"]
    # 每个容器的原始状态都要留痕，供人复核
    containers = degradation["containers"]
    assert len(containers) == 3
    assert [c["counted"] for c in containers] == [False, True, True]


def test_degradation_passes_below_threshold_but_still_reports_counts() -> None:
    """低于阈值 ⇒ PASS，但实测值照写（票面：实测值写进报告）。"""
    states = [_container("flink-tm", exit_code=0), _container("kafka")]
    result, degradation = check_degradation_ladder_result(states)

    assert result.status is Status.PASS
    assert degradation is not None
    assert degradation["oom_killed_count"] == 0
    assert degradation["threshold"] == DEGRADATION_OOM_THRESHOLD
    assert degradation["degraded"] is False


def test_degradation_skips_when_containers_unavailable() -> None:
    """取不到容器状态 ⇒ SKIP（**不**算通过），且降级字段为 None。"""
    result, degradation = check_degradation_ladder_result(None)
    assert result.status is Status.SKIP
    assert not result.is_pass
    assert degradation is None


def test_degradation_respects_custom_threshold() -> None:
    """阈值可注入：同一组状态在阈值 3 下不降级、阈值 1 下降级（证明判定用入参）。"""
    states = [_container("a", exit_code=137), _container("b", oom_killed=True)]

    below, deg_below = check_degradation_ladder_result(states, threshold=3)
    at, deg_at = check_degradation_ladder_result(states, threshold=1)

    assert below.status is Status.PASS
    assert deg_below is not None
    assert deg_below["threshold"] == 3
    assert at.status is Status.FAIL
    assert deg_at is not None
    assert deg_at["threshold"] == 1


# ── 3. `run_verify()` 的接线（degradation 必须进报告）────────────────


def test_run_verify_appends_degradation_check_and_fills_report(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """降级判据既进 checks，也进 report.degradation（构造顺序坑已修）。"""
    report_dir = tmp_path / "verify"
    stub = CheckResult(Status.PASS, "stub", "1", "1")
    monkeypatch.setattr(verify_runner, "P1_CHECKS", (lambda: stub,))
    monkeypatch.setattr(verify_runner, "P1_EXTRA_CHECKS", ())
    expected_deg = {"threshold": 2, "oom_killed_count": 1, "degraded": False}
    monkeypatch.setattr(
        verify_runner,
        "check_degradation_ladder",
        lambda: (
            CheckResult(Status.FAIL, "degradation-ladder", "oom_killed=1", "<2"),
            expected_deg,
        ),
    )

    report = verify_runner.run_verify(str(report_dir))

    assert report.degradation == expected_deg
    assert [c.name for c in report.checks] == ["stub", "degradation-ladder"]
    assert report.exit_code() == 1

    # 落盘报告里也必须带实测值（票面：实测值写进报告，不仅是内存对象）
    written = sorted(report_dir.glob("verify-report-*.json"))
    assert len(written) == 1
    payload = json.loads(written[0].read_text(encoding="utf-8"))
    assert payload["degradation"] == expected_deg


def test_run_verify_extra_checks_include_settled_two_states() -> None:
    """`check_is_settled_two_states` 已接进 P1_EXTRA_CHECKS。"""
    assert verify_runner.check_is_settled_two_states in verify_runner.P1_EXTRA_CHECKS


# ── 4. 重复通道口径防回归 ────────────────────────────────────────────


def test_duplicate_count_sql_uses_metric_name_not_revision() -> None:
    """重复计数只汇总 `duplicate_absorbed` 的 metric_value，**不再**用 SUM(revision)。"""
    assert "metric_name='duplicate_absorbed'" in DUPLICATE_COUNT_SQL
    assert "SUM(metric_value)" in DUPLICATE_COUNT_SQL
    assert "revision" not in DUPLICATE_COUNT_SQL
