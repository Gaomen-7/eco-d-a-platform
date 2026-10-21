"""verify-fault 判据的测试（TDD 第十片：故障恢复与幂等吸收证明）。

判据来源：spec.md §4.4（全参数化流程图逐行）、票面验收第 6/7 条。
接缝 = `ecomgen.fault` 的**纯判据函数**：给定 kill 前后的快照，判定是否通过。

★ 关键反例保护（票面明确要求）：**条数一致但 DLQ 上升 ⇒ fail**。
若把重复误当脏，重放 5 条合法事件就能把脏率从 0% 推到 50%（gdd 原型实测），
所以这条判据必须真的能证伪。
"""

from __future__ import annotations

from ecomgen.fault import (
    Baseline,
    FaultVerdict,
    judge_recovery,
)


def _baseline(**overrides: object) -> Baseline:
    base: dict[str, object] = {
        "window_metric_count": 100,
        "window_metric_sum": 5000,
        "funnel_step_count": 40,
        "funnel_step_sum": 800,
        "dlq_offset": 10,
        "duplicate_count": 100,
    }
    base.update(overrides)
    return Baseline(**base)  # type: ignore[arg-type]


def test_recovery_passes_when_counts_equal_and_dlq_flat_and_duplicates_rose() -> None:
    """正常恢复：两表 count/sum 与基线全等、DLQ 增量为 0、重复通道上升。"""
    before = _baseline()
    after = _baseline(duplicate_count=137)  # 重复上升 37
    verdict = judge_recovery(before, after)

    assert isinstance(verdict, FaultVerdict)
    assert verdict.passed
    assert verdict.exit_code() == 0


def test_recovery_fails_when_row_count_changed() -> None:
    """条数变了 ⇒ 幂等吸收没做到。"""
    verdict = judge_recovery(
        _baseline(), _baseline(window_metric_count=101, duplicate_count=137)
    )
    assert not verdict.passed


def test_recovery_fails_when_sum_changed() -> None:
    """和变了（即使条数没变）⇒ 值被重复累加，同样不通过。"""
    verdict = judge_recovery(
        _baseline(), _baseline(window_metric_sum=5100, duplicate_count=137)
    )
    assert not verdict.passed


def test_recovery_fails_when_dlq_increased() -> None:
    """★ 反例保护：**条数一致但 DLQ 上升** ⇒ fail（重复被误当脏）。"""
    verdict = judge_recovery(_baseline(), _baseline(dlq_offset=15, duplicate_count=137))
    assert not verdict.passed
    assert "dlq" in verdict.reason.lower() or "脏" in verdict.reason


def test_recovery_fails_when_duplicates_did_not_rise() -> None:
    """重复通道没上升 ⇒ 没证据表明「确实发生了重放」（票面验收第 6 条）。"""
    verdict = judge_recovery(_baseline(), _baseline(duplicate_count=100))
    assert not verdict.passed


def test_verdict_states_both_conclusions_in_stdout_lines() -> None:
    """stdout 必须**明示**两条结论（票面验收第 6 条）。"""
    verdict = judge_recovery(_baseline(), _baseline(duplicate_count=140))
    text = "\n".join(verdict.format_lines())

    assert "重复通道计数上升" in text
    assert "DLQ 增量==0" in text or "DLQ 增量 == 0" in text


def test_baseline_rejects_negative_values() -> None:
    import pytest

    with pytest.raises(ValueError):
        Baseline(
            window_metric_count=-1,
            window_metric_sum=0,
            funnel_step_count=0,
            funnel_step_sum=0,
            dlq_offset=0,
            duplicate_count=0,
        )
