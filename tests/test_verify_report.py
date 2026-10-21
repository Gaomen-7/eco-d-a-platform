"""verify 分层判据的测试（TDD 第八片：判据才是本票的交付物）。

判据来源：spec.md §4.3（P1 = 4 真 + 1 显式 SKIP）、§4.5（fail 三层痕迹）、
票面验收第 5/8 条。接缝 = `ecomgen.verify` 的**结果模型**：

- 每条断言产出 `CheckResult(status, name, actual, expected, detail)`
- `status` ∈ PASS / FAIL / SKIP，**SKIP 不计通过也不计 fail**（这是结构性堵死假通过的关键）
- 汇总产出可写盘的 JSON 报告

这里测的是**判据语义**（什么算通过/失败/跳过、退出码怎么算），不是 Docker 调用。
真正碰容器的部分在 `verify.run_verify()` 里，单独用集成测覆盖。
"""

from __future__ import annotations

import json
from pathlib import Path

from ecomgen.verify import CheckResult, Status, VerifyReport


def test_pass_result_is_not_a_failure() -> None:
    r = CheckResult(status=Status.PASS, name="kafka", actual="1", expected=">0")
    assert r.status is Status.PASS
    assert not r.is_failure


def test_fail_result_is_a_failure() -> None:
    r = CheckResult(status=Status.FAIL, name="kafka", actual="0", expected=">0")
    assert r.is_failure


def test_skip_is_neither_pass_nor_fail() -> None:
    """★ 核心：SKIP 不计通过也不计 fail（否则第三项会假通过或假失败）。"""
    r = CheckResult(
        status=Status.SKIP, name="paimon", actual="n/a", expected="5 layers"
    )
    assert not r.is_failure
    assert not r.is_pass


def test_report_exit_code_is_zero_with_four_pass_and_one_skip() -> None:
    """P1 形态：4 真 + 1 SKIP ⇒ 退出码 0（票面验收第 5 条）。"""
    report = VerifyReport(
        checks=[
            CheckResult(Status.PASS, "kafka", "ok", "ok"),
            CheckResult(Status.PASS, "flink-running", "RUNNING", "RUNNING"),
            CheckResult(Status.SKIP, "paimon-five-layers", "n/a", "5 层有行"),
            CheckResult(Status.PASS, "doris-ads-rows", "12", ">0"),
            CheckResult(Status.PASS, "superset-health", "OK", "OK"),
        ]
    )
    assert report.exit_code() == 0
    assert report.counts() == {"PASS": 4, "FAIL": 0, "SKIP": 1}


def test_report_exit_code_is_nonzero_with_any_fail() -> None:
    report = VerifyReport(
        checks=[
            CheckResult(Status.PASS, "a", "1", "1"),
            CheckResult(Status.FAIL, "b", "0", ">0"),
            CheckResult(Status.SKIP, "c", "n/a", "x"),
        ]
    )
    assert report.exit_code() == 1


def test_report_prints_one_line_per_check() -> None:
    """stdout 人读面：每行 `PASS/FAIL/SKIP <名> <实测> vs <期望>`（spec §4.5 第 1 层）。"""
    report = VerifyReport(
        checks=[
            CheckResult(Status.PASS, "kafka-retention", "604800000", "604800000"),
            CheckResult(Status.SKIP, "paimon", "n/a", "5 层有行"),
        ]
    )
    lines = report.format_lines()
    assert len(lines) == 2
    assert lines[0].startswith("PASS kafka-retention 604800000 vs 604800000")
    assert lines[1].startswith("SKIP paimon n/a vs 5 层有行")


def test_report_writes_json_with_timestamped_name(tmp_path: Path) -> None:
    """JSON 报告面：`artifacts/verify/verify-report-<UTC>.json`（spec §4.5 第 2 层）。"""
    report = VerifyReport(checks=[CheckResult(Status.PASS, "x", "1", "1")])
    path = report.write_json(tmp_path / "verify")

    assert path.name.startswith("verify-report-")
    assert path.name.endswith(".json")
    data = json.loads(path.read_text(encoding="utf-8"))
    assert data["checks"][0]["status"] == "PASS"
    assert "generated_at" in data


def test_fail_snapshot_is_written_next_to_report(tmp_path: Path) -> None:
    """FAIL 原始响应快照（spec §4.5 第 3 层）。"""
    report = VerifyReport(
        checks=[CheckResult(Status.FAIL, "doris", "0", ">0", detail="raw stderr here")]
    )
    path = report.write_json(tmp_path / "verify")
    snapshot = path.with_suffix(".snapshot.txt")

    assert snapshot.exists()
    assert "raw stderr here" in snapshot.read_text(encoding="utf-8")


def test_no_snapshot_when_everything_passes(tmp_path: Path) -> None:
    report = VerifyReport(checks=[CheckResult(Status.PASS, "x", "1", "1")])
    path = report.write_json(tmp_path / "verify")
    assert not path.with_suffix(".snapshot.txt").exists()
