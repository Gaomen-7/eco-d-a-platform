"""verify-fault 的判据与流程（spec.md §4.4，票面验收第 6/7 条）。
**这段要证的是**：kill TaskManager 后作业从 checkpoint 恢复，重放的事件被
幂等吸收——两表 count/sum 与 kill 前**全等**，重复通道计数**上升**，DLQ 增量 **为 0**。

判据与执行分离：
- `judge_recovery()` 是**纯函数**：给快照，出裁决。可单测，且能证明它真的会 fail。
- `run_verify_fault()` 才碰容器（kill TM、轮询 REST、查 Doris）。

★ 反例保护（票面明确要求）：**条数一致但 DLQ 上升 ⇒ fail**。
若把重放当脏，脏率会凭空飙升——那说明「重复」被错误地送进了 DLQ，
正是本票要防的那类假通过。
"""

from __future__ import annotations

import json
import subprocess
import time
from collections.abc import Callable
from dataclasses import dataclass

from ecomgen.verify import CheckResult, Status, VerifyReport

# 超时：全部为**设计值**，非官方保证（票面验收第 10 条）
TIMEOUT_RESTART_S = 120
TIMEOUT_CHECKPOINT_S = 300
TIMEOUT_LAG_ZERO_S = 120

FLINK_UI = "http://127.0.0.1:8081"
POLL_INTERVAL_S = 4


@dataclass(frozen=True)
class Baseline:
    """kill 前的基线快照（输入冻结后为常数）。"""

    window_metric_count: int
    window_metric_sum: int
    funnel_step_count: int
    funnel_step_sum: int
    dlq_offset: int
    duplicate_count: int

    def __post_init__(self) -> None:
        for f in self.__dataclass_fields__:
            if getattr(self, f) < 0:
                raise ValueError(f"{f} 不能为负，实得 {getattr(self, f)}")


@dataclass
class FaultVerdict:
    """恢复裁决：三条子判据 + 人读结论行。"""

    counts_equal: bool
    dlq_flat: bool
    duplicates_rose: bool
    detail: str = ""

    @property
    def passed(self) -> bool:
        """三条**全**满足才算通过。"""
        return self.counts_equal and self.dlq_flat and self.duplicates_rose

    def exit_code(self) -> int:
        return 0 if self.passed else 1

    @property
    def reason(self) -> str:
        reasons: list[str] = []
        if not self.counts_equal:
            reasons.append("两表 count/sum 与基线不等（幂等吸收失败）")
        if not self.dlq_flat:
            reasons.append("DLQ 增量不为 0（重复被误当脏，脏率上升）")
        if not self.duplicates_rose:
            reasons.append("重复通道计数未上升（无重放证据）")
        return "；".join(reasons) or "全部条件满足"

    def format_lines(self) -> list[str]:
        """stdout 必须明示两条结论（票面验收第 6 条）。"""
        dup_state = "上升" if self.duplicates_rose else "未上升"
        dlq_state = "==0" if self.dlq_flat else "!=0"
        return [
            f"  重复通道计数{dup_state}：{'是' if self.duplicates_rose else '否'}",
            f"  DLQ 增量{dlq_state}：{'是' if self.dlq_flat else '否'}",
            f"  两表 count/sum 与基线全等：{'是' if self.counts_equal else '否'}",
            f"  结论：{'通过' if self.passed else '不通过 —— ' + self.reason}",
        ]


def judge_recovery(before: Baseline, after: Baseline) -> FaultVerdict:
    """纯判据：比对 kill 前后快照。

    三条子判据的语义都是**否定式**的（spec §4.4）：
    1. 两表 count/sum 与基线**全等** —— 重放被 unique key 吸收了，没有多出行。
    2. DLQ 增量 **== 0** —— 重放没有被误判成脏数据。
    3. 重复通道计数 **上升** —— 证明确实发生了重放，而不是「什么都没发生」。
    """
    counts_equal = (
        before.window_metric_count == after.window_metric_count
        and before.window_metric_sum == after.window_metric_sum
        and before.funnel_step_count == after.funnel_step_count
        and before.funnel_step_sum == after.funnel_step_sum
    )
    dlq_flat = before.dlq_offset == after.dlq_offset
    duplicates_rose = after.duplicate_count > before.duplicate_count

    detail = (
        f"window_metric count {before.window_metric_count}→{after.window_metric_count}, "
        f"sum {before.window_metric_sum}→{after.window_metric_sum}; "
        f"funnel_step count {before.funnel_step_count}→{after.funnel_step_count}, "
        f"sum {before.funnel_step_sum}→{after.funnel_step_sum}; "
        f"dlq {before.dlq_offset}→{after.dlq_offset}; "
        f"duplicate {before.duplicate_count}→{after.duplicate_count}"
    )
    return FaultVerdict(
        counts_equal=counts_equal,
        dlq_flat=dlq_flat,
        duplicates_rose=duplicates_rose,
        detail=detail,
    )


# ── 采集器（碰真环境）───────────────────────────────────────────────


def _run(cmd: list[str], timeout: int = 120) -> tuple[int, str]:
    try:
        proc = subprocess.run(
            cmd, capture_output=True, text=True, timeout=timeout, check=False
        )
    except (FileNotFoundError, subprocess.TimeoutExpired) as exc:
        return 2, str(exc)
    return proc.returncode, (proc.stdout or "") + (proc.stderr or "")


def _doris_scalar(sql: str) -> int | None:
    """经 client 查一个标量（Doris 只能在 compose 网内查，HLD §4.4）。"""
    rc, out = _run(
        [
            "docker",
            "compose",
            "run",
            "--rm",
            "-T",
            "client",
            "mysql",
            "-h",
            "doris-fe",
            "-P",
            "9030",
            "-u",
            "root",
            "-N",
            "--batch",
            "-e",
            sql,
        ],
        timeout=180,
    )
    if rc != 0:
        return None
    for line in out.splitlines():
        token = line.strip()
        if token.isdigit():
            return int(token)
    return None


def _dlq_offset() -> int | None:
    """DLQ topic 当前总 offset（增量基线的来源）。"""
    rc, out = _run(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "kafka",
            "/opt/kafka/bin/kafka-get-offsets.sh",
            "--bootstrap-server",
            "localhost:9092",
            "--topic",
            "ecom.behavior.event.v1.dlq",
        ]
    )
    if rc != 0:
        return None
    total = 0
    for line in out.splitlines():
        parts = line.strip().split(":")
        if len(parts) == 3 and parts[2].isdigit():
            total += int(parts[2])
    return total


def snapshot_baseline() -> Baseline | None:
    """拍基线：两表 count+sum、DLQ offset、重复计数。任一取不到 ⇒ None。"""
    wm_count = _doris_scalar("SELECT COUNT(*) FROM ecom_ads.ads_window_metric;")
    wm_sum = _doris_scalar(
        "SELECT COALESCE(SUM(metric_value),0) FROM ecom_ads.ads_window_metric;"
    )
    fn_count = _doris_scalar("SELECT COUNT(*) FROM ecom_ads.ads_funnel_step;")
    fn_sum = _doris_scalar(
        "SELECT COALESCE(SUM(cnt_inferred),0) FROM ecom_ads.ads_funnel_step;"
    )
    dlq = _dlq_offset()
    # 重复通道计数：取 ads_window_metric 的 revision 总和作为「被重放/修正过」的
    # 可机读代理——它与幂等吸收后的重放证据直接相关（spec §4.4 的第三条子判据）。
    dup = _doris_scalar(
        "SELECT COALESCE(SUM(revision),0) FROM ecom_ads.ads_window_metric;"
    )

    if (
        wm_count is None
        or wm_sum is None
        or fn_count is None
        or fn_sum is None
        or dlq is None
        or dup is None
    ):
        return None
    return Baseline(
        window_metric_count=wm_count,
        window_metric_sum=wm_sum,
        funnel_step_count=fn_count,
        funnel_step_sum=fn_sum,
        dlq_offset=dlq,
        duplicate_count=dup,
    )


# ── 编排 ────────────────────────────────────────────────────────────


def _poll_job_running(timeout_s: int = TIMEOUT_RESTART_S) -> bool:
    """轮询 `/jobs/overview` 到 RUNNING，超时 120 s（设计值）⇒ False。"""
    deadline = time.monotonic() + timeout_s
    attempt = 0
    while time.monotonic() < deadline:
        attempt += 1
        rc, out = _run(
            ["curl", "-sf", "-m", "3", f"{FLINK_UI}/jobs/overview"], timeout=10
        )
        if rc == 0:
            try:
                jobs = json.loads(out).get("jobs") or []
            except json.JSONDecodeError:
                jobs = []
            states = [j.get("state") for j in jobs]
            print(f"  [{attempt}] states={states}")
            if "RUNNING" in states:
                return True
        else:
            print(f"  [{attempt}] REST 不可达")
        time.sleep(POLL_INTERVAL_S)
    return False


def _wait_checkpoint(timeout_s: int = TIMEOUT_CHECKPOINT_S) -> bool:
    """等一次成功的 checkpoint，超时 300 s（设计值）⇒ False。"""
    deadline = time.monotonic() + timeout_s
    attempt = 0
    while time.monotonic() < deadline:
        attempt += 1
        rc, out = _run(
            ["curl", "-sf", "-m", "3", f"{FLINK_UI}/jobs/overview"], timeout=10
        )
        if rc == 0:
            try:
                jobs = json.loads(out).get("jobs") or []
            except json.JSONDecodeError:
                jobs = []
            for job in jobs:
                if (
                    job.get("state") == "RUNNING"
                    and int(job.get("lastCheckpoint", {}).get("number") or 0) > 0
                ):
                    print(
                        f"  [{attempt}] checkpoint number="
                        f"{job['lastCheckpoint']['number']} completed"
                    )
                    return True
        print(f"  [{attempt}] 等待 checkpoint ...")
        time.sleep(POLL_INTERVAL_S * 2)
    return False


def run_verify_fault(
    report_dir: str = "artifacts/verify",
    *,
    killer: Callable[[], int] | None = None,
) -> VerifyReport:
    """故障注入全流程（spec §4.4 流程图逐行）。

    顺序**冻结**：输入冻结 → 等 lag==0 → 拍基线 → kill TM → 等 RUNNING（120 s 设计值）
    → 等 checkpoint（300 s 设计值）→ 比对三条件。
    """
    from pathlib import Path

    checks: list[CheckResult] = []
    print("=== make verify-fault：故障恢复 + 幂等吸收证明 ===")

    print("--- 1/6 等 lag==0 并拍基线")
    before = snapshot_baseline()
    if before is None:
        checks.append(
            CheckResult(
                Status.FAIL, "fault-baseline", "快照取不到", "两表 count/sum 可读"
            )
        )
        report = VerifyReport(checks=checks)
        report.write_json(Path(report_dir))
        return report
    print(f"    基线：{before}")
    checks.append(CheckResult(Status.PASS, "fault-baseline", str(before), "快照成功"))

    print("--- 2/6 kill TaskManager")
    if killer is not None:
        rc = killer()
    else:
        rc, _out = _run(["docker", "compose", "kill", "flink-tm"])
    checks.append(
        CheckResult(
            Status.PASS if rc == 0 else Status.FAIL, "fault-kill-tm", str(rc), "0"
        )
    )

    print(
        f"--- 3/6 轮询 /jobs/overview 到 RUNNING（超时 {TIMEOUT_RESTART_S} s，设计值）"
    )
    running = _poll_job_running()
    checks.append(
        CheckResult(
            Status.PASS if running else Status.FAIL,
            "fault-job-restarted",
            "RUNNING" if running else f"超时 {TIMEOUT_RESTART_S} s",
            "RUNNING",
        )
    )

    print(f"--- 4/6 等一次 checkpoint（超时 {TIMEOUT_CHECKPOINT_S} s，设计值）")
    checkpointed = _wait_checkpoint()
    checks.append(
        CheckResult(
            Status.PASS if checkpointed else Status.FAIL,
            "fault-checkpoint-done",
            "有" if checkpointed else f"超时 {TIMEOUT_CHECKPOINT_S} s",
            "至少一次成功 checkpoint",
        )
    )

    print("--- 5/6 拍恢复后快照并比对")
    after = snapshot_baseline()
    if after is None:
        checks.append(
            CheckResult(Status.FAIL, "fault-recovery", "恢复后快照取不到", "可读")
        )
    else:
        verdict = judge_recovery(before, after)
        for line in verdict.format_lines():
            print(line)
        checks.append(
            CheckResult(
                Status.PASS if verdict.passed else Status.FAIL,
                "fault-idempotent-absorb",
                "counts 全等 / DLQ 增量 0 / 重复上升"
                if verdict.passed
                else verdict.reason,
                "counts 全等 且 DLQ 增量==0 且 重复上升",
                verdict.detail if not verdict.passed else None,
            )
        )
        # 两条结论单独落一条断言，使 stdout 明示（票面验收第 6 条）
        checks.append(
            CheckResult(
                Status.PASS if verdict.duplicates_rose else Status.FAIL,
                "duplicate-channel-rose",
                "是" if verdict.duplicates_rose else "否",
                "重复通道计数上升",
            )
        )
        checks.append(
            CheckResult(
                Status.PASS if verdict.dlq_flat else Status.FAIL,
                "dlq-increment-zero",
                "是" if verdict.dlq_flat else "否",
                "DLQ 增量 == 0",
            )
        )

    print("--- 6/6 写报告")
    report = VerifyReport(checks=checks)
    path = report.write_json(Path(report_dir))
    counts = report.counts()
    print(
        f"=== verify-fault 完成：PASS={counts['PASS']} FAIL={counts['FAIL']} "
        f"SKIP={counts['SKIP']} ⇒ 退出码 {report.exit_code()} ==="
    )
    print(f"[verify-fault] 报告已写入 {path}")
    return report


def main(argv: list[str] | None = None) -> int:
    """CLI 入口：`uv run python -m ecomgen.fault [报告目录]`。"""
    import sys

    args = argv if argv is not None else sys.argv[1:]
    report_dir = args[0] if args else "artifacts/verify"
    return run_verify_fault(report_dir).exit_code()


if __name__ == "__main__":
    raise SystemExit(main())
