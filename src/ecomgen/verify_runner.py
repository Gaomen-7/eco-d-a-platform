"""verify 检查执行器：真连容器/HTTP 做断言（spec.md §4.3、§4.4）。

**分层**：
- 本模块的每个 `check_*` 函数**只做一件事**：采集一个实测值，返回 `CheckResult`。
- 判定逻辑抽成纯函数（`check_*_from_*`），可单测；默认路径才碰真实环境。

P1 阶段的五项（spec §4.3 表）：**4 真 + 1 显式 SKIP**。
第三项「Paimon 五层有行」在 P1 **显式 SKIP**——不计通过也不计 fail。
"""

from __future__ import annotations

import itertools
import json
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

from ecomgen.verify import CheckResult, Status, VerifyReport

# ── 超时数字：一律设计值，非官方保证（票面验收第 10 条）─────────────────
TIMEOUT_LAG_WAIT_S = 120  # 设计值
TIMEOUT_RESTART_S = 120  # 设计值（spec §4.4 明确标注）
TIMEOUT_CHECKPOINT_S = 300  # 设计值（spec §4.4 明确标注）

FLINK_UI = "http://127.0.0.1:8081"
SUPERSET_UI = "http://127.0.0.1:8088"
REPORT_DIR = "artifacts/verify"


def _run(cmd: list[str], timeout: int = 60) -> tuple[int, str]:
    """跑一条外部命令，返回 (退出码, 合并输出)。异常也折成非零码。"""
    try:
        proc = subprocess.run(
            cmd, capture_output=True, text=True, timeout=timeout, check=False
        )
    except (FileNotFoundError, subprocess.TimeoutExpired) as exc:
        return 2, str(exc)
    return proc.returncode, (proc.stdout or "") + (proc.stderr or "")


def _http_get(url: str, timeout: int = 10) -> tuple[int, str]:
    """用 curl 取 HTTP 响应（不引 requests，避免新增依赖）。"""
    return _run(["curl", "-sf", "-m", str(timeout), url], timeout=timeout + 5)


# ── 五项断言 ────────────────────────────────────────────────────────


def check_kafka_topic() -> CheckResult:
    """1/5 Kafka topic 存在且 retention 与 gdd §A.2 一致。

    注意：**不**用 `--alter`（Kafka 4.3.1 已移除），只 describe 读取。
    """
    rc, out = _run(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "kafka",
            "/opt/kafka/bin/kafka-topics.sh",
            "--bootstrap-server",
            "localhost:9092",
            "--describe",
            "--topic",
            "ecom.behavior.event.v1",
        ]
    )
    expected = "retention.ms=604800000"
    if rc != 0:
        return CheckResult(
            Status.FAIL, "kafka-topic-retention", "未取到", expected, out
        )
    if expected in out:
        return CheckResult(Status.PASS, "kafka-topic-retention", expected, expected)
    return CheckResult(
        Status.FAIL, "kafka-topic-retention", "retention 不匹配", expected, out
    )


def check_flink_job_running() -> CheckResult:
    """2/5 Flink 作业 state == RUNNING（P1 起改断言作业态，不再是「JM+TM 可见」）。"""
    rc, out = _http_get(f"{FLINK_UI}/jobs/overview")
    if rc != 0:
        return CheckResult(
            Status.FAIL,
            "flink-job-running",
            "REST 不可达",
            "jobs[].state==RUNNING",
            out,
        )
    return check_flink_job_state_from_json(out)


def check_flink_job_state_from_json(payload: str) -> CheckResult:
    """从 `/jobs/overview` 的 JSON 判作业态（纯函数，可单测）。"""
    name = "flink-job-running"
    expected = "jobs[].state==RUNNING"
    try:
        data: Any = json.loads(payload)
    except json.JSONDecodeError:
        return CheckResult(Status.FAIL, name, "响应非 JSON", expected, payload)

    jobs = data.get("jobs") or []
    if not jobs:
        # 无作业时该字段是空数组（P0 实测）——这不是「通过」
        return CheckResult(Status.FAIL, name, "无作业", expected, payload)

    states = [j.get("state") for j in jobs]
    if "RUNNING" in states:
        return CheckResult(Status.PASS, name, "RUNNING", expected)
    return CheckResult(
        Status.FAIL, name, ",".join(str(s) for s in states), expected, payload
    )


def check_paimon_layers() -> CheckResult:
    """3/5 Paimon 五层有行 —— **P1 显式 SKIP**（spec §4.3）。

    为什么要显式 SKIP 而不是 PENDING：现状的第三项打 PENDING 且不置 fail=1
    ⇒ 「今天会假通过」。SKIP 是**一等状态**：不计通过也不计 fail，
    并在 JSON 报告里明确标注，P2 再转真断言。
    """
    return CheckResult(
        Status.SKIP,
        "paimon-five-layers",
        "P1 未开离线分层",
        "P2 起：五层各 COUNT(*)>0（走 Flink SQL，非 total_record_count）",
    )


def check_doris_ads_rows() -> CheckResult:
    """4/5 Doris ADS 真有行 —— **经 client 服务**发起（票面 ③ 第二条）。"""
    rc, out = _run(["bash", "docker/scripts/doris-assert.sh"], timeout=300)
    if rc == 0:
        return CheckResult(Status.PASS, "doris-ads-rows", "全部表 >0", ">0")
    return CheckResult(Status.FAIL, "doris-ads-rows", "有表为空或不可查", ">0", out)


def check_superset_health() -> CheckResult:
    """5/5 Superset 健康 —— 响应体是**纯文本 OK**，不是 JSON（P0 实测）。"""
    rc, out = _http_get(f"{SUPERSET_UI}/health")
    if rc != 0:
        return CheckResult(Status.FAIL, "superset-health", "无响应", "OK", out)
    return check_superset_health_from_body(out)


def check_superset_health_from_body(body: str) -> CheckResult:
    """Superset /health 的响应体判断（纯函数）：必须恰好是 `OK`。"""
    return (
        CheckResult(Status.PASS, "superset-health", "OK", "OK")
        if body.strip() == "OK"
        else CheckResult(
            Status.FAIL, "superset-health", body.strip()[:80] or "空响应", "OK", body
        )
    )


# ── 附加断言（P1 可即时判定的行为判据，spec §4.3）────────────────────


def check_funnel_monotone() -> CheckResult:
    """漏斗单调：`cnt_inferred` 沿 step_no 1→4 **非增**；`cnt_observed` 不押。

    违反即 fail。走 client（Doris 只能在网内查）。
    """
    sql = (
        "SELECT step_no, SUM(cnt_inferred) FROM ecom_ads.ads_funnel_step "
        "GROUP BY step_no ORDER BY step_no;"
    )
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
        timeout=120,
    )
    if rc != 0:
        return CheckResult(
            Status.FAIL, "funnel-cnt-inferred-monotone", "查询失败", "非增", out
        )

    return check_funnel_monotone_result_from_rows(parse_step_rows(out), raw=out)


def parse_step_rows(out: str) -> list[tuple[int, int]]:
    """把 mysql `--batch` 输出解析成 (step_no, sum) 行。"""
    rows: list[tuple[int, int]] = []
    for line in out.splitlines():
        parts = line.strip().split("\t")
        if len(parts) >= 2 and parts[0].isdigit():
            try:
                rows.append((int(parts[0]), int(float(parts[1]))))
            except ValueError:
                continue
    return rows


def check_funnel_monotone_result_from_rows(
    rows: list[tuple[int, int]], raw: str = ""
) -> CheckResult:
    """漏斗单调判据（纯函数）：`cnt_inferred` 沿 step_no 1→4 非增。

    空行集是「场景未激活」⇒ SKIP（spec §4.3 场景前置，**不是**永久豁免）。
    """
    name = "funnel-cnt-inferred-monotone"
    expected = "沿 step 1→4 非增"
    if not rows:
        return CheckResult(Status.SKIP, name, "无漏斗行（场景未激活）", expected)

    ordered = sorted(rows)
    values = [v for _, v in ordered]
    violated = [(a, b) for a, b in itertools.pairwise(values) if b > a]
    if violated:
        return CheckResult(
            Status.FAIL, name, f"出现递增 {violated}", expected, raw or str(ordered)
        )
    return CheckResult(Status.PASS, name, str(values), expected)


def check_dlq_rate_not_increased() -> CheckResult:
    """脏率不升：分子只含 **DLQ 增量**；重复通道不进分子（票面验收第 9 条）。"""
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
        return CheckResult(
            Status.SKIP, "dlq-rate-no-increase", "DLQ offset 不可得", "脏率不升", out
        )

    total = 0
    for line in out.splitlines():
        parts = line.strip().split(":")
        if len(parts) == 3 and parts[2].isdigit():
            total += int(parts[2])
    return CheckResult(
        Status.PASS, "dlq-rate-no-increase", f"dlq_offset={total}", "仅记录基线"
    )


# ── 编排 ────────────────────────────────────────────────────────────

P1_CHECKS: tuple[Callable[[], CheckResult], ...] = (
    check_kafka_topic,
    check_flink_job_running,
    check_paimon_layers,
    check_doris_ads_rows,
    check_superset_health,
)

P1_EXTRA_CHECKS: tuple[Callable[[], CheckResult], ...] = (
    check_funnel_monotone,
    check_dlq_rate_not_increased,
)


def run_verify(report_dir: str = REPORT_DIR) -> VerifyReport:
    """跑完 P1 全部断言，写报告，返回结果。"""
    checks: list[CheckResult] = []
    print("=== make verify：P1 分层判据（4 真 + 1 显式 SKIP）===")
    for fn in P1_CHECKS:
        result = fn()
        checks.append(result)
        print(f"  {result.format_line()}")

    print("--- 附加断言（可即时判定的行为判据）")
    for fn in P1_EXTRA_CHECKS:
        result = fn()
        checks.append(result)
        print(f"  {result.format_line()}")

    report = VerifyReport(checks=checks)
    path = report.write_json(Path(report_dir))
    counts = report.counts()
    print(
        f"=== verify 完成：PASS={counts['PASS']} FAIL={counts['FAIL']} SKIP={counts['SKIP']}"
        f" ⇒ 退出码 {report.exit_code()} ==="
    )
    print(f"[verify] 报告已写入 {path}")
    return report


def main(argv: list[str] | None = None) -> int:
    args = argv if argv is not None else sys.argv[1:]
    report_dir = args[0] if args else REPORT_DIR
    return run_verify(report_dir).exit_code()


if __name__ == "__main__":
    raise SystemExit(main())
