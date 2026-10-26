"""`make spike` 的判据面：≥1000 events/s 的一手速率证据（impl-06g）。

**为什么独立成模块而不是塞进 Makefile echo**：SRS `REQ-F-GEN-04`（SHOULD）
与 `REQ-NF-PERF-03`（MUST）的尖峰判据此前**从未被实测**——`make spike` 只跑
`--dry-run`，速率判据没有一手证据。票面 AC 第 6 条要求「判据进码：新增一条
可机读断言，使『速率 ≥1000/s』不是只活在本票文字里」。本模块就是那条机读
断言的落点，与 `verify_runner` 同一判据纪律（分层：纯函数判语义、默认路径
才碰真实环境）。

**一手口径**（票面 AC 第 2 条，实现面在 `cli.SendMeasurement`）：
分子 = 实际 `produce()` 成功条数（不是 plan 的计划条数——「请求条数 ≠
实际产出条数」是已实测的反例教训）；分母 = 首条 produce 至 flush 完成的
单调时钟秒数（含 flush：未送达 broker 的投递不算「发出」）。

**编排**（`run_spike`）：
前置 RUNNING 检查 → 基线采样（DLQ / 输入 topic 总 offset、exception 条数、
消费组 lag）→ 子进程跑 `ecomgen live` 真发（期间后台线程按秒采样 Flink
作业态与异常数——「尖峰**期间**不崩」是期间证据，不是端点差分）→ 等消费组
lag 回落 → 终态采样 → DLQ 增量归因 → 判据列表 → 报告落 `artifacts/spike/`。

**与 `dlq-rate-no-increase` 的耦合**（票面一手事实）：尖峰若产生真脏会让
后续 `make verify` 变红。本模块把 DLQ 增量的**归因证据**（新增消息的
reason 分布）写进报告——归因纪律同 impl-03f：无归因证据不得重立基线。
"""

from __future__ import annotations

import json
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ecomgen.cli import SendMeasurement
from ecomgen.verify import CheckResult, Status

# ── 判据常数 ──────────────────────────────────────────────────────────
# SRS REQ-NF-PERF-03 / REQ-F-GEN-04 的尖峰下限。**冻结判据**：实测达不到时
# 按票面 Notes 拆副票归因，不得调低这个数字。
RATE_TARGET_EVENTS_S = 1000

# ── 尖峰参数（与旧 Makefile spike 目标同参，内容与 seed 绑定可复现）────
SPIKE_SEED = 1
SPIKE_SESSIONS = 5000
SPIKE_RATE = 1000

# 宿主可达的 Kafka HOST listener：docker-compose.yml 的双 listener 面
# （HOST=127.0.0.1:29092，注释逐字「宿主 ecomgen live / Makefile spike 用」）。
# ⚠️ 不是 127.0.0.1:9092——那个口发布的 advertised 名是 kafka:9092，
#    宿主解析不了（P0 实测的 FAIL，见 compose 内注释）。
KAFKA_BOOTSTRAP = "127.0.0.1:29092"

# ── 超时数字：一律设计值，非官方保证（与 verify_runner 同纪律）─────────
# 尖峰后 lag 回落窗口：消费端按 checkpoint 提交 offset（30 s 周期），
# 兜 3 个周期 + 消化时间。
LAG_SETTLE_TIMEOUT_S = 180  # 设计值
LAG_POLL_INTERVAL_S = 5  # 设计值
# 期间监控线程的采样间隔（设计值）：1 s 足以捕捉 11 s 尖峰里的状态翻转。
MONITOR_INTERVAL_S = 1.0  # 设计值
# live 子进程的超时：5000 会话 ≈ 11.1 k 条 @1000/s ≈ 12 s，批生成 + 探测
# 余量给 300 s（设计值；真实超时只会更长不会更短地暴露瓶颈）。
LIVE_TIMEOUT_S = 300  # 设计值
DLQ_SAMPLE_TIMEOUT_S = 60  # 设计值

FLINK_UI = "http://127.0.0.1:8081"
KAFKA_TOPIC = "ecom.behavior.event.v1"
DLQ_TOPIC = "ecom.behavior.event.v1.dlq"
CONSUMER_GROUP = "ecom-p1-consumer"
REPORT_DIR = "artifacts/spike"
LIVE_PLAN_JSON = "live-plan.json"


def _run(cmd: list[str], timeout: int = 60) -> tuple[int, str]:
    """跑一条外部命令，返回 (退出码, 合并输出)。异常折成非零码（与 verify_runner 同式）。"""
    try:
        proc = subprocess.run(
            cmd, capture_output=True, text=True, timeout=timeout, check=False
        )
    except (FileNotFoundError, subprocess.TimeoutExpired) as exc:
        return 2, str(exc)
    return proc.returncode, (proc.stdout or "") + (proc.stderr or "")


def _http_get(url: str, timeout: int = 10) -> tuple[int, str]:
    return _run(["curl", "-sf", "-m", str(timeout), url], timeout=timeout + 5)


# ── 采集面（解析全部抽成纯函数，供单测）────────────────────────────────


def parse_running_state(payload: str) -> str | None:
    """从 `/jobs/overview` JSON 取 RUNNING 作业的 state；无 RUNNING ⇒ None。"""
    try:
        jobs = json.loads(payload).get("jobs") or []
    except json.JSONDecodeError:
        return None
    for job in jobs:
        if job.get("state") == "RUNNING":
            return "RUNNING"
    return None


def parse_exception_count(payload: str) -> int | None:
    """从 `/jobs/<jid>/exceptions` JSON 取异常条目数。

    Flink 2.x 形状（实机确认）：`{"exceptionHistory":{"entries":[...],"truncated":...}}`。
    解析不出 ⇒ None（**不**当 0——「取不到」与「没有异常」是两回事）。
    """
    try:
        data = json.loads(payload)
    except json.JSONDecodeError:
        return None
    history = data.get("exceptionHistory")
    if not isinstance(history, dict):
        return None
    entries = history.get("entries")
    if not isinstance(entries, list):
        return None
    return len(entries)


def parse_consumer_lag(out: str) -> int | None:
    """消费组各分区 LAG 之和（分区行第 6 列；与 fault.py `_consumer_lag` 同规则）。

    「组无活跃成员」「表头」行不满足列形状自然排除；一行都没匹配 ⇒ None
    （不当 lag==0——「取不到」不是「无积压」）。
    """
    total = 0
    seen_any = False
    for line in out.splitlines():
        parts = line.split()
        if len(parts) < 6:
            continue
        lag = parts[5]
        if lag.lstrip("-").isdigit():
            total += int(lag)
            seen_any = True
    return total if seen_any else None


def parse_topic_total_offset(out: str) -> int | None:
    """`kafka-get-offsets.sh` 输出的各分区总 offset 之和（与 verify_runner 同式）。"""
    total = 0
    seen_any = False
    for line in out.splitlines():
        parts = line.strip().split(":")
        if len(parts) == 3 and parts[2].isdigit():
            total += int(parts[2])
            seen_any = True
    return total if seen_any else None


def parse_dlq_reasons(out: str) -> dict[str, int]:
    """从 DLQ 消息样本统计 reason 分布（归因证据）。

    每行一条 DLQ JSON（`RawEvent.toDlqJson`：`{"reason":...,"event_id":...,...}`）。
    非行（提示/表头）解析失败自然跳过。
    """
    counts: dict[str, int] = {}
    for line in out.splitlines():
        token = line.strip()
        if not token.startswith("{"):
            continue
        try:
            data = json.loads(token)
        except json.JSONDecodeError:
            continue
        reason = data.get("reason")
        if isinstance(reason, str):
            counts[reason] = counts.get(reason, 0) + 1
    return counts


def _kafka_script(script: str, *extra: str) -> tuple[int, str]:
    return _run(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "kafka",
            f"/opt/kafka/bin/{script}",
            "--bootstrap-server",
            "localhost:9092",
            *extra,
        ],
        timeout=90,
    )


def _topic_total_offset(topic: str) -> int | None:
    rc, out = _kafka_script("kafka-get-offsets.sh", "--topic", topic)
    if rc != 0:
        return None
    return parse_topic_total_offset(out)


def _consumer_lag() -> int | None:
    rc, out = _kafka_script(
        "kafka-consumer-groups.sh", "--describe", "--group", CONSUMER_GROUP
    )
    if rc != 0:
        return None
    return parse_consumer_lag(out)


def _running_jid() -> str | None:
    rc, out = _http_get(f"{FLINK_UI}/jobs/overview")
    if rc != 0:
        return None
    try:
        jobs = json.loads(out).get("jobs") or []
    except json.JSONDecodeError:
        return None
    return next(
        (j.get("jid") for j in jobs if j.get("state") == "RUNNING" and j.get("jid")),
        None,
    )


def _exception_count(jid: str) -> int | None:
    rc, out = _http_get(f"{FLINK_UI}/jobs/{jid}/exceptions")
    if rc != 0:
        return None
    return parse_exception_count(out)


def _dlq_reason_counts(from_partition_offsets: dict[int, int]) -> dict[str, int]:
    """按「逐分区起点」抓 DLQ 新增消息并统计 reason 分布（归因证据面）。

    `kafka-console-consumer.sh --partition P --offset K --max-messages N`
    只读该分区自 K 起的消息。⚠️ 退出码**不可当失败判据**：读满 max-messages
    前流尽会以超时退出，stdout 里已有的消息仍是有效样本——故无论 rc 几，
    都对 stdout 做 reason 统计。
    """
    merged: dict[str, int] = {}
    for partition, start in sorted(from_partition_offsets.items()):
        want = 64  # 设计值：归因样本上限，够分辨 reason 家族即可
        _rc, out = _run(
            [
                "docker",
                "compose",
                "exec",
                "-T",
                "kafka",
                "/opt/kafka/bin/kafka-console-consumer.sh",
                "--bootstrap-server",
                "localhost:9092",
                "--topic",
                DLQ_TOPIC,
                "--partition",
                str(partition),
                "--offset",
                str(start),
                "--max-messages",
                str(want),
                "--timeout-ms",
                "5000",
            ],
            timeout=DLQ_SAMPLE_TIMEOUT_S,
        )
        for reason, count in parse_dlq_reasons(out).items():
            merged[reason] = merged.get(reason, 0) + count
    return merged


# ── 判据（纯函数，可单测）─────────────────────────────────────────────


def rate_result(rate_per_sec: float | None, events: int, wall_ms: int) -> CheckResult:
    """判据 1：实测速率 ≥ 1000 events/s（SRS REQ-F-GEN-04 / REQ-NF-PERF-03）。

    - 速率无定义（空批次 / 测量缺失）⇒ **FAIL**——没有一手证据就是没证据，
      不是 SKIP：尖峰判据的前提是「真发过」。
    - 阈值是 SRS 冻结判据；达不到 ⇒ FAIL + 实测值进 `actual`（按票面 Notes
      拆副票归因，**不得**调低阈值）。
    """
    name = "spike-rate"
    expected = f"实测速率 ≥ {RATE_TARGET_EVENTS_S} events/s（分子=实际发出条数，分母=含 flush 的发送墙钟）"
    if rate_per_sec is None:
        return CheckResult(
            Status.FAIL,
            name,
            f"速率无定义（events={events}, wall_ms={wall_ms}）",
            expected,
        )
    actual = f"{rate_per_sec:.1f} events/s（{events} 条 / {wall_ms} ms，含 flush）"
    if rate_per_sec < RATE_TARGET_EVENTS_S:
        return CheckResult(Status.FAIL, name, actual, expected, actual)
    return CheckResult(Status.PASS, name, actual, expected)


def liveness_result(during: dict[str, Any]) -> CheckResult:
    """判据 2：尖峰期间链路不崩（作业保持 RUNNING、异常条目零新增）。

    `during` 是期间监控线程的采样汇总：`samples`（采样数）、
    `non_running_samples`（非 RUNNING 采样数）、`new_exceptions`
    （期间新增的异常条目数）。任一采集面缺失 ⇒ 该维度按最坏情形判
    （不静默当 0——「取不到」不是「没崩」）。
    """
    name = "spike-liveness"
    expected = "期间采样全 RUNNING 且异常条目零新增"
    samples = during.get("samples")
    non_running = during.get("non_running_samples")
    new_exc = during.get("new_exceptions")
    if not isinstance(samples, int) or not isinstance(non_running, int):
        return CheckResult(
            Status.FAIL, name, "期间监控数据缺失（未采样到任何状态）", expected
        )
    if not isinstance(new_exc, int):
        return CheckResult(
            Status.FAIL, name, "异常计数缺失（取不到 ≠ 零新增）", expected
        )
    if samples <= 0:
        return CheckResult(
            Status.FAIL, name, f"采样数 {samples} ≤ 0（期间监控未生效）", expected
        )
    actual = f"samples={samples} non_running={non_running} new_exceptions={new_exc}"
    if non_running > 0 or new_exc > 0:
        return CheckResult(Status.FAIL, name, actual, expected, actual)
    return CheckResult(Status.PASS, name, actual, expected)


def lag_settle_result(lag: int | None, waited_s: float) -> CheckResult:
    """判据 3：消费组 lag 在尖峰后回落到 0。

    lag 取不到 ⇒ FAIL（不是 SKIP：消费组与 topic 都在判据前提里）。
    超时未回落 ⇒ FAIL，留最后读数供归因（背压/重启重放）。
    """
    name = "spike-lag-settle"
    expected = f"尖峰后 lag 回落 0（等待上限 {LAG_SETTLE_TIMEOUT_S} s，设计值）"
    if lag is None:
        return CheckResult(Status.FAIL, name, "lag 取不到", expected)
    actual = f"lag={lag}（等待 {waited_s:.0f} s 后）"
    if lag != 0:
        return CheckResult(Status.FAIL, name, actual, expected, actual)
    return CheckResult(Status.PASS, name, actual, expected)


def dlq_delta_result(delta: int, reason_counts: dict[str, int]) -> CheckResult:
    """判据 4：尖峰对 DLQ 的增量被归因（预期 0——窗尾对齐下无越界事件）。

    - delta == 0 ⇒ PASS（无真脏：窗尾对齐使 event_time ≤ now，质量门上界
      `now + 5 min` 不可能触发——impl-06d 的语义保证，这里验它的实机效果）。
    - delta > 0 ⇒ **FAIL**：按 impl-03f 归因纪律，无「逐条归因证据链」不得
      重立脏率基线；`actual` 携带 reason 分布（归因证据面），恢复办法 =
      按 reason 家族归因后重立基线再 `make verify`。
    - delta < 0（offset 收缩，理论不可能）按数据异常 FAIL。
    """
    name = "spike-dlq-delta-attributed"
    expected = "DLQ 增量 = 0（窗尾对齐下无 time_out_of_range 真脏）或逐条归因"
    if delta == 0:
        return CheckResult(Status.PASS, name, "dlq 增量 0（无真脏）", expected)
    reasons = (
        ", ".join(f"{k}×{v}" for k, v in sorted(reason_counts.items()))
        if reason_counts
        else "（reason 样本未取到——需人工抓 DLQ 消息归因）"
    )
    actual = f"dlq 增量 {delta}（reason 分布：{reasons}）"
    return CheckResult(Status.FAIL, name, actual, expected, actual)


def spike_checks_from_facts(facts: SpikeFacts) -> list[CheckResult]:
    """把一次尖峰的采集事实折成判据列表（纯函数）。"""
    rate = None if facts.measurement is None else facts.measurement.rate_per_sec
    events = -1 if facts.measurement is None else facts.measurement.events
    wall_ms = -1 if facts.measurement is None else facts.measurement.wall_ms
    return [
        rate_result(rate, events, wall_ms),
        liveness_result(facts.during),
        lag_settle_result(facts.lag_after_settle, facts.lag_wait_s),
        dlq_delta_result(facts.dlq_delta, facts.dlq_reasons),
    ]


# ── 事实模型 ──────────────────────────────────────────────────────────


@dataclass(frozen=True)
class SpikeFacts:
    """一次尖峰跑的全部采集事实（判定面只做纯函数）。"""

    measurement: SendMeasurement | None
    during: dict[str, Any]
    lag_after_settle: int | None
    lag_wait_s: float
    dlq_delta: int
    dlq_reasons: dict[str, int]
    input_delta: int


# ── 期间监控（线程）：尖峰**期间**的证据，不是端点差分 ──────────────────


@dataclass
class _DuringMonitor:
    """按秒采样 Flink 作业态与异常条目数；`summary()` 折成判据输入。"""

    jid: str
    stop_event: threading.Event
    samples: int = 0
    non_running: int = 0
    exc_start: int | None = None
    exc_last: int | None = None
    errors: list[str] = field(default_factory=list)

    def run(self) -> None:
        while not self.stop_event.is_set():
            rc, out = _http_get(f"{FLINK_UI}/jobs/overview", timeout=5)
            if rc == 0:
                state = parse_running_state(out)
                if state == "RUNNING":
                    self.samples += 1
                else:
                    # 无 RUNNING 作业（挂了）或解析失败——取不到 ≠ RUNNING，
                    # 保守计入 non_running（判「期间崩没崩」要的就是最坏情形）。
                    self.non_running += 1
            else:
                self.errors.append(f"jobs/overview 不可达 rc={rc}")
            rc, out = _http_get(f"{FLINK_UI}/jobs/{self.jid}/exceptions", timeout=5)
            if rc == 0:
                count = parse_exception_count(out)
                if count is not None:
                    if self.exc_start is None:
                        self.exc_start = count
                    self.exc_last = count
            else:
                self.errors.append(f"exceptions 不可达 rc={rc}")
            self.stop_event.wait(MONITOR_INTERVAL_S)

    def summary(self) -> dict[str, Any]:
        """折成 `liveness_result` 的输入；采集缺失的维度按最坏情形填。"""
        new_exceptions = None
        if self.exc_start is not None and self.exc_last is not None:
            new_exceptions = max(self.exc_last - self.exc_start, 0)
        return {
            "samples": self.samples,
            "non_running_samples": self.non_running,
            "new_exceptions": new_exceptions,
            "probe_errors": len(self.errors),
        }


# ── 报告 ──────────────────────────────────────────────────────────────


def write_spike_report(
    facts: SpikeFacts,
    checks: list[CheckResult],
    live_plan: dict[str, Any] | None,
    report_dir: str = REPORT_DIR,
) -> Path:
    """写尖峰报告 JSON（一层：stdout 判据行；二层：机读报告）。"""
    directory = Path(report_dir)
    directory.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    path = directory / f"spike-report-{stamp}.json"
    payload: dict[str, Any] = {
        "generated_at": datetime.now(UTC).isoformat(),
        "rate_target": RATE_TARGET_EVENTS_S,
        "params": {
            "seed": SPIKE_SEED,
            "sessions": SPIKE_SESSIONS,
            "rate": SPIKE_RATE,
            "bootstrap": KAFKA_BOOTSTRAP,
        },
        "live_plan": live_plan,
        "facts": {
            "during": facts.during,
            "lag_after_settle": facts.lag_after_settle,
            "lag_wait_s": round(facts.lag_wait_s, 1),
            "dlq_delta": facts.dlq_delta,
            "dlq_reasons": facts.dlq_reasons,
            "input_delta": facts.input_delta,
        },
        "checks": [
            {
                "status": c.status.value,
                "name": c.name,
                "actual": c.actual,
                "expected": c.expected,
            }
            for c in checks
        ],
        "exit_code": 1 if any(c.is_failure for c in checks) else 0,
    }
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    return path


# ── 编排 ──────────────────────────────────────────────────────────────


def _wait_lag_zero(timeout_s: int = LAG_SETTLE_TIMEOUT_S) -> tuple[int | None, float]:
    """轮询消费组 lag 到 0。返回 (最后读数, 等了多久)；读数 None=取不到。"""
    deadline = time.monotonic() + timeout_s
    start = time.monotonic()
    lag: int | None = None
    while time.monotonic() < deadline:
        lag = _consumer_lag()
        if lag == 0:
            return 0, time.monotonic() - start
        time.sleep(LAG_POLL_INTERVAL_S)
    return lag, time.monotonic() - start


def run_spike(report_dir: str = REPORT_DIR) -> int:
    """跑一轮完整尖峰：发送 → 期间监控 → 回落 → 归因 → 判据 → 报告。"""
    print("=== make spike：尖峰模式真发（判据 ≥1000 events/s，SRS REQ-F-GEN-04）===")

    jid = _running_jid()
    if jid is None:
        print(
            "[spike] ✗ 无 RUNNING 的 Flink 作业——尖峰判据的前提是链路在跑"
            "（先 make init / make up），本目标不代拉栈",
            file=sys.stderr,
        )
        return 2

    # ── 基线采样（票面一手事实：跑之前记录 DLQ 基线偏移）──────────────
    dlq_before = _topic_total_offset(DLQ_TOPIC)
    input_before = _topic_total_offset(KAFKA_TOPIC)
    dlq_partition_before = _dlq_partition_offsets()
    print(
        f"[spike] 基线：input={input_before} dlq={dlq_before} "
        f"lag={_consumer_lag()} exceptions={_exception_count(jid)}"
    )

    # ── 期间监控线程（1 s 采样；线程与 live 子进程并行）────────────────
    monitor = _DuringMonitor(jid=jid, stop_event=threading.Event())
    thread = threading.Thread(target=monitor.run, daemon=True)
    thread.start()

    # ── 真发：子进程跑 ecomgen live（判据测的就是这个用户面本体）───────
    plan_path = Path(report_dir) / LIVE_PLAN_JSON
    live_cmd = [
        sys.executable,
        "-m",
        "ecomgen.cli",
        "live",
        "--seed",
        str(SPIKE_SEED),
        "--sessions",
        str(SPIKE_SESSIONS),
        "--rate",
        str(SPIKE_RATE),
        "--bootstrap",
        KAFKA_BOOTSTRAP,
        "--plan",
        str(plan_path),
    ]
    print(f"[spike] 真发：{' '.join(live_cmd[4:])}")
    live_rc = 0
    live_out = ""
    try:
        proc = subprocess.run(
            live_cmd,
            capture_output=True,
            text=True,
            timeout=LIVE_TIMEOUT_S,
            check=False,
        )
        live_rc, live_out = proc.returncode, (proc.stdout or "") + (proc.stderr or "")
    except subprocess.TimeoutExpired as exc:
        live_rc, live_out = 2, f"live 超时（>{LIVE_TIMEOUT_S} s）：{exc}"
    print(live_out.strip())
    monitor.stop_event.set()
    thread.join(timeout=5)

    if live_rc != 0:
        print(f"[spike] ✗ ecomgen live 退出码 {live_rc}——发送面失败", file=sys.stderr)
        return 2

    # ── 读回 plan 的一手测量（cli.SendMeasurement 写的 measured_send）──
    live_plan: dict[str, Any] | None = None
    measurement: Any | None = None
    try:
        live_plan = json.loads(plan_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        print(f"[spike] ⚠️ live plan 读取失败：{exc}", file=sys.stderr)
    if live_plan is not None:
        block = live_plan.get("measured_send")
        if isinstance(block, dict):
            measurement = SendMeasurement.from_plan_block(block)

    # ── 回落与终态 ─────────────────────────────────────────────────────
    print(f"[spike] 等消费组 lag 回落（上限 {LAG_SETTLE_TIMEOUT_S} s，设计值）……")
    lag, waited_s = _wait_lag_zero()
    print(f"[spike] lag={lag}（等待 {waited_s:.0f} s）")

    dlq_after = _topic_total_offset(DLQ_TOPIC)
    input_after = _topic_total_offset(KAFKA_TOPIC)
    dlq_delta = (
        -1 if (dlq_before is None or dlq_after is None) else dlq_after - dlq_before
    )
    input_delta = (
        -1
        if (input_before is None or input_after is None)
        else input_after - input_before
    )

    # ── DLQ 归因证据：有增量才抓 reason 分布（无增量无需抓）────────────
    reasons: dict[str, int] = {}
    if dlq_delta > 0 and dlq_partition_before:
        reasons = _dlq_reason_counts(dlq_partition_before)
        print(f"[spike] DLQ 增量 {dlq_delta}，reason 分布：{reasons}")

    facts = SpikeFacts(
        measurement=measurement,
        during=monitor.summary(),
        lag_after_settle=lag,
        lag_wait_s=waited_s,
        dlq_delta=dlq_delta,
        dlq_reasons=reasons,
        input_delta=input_delta,
    )
    checks = spike_checks_from_facts(facts)
    for c in checks:
        print(f"  {c.format_line()}")

    path = write_spike_report(facts, checks, live_plan, report_dir)
    failed = any(c.is_failure for c in checks)
    counts = {"PASS": 0, "FAIL": 0, "SKIP": 0}
    for c in checks:
        counts[c.status.value] += 1
    print(
        f"=== spike 完成：PASS={counts['PASS']} FAIL={counts['FAIL']} "
        f"SKIP={counts['SKIP']} ⇒ 退出码 {1 if failed else 0} ==="
    )
    print(f"[spike] 报告已写入 {path}")
    return 1 if failed else 0


def _dlq_partition_offsets() -> dict[int, int]:
    """DLQ 各分区的当前总 offset（归因抓取的逐分区起点）。"""
    rc, out = _kafka_script("kafka-get-offsets.sh", "--topic", DLQ_TOPIC)
    if rc != 0:
        return {}
    offsets: dict[int, int] = {}
    for line in out.splitlines():
        parts = line.strip().split(":")
        if len(parts) == 3 and parts[1].isdigit() and parts[2].isdigit():
            offsets[int(parts[1])] = int(parts[2])
    return offsets


def main(argv: list[str] | None = None) -> int:
    args = argv if argv is not None else sys.argv[1:]
    report_dir = args[0] if args else REPORT_DIR
    return run_spike(report_dir)


if __name__ == "__main__":
    raise SystemExit(main())
