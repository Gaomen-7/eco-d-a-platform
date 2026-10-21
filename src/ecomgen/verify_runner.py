"""verify 检查执行器：真连容器/HTTP 做断言（spec.md §4.3、§4.4）。

**分层**：
- 本模块的每个 `check_*` 函数**只做一件事**：采集一个实测值，返回 `CheckResult`。
- 判定逻辑抽成纯函数（`check_*_from_*`），可单测；默认路径才碰真实环境。

P1 阶段的五项（spec §4.3 表）：**4 真 + 1 显式 SKIP**。
第三项「Paimon 五层有行」在 P1 **显式 SKIP**——不计通过也不计 fail。

附加断言（P1 可即时判定，spec §4.3）：
- 漏斗单调（`cnt_inferred` 沿 step_no 非增）
- **脏率不升**（分子＝DLQ 总 offset，分母＝输入 topic 总 offset，与基线名次比较：
  取不到基线 ⇒ SKIP，**不是** PASS——旧实现「取到 offset 就 PASS」是恒真假通过）
- **`is_settled` 两态**（暂定 0 与落定 1 都必须 >0；只有落定 ⇒ FAIL）
- **F6 降级阶梯**（`docker inspect` 的 OOMKilled/exit 137 计数 ≥ 阈值 ⇒ FAIL；
  阈值是**设计值**，实测值写进报告 `degradation` 字段）

所有超时/阈值数字一律标 **设计值**，非官方保证（票面验收第 10 条）。
"""

from __future__ import annotations

import itertools
import json
import subprocess
import sys
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ecomgen.verify import CheckResult, Status, VerifyReport

# ── 超时数字：一律设计值，非官方保证（票面验收第 10 条）─────────────────
TIMEOUT_LAG_WAIT_S = 120  # 设计值
TIMEOUT_RESTART_S = 120  # 设计值（spec §4.4 明确标注）
TIMEOUT_CHECKPOINT_S = 300  # 设计值（spec §4.4 明确标注）

# ── F6 降级阶梯（spec 附录 A·Q，用户 2026-10-21 裁决）──────────────────
# 「容器被 OOM 杀两次即降级」——2 是**设计值**，不是官方给出的阈值保证。
DEGRADATION_OOM_THRESHOLD = 2  # 设计值（非官方保证）
DEGRADATION_THRESHOLD_BASIS = "设计值（spec 附录 A·Q 用户裁决，非官方保证）"

FLINK_UI = "http://127.0.0.1:8081"
SUPERSET_UI = "http://127.0.0.1:8088"
REPORT_DIR = "artifacts/verify"

KAFKA_TOPIC = "ecom.behavior.event.v1"
DLQ_TOPIC = "ecom.behavior.event.v1.dlq"
DIRTY_RATE_BASELINE = "dirty-rate-baseline.json"


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


@dataclass(frozen=True)
class DirtyRateSample:
    """一次脏率采样：分子＝DLQ 总 offset，分母＝输入 topic 总 offset。

    `input_events == 0` 表示「还没产生输入」——此时脏率**无定义**，
    判据只能 SKIP（不能因为 0/0 就算不升）。
    """

    dlq_offset: int
    input_events: int

    def __post_init__(self) -> None:
        if self.dlq_offset < 0:
            raise ValueError(f"dlq_offset 不能为负，实得 {self.dlq_offset}")
        if self.input_events < 0:
            raise ValueError(f"input_events 不能为负，实得 {self.input_events}")

    @property
    def rate(self) -> float | None:
        """脏率。分母为 0 时返回 None（无定义，不可比较）。"""
        if self.input_events == 0:
            return None
        return self.dlq_offset / self.input_events


def check_dlq_rate_not_increased_result(
    baseline: DirtyRateSample | None, current: DirtyRateSample | None
) -> CheckResult:
    """脏率不升判据（纯函数，spec §4.4 / 票面验收第 9 条）。

    - 基线或当前值取不到 ⇒ **SKIP**（旧实现是「取到就 PASS」，那是恒真假通过）。
    - 输入事件数为 0 ⇒ 脏率无定义 ⇒ **SKIP**（0/0 不能算「不升」）。
    - 当前脏率 **>** 基线脏率 ⇒ **FAIL**。
    - 否则 **PASS**。

    分子只含 DLQ 增量，**重复通道不进分子**（重复走幂等吸收，spec §4.4）。
    """
    name = "dlq-rate-no-increase"
    expected = "脏率 ≤ 基线（分子只含 DLQ，分母＝输入事件总数）"
    if baseline is None or current is None:
        return CheckResult(Status.SKIP, name, "基线或当前值取不到", expected)

    base_rate = baseline.rate
    cur_rate = current.rate
    if base_rate is None or cur_rate is None:
        return CheckResult(
            Status.SKIP,
            name,
            f"输入事件为 0，脏率无定义（dlq={current.dlq_offset}）",
            expected,
        )

    actual = (
        f"{cur_rate:.6f}（dlq={current.dlq_offset}/{current.input_events}） "
        f"vs 基线 {base_rate:.6f}（{baseline.dlq_offset}/{baseline.input_events}）"
    )
    if cur_rate > base_rate:
        return CheckResult(Status.FAIL, name, actual, expected, actual)
    return CheckResult(Status.PASS, name, actual, expected)


def _topic_total_offset(topic: str) -> int | None:
    """读某 topic 各分区的**总 offset**（`kafka-get-offsets.sh` 的第三列之和）。

    为什么用「总 offset」而不是「committed offset」：输入冻结后总 offset 是常数，
    且不会因为消费组的提交时机而抖动；DLQ 总 offset 与输入总 offset 同量纲，
    两者之比就是「脏/总」的机读代理（spec §4.4 的分子分母定义）。
    """
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
            topic,
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


def sample_dirty_rate() -> DirtyRateSample | None:
    """采一次脏率（分子 DLQ / 分母输入）。任一取不到 ⇒ None。"""
    dlq = _topic_total_offset(DLQ_TOPIC)
    total = _topic_total_offset(KAFKA_TOPIC)
    if dlq is None or total is None:
        return None
    return DirtyRateSample(dlq_offset=dlq, input_events=total)


def _baseline_path(report_dir: str) -> Path:
    return Path(report_dir) / DIRTY_RATE_BASELINE


def load_dirty_rate_baseline(report_dir: str = REPORT_DIR) -> DirtyRateSample | None:
    """读上一次落盘的脏率基线。文件不存在/损坏 ⇒ None（⇒ 判据 SKIP）。"""
    path = _baseline_path(report_dir)
    try:
        data: Any = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(data, dict):
        return None
    dlq = data.get("dlq_offset")
    total = data.get("input_events")
    if not isinstance(dlq, int) or not isinstance(total, int):
        return None
    try:
        return DirtyRateSample(dlq_offset=dlq, input_events=total)
    except ValueError:
        return None


def save_dirty_rate_baseline(
    sample: DirtyRateSample, report_dir: str = REPORT_DIR
) -> Path:
    """把脏率基线落盘（下一次运行与之比较）。写不了就抛 OSError，由调用方兜底。"""
    path = _baseline_path(report_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "dlq_offset": sample.dlq_offset,
        "input_events": sample.input_events,
    }
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    return path


def check_dlq_rate_not_increased(report_dir: str = REPORT_DIR) -> CheckResult:
    """脏率不升：有基线才判，没基线 **SKIP 并落基线**（票面验收第 9 条）。

    ★ 旧实现的缺陷：`docker compose exec` 一成功就 PASS，`expected` 写「仅记录
    基线」——**只要 Kafka 活着就恒真通过**。现在改成：
    首次运行 ⇒ SKIP 且写 `dirty-rate-baseline.json`；此后每次真比脏率。
    """
    current = sample_dirty_rate()
    if current is None:
        return CheckResult(
            Status.SKIP,
            "dlq-rate-no-increase",
            "DLQ/输入 offset 不可得",
            "脏率 ≤ 基线（分子只含 DLQ，分母＝输入事件总数）",
        )

    baseline = load_dirty_rate_baseline(report_dir)
    if baseline is None:
        try:
            save_dirty_rate_baseline(current, report_dir)
        except OSError as exc:
            return CheckResult(
                Status.SKIP,
                "dlq-rate-no-increase",
                f"基线写入失败：{exc}",
                "脏率 ≤ 基线（分子只含 DLQ，分母＝输入事件总数）",
            )
        return CheckResult(
            Status.SKIP,
            "dlq-rate-no-increase",
            f"首次运行，基线已落盘（dlq={current.dlq_offset}/{current.input_events}）",
            "脏率 ≤ 基线（分子只含 DLQ，分母＝输入事件总数）",
        )

    result = check_dlq_rate_not_increased_result(baseline, current)
    # 通过时把基线推到当前（脏率不升 ⇒ 新基线仍是安全的上界）
    if result.is_pass:
        try:
            save_dirty_rate_baseline(current, report_dir)
        except OSError:
            pass
    return result


# ── `is_settled` 两态判据（spec §4.2 暂定/落定/修正同表同一行）──────────


@dataclass(frozen=True)
class SettledCounts:
    """`ads_window_metric` 中暂定（0）与落定（1）的行数。"""

    pending: int
    settled: int

    def __post_init__(self) -> None:
        if self.pending < 0 or self.settled < 0:
            raise ValueError(f"行数不能为负：{self}")


def check_is_settled_two_states_from_counts(
    counts: SettledCounts | None, raw: str = ""
) -> CheckResult:
    """`is_settled` 两态判据（纯函数）。

    窗口「暂定 → 落定」是同表同一行的状态迁移，所以要证三种形态都留过痕：
    - 两者都 >0 ⇒ **PASS**（既观察到暂定行，也观察到落定行）；
    - 只有落定（`pending==0`）⇒ **FAIL**（看不到暂定态 ⇒ 无法证迁移发生，
      也说明断言跑得太晚或暂定态从未写入）；
    - 只有暂定（`settled==0`）⇒ **FAIL**（窗口从未关闭）；
    - 无行 / 取不到 ⇒ **SKIP**（场景未激活，**不是**永久豁免，spec §4.3）。
    """
    name = "is-settled-two-states"
    expected = "is_settled=0 与 =1 都 >0（暂定与落定两态同表可见）"
    if counts is None:
        return CheckResult(Status.SKIP, name, "行数取不到（场景未激活）", expected)
    actual = f"pending={counts.pending} settled={counts.settled}"
    if counts.pending == 0 and counts.settled == 0:
        return CheckResult(Status.SKIP, name, f"{actual}（无行，场景未激活）", expected)
    if counts.pending == 0:
        return CheckResult(
            Status.FAIL,
            name,
            f"{actual} ⇒ 只有落定行，观察不到暂定→落定迁移",
            expected,
            raw or actual,
        )
    if counts.settled == 0:
        return CheckResult(
            Status.FAIL,
            name,
            f"{actual} ⇒ 窗口从未关闭（只有暂定行，无落定行）",
            expected,
            raw or actual,
        )
    return CheckResult(Status.PASS, name, actual, expected)


def _doris_scalar_runner(sql: str) -> tuple[int, str]:
    """经 compose 网内 `client` 查一个标量（Doris 不能在宿主直连，HLD §4.4）。"""
    return _run(
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


def parse_scalar(out: str) -> int | None:
    """从 `mysql --batch -N` 输出取最后一个纯整数行。"""
    value: int | None = None
    for line in out.splitlines():
        token = line.strip()
        if token.isdigit():
            value = int(token)
    return value


def check_is_settled_two_states() -> CheckResult:
    """查 `ads_window_metric` 的 is_settled 两态行数并判定。"""
    rc0, out0 = _doris_scalar_runner(
        "SELECT COUNT(*) FROM ecom_ads.ads_window_metric WHERE is_settled = 0;"
    )
    rc1, out1 = _doris_scalar_runner(
        "SELECT COUNT(*) FROM ecom_ads.ads_window_metric WHERE is_settled = 1;"
    )
    if rc0 != 0 or rc1 != 0:
        return CheckResult(
            Status.SKIP,
            "is-settled-two-states",
            "查询失败",
            "is_settled=0 与 =1 都 >0（暂定与落定两态同表可见）",
            (out0 or "") + (out1 or ""),
        )
    pending = parse_scalar(out0)
    settled = parse_scalar(out1)
    if pending is None or settled is None:
        return CheckResult(
            Status.SKIP,
            "is-settled-two-states",
            "返回值不可解析",
            "is_settled=0 与 =1 都 >0（暂定与落定两态同表可见）",
            (out0 or "") + (out1 or ""),
        )
    return check_is_settled_two_states_from_counts(
        SettledCounts(pending=pending, settled=settled), raw=(out0 or "") + (out1 or "")
    )


# ── F6 降级阶梯（spec 附录 A·Q，用户 2026-10-21 裁决）───────────────────


@dataclass(frozen=True)
class ContainerOomState:
    """单个容器的 OOM 信号：`OOMKilled` 或 exit 137 任一为真即算一次。"""

    name: str
    oom_killed: bool
    exit_code: int

    @property
    def is_oom_signal(self) -> bool:
        """`State.OOMKilled==true` **或** `ExitCode==137` ⇒ 计一次 OOM 杀。

        137 = 128 + SIGKILL(9)，是「被强杀」的 shell 口径；与 OOMKilled 二者
        取「或」而不是「与」——官方没有保证 OOM 一定会被标成 OOMKilled。
        """
        return self.oom_killed or self.exit_code == 137


def parse_container_oom_states(payload: str) -> list[ContainerOomState] | None:
    """解析 `docker compose ps -a --format json` 的输出（纯函数，可单测）。

    该命令**每行一个 JSON 对象**（不是一个大数组）。`OOMKilled` 不在 `compose ps`
    的字段里，所以真实路径走 `docker inspect`；本函数只处理 inspect 风格的单对象
    或 compose ps 的逐行对象两种输入——两者都缺 OOM 字段时按 False 处理，
    不会凭空报「没被 OOM 杀过」（缺字段 ≠ 健康，另一条 `docker_available` 兜底）。
    """
    states: list[ContainerOomState] = []
    for line in payload.splitlines():
        token = line.strip()
        if not token:
            continue
        try:
            data: Any = json.loads(token)
        except json.JSONDecodeError:
            # 空输出/非 JSON 行整体判为「取不到」，不静默当健康
            return None
        states.extend(_states_from_inspect_objects(data))
    return states or None


def _states_from_inspect_objects(data: Any) -> list[ContainerOomState]:
    """从 inspect 输出（单个对象或数组）抽出 OOM 状态。"""
    objects: list[Any]
    if isinstance(data, list):
        objects = data
    elif isinstance(data, dict):
        objects = [data]
    else:
        return []

    states: list[ContainerOomState] = []
    for obj in objects:
        if not isinstance(obj, dict):
            continue
        name = obj.get("Name") or obj.get("name") or "?"
        if isinstance(name, str):
            name = name.lstrip("/")
        state = obj.get("State") or {}
        if not isinstance(state, dict):
            state = {}
        oom_killed = bool(state.get("OOMKilled", obj.get("OOMKilled", False)))
        raw_exit = state.get("ExitCode", obj.get("ExitCode", 0))
        exit_code = raw_exit if isinstance(raw_exit, int) else 0
        states.append(
            ContainerOomState(
                name=str(name), oom_killed=oom_killed, exit_code=exit_code
            )
        )
    return states


def summarize_degradation(
    states: list[ContainerOomState],
    threshold: int = DEGRADATION_OOM_THRESHOLD,
) -> dict[str, Any]:
    """把各容器 OOM 状态折成报告字段 `degradation`（纯函数，可单测）。

    ⚠️ `threshold` 的 2 次是 **设计值**（spec 附录 A·Q 用户裁决），**不是**官方
    给出的阈值保证；报告里必须同时写实测值与依据，供人复核。
    """
    hits = [s for s in states if s.is_oom_signal]
    return {
        "threshold": threshold,
        "threshold_basis": DEGRADATION_THRESHOLD_BASIS,
        "oom_killed_count": len(hits),
        "degraded": len(hits) >= threshold,
        "containers": [
            {
                "name": s.name,
                "oom_killed": s.oom_killed,
                "exit_code": s.exit_code,
                "counted": s.is_oom_signal,
            }
            for s in states
        ],
    }


def check_degradation_ladder_result(
    states: list[ContainerOomState] | None,
    threshold: int = DEGRADATION_OOM_THRESHOLD,
) -> tuple[CheckResult, dict[str, Any] | None]:
    """F6 降级判据（纯函数）：OOM 杀次数 **≥ 阈值** ⇒ FAIL 并判「已降级」。

    - 取不到容器状态 ⇒ **SKIP**（技能/权限/容器不存在；**不**算通过）。
    - `>= 2` 次（设计值） ⇒ **FAIL**——降级阶梯已触发，链路不能算 P1 成立。
    - `< 2` 次 ⇒ **PASS**，但报告里仍写实测值。

    返回 (CheckResult, degradation 字段)；降级字段在 SKIP 时也写实测容器列表。
    """
    name = "degradation-ladder"
    expected = f"OOM 杀 < {threshold} 次（阈值 {threshold} 为设计值，非官方保证）"
    if states is None:
        return (
            CheckResult(Status.SKIP, name, "容器状态取不到", expected),
            None,
        )

    degradation = summarize_degradation(states, threshold)
    count = degradation["oom_killed_count"]
    actual = f"oom_killed={count}（阈值 {threshold}，设计值）"
    if degradation["degraded"]:
        names = [c["name"] for c in degradation["containers"] if c["counted"]]
        return (
            CheckResult(
                Status.FAIL,
                name,
                f"{actual} ⇒ 已降级：{','.join(names)}",
                expected,
                json.dumps(degradation, ensure_ascii=False, indent=2),
            ),
            degradation,
        )
    return CheckResult(Status.PASS, name, actual, expected), degradation


def collect_container_oom_states() -> list[ContainerOomState] | None:
    """用 `docker inspect` 读各容器的 `State.OOMKilled` / `State.ExitCode`。

    为什么是 `docker inspect` 而不是 `compose ps`：`OOMKilled` 只在 inspect 里
    （spec 附录 A·Q 明写「以 docker inspect 的 OOMKilled / exit 137 计数为机读信号」）。
    """
    rc, out = _run(["docker", "compose", "ps", "-a", "-q"], timeout=60)
    if rc != 0:
        return None
    ids = [line.strip() for line in out.splitlines() if line.strip()]
    if not ids:
        return None
    rc, out = _run(["docker", "inspect", *ids], timeout=120)
    if rc != 0:
        return None
    try:
        data: Any = json.loads(out)
    except json.JSONDecodeError:
        return None
    states = _states_from_inspect_objects(data)
    return states or None


def check_degradation_ladder(
    threshold: int = DEGRADATION_OOM_THRESHOLD,
) -> tuple[CheckResult, dict[str, Any] | None]:
    """F6 降级判据的采集器：读容器状态 → 判定 → 同时回实测值。"""
    return check_degradation_ladder_result(
        collect_container_oom_states(), threshold=threshold
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
    check_is_settled_two_states,
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

    # F6 降级阶梯：返回值是 (CheckResult, degradation_dict)，与 P1_EXTRA_CHECKS
    # 的 `Callable[[], CheckResult]` 签名不兼容，所以**单独调用一次**；
    # 断言进 checks，实测值进 report.degradation（票面要求「实测值写进报告」）。
    # ⚠️ 必须在构造 VerifyReport 之前拿到 degradation，否则 report 里永远是 None。
    deg_result, degradation = check_degradation_ladder()
    checks.append(deg_result)
    print(f"  {deg_result.format_line()}")

    report = VerifyReport(checks=checks, degradation=degradation)
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
