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
TIMEOUT_RESTART_S = 120  # 设计值
TIMEOUT_CHECKPOINT_S = 300  # 设计值
TIMEOUT_LAG_ZERO_S = 120  # 设计值

# 快照前的静默判据（设计值）：实测「喂完 → 行数稳定」约 9~12 s，且抖动大；
# 故不睡固定时长，而是等 count/sum 指纹**连续两次采样全等**。
DORIS_POLL_S = 4  # 设计值（指纹采样间隔）

FLINK_UI = "http://127.0.0.1:8081"
POLL_INTERVAL_S = 4

# 消费组：lag==0 与「输入冻结」的判据来源（spec §4.4 流程图第 2 步）。
CONSUMER_GROUP = "ecom-p1-consumer"

# ── 故障注入用的固定输入（verify-fault 自己喂，不靠外部碰运气）──────────
# 为什么必须自己喂：checkpoint 每 30 s 一次，若 kill 时没有「已消费但未
# checkpoint」的数据，源端重放量为 0 ⇒ 「确有重放」这条判据永远证不出来。
# 时序锚点见 `run_verify_fault`：等一次 checkpoint 完成 → 立刻喂 →
# 静默 15 s 拍基线（此时 dump 已落 Doris、且下一次 checkpoint 还没到）→ kill。
DUMP_SEED = 71
DUMP_SESSIONS = 120
DUMP_RATE = 600
FEED_BOOTSTRAP = "127.0.0.1:29092"  # 宿主 listener（容器内是 kafka:9092）

# 「源端读入是否还在增长」的判稳轮数（设计值）：连续 2 次采样不变即视为读完。
STABLE_POLLS = 2

# 重复通道计数的 SQL（设计约束见 `snapshot_baseline` 的注释）。
# 为什么是 metric_name 过滤而不是 `SUM(revision)`：`revision` 的语义是
# **迟到修正次数**（同一行被修正过几回），不是**重放重复条数**；两者是不同事实，
# 混在一个 SUM 里会让「重复上升」这条子判据失去含义（重放被吸收但没修正 ⇒
# revision 不动 ⇒ 误判「没有重放」；反之迟到修正会让它假升）。
# 重复被幂等吸收这件事在 Doris 侧的唯一可机读痕迹是 Flink 侧
# `MetricSegment.duplicates()` 产出的 `metric_name='duplicate_absorbed'` 行
# （每吸收一条重复计 metric_value=1），所以只汇总这些行。
DUPLICATE_COUNT_SQL = (
    "SELECT COALESCE(SUM(metric_value),0) FROM ecom_ads.ads_window_metric "
    "WHERE metric_name='duplicate_absorbed';"
)


@dataclass(frozen=True)
class Baseline:
    """kill 前的基线快照（输入冻结后为常数）。

    `duplicate_count` 的语义是**被幂等吸收的重放重复条数**（Doris 侧
    `metric_name='duplicate_absorbed'` 的 `metric_value` 之和），
    **不是** `revision`（迟到修正次数）之和。字段名沿用旧名是因为
    `judge_recovery` 与测试已在用，但取值口径以 `DUPLICATE_COUNT_SQL` 为准。
    """

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
    replay_observed: bool
    detail: str = ""

    @property
    def passed(self) -> bool:
        """三条**全**满足才算通过。"""
        return self.counts_equal and self.dlq_flat and self.replay_observed

    def exit_code(self) -> int:
        return 0 if self.passed else 1

    @property
    def reason(self) -> str:
        reasons: list[str] = []
        if not self.counts_equal:
            reasons.append("两表 count/sum 与基线不等（幂等吸收失败）")
        if not self.dlq_flat:
            reasons.append("DLQ 增量不为 0（重复被误当脏，脏率上升）")
        if not self.replay_observed:
            reasons.append("源端没有重读（无重放证据，kill 未命中在途数据）")
        return "；".join(reasons) or "全部条件满足"

    def format_lines(self) -> list[str]:
        """stdout 必须明示两条结论（票面验收第 6 条）。"""
        replay_state = "上升" if self.replay_observed else "未上升"
        dlq_state = "==0" if self.dlq_flat else "!=0"
        return [
            f"  重放证据（源端重读条数{replay_state}）：{'是' if self.replay_observed else '否'}",
            f"  DLQ 增量{dlq_state}：{'是' if self.dlq_flat else '否'}",
            f"  两表 count/sum 与基线全等：{'是' if self.counts_equal else '否'}",
            f"  结论：{'通过' if self.passed else '不通过 —— ' + self.reason}",
        ]


def judge_recovery(
    before: Baseline, after: Baseline, *, replay_observed: bool
) -> FaultVerdict:
    """纯判据：比对 kill 前后快照 + 重放证据。

    三条子判据的语义都是**否定式**的（spec §4.4）：
    1. 两表 count/sum 与基线**全等** —— 重放被 unique key 吸收了，没有多出行。
    2. DLQ 增量 **== 0** —— 重放没有被误判成脏数据。
    3. `replay_observed` **为真** —— 证明确实发生了重放，而不是「什么都没发生」。

    ★ 为什么第 3 条不再是「重复通道计数上升」（impl-02d 实机证伪）：
    - `ads_window_metric` 是 `UNIQUE KEY (window_start, metric_name, dim_key)`，
      重复行 `dim_key=''` ⇒ 每分钟只留 1 行、`metric_value=1`（实测 49 行 / SUM=49），
      同样的分钟再吸收一次也不会变；
    - 更重要的是，重放事件位于**恢复点之后**，不在被恢复的去重状态里，
      因此会被当成**新事件**处理，根本不进 `duplicate` 侧输出。
    ⇒ `duplicate_absorbed` 观测的是「在流重复」（同 seed 重复投递），
    不是故障重放。重放的真证据是**源端重读条数增加**（`source-records-in`）。
    """
    counts_equal = (
        before.window_metric_count == after.window_metric_count
        and before.window_metric_sum == after.window_metric_sum
        and before.funnel_step_count == after.funnel_step_count
        and before.funnel_step_sum == after.funnel_step_sum
    )
    dlq_flat = before.dlq_offset == after.dlq_offset

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
        replay_observed=replay_observed,
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


_CLIENT_READY = False


def _ensure_client() -> bool:
    """把 `client` 服务拉成**常驻**（仅在 verify-fault 期间），供后续 `exec` 复用。

    ★ 为什么不能继续用 `compose run --rm`：实测每次要 8~12 s（起容器 + 连 FE），
    而本判据有「kill 必须卡在两次 checkpoint 之间」的时序要求（30 s 周期），
    多次起容器会把窗口整个吃掉 —— 上一版就因此恒拿不到重放。
    改成「起一次 + exec」后单次查询 <1 s。
    """
    global _CLIENT_READY
    if _CLIENT_READY:
        return True
    # ★ 只用 `docker start`，**绝不** `docker compose up`：实测后者会连带重建
    #   依赖服务（Doris FE 被重启过一次，`compose ps` 显示 "Up 46 seconds"），
    #   9030 在重启窗口内直接拒连。`docker start` 只唤醒同一个容器。
    rc, _out = _run(["docker", "exec", "eco-client", "true"], timeout=15)
    if rc != 0:
        rc, _out = _run(["docker", "start", "eco-client"], timeout=120)
    _CLIENT_READY = rc == 0
    return _CLIENT_READY


def _doris_rows(sql: str) -> dict[str, int] | None:
    """经 client 查多行 `键\\t值`（一条 SQL 取多个标量，见 `SNAPSHOT_SQL`）。"""
    if not _ensure_client():
        return None
    rc, out = _run(
        [
            "docker",
            "exec",
            "eco-client",
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
        timeout=60,
    )
    if rc != 0:
        return None
    values: dict[str, int] = {}
    for line in out.splitlines():
        parts = line.strip().split("\t")
        if len(parts) != 2 or not parts[1].isdigit():
            continue
        values[parts[0]] = int(parts[1])
    return values or None


# ★ 一条 SQL 取全部标量（**不是**六个 `docker compose run`）：
#   实测每次 `compose run --rm` 约 8~12 s，六次就把 30 s 的 checkpoint 周期
#   整个吃掉 ⇒ kill 时数据必然已被 checkpoint，重放量恒 0。
SNAPSHOT_SQL = (
    "SELECT 'wm_count', COUNT(*) FROM ecom_ads.ads_window_metric "
    "UNION ALL SELECT 'wm_sum', COALESCE(SUM(metric_value),0) "
    "FROM ecom_ads.ads_window_metric "
    "UNION ALL SELECT 'fn_count', COUNT(*) FROM ecom_ads.ads_funnel_step "
    "UNION ALL SELECT 'fn_sum', COALESCE(SUM(cnt_inferred),0) "
    "FROM ecom_ads.ads_funnel_step "
    "UNION ALL SELECT 'dup', v FROM (" + DUPLICATE_COUNT_SQL.rstrip(";") + ") d;"
)


def snapshot_baseline() -> Baseline | None:
    """拍基线：两表 count+sum、DLQ offset、重复计数。任一取不到 ⇒ None。

    两表四个标量走**一条** SQL（`SNAPSHOT_SQL`），避免多次起容器拖垮时序。
    """
    values = _doris_rows(SNAPSHOT_SQL)
    dlq = _dlq_offset()
    if values is None or dlq is None:
        return None

    wm_count = values.get("wm_count")
    wm_sum = values.get("wm_sum")
    fn_count = values.get("fn_count")
    fn_sum = values.get("fn_sum")
    dup = values.get("dup")

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


def _consumer_lag() -> int | None:
    """消费组各分区 LAG 之和（`kafka-consumer-groups.sh --describe` 第 6 列）。

    取不到（组还没提交过 offset / 命令失败）⇒ None：**不**当 lag==0。
    """
    rc, out = _run(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "kafka",
            "/opt/kafka/bin/kafka-consumer-groups.sh",
            "--bootstrap-server",
            "localhost:9092",
            "--describe",
            "--group",
            CONSUMER_GROUP,
        ]
    )
    if rc != 0:
        return None
    total = 0
    seen_any = False
    for line in out.splitlines():
        parts = line.split()
        # 只有分区行是 9 列且第 4/5/6 列是数字；表头与提示行不满足
        if len(parts) < 6:
            continue
        lag = parts[5]
        if lag.lstrip("-").isdigit():
            total += int(lag)
            seen_any = True
    return total if seen_any else None


def _wait_lag_zero(timeout_s: int = TIMEOUT_LAG_ZERO_S) -> bool:
    """轮询消费组 lag 到 0（**仅作辅助信息**，不作「读完」判据）。

    ★ 为什么不拿它当判据：消费组 lag 用的是**已提交 offset**，而 Flink 只在
    checkpoint 时提交 ⇒ 数据其实读完了、lag 仍是正数；反过来恢复后 lag 归零
    也只说明「提交追上了」，区分不了「重放」与「正常提交」。
    「读没读完 / 有没有重读」用**源端 read-records**（见 {@link _source_records_in}）。
    """
    deadline = time.monotonic() + timeout_s
    attempt = 0
    while time.monotonic() < deadline:
        attempt += 1
        lag = _consumer_lag()
        print(f"  [{attempt}] lag={lag}")
        if lag == 0:
            return True
        time.sleep(POLL_INTERVAL_S)
    return False


def _running_jid() -> str | None:
    """当前 RUNNING 作业的 jid（没有则 None）。"""
    rc, out = _run(["curl", "-sf", "-m", "3", f"{FLINK_UI}/jobs/overview"], timeout=10)
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


def _checkpoint_number() -> int | None:
    """当前 RUNNING 作业已完成的 checkpoint 编号（取不到 ⇒ None）。

    ★ 两个坑都实测踩过：
    1. `/jobs/overview` **没有** `lastCheckpoint` 字段（只有 jid/state/tasks 等），
       在它上面取编号恒为 None ⇒ 判据恒假 FAIL。编号在
       `/jobs/<jid>/checkpoints` 的 `latest.completed.id`。
    2. 不能判「编号 > 0」：作业从 checkpoint 恢复后，`latest.completed.id`
       一开始就是恢复点那个非 0 编号，`>0` 会立刻假通过。
    """
    jid = _running_jid()
    if not jid:
        return None

    rc, out = _run(
        ["curl", "-sf", "-m", "5", f"{FLINK_UI}/jobs/{jid}/checkpoints"], timeout=10
    )
    if rc != 0:
        return None
    try:
        data: dict[str, object] = json.loads(out)
    except json.JSONDecodeError:
        return None
    latest = data.get("latest")
    if not isinstance(latest, dict):
        return None
    completed = latest.get("completed")
    if not isinstance(completed, dict):
        return None
    number = completed.get("id")
    return number if isinstance(number, int) else None


def _source_records_in() -> int | None:
    """源端（`gate-source`）已读入的记录总数 —— **重放证据的唯一口径**。

    <h2>为什么是「源端重读条数」而不是「重复通道计数」</h2>
    见 {@link judge_recovery} 的注释：重放事件在恢复点之后，会被当成新事件，
    不进 duplicate 侧输出。唯一能直接看到「源端又读了一遍」的就是这个计数。

    <h2>为什么这个计数器可信</h2>
    impl-02a 的取证结论：具名算子计数器在**数据真的流过时**是按 subtask 逐点
    可靠的（曾实测 探针 = count-expired = view_late = Calc = Writer 全等）。
    名称冲突只影响 `pendingCommittables` gauge，不影响 `numRecordsIn/Out`。
    且本判据只看**源端**的增量，与超期顶点无关。
    """
    jid = _running_jid()
    if not jid:
        return None
    rc, out = _run(["curl", "-sf", "-m", "5", f"{FLINK_UI}/jobs/{jid}"], timeout=10)
    if rc != 0:
        return None
    try:
        data: dict[str, object] = json.loads(out)
    except json.JSONDecodeError:
        return None
    vertices = data.get("vertices")
    if not isinstance(vertices, list):
        return None
    for vertex in vertices:
        if not isinstance(vertex, dict):
            continue
        name = str(vertex.get("name") or "")
        # 实测顶点名是 `Source: gate-source -> parse-event`（带 `Source: ` 前缀）
        if "gate-source" not in name:
            continue
        metrics = vertex.get("metrics")
        if not isinstance(metrics, dict):
            continue
        value = metrics.get("read-records")
        try:
            return int(value)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            return None
    return None


def _wait_source_stable(timeout_s: int = TIMEOUT_LAG_ZERO_S) -> int | None:
    """等源端读入计数**连续 `STABLE_POLLS` 次不再增长**，返回稳定值。

    「lag==0」不能当读完的判据：消费组 lag 用的是**已提交 offset**，而 Flink
    只在 checkpoint 时提交 ⇒ 数据读完了、lag 仍是正数。源端计数才是「读没读完」。
    """
    deadline = time.monotonic() + timeout_s
    last: int | None = None
    stable = 0
    while time.monotonic() < deadline:
        current = _source_records_in()
        print(f"  source read-records={current}")
        if current is None:
            return None
        if last is not None and current == last:
            stable += 1
            if stable >= STABLE_POLLS:
                return current
        else:
            stable = 0
            last = current
        time.sleep(1)
    return last


def _feed_dump() -> int:
    """喂一份**固定 seed** 的事件（verify-fault 自己造在途数据）。

    内容由 `--seed` 决定（F3 裁决），节奏由墙钟决定 —— 本判据只关心
    「源端读到多少」，不押内容。
    """
    import sys

    cmd = [
        sys.executable,
        "-m",
        "ecomgen.cli",
        "live",
        "--seed",
        str(DUMP_SEED),
        "--sessions",
        str(DUMP_SESSIONS),
        "--rate",
        str(DUMP_RATE),
        "--late-correctable",
        "0.03",
        "--late-expired",
        "0.02",
        "--bootstrap",
        FEED_BOOTSTRAP,
    ]
    rc, out = _run(cmd, timeout=180)
    tail = out.strip().splitlines()[-2:]
    print(f"  [feed] rc={rc} {' / '.join(tail)}")
    return rc


def _wait_checkpoint_after(
    before_number: int,
    timeout_s: int = TIMEOUT_CHECKPOINT_S,
    poll_s: float = 1.0,
) -> bool:
    """等**新的一次**成功 checkpoint（编号严格大于 `before_number`）。

    超时 300 s（设计值）。「等一次 checkpoint」若只判 `>0`，恢复后立刻即真，
    等于没有断言——这正是本票要堵的那类假通过。

    `poll_s` 默认 1 s：用作**时序锚点**时必须尽快发现「刚完成」的那一刻，
    否则会白白吃掉下一个 30 s 周期里本该留给喂数据的时间。
    """
    deadline = time.monotonic() + timeout_s
    attempt = 0
    while time.monotonic() < deadline:
        attempt += 1
        number = _checkpoint_number()
        print(f"  [{attempt}] checkpoint number={number}（需 > {before_number}）")
        if number is not None and number > before_number:
            return True
        time.sleep(poll_s)
    return False


def _snapshot_signature() -> tuple[int, ...] | None:
    """Doris 两表 count/sum 的指纹（判「落库是否已稳定」用）。"""
    sample = snapshot_baseline()
    if sample is None:
        return None
    return (
        sample.window_metric_count,
        sample.window_metric_sum,
        sample.funnel_step_count,
        sample.funnel_step_sum,
    )


def _wait_doris_stable(timeout_s: int = 120) -> Baseline | None:
    """等 Doris 两表 count/sum **连续两次采样全等**，返回当时快照。

    ★ 为什么不能只 `sleep 15`：Stream Load 批次的实际落库时延抖动大
    （实测 9~12 s，且 kill 前会有在途批次），固定睡眠拍到的基线会偏少，
    恢复后补齐的行会被误判成「幂等吸收失败」——本票实机已踩过一次。
    """
    deadline = time.monotonic() + timeout_s
    last: tuple[int, ...] | None = None
    while time.monotonic() < deadline:
        current = _snapshot_signature()
        print(f"  doris signature={current}")
        if current is not None and current == last:
            return snapshot_baseline()
        last = current
        time.sleep(DORIS_POLL_S)
    return None


def _poll_job_running(timeout_s: int = TIMEOUT_RESTART_S) -> bool:
    """轮询 `/jobs/overview` 到**全部 subtask 都在跑**的 RUNNING（超时 120 s，设计值）。

    ★ 只判 `state=="RUNNING"` 会假通过：TM 刚被 kill 的那一瞬间，JM 还没来得及
    把作业翻成 RESTARTING，`state` 仍是 RUNNING，但 `tasks.running==0`。
    实测踩过——真正的恢复要等 TM 容器回来、作业从 checkpoint 重新部署。
    """
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
            summary = [
                f"{j.get('state')}({((j.get('tasks') or {}).get('running'))}"
                f"/{((j.get('tasks') or {}).get('total'))})"
                for j in jobs
            ]
            print(f"  [{attempt}] {summary}")
            for job in jobs:
                tasks = job.get("tasks") or {}
                running = int(tasks.get("running") or 0)
                total = int(tasks.get("total") or 0)
                if job.get("state") == "RUNNING" and total > 0 and running == total:
                    return True
        else:
            print(f"  [{attempt}] REST 不可达")
        time.sleep(POLL_INTERVAL_S)
    return False


def _ensure_tm_container() -> int:
    """把被 kill 的 TaskManager 容器重新拉起（故障后的环境恢复步骤）。

    ★ 两个实测坑：
    1. `docker compose kill flink-tm` 之后容器停在 **exited**，
       `restart: unless-stopped` **不会**把它带回来（compose 认为这是被主动停掉的）
       ⇒ 作业永远等不到 RUNNING。kill 是注入的故障，拉起容器是恢复动作。
    2. **必须用 `docker start`，不能用 `docker compose up -d flink-tm`**：
       实测后者会连带重建 `flink-jm`，而 session 模式没有持久化 job graph
       （JM 日志逐字 "Successfully recovered 0 persisted job graphs"）
       ⇒ 作业直接被抹掉，`/jobs/overview` 恒空数组。`docker start`
       只唤醒同一个容器，JM 不认识它也没关系，TM 重新注册后作业照样重新部署。
    """
    rc, out = _run(["docker", "start", "eco-flink-tm"], timeout=180)
    if rc != 0:
        print(f"  [tm-restore] docker start 失败，回退 compose：{out.strip()[:200]}")
        rc, out = _run(["docker", "compose", "up", "-d", "flink-tm"], timeout=180)
    if rc != 0:
        print(f"  [tm-restore] 拉起失败：{out.strip()[:200]}")
    else:
        print("  [tm-restore] flink-tm 容器已拉起")
    return rc


def run_verify_fault(
    report_dir: str = "artifacts/verify",
    *,
    killer: Callable[[], int] | None = None,
) -> VerifyReport:
    """故障注入全流程（spec §4.4 流程图逐行）。

    顺序**冻结**：外部输入冻结 → 完成一次 checkpoint（时序锚点）→ 喂固定 dump
    → 源端读完 → 静默 → 拍基线 → kill TM → 等 RUNNING（120 s 设计值）
    → 等重放读完并完成一次新 checkpoint（300 s 设计值）→ 比对三条件
    （两表 count/sum 全等、DLQ 增量==0、源端确有重读）。
    """
    checks: list[CheckResult] = []
    print("=== make verify-fault：故障恢复 + 幂等吸收证明 ===")

    print("--- 1/8 等外部输入冻结（源端读入不再增长）")
    idle = _wait_source_stable()
    checks.append(
        CheckResult(
            Status.PASS if idle is not None else Status.FAIL,
            "fault-input-frozen",
            f"source read-records={idle}",
            "源端读入稳定（外部已停喂）",
        )
    )
    if idle is None:
        return _finish(checks, report_dir)

    print("--- 2/8 等一次 checkpoint 完成（时序锚点）")
    anchor = _checkpoint_number()
    anchored = _wait_checkpoint_after(anchor) if anchor is not None else False
    checks.append(
        CheckResult(
            Status.PASS if anchored else Status.FAIL,
            "fault-checkpoint-anchor",
            f"checkpoint>{anchor}" if anchored else "取不到/超时",
            "先拿到一个基线 checkpoint 编号",
        )
    )
    if not anchored:
        return _finish(checks, report_dir)

    print(f"--- 3/8 喂固定 dump（seed={DUMP_SEED}, {DUMP_SESSIONS} 会话）并等源端读完")
    feed_rc = _feed_dump()
    drained = _wait_source_stable()
    checks.append(
        CheckResult(
            Status.PASS if feed_rc == 0 and drained is not None else Status.FAIL,
            "fault-feed-dump",
            f"rc={feed_rc} read-records={drained}",
            "投递成功且源端已读完",
        )
    )
    if drained is None:
        return _finish(checks, report_dir)

    print("--- 4/8 等 Doris 落库稳定后拍基线")
    before = _wait_doris_stable()
    if before is None:
        checks.append(
            CheckResult(
                Status.FAIL, "fault-baseline", "快照取不到", "两表 count/sum 可读"
            )
        )
        return _finish(checks, report_dir)
    print(f"    基线：{before}")
    checks.append(CheckResult(Status.PASS, "fault-baseline", str(before), "快照成功"))

    # ★ 记下 kill 前的 checkpoint 编号：恢复后必须出现**更大的**编号，
    #   否则「等一次 checkpoint」会被恢复点自带的编号立刻满足（假通过）。
    checkpoint_before = _checkpoint_number()
    records_at_kill = _source_records_in()
    print(f"    kill 前：checkpoint={checkpoint_before} read-records={records_at_kill}")

    print("--- 5/8 kill TaskManager")
    if killer is not None:
        rc = killer()
    else:
        rc, _out = _run(["docker", "compose", "kill", "flink-tm"])
    checks.append(
        CheckResult(
            Status.PASS if rc == 0 else Status.FAIL, "fault-kill-tm", str(rc), "0"
        )
    )
    _ensure_tm_container()

    print(
        f"--- 4/7 轮询 /jobs/overview 到 RUNNING（超时 {TIMEOUT_RESTART_S} s，设计值）"
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

    print(
        f"--- 6/8 等重放读完 + 完成一次新 checkpoint（超时 {TIMEOUT_CHECKPOINT_S} s，设计值）"
    )
    after_records = _wait_source_stable() if running else None
    replay = (
        after_records - records_at_kill
        if after_records is not None and records_at_kill is not None
        else 0
    )
    replay_observed = replay > 0
    # 辅助证据（不参与判定）：恢复后消费组 lag 是否归零。
    lag_drained = _wait_lag_zero() if running else False
    checks.append(
        CheckResult(
            Status.PASS if replay_observed else Status.FAIL,
            "replay-observed",
            f"源端重读 {replay} 条（{records_at_kill}→{after_records}），"
            f"lag 归零={lag_drained}",
            ">0（证明 kill 后确有重放）",
        )
    )
    checkpointed = (
        _wait_checkpoint_after(
            checkpoint_before if checkpoint_before is not None else 0
        )
        if running
        else False
    )
    checks.append(
        CheckResult(
            Status.PASS if checkpointed else Status.FAIL,
            "fault-checkpoint-done",
            f"新 checkpoint（>{checkpoint_before}）"
            if checkpointed
            else f"超时 {TIMEOUT_CHECKPOINT_S} s",
            "kill 后再完成一次成功 checkpoint",
        )
    )

    print("--- 7/8 等 Doris 落库稳定后拍恢复后快照并比对")
    after = _wait_doris_stable()
    verdict: FaultVerdict | None = None
    if after is None:
        checks.append(
            CheckResult(Status.FAIL, "fault-recovery", "恢复后快照取不到", "可读")
        )
    else:
        verdict = judge_recovery(before, after, replay_observed=replay_observed)
        for line in verdict.format_lines():
            print(line)
        checks.append(
            CheckResult(
                Status.PASS if verdict.passed else Status.FAIL,
                "fault-idempotent-absorb",
                "counts 全等 / DLQ 增量 0 / 有重放"
                if verdict.passed
                else verdict.reason,
                "counts 全等 且 DLQ 增量==0 且 源端确有重读",
                verdict.detail if not verdict.passed else None,
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

    print("--- 8/8 写报告")
    report = _finish(checks, report_dir)
    if report.exit_code() != 0:
        _dump_fault_snapshot(
            report_dir, before, after, verdict, replay_observed, replay, rc
        )
    return report


def _finish(checks: list[CheckResult], report_dir: str) -> VerifyReport:
    """写 JSON 报告并打印汇总（两条退出路径共用）。"""
    from pathlib import Path

    report = VerifyReport(checks=checks)
    path = report.write_json(Path(report_dir))
    counts = report.counts()
    print(
        f"=== verify-fault 完成：PASS={counts['PASS']} FAIL={counts['FAIL']} "
        f"SKIP={counts['SKIP']} ⇒ 退出码 {report.exit_code()} ==="
    )
    print(f"[verify-fault] 报告已写入 {path}")
    return report


def _dump_fault_snapshot(
    report_dir: str,
    before: Baseline | None,
    after: Baseline | None,
    verdict: FaultVerdict | None,
    replay_observed: bool,
    replay: int,
    kill_rc: int,
) -> None:
    """失败时把关键快照落进 artifacts/（票面验收第 7 条：供事后复盘）。

    verify 的失败痕迹是「原始响应全文」，fault 的失败痕迹是**三条件实测值**：
    基线、恢复后、裁决明细。缺了它，事后只能看一行 reason。
    """
    import dataclasses
    from datetime import UTC, datetime
    from pathlib import Path

    directory = Path(report_dir)
    directory.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    path = directory / f"verify-fault-{stamp}.json"
    payload = {
        "generated_at": datetime.now(UTC).isoformat(),
        "kill_rc": kill_rc,
        "replay_observed": replay_observed,
        "replay_records": replay,
        "baseline": dataclasses.asdict(before) if before else None,
        "after": dataclasses.asdict(after) if after else None,
        "verdict": dataclasses.asdict(verdict) if verdict else None,
        "reason": verdict.reason if verdict else "未得到裁决",
    }
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(f"[verify-fault] 失败快照已写入 {path}")


def main(argv: list[str] | None = None) -> int:
    """CLI 入口：`uv run python -m ecomgen.fault [报告目录]`。"""
    import sys

    args = argv if argv is not None else sys.argv[1:]
    report_dir = args[0] if args else "artifacts/verify"
    return run_verify_fault(report_dir).exit_code()


if __name__ == "__main__":
    raise SystemExit(main())
