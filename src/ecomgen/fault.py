"""verify-fault 的判据与流程（spec.md §4.4，票面验收第 6/7/9 条）。

**这段要证的是**：kill TaskManager 后作业从 checkpoint 恢复，重放的事件被
幂等吸收——两表 count/sum 与 kill 前**全等**，DLQ 增量 **为 0**，
且**确有重放**（源端重读条数 >0）。

判据与执行分离：
- `judge_recovery()` 是**纯函数**：给快照，出裁决。可单测，且能证明它真的会 fail。
- `run_verify_fault()` 才碰容器（kill TM、轮询 REST、查 Doris）。

★ 反例保护（票面明确要求）：**条数一致但 DLQ 上升 ⇒ fail**。
若把重放当脏，脏率会凭空飙升——那说明「重复」被错误地送进了 DLQ，
正是本票要防的那类假通过。

★ 八条实机反证（impl-02d，2026-10-21~22，均已写进对应函数的注释）：

| # | 旧实现 | 实测现象 | 修法 |
|---|---|---|---|
| A | 读顶点汇总 `metrics['read-records']` | 该字段**恒为 0**（同刻 subtask 之和 = 263） | 按 subtask 求和，见 `_subtask_metric_sum` |
| B | `live` 喂两遍当「重复投递」 | `live` 每轮把 event_time 平移到**当前墙钟** ⇒ 同 seed 是**两批不同窗口键**的数据 | 喂**字节完全相同的固定 dump**，见 `_fixed_batch` |
| C | `state==RUNNING` 即「已恢复」 | kill 后 JM 仍报 `RUNNING 28/28` 约 14 s，第一次轮询就假通过 | 先见 `running<total`，再用 `/checkpoints` 的 `restored` 递增 |
| D | 「连续两次采样全等」即落定 | 8 s「稳定」之后还会再涨 34 行（窗口等 source idle 30 s 才 fire） | 要**静默窗口**（`QUIET_S`），见 `_wait_doris_settled` |
| E | 「投完立刻判读完」 | 源端还没开始读时读数与投递前相同 ⇒ 「连续两次不变」当场成立，基线拍早了 | `_wait_source_stable(at_least=…)` |
| F | DLQ sink 挂在 `env` 上 | **从未进作业图**（plan 里没有 dlq 顶点，投必脏事件 DLQ offset 恒 0） | 改走 StatementSet，见 `GateSegment.registerDlqSink` |
| G | 靠「投数据」顺便推水印 | 批次的 `event_time` 最大值只到 `now−40 s`，而水印 = **最大事件时间 − 120 s**（`forBoundedOutOfOrderness`）⇒ 批次自己的窗口**永远等不到关闭**，「静默 110 s」是假落定；下一轮把水印推上去时，上一轮的行才一次性落库 | 显式投**水印推进 tick**（每个分区一条），见 `tick_events_for_batch` |
| H | 基线/恢复后都用**全表** count/sum | 上一轮遗留的窗口行、以及本轮 tick 与心跳的边界效应，让「全等」在小批次下不可达（实测 +2/+4 行都落在**别的**分钟窗口） | 按**本批窗口**限定比对，见 `scoped_snapshot_sql` |

★ 第 9 条（重复升而脏率不升）的**可观测前提**（本票实测）：
`duplicate_absorbed` 的 `dim_key=''`、`metric_value=1`，而 `ads_window_metric`
是 `UNIQUE KEY (window_start, metric_name, dim_key)` ⇒ **同一分钟内**的重复会
覆盖同一行、`SUM` 不变。只有落在**不同分钟**的重复才新增行、`SUM` 才上升。
实测（seed=71 同字节两遍，中间等过整分钟边界）：dup SUM 162→163，
DLQ 247→247 不变，脏率 0.006567→0.006521 不升 ⇒ PASS。
⇒ 想观测「重复上升」必须跨分钟；同一分钟内反复投只会看到「纹丝不动」。
"""

from __future__ import annotations

import json
import subprocess
import time
from collections.abc import Callable
from dataclasses import dataclass

from ecomgen.verify import CheckResult, Status, VerifyReport
from ecomgen.verify_runner import DirtyRateSample

# 反例注入条数（票面验收第 7 条）：一次 `make verify-fault-corrupt` 投这么多条
# schema_invalid。它是**设计值**，不是官方保证；同时是「夹具记账」的唯一口径来源
# （见 `verify_runner.absorb_fixture_dirty`）——所以必须只有一处定义。
CORRUPT_INJECT_COUNT = 5

# 超时：全部为**设计值**，非官方保证（票面验收第 10 条）
TIMEOUT_RESTART_S = 120  # 设计值
TIMEOUT_CHECKPOINT_S = 300  # 设计值
TIMEOUT_LAG_ZERO_S = 120  # 设计值

# 快照前的静默判据（设计值）：实测「喂完 → 行数稳定」约 9~12 s，且抖动大；
# 故不睡固定时长，而是等 count/sum 指纹在一段**静默窗口**内一动不动。
DORIS_POLL_S = 4  # 设计值（指纹采样间隔）

# ★ 落定所需的静默窗口（设计值）：实测「连续两次采样相等」（8 s）之后还会再涨
#   34 行 —— 窗口聚合要等 `table.exec.source.idle-timeout = 30s` 才 fire。
#   取 30 s（≥ idle-timeout），否则基线总是在「还没落完」时被拍下。
#
# ★★ 静默窗口**不再随批次跨度放大**（impl-02d 第四轮修正）★★
#   上一版把静默窗口抬到 `跨度 + 60 s`，理由是「窗口关闭要等水印走过整个跨度」。
#   那条推理只在「水印由本批数据推进」时成立；而本批数据的最大值只到
#   `now−200 s`，根本推不动自己的窗口（见 `tick_event_time`）。
#   ⇒ 现在改为**显式投 tick 推水印**，窗口关闭变成确定事件；静默窗口
#     回到固定 30 s，一轮耗时由 6~15 分钟降到 3 分钟量级。
QUIET_S = 30  # 设计值

FLINK_UI = "http://127.0.0.1:8081"
POLL_INTERVAL_S = 4

# 消费组：lag==0 与「输入冻结」的判据来源（spec §4.4 流程图第 2 步）。
CONSUMER_GROUP = "ecom-p1-consumer"

# ── 故障注入用的固定输入（verify-fault 自己喂，不靠外部碰运气）──────────
# 为什么必须自己喂：checkpoint 每 30 s 一次，若 kill 时没有「已消费但未
# checkpoint」的数据，源端重放量为 0 ⇒ 「确有重放」这条判据永远证不出来。
#
# ★★ 为什么喂**固定字节文件**而不是 `live`（impl-02d 实机证伪）★★
#   `ecomgen live` 每轮都把整批 `event_time` 平移到**当前墙钟**
#   （`cli._live` 的 `round_start_ms`，F7 裁决），所以「同 seed 跑两遍」得到的是
#   **同一批 event_id、但落在不同分钟窗口**的两批数据 ⇒ 第二遍必然长新行，
#   与「幂等吸收」无关。实测：同 seed 连投两次，每个 metric 各 +1 行、和 +1。
#   ⇒ 幂等证明的输入必须是**同一份字节**：dump 一次、把 event_time 平移到
#     「最近」，写进固定文件，两遍投喂都读它。实测同一份字节重投 + kill：
#     replay=263、两表 count/sum 全等、DLQ=0（PASS）。
DUMP_SEED = 71
# ★ 会话数从 120 降到 20（impl-02d 第二轮实机证伪）：批次的 event_time 跨度
#   主要由**迟到偏移量程**决定（超期通道最多回溯 300 s），实测
#   `--sessions 120` 跨度 640 s / 11 个分钟窗口、`--sessions 20` 跨度 313 s / 4 个窗口。
#   而「整批窗口都关掉」的等待时间正比于跨度（见 `_wait_doris_settled`），
#   640 s 的等待让整轮跑不进可用时间。20 会话（45 条）足够触发
#   「重放 + 幂等吸收 + DLQ 不动」三条判据所需的全部行为。
DUMP_SESSIONS = 20

# 固定批次被压缩到的 event_time 跨度（秒，设计值）。
# 为什么压：原始跨度由迟到偏移量程决定（约 310~640 s），而窗口关闭要等水印推进，
# 等待量级与跨度相关 ⇒ 一轮要跑 15 分钟。本判据不测超期通道，故压进单个窗口区间。
# 取 50 s：落在 1~2 个分钟窗口内；水印由 tick 显式推进，静默窗口固定 QUIET_S=30 s。
BATCH_SPAN_S = 50  # 设计值

# ★★ 批次在墙钟上的落点（设计值，impl-02d 第四轮定稿）★★
# 批次被线性映射到 `[now − BATCH_END_LEAD_S − BATCH_SPAN_S, now − BATCH_END_LEAD_S]`。
# 为什么留 200 s：给「水印推进 tick」留出足够空间。窗口首次 fire 的充分条件是
# `watermark > windowEnd`，即 `最大事件时间 > windowEnd + 120 s`。
# 批次自己做不到（它的最大值 ≤ 自己的 windowEnd），所以**必须显式投 tick**：
# tick = `windowEnd + 120 s + TICK_PAD_MS`，最坏情况下（批次最大值恰在分钟边界前）
# 落在 `now − 15 s`，最好情况落在 `now − 75 s` —— **恒为过去事件**，
# 绝不会命中规则①的 `time_out_of_range`（见 `test_tick_is_never_a_future_event`）。
BATCH_END_LEAD_S = 200  # 设计值
# tick 相对「windowEnd + ALLOWED_LATENESS_MS」的额外裕度（毫秒）：水印严格大于
# `windowEnd` 才关窗，刚好等于时存在 1 ms 级边界抖动。
TICK_PAD_MS = 5_000  # 设计值

# 窗口宽度与允许延迟（与 flink JobConfig 逐字对齐，仅用于算 tick 与窗口字面量）。
WINDOW_MS = 60_000
ALLOWED_LATENESS_MS = 120_000

DUMP_RATE = 600
FEED_BOOTSTRAP = "127.0.0.1:29092"  # 宿主 listener（容器内是 kafka:9092）
FEED_TOPIC = "ecom.behavior.event.v1"

# 固定批次的落盘位置（幂等证明的输入，必须两遍一致）
FEED_BATCH_PATH = "artifacts/verify/fault-batch.jsonl"

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
#
# ★ 别名 `AS v` 写在**这里**（代码评审发现）：快照 SQL 需要把它当子查询引用，
#   以前是在两处快照 SQL 里各做一次字符串替换，
#   同一段 4 行注释也抄了两遍。改为源头带别名，两个调用点直接内联。
DUPLICATE_COUNT_ALIASED_SQL = (
    "SELECT COALESCE(SUM(metric_value),0) AS v FROM ecom_ads.ads_window_metric "
    "WHERE metric_name='duplicate_absorbed'"
)

# 兼容旧引用（`tests/test_verify_gaps.py` 锁的是这条 SQL 的口径：
# 取 `duplicate_absorbed` 的 metric_value，**不是** SUM(revision)）。
DUPLICATE_COUNT_SQL = DUPLICATE_COUNT_ALIASED_SQL + ";"


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
    - 更重要的是，重放事件的 `event_time` 位于**恢复点之后**，若它不在被恢复的
      去重状态里（例如 kill 前那批 offset 尚未进 checkpoint，但 event_time 已推进），
      就会被当成**新事件**处理，根本不进 `duplicate` 侧输出；
    - 实测反证：同一份字节重投、且 kill 命中在途数据时，`replay=263`、
      两表 count/sum 全等、`duplicate_absorbed` **完全没动**（115→115）。
    ⇒ `duplicate_absorbed` 观测的不是故障重放。重放的真证据是
    **源端重读条数 >0**（`read-records`），见 {@link _source_records_in}。

    ★ `duplicate_count` 仍留在快照里，但**不参与判定**：它是「在流重复」的
    旁证（实机可见 `duplicate_absorbed` 随重投上升，见 §9 的脏率判据），
    把它当「重放证据」正是上一版卡住的根因。
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
    """经 client 查多行 `键\\t值`（一条 SQL 取多个标量，见 `scoped_snapshot_sql`）。"""
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
#
# ★★ 为什么两表都要排除 `provisional_emitted`（impl-02d 实机证伪，第 5 个判据缺陷）★★
#   `provisional_emitted` 是 `WindowSegment.provisional` 用**处理时间定时器**
#   （每 `PROVISIONAL_INTERVAL_MS = 10 s`）发出的暂定态行，落点按
#   「当前墙钟所在分钟」分桶。它与重放**毫无关系**：只要墙钟往前走，
#   它就不断产出新的 `window_start` 行 ⇒ 表行数永远在涨。
#   实测：一次 verify-fault 的 ±12 分钟增量里，26 行里有 19 行是它
#   （`updated_at` 一直在 03:23:50 这种「刚刚」更新）。
#   ⇒ 不排除它，「两表 count/sum 与基线全等」在任何耗时超过一分钟的
#     故障流程里都不可达 —— 那是**判据不可达**，不是幂等失败。
#   排除的是「暂定态心跳」，业务行（event_*/uv/funnel）与 duplicate 一律保留。
_PROVISIONAL_METRIC = "provisional_emitted"

# 重复通道（**期望上升**的那一侧），比对业务行时排除、单独作 `dup` 旁证。
_DUPLICATE_METRIC = "duplicate_absorbed"


def scoped_snapshot_sql(windows: list[str]) -> str:
    """按**本批窗口**限定范围的快照 SQL（一条取全部标量）。

    <h2>为什么必须限定窗口（impl-02d 第四轮实机定位的第 8 个判据缺陷）</h2>
    旧实现用**全表** count/sum 比对。实机里「不全等」的增量总是落在
    **别的分钟窗口**（上一轮遗留的窗口行迟到落库、或心跳换分钟），
    与本轮这批数据毫无关系。实测 span2：基线 `(10972, 21971, 1290, 1290)`
    → 恢复后 `(10974, 21973, 1294, 1294)`，差的 2 行窗口指标 + 4 行漏斗
    全部来自 03:53 那一分钟 —— 而 span2 自己的批次窗口是 04:00。
    ⇒ 全表口径下，「全等」在小批次 + 跨轮残留的环境里**不可达**，
      那不是幂等失败，是判据把别人的数据算进来了。

    <h2>三条过滤，各有实机理由</h2>
    1. `window_start IN (...)`：只比本轮窗口（解跨轮残留）。
    2. `metric_name <> 'provisional_emitted'`：处理时间心跳，墙钟一走就长新行。
    3. `metric_name <> 'duplicate_absorbed'`：重复通道正是**期望上升**的那一侧，
       把它算进「业务行全等」会让断言自相矛盾（它单独取 `dup` 作旁证）。

    ★ 窗口字面量必须是 **UTC** 形状（见 `_utc_literal`）：Doris 的 `window_start`
    是 epoch 直接存成无时区 DATETIME，实机读到的就是 `2026-10-22 03:53:00`。
    """
    if not windows:
        raise ValueError("窗口集合为空：scoped 快照会匹配不到任何行 ⇒ 判据退化成 0==0")
    literal_list = ", ".join(f"'{w}'" for w in windows)
    business = (
        f"window_start IN ({literal_list}) "
        f"AND metric_name <> '{_PROVISIONAL_METRIC}' "
        f"AND metric_name <> '{_DUPLICATE_METRIC}'"
    )
    return (
        "SELECT 'wm_count', COUNT(*) FROM ecom_ads.ads_window_metric "
        f"WHERE {business} "
        "UNION ALL SELECT 'wm_sum', COALESCE(SUM(metric_value),0) "
        f"FROM ecom_ads.ads_window_metric WHERE {business} "
        "UNION ALL SELECT 'fn_count', COUNT(*) FROM ecom_ads.ads_funnel_step "
        f"WHERE window_start IN ({literal_list}) "
        "UNION ALL SELECT 'fn_sum', COALESCE(SUM(cnt_inferred),0) "
        f"FROM ecom_ads.ads_funnel_step WHERE window_start IN ({literal_list}) "
        # ★ 别名 `v` 由 `DUPLICATE_COUNT_ALIASED_SQL` 自带（否则报
        #   `Unknown column 'v' in 'table list'`，整个快照返回 None —— 实机踩过一次）。
        "UNION ALL SELECT 'dup', v FROM (" + DUPLICATE_COUNT_ALIASED_SQL + ") d;"
    )


def snapshot_baseline(windows: list[str]) -> Baseline | None:
    """拍快照：两表 count+sum（**限定在 `windows` 内**）、DLQ offset、重复计数。

    五个业务标量走**一条** SQL，避免多次起容器拖垮时序。

    ★ `windows` 是必填（不再是可选的全表回退）：全表口径已被实机证伪
    （见 `scoped_snapshot_sql`），留着那条路只会让人在某天误用。
    """
    values = _doris_rows(scoped_snapshot_sql(windows))
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

    <h2>★ 必须按 subtask 求和（impl-02d 实机定位，这是 replay 恒 0 的真根因）</h2>
    实机取证（同一时刻、同一个作业 `b87d308b…`）：

    ```
    /jobs/<jid>          → 顶点 'Source: gate-source -> parse-event'  metrics['read-records'] = 0
    /jobs/<jid>/vertices/<vid> → 各 subtask 的 read-records 之和            = 263
    ```

    即**顶点级汇总字段恒为 0**（Flink 2.2 上该顶点的 aggregate 未被填充），
    而按 subtask 求和是准的、且与投递条数逐条对得上。旧实现读顶点汇总 ⇒
    恒返回 0 ⇒ `replay = 0 - 0 = 0` ⇒ 「重放证据」永远 FAIL。
    这正是票面「重放=0」卡了整夜的直接原因，**不是**时序窗口问题。
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
        return _subtask_metric_sum(jid, str(vertex.get("id") or ""), "read-records")
    return None


def _subtask_metric_sum(jid: str, vid: str, metric: str) -> int | None:
    """某顶点**按 subtask 求和**的计数器（顶点汇总字段不可信，见上）。"""
    if not vid:
        return None
    rc, out = _run(
        ["curl", "-sf", "-m", "5", f"{FLINK_UI}/jobs/{jid}/vertices/{vid}"], timeout=10
    )
    if rc != 0:
        return None
    try:
        data: dict[str, object] = json.loads(out)
    except json.JSONDecodeError:
        return None
    subtasks = data.get("subtasks")
    if not isinstance(subtasks, list):
        return None
    total = 0
    seen = False
    for subtask in subtasks:
        if not isinstance(subtask, dict):
            continue
        metrics = subtask.get("metrics")
        if not isinstance(metrics, dict):
            continue
        value = metrics.get(metric)
        try:
            total += int(value)  # type: ignore[arg-type]
            seen = True
        except (TypeError, ValueError):
            continue
    return total if seen else None


def _wait_source_stable(
    timeout_s: int = TIMEOUT_LAG_ZERO_S, *, at_least: int | None = None
) -> int | None:
    """等源端读入计数**连续 `STABLE_POLLS` 次不再增长**，返回稳定值。

    「lag==0」不能当读完的判据：消费组 lag 用的是**已提交 offset**，而 Flink
    只在 checkpoint 时提交 ⇒ 数据读完了、lag 仍是正数。源端计数才是「读没读完」。

    ★ `at_least`（impl-02d 实机踩过）：投完数据立刻轮询时，源端**还没开始读**，
    读数与投递前相同 ⇒ 「连续两次不变」当场成立 ⇒ 误判「已读完」，
    于是基线拍在数据尚未落库的时刻，恢复后补齐的行被当成「幂等吸收失败」。
    实测：预热投完 read-records 仍是 268（正好等于投递前的值），
    下一轮才涨到 794。传 `at_least = 投递前值 + 投递条数` 可堵住这个假稳定。

    ★★ 超时时**不能**把未达 `at_least` 的读数当结果返回（代码评审发现）★★
    旧实现末尾 `return last` —— 于是「源端没读完」被当成「读完了」，
    调用方只判 `is not None` 就 PASS。这正是它自己的 docstring 说
    「`at_least` 可堵住假稳定」所防的那个假通过，只是从超时路径漏了回来。
    ⇒ 给了 `at_least` 而始终没达到 ⇒ 返回 `None`（判据该 FAIL，不该凑数）。
    """
    deadline = time.monotonic() + timeout_s
    last: int | None = None
    stable = 0
    while time.monotonic() < deadline:
        current = _source_records_in()
        print(f"  source read-records={current}")
        if current is None:
            return None
        reached = at_least is None or current >= at_least
        if reached and last is not None and current == last:
            stable += 1
            if stable >= STABLE_POLLS:
                return current
        else:
            stable = 0
            last = current
        time.sleep(1)
    if at_least is not None and (last is None or last < at_least):
        print(
            f"  source 超时仍未达到 at_least={at_least}（最后读数 {last}）⇒ 判为未读完"
        )
        return None
    return last


def _inject_dirty(count: int = CORRUPT_INJECT_COUNT) -> int:
    """投 `count` 条**必然被判脏**的事件（反例证伪用，票面验收第 7 条）。

    为什么选 `schema_invalid`（缺 `event_type`）而不是别的：
    - `EventQuality.reason()` 的**第一条**规则就是必填字段非空，缺 `event_type`
      必然命中，不依赖时钟、不依赖窗口，判定路径最短最确定；
    - 不用「amount 为负」这类取值越界：那条排在必填检查之后，注入成本一样但
      理由链更长；
    - **不能**用重复事件（同 event_id）：重复走 `duplicate` 侧输出、**绝不进 DLQ**，
      用它注入脏数据恰恰会证明「重复没被当成脏」——那是另一条判据要的事。

    返回值 = 实际投递条数（调用方据此判 FAIL，不静默放过）。
    """
    try:
        from confluent_kafka import Producer
    except ImportError:
        print("  [counterexample] ✗ 缺少 confluent-kafka")
        return -1

    producer = Producer(
        {
            "bootstrap.servers": FEED_BOOTSTRAP,
            "client.id": "verify-fault-counterexample",
        }
    )
    now_ms = int(time.time() * 1000)
    sent = 0
    for index in range(count):
        payload = {
            # ★ 故意缺 event_type ⇒ schema_invalid；其余字段给全，确保只触发这一条
            "event_id": f"00000000-0000-4000-8000-{index:012d}",
            "event_time": now_ms - 1_000,
            "user_id": f"counterexample-user-{index}",
            "item_id": "counterexample-item",
            "category_id": "counterexample-category",
            "payload": {"session_id": f"counterexample-user-{index}"},
            "schema_version": "1.0",
        }
        producer.produce(
            topic=FEED_TOPIC,
            key=f"counterexample-{index}".encode(),
            value=json.dumps(payload, ensure_ascii=False, sort_keys=True).encode(
                "utf-8"
            ),
        )
        producer.poll(0)
        sent += 1
    producer.flush(30)
    return sent


def batch_end_ms(now_ms: int) -> int:
    """批次 `event_time` 的上界（= tick 之前的那条线）。

    批次被映射到 `[end − span, end]`，其中 `end = now − BATCH_END_LEAD_S`。
    这样 tick（落在 `end` 之后）既能把水印推过批次窗口，又仍是过去事件。
    """
    return now_ms - BATCH_END_LEAD_S * 1000


def tick_event_time(batch_max_ms: int) -> int:
    """水印推进 tick 的 `event_time` —— 让水印**一定**越过最后一批窗口的 `windowEnd`。

    <h2>为什么必须有 tick（impl-02d 第四轮实机定位的第 7 个判据缺陷）</h2>
    作业的水印是 `forBoundedOutOfOrderness(120 s)`：`watermark = 最大事件时间 − 120 s`，
    且多分区时取**各分区最小值**。滚动窗口**首次** fire 的条件是
    `watermark > windowEnd`。固定批次被压在 `[now−250 s, now−200 s]`，于是：

    - 批次最后的窗口 `windowEnd ≈ now−140 s`，关它需要 `最大事件时间 ≥ windowEnd + 120 s`；
    - 而批次自己的最大值只有 `now−200 s` —— **永远差一大截**。

    实机证据（2026-10-22 12:00，`/tmp/span2.log` + Doris `updated_at`）：
    第 3 步「Doris 已静默 110 s ⇒ 视为落定」拍下的基线是
    `(10972, 21971, 1290, 1290)`；第 9 步再「静默 110 s」后是
    `(10974, 21973, 1294, 1294)`，差 `+2/+2/+4/+4`。而这些行的
    `updated_at` 全是 **04:01:06** —— 正是**下一轮**（span3 的 45 条）把水印
    推上去的那一刻。即：两轮看到的「静默」都是「窗口还没 fire」，
    比对的是两个都还没落库的空转状态；`+2/+4` 是**上一轮迟到落库**混进来的。

    ⇒ 修法：显式投一条**事件时间足够晚**的合法事件（tick）把水印顶过去，
    再等窗口真正 fire。这里再留 `ALLOWED_LATENESS_MS` + `TICK_PAD_MS`：
    前者让窗口越过「允许迟到」边界（此后即使有迟到重触发也已经定稿），
    后者吸收 1 ms 级边界抖动。

    ★ 为什么 tick 必须「每个分区一条」：水印取各分区最小值，只投一个分区
    推不动其它分区的水印，窗口还是不会关。
    """
    window_end = (batch_max_ms // WINDOW_MS + 1) * WINDOW_MS
    return window_end + ALLOWED_LATENESS_MS + TICK_PAD_MS


def build_tick_events(
    *, nonce: str, tick_time_ms: int, partitions: int = 6
) -> list[dict[str, object]]:
    """产出「水印推进 tick」事件：每个分区一条，**字节稳定**（可重复投递）。

    <h2>为什么必须是合法事件</h2>
    tick 走的是**主流**（同一 topic、同一套校验）。若它被判脏，就会自己
    推高 DLQ ⇒ 「DLQ 增量==0」判据被自己污染。故字段给全、事件类型取
    `page_view`（不在漏斗四步里，不会推进任何漏斗行；且它会让
    `event_page_view` 的计数行出现——这正是**期望**的：它在批次窗口内，
    基线里已经含它，重放被吸收后不变）。

    <h2>为什么 `event_id` 带 nonce 且每轮不同</h2>
    第 2 步（预热）与第 5 步（重投）投的是**同一份字节**，这是幂等证明的输入。
    而 tick 在两处都要投（预热后关窗口、恢复后再关窗口），若两次 tick
    字节相同，第二次会命中去重状态 → 被吸收（不影响 counts，但会白增
    `duplicate_absorbed` 旁证）。若字节**不同**（不同 event_id），第二次会
    以新 key 落**新行** ⇒ counts 上涨 ⇒ 假 FAIL。
    ⇒ tick 的字节在**本轮内固定**、**跨轮不同**（nonce 含时间戳），
      且 `tick_time_ms` 一轮只算一次，所有 tick 复用（预热与恢复后同步）。
    """
    events: list[dict[str, object]] = []
    for partition in range(partitions):
        events.append(
            {
                "partition": partition,
                "payload": {
                    "event_id": f"00000000-0000-4000-9000-{nonce}-{partition:02d}",
                    "event_time": tick_time_ms,
                    "user_id": f"tick-user-{nonce}",
                    "item_id": "tick-item",
                    "category_id": "tick-category",
                    "event_type": "page_view",
                    "payload": {"session_id": f"tick-user-{nonce}"},
                    "schema_version": "1.0",
                },
            }
        )
    return events


def tick_events_for_batch(
    batch: list[dict[str, object]], *, nonce: str, partitions: int = 6
) -> list[dict[str, object]]:
    """从批次的 `event_time` 直接算 tick（不留「谁记得传哪个最大值」的坑）。"""
    if not batch:
        raise ValueError("批次为空，无法计算水印推进 tick")
    latest = max(int(r["event_time"]) for r in batch)  # type: ignore[arg-type]
    return build_tick_events(
        nonce=nonce, tick_time_ms=tick_event_time(latest), partitions=partitions
    )


def batch_window_literals(batch: list[dict[str, object]]) -> list[str]:
    """批次覆盖的分钟窗口起点（Doris `window_start` 的**字面量**，UTC 形状）。

    ★ 必须按 **UTC** 格式化：Doris 里的 `window_start` 是按 epoch 直接存成
    `DATETIME`（无时区），实机读到的就是 `2026-10-22 03:53:00`（= epoch/60000）。
    若按本地 +08:00 格式化，SQL 一个字面量都匹配不到 ⇒ scoped 快照恒空
    ⇒ 判据退化成 `0 == 0` 的恒真断言（比原来的假 FAIL 更糟）。

    批次是闭区间 `[earliest, latest]`，窗口按 `WINDOW_MS` 对齐 ⇒ 端点各自取桶。

    ★ 只覆盖**批次自己的**窗口，**不含 tick 所在窗口**（理由见 `scope_windows`）。
    """
    if not batch:
        raise ValueError("批次为空，无法计算窗口集合")
    times = [int(r["event_time"]) for r in batch]  # type: ignore[arg-type]
    first = (min(times) // WINDOW_MS) * WINDOW_MS
    last = (max(times) // WINDOW_MS) * WINDOW_MS
    windows: list[str] = []
    boundary = first
    while boundary <= last:
        windows.append(_utc_literal(boundary))
        boundary += WINDOW_MS
    return windows


def _utc_literal(epoch_ms: int) -> str:
    """epoch ms → `YYYY-MM-DD HH:MM:SS`（UTC，与 Doris DATETIME 无时区存法一致）。"""
    from datetime import UTC, datetime

    return datetime.fromtimestamp(epoch_ms / 1000, UTC).strftime("%Y-%m-%d %H:%M:%S")


def scope_windows(batch: list[dict[str, object]]) -> list[str]:
    """本轮比对要覆盖的窗口集合 = 批次覆盖的分钟窗口（**不含 tick 窗口**）。

    ★ 为什么排除 tick 窗口：tick 的 `event_time` 被刻意推到
    `windowEnd + ALLOWED_LATENESS_MS + TICK_PAD_MS`，它自己那一分钟的窗口
    **在本轮内不会关闭**（没有更晚的事件能再推水印）。它的业务行
    （`event_page_view` / `uv`）要等到下一轮才会落库 —— 把它纳入比对，
    只会在某些时序下变成幽灵增量。故：tick 只负责**推水印**，不计入比对范围。
    """
    return batch_window_literals(batch)


def baseline_looks_populated(baseline: Baseline) -> bool:
    """基线非空护栏：两表在本批窗口里**至少有一行**，否则比对是 `0 == 0` 的恒真。

    这是 scoped 比对带来的新风险的对冲：窗口限定写错（时区、格式、集合为空）
    时，快照会全 0 并且「全等」轻松通过。护栏让这种失败**可见**。
    """
    return (
        baseline.window_metric_count > 0
        and baseline.funnel_step_count > 0
        and baseline.window_metric_sum > 0
        and baseline.funnel_step_sum > 0
    )


def _fixed_batch(path: str = FEED_BATCH_PATH, *, nonce: str) -> list[dict[str, object]]:
    """产出/读取**字节固定**的一批事件（幂等证明的输入）。

    ★ 为什么不能直接用 `ecomgen live`（impl-02d 实机证伪，见模块顶部表 B）：
    `live` 每轮把 `event_time` 平移到当前墙钟，同 seed 跑两遍得到的是
    「同 event_id、不同分钟窗口」的两批数据 ⇒ 第二遍必然长新行。
    这里改为：`dump` 一次 → 把整批 `event_time` 平移进本轮窗口
    （但**保持批内相对时序**）→ 落成固定 JSONL → **本轮的两遍投喂都读它**。

    ★★ `event_id` 必须**每轮换一次**（impl-02d 第四轮实机定位）★★
    同 seed 的 dump 每轮产出**同一批 event_id**，而作业的去重状态按
    event_id 保 1 小时（事件时间 TTL）。于是第 2 轮的预热批次会被第 1 轮
    留下的去重状态**整批吸收** —— 实测 span2 的基线在预热 45 条之后
    一行都没长（`10970→10972`），「本批窗口里没有行」直接把比对变成
    `0 == 0` 的恒真断言（`baseline_looks_populated` 就是拦这个的）。
    修法：本轮所有 event_id 用 `nonce` 重写（uuid5，确定性），
    **轮内两遍投喂仍逐字节相同**（幂等证明不受影响），跨轮不再撞车。

    ★ **每次运行都重新生成**（不跨轮复用磁盘上的旧文件）：旧文件的
    `event_time` 会越来越旧，而窗口清理按事件时间推进 ⇒ 复用旧批次会让
    迟到数据大面积变成「超期」，`ads_late_expired_count` 相关断言失真。
    「固定」的语义是**同一轮内两遍投喂逐字节相同**，不是跨轮冻结。
    """
    import json
    import uuid
    from pathlib import Path

    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)

    import subprocess
    import sys

    raw = target.with_suffix(".raw.jsonl")
    proc = subprocess.run(
        [
            sys.executable,
            "-m",
            "ecomgen.cli",
            "dump",
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
            "--out",
            str(raw),
        ],
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"dump 失败：{proc.stdout[-300:]}{proc.stderr[-300:]}")

    records = [
        json.loads(line)
        for line in raw.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    if not records:
        raise RuntimeError("dump 产出为空")
    latest = max(int(r["event_time"]) for r in records)  # type: ignore[arg-type]
    earliest = min(int(r["event_time"]) for r in records)  # type: ignore[arg-type]
    # ★★ 把 event_time 压进**单个窗口区间**（impl-02d 第三轮实机证伪）★★
    #   原始跨度由迟到偏移量程决定（约 310~640 s，跨 4~11 个分钟窗口）。
    #   本判据只关心「重放是否被幂等吸收」，与迟到/超期通道无关（那两条由
    #   `make verify` 的独立判据覆盖）。故把整批线性映射到
    #   `[end − 50 s, end]`（`end = now − BATCH_END_LEAD_S`，见 `batch_end_ms`）：
    #   **保持相对顺序**（同用户仍有序），只压缩绝对跨度 ⇒ 最多 2 个分钟窗口。
    #   ⚠️ 压缩后迟到事件可能不再「超期」，这是**有意的**。
    end_ms = batch_end_ms(int(time.time() * 1000))
    span_ms = BATCH_SPAN_S * 1000
    raw_span = max(1, latest - earliest)

    def compress(value: int) -> int:
        return end_ms - span_ms + int((value - earliest) * span_ms / raw_span)

    records.sort(key=lambda r: int(r["event_time"]))  # type: ignore[arg-type]
    namespace = uuid.uuid5(uuid.NAMESPACE_URL, "ecomgen:verify-fault")
    with target.open("w", encoding="utf-8", newline="\n") as handle:
        for record in records:
            record["event_time"] = compress(int(record["event_time"]))  # type: ignore[arg-type]
            # ★ 跨轮唯一的 event_id（见 docstring）：确定性 uuid5，轮内两遍一致
            record["event_id"] = str(
                uuid.uuid5(namespace, f"{nonce}:{record['event_id']}")
            )
            handle.write(
                json.dumps(
                    record, ensure_ascii=False, sort_keys=True, separators=(",", ":")
                )
            )
            handle.write("\n")
    raw.unlink(missing_ok=True)
    return records


def _feed_batch(records: list[dict[str, object]] | None = None) -> int:
    """把固定批次按速率投到 Kafka（分区键 = `user_id`，与 live 一致）。

    返回投递条数；失败返回 -1（调用方据此判 FAIL，不静默放过）。
    """
    try:
        from confluent_kafka import Producer
    except ImportError:
        print("  [feed] ✗ 缺少 confluent-kafka")
        return -1

    try:
        batch = records if records is not None else _fixed_batch(nonce="adhoc")
    except RuntimeError as exc:
        print(f"  [feed] ✗ {exc}")
        return -1

    producer = Producer(
        {"bootstrap.servers": FEED_BOOTSTRAP, "client.id": "verify-fault"}
    )
    interval = 1.0 / DUMP_RATE
    next_at = time.monotonic()
    sent = 0
    for record in batch:
        producer.produce(
            topic=FEED_TOPIC,
            key=str(record["user_id"]).encode(),
            value=json.dumps(
                record, ensure_ascii=False, sort_keys=True, separators=(",", ":")
            ).encode("utf-8"),
        )
        producer.poll(0)
        sent += 1
        next_at += interval
        sleep_for = next_at - time.monotonic()
        if sleep_for > 0:
            time.sleep(sleep_for)
        else:
            next_at = time.monotonic()
    producer.flush(30)
    latest = 0
    if batch:
        latest = int(batch[-1]["event_time"])  # type: ignore[arg-type]
    print(f"  [feed] 已投递 {sent} 条（最新 event_time={latest}）")
    return sent


def _topic_partition_count(topic: str = FEED_TOPIC) -> int | None:
    """读 topic 的分区数（**不写死 6**，见 `_feed_tick`）。

    代码评审指出：`partitions` 写死 6 时，若 topic 实际是 4 或 8 个分区，
    tick 会投到不存在的分区（被 broker 拒绝）或漏掉分区（那一边水印不动），
    而 `fault-watermark-tick` 只看「调用了几次 produce」就 PASS —— 又是恒真断言。
    分区数以 `kafka-topics.sh --describe` 的 `PartitionCount` 为准。
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
            topic,
        ]
    )
    if rc != 0:
        return None
    marker = "PartitionCount:"
    for line in out.splitlines():
        idx = line.find(marker)
        if idx < 0:
            continue
        token = line[idx + len(marker) :].strip().split()[0]
        if token.isdigit() and int(token) > 0:
            return int(token)
    return None


def _feed_tick(events: list[dict[str, object]]) -> int:
    """投「水印推进 tick」：每个分区一条，显式指定分区（水印取各分区最小值）。

    ★ 为什么必须显式分区：Kafka producer 默认按 hash(key) 分区，
    同一份 key 的多条会全落到**同一个**分区 ⇒ 其它分区的水印不动。
    这里按 `partition` 字段逐条指定，保证每个分区各有一条把水印顶上去。

    ★ 条数按**实际投递成功**计（代码评审发现）：旧实现只数 `produce()` 调用，
    分区不存在时 broker 报错也不影响返回值 ⇒ `ticked=6` 什么都证明不了。
    现在用 `flush()` 之后统计回调里的成功数：`produce` 的成功/失败都记在
    `_delivery_report`，失败条数与首条错误一起打印出来（失败原因要能复盘）。

    返回**成功投递**条数；producer 不可用 -1。
    """
    try:
        from confluent_kafka import Producer
    except ImportError:
        print("  [tick] ✗ 缺少 confluent-kafka")
        return -1

    producer = Producer(
        {"bootstrap.servers": FEED_BOOTSTRAP, "client.id": "verify-fault-tick"}
    )
    ok = 0
    errors: list[str] = []

    def _delivery_report(err: object, _msg: object) -> None:
        nonlocal ok
        if err is not None:
            if len(errors) < 3:
                errors.append(str(err))
            return
        ok += 1

    for event in events:
        payload = event["payload"]
        producer.produce(
            topic=FEED_TOPIC,
            partition=int(event["partition"]),  # type: ignore[arg-type]
            key=str(payload["user_id"]).encode(),  # type: ignore[index]
            value=json.dumps(
                payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
            ).encode("utf-8"),
            on_delivery=_delivery_report,
        )
        producer.poll(0)
    remaining = producer.flush(30)
    if remaining:
        errors.append(f"flush 后仍有 {remaining} 条未确认")
    tick_time = events[0]["payload"].get("event_time") if events else None  # type: ignore[union-attr]
    print(
        f"  [tick] 水印推进 tick 成功 {ok}/{len(events)} 条（event_time={tick_time}）"
    )
    for message in errors:
        print(f"  [tick]   ✗ {message}")
    return ok if not errors else -1


def _wait_replay_after_failover(
    kill_before: int | None, timeout_s: int = TIMEOUT_LAG_ZERO_S
) -> tuple[int | None, bool]:
    """恢复后等源端重读稳定，并**机读验证「计数器确实重置过」**。

    <h2>为什么光看「恢复后读数 > 0」不够（代码评审指出的恒真风险）</h2>
    旧实现 `replay = after_records`、`replay_observed = replay > 0`。
    它的前提是「failover 之后 `read-records` 从 0 重新计数」——这是**实测结论**，
    但代码里**没有任何断言**去证它：若某天 Flink 不重置计数器，那么恢复后
    任何一次读数都 > 0，`replay_observed` 立刻恒真，判据重新变成假断言。

    ⇒ 这里把前提本身变成判据：恢复后**必须观测到**读数掉到 kill 前之下
    （`min_seen < kill_before`，实测是 0）。只有观测到重置，「恢复后读数即重放条数」
    这个口径才成立；观测不到 ⇒ `reset_seen=False`，调用方据此判 FAIL
    （此时真实重放量应是 `after − kill_before`，而那条路已在 §9 被证伪为假 FAIL，
    所以本轮直接判「口径不适用」而不是猜一个数）。

    返回 `(稳定后读数, 是否观测到重置)`。
    """
    deadline = time.monotonic() + timeout_s
    last: int | None = None
    min_seen: int | None = None
    stable = 0
    while time.monotonic() < deadline:
        current = _source_records_in()
        print(f"  source read-records={current}（kill 前 {kill_before}）")
        if current is None:
            return None, False
        min_seen = current if min_seen is None else min(min_seen, current)
        if last is not None and current == last:
            stable += 1
            if stable >= STABLE_POLLS:
                reset_seen = kill_before is not None and min_seen < kill_before
                if not reset_seen:
                    print(
                        f"  ✗ 未观测到计数器重置（最小读数 {min_seen} ≥ kill 前 {kill_before}）"
                        " ⇒ 「恢复后读数即重放条数」的口径不成立"
                    )
                return current, reset_seen
        else:
            stable = 0
            last = current
        time.sleep(1)
    return last, False


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


def _snapshot_signature(windows: list[str]) -> tuple[int, ...] | None:
    """Doris 两表 count/sum 的指纹（限本批窗口；判「落库是否已稳定」用）。"""
    sample = snapshot_baseline(windows)
    if sample is None:
        return None
    return (
        sample.window_metric_count,
        sample.window_metric_sum,
        sample.funnel_step_count,
        sample.funnel_step_sum,
    )


def _wait_doris_settled(
    windows: list[str],
    timeout_s: int = 300,
    quiet_s: float = QUIET_S,
) -> Baseline | None:
    """等**本批窗口内**的 count/sum 指纹在 `quiet_s` 秒内一动不动，返回该快照。

    <h2>口径演进（三轮实机证伪）</h2>
    1. 「连续两次采样相等」（8 s）⇒ 之后又涨 34 行：窗口要等水印越过后才 fire。
       ⇒ 改成**静默窗口**。
    2. 「静默窗口 = 批次跨度 + 60 s」⇒ 仍不成立：窗口 fire 的充分条件是
       **水印**越过 `windowEnd + allowedLateness`，而水印由**最晚事件时间**决定。
       批次被压在 `now−200 s` 一带时，它自己的窗口永远等不到关闭（见
       `tick_event_time`）——「静默」只是「还没开始写」。⇒ 改为**先投 tick
       把水印顶过去**（见 `--`/`_feed_tick`），静默窗口恢复成固定值。
    3. 全表口径 ⇒ 上一轮的窗口行会混进来（`+2/+4` 假 FAIL）。
       ⇒ **限定本批窗口**（`scoped_snapshot_sql`）。

    ★ 另加**非空护栏**：本批窗口里两表都至少有行，否则「全等」是 `0 == 0`
    的恒真断言（scoped 口径写错时最危险，见 `baseline_looks_populated`）。

    超时或护栏不过 ⇒ None（判据不能用，不拿旧值凑数）。
    """
    deadline = time.monotonic() + timeout_s
    last: tuple[int, ...] | None = None
    quiet_since: float | None = None
    while time.monotonic() < deadline:
        current = _snapshot_signature(windows)
        now = time.monotonic()
        if current != last:
            last = current
            quiet_since = now
            print(f"  doris signature={current}（{DORIS_POLL_S * 1000:.0f} ms 后复查）")
        elif quiet_since is not None and now - quiet_since >= quiet_s:
            sample = snapshot_baseline(windows)
            if sample is None:
                return None
            if not baseline_looks_populated(sample):
                print(
                    f"  doris 已静默 {quiet_s:.0f} s，但本批窗口内快照为空"
                    f"（{sample}）⇒ 不作为基线"
                )
                return None
            print(f"  doris 已静默 {quiet_s:.0f} s ⇒ 视为落定（{len(windows)} 个窗口）")
            return sample
        time.sleep(DORIS_POLL_S)
    print(f"  doris 在 {timeout_s} s 内未取得 {quiet_s:.0f} s 静默窗口")
    return None


def _restored_count() -> int | None:
    """`/checkpoints` 的 `counts.restored` —— 「**确实**发生过一次恢复」的机读凭证。

    ★ 为什么需要它（impl-02d 实测）：`docker compose kill flink-tm` 之后，JM 有
    **约 14 s** 仍然报 `state==RUNNING`、`tasks.running==28`（TM 的心跳超时还没到）；
    这一瞬间任何只看 `RUNNING` 的判据都会假通过。实测该字段在真恢复后 +1
    （`restored 9 → 10`），是这条事实最干净的观测面。
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
    counts = data.get("counts")
    if not isinstance(counts, dict):
        return None
    value = counts.get("restored")
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def _tasks_summary() -> tuple[str, int, int] | None:
    """(state, running, total) —— 用于区分「看着 RUNNING」与「真的在跑」。"""
    rc, out = _run(["curl", "-sf", "-m", "3", f"{FLINK_UI}/jobs/overview"], timeout=10)
    if rc != 0:
        return None
    try:
        jobs = json.loads(out).get("jobs") or []
    except json.JSONDecodeError:
        return None
    for job in jobs:
        state = str(job.get("state") or "")
        if state not in {"RUNNING", "RESTARTING", "FAILED", "CANCELLED"}:
            continue
        tasks = job.get("tasks") or {}
        return (
            state,
            int(tasks.get("running") or 0),
            int(tasks.get("total") or 0),
        )
    return None


def _poll_job_recovered(
    restored_before: int | None,
    timeout_s: int = TIMEOUT_RESTART_S,
) -> bool:
    """轮询到「**真的**恢复过一次」的 RUNNING（超时 120 s，设计值）。

    ★ 旧实现只等 `state==RUNNING and running==total`，实机第一次轮询就满足
    （kill 的效果还没被 JM 观察到）⇒ 假通过，`restored` 不动、根本没有恢复。
    这里要求三件事**同时**成立：
      1. 先看到过 `running < total`（或非 RUNNING）—— 证明 JM 已察觉故障；
      2. 之后回到 `running == total > 0`；
      3. `counts.restored` 严格大于 fault 前的值 —— 机读凭证。
    """
    deadline = time.monotonic() + timeout_s
    saw_down = False
    attempt = 0
    while time.monotonic() < deadline:
        attempt += 1
        summary = _tasks_summary()
        restored = _restored_count()
        if summary is None:
            print(f"  [{attempt}] REST 不可达")
            time.sleep(POLL_INTERVAL_S)
            continue
        state, running, total = summary
        if total > 0 and (running < total or state != "RUNNING"):
            saw_down = True
        print(
            f"  [{attempt}] {state}({running}/{total}) restored={restored} "
            f"saw_down={saw_down}"
        )
        if (
            saw_down
            and state == "RUNNING"
            and total > 0
            and running == total
            and restored is not None
            and restored_before is not None
            and restored > restored_before
        ):
            return True
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


def _feed_tick_verified(tick_events: list[dict[str, object]], *, when: str) -> int:
    """投 tick、确认源端真读到了。`when` 只用于打印（预热后 / 恢复后）。

    ★ 为什么恢复后要再投一次：failover 会把**窗口状态**回滚到 checkpoint 点，
    但**水印**是流里的运行态、随 source offset 一起回滚 ⇒ 恢复后水印可能退回
    批次末尾之下，已 fire 的窗口不会重新 fire，未 fire 的窗口更不会。
    再投同一批 tick（字节相同 ⇒ 命中去重/唯一键，counts 不变）把水印再顶一次，
    让「恢复后该关的窗口」真的关、该写的行真的写。

    ★ 为什么带 `at_least`（同一个坑的复发点，见 `_wait_source_stable`）：
    投完立刻轮询时源端**还没开始读**，读数与投递前相同 ⇒「连续两次不变」当场成立
    ⇒ 误判「已读完」，于是紧接着的静默等待会在 tick 尚未被消费时就开始计时。
    这里要求读数涨到「投递前 + tick 条数」才算真读完。
    """
    before = _source_records_in() or 0
    sent = _feed_tick(tick_events)
    if sent < 0:
        return sent
    read = _wait_source_stable(at_least=before + sent, timeout_s=60)
    print(f"  [tick] {when}：源端 read-records={read}（投递前 {before} + tick {sent}）")
    return sent


def run_verify_fault(
    report_dir: str = "artifacts/verify",
    *,
    killer: Callable[[], int] | None = None,
    corrupt: bool = False,
) -> VerifyReport:
    """故障注入全流程（spec §4.4 流程图逐行）。

    顺序（11 步，impl-02d 实机定稿；`spec.md` §4.4 定的是**判据**，步序是实现细节）：

    ```
    1  等外部输入冻结
    2  喂固定批次（预热：让这批 key 先进库并落定）
    2b 投水印推进 tick（否则本批窗口永远不会关闭，见 `tick_event_time`）
    3  等本批窗口静默落定 → 拍基线（静默窗口 30 s，设计值）
    4  等一次 checkpoint 完成（时序锚点）
    5  再投**同一份字节** → 等源端读完 → 确认 checkpoint 未前进
    5b 反例模式才执行：注入 5 条 schema_invalid（票面验收第 7 条）
    6  kill TM → docker start → 等**真的**恢复（saw_down + restored 递增）
    7  等重放读完（并断言计数器确实重置过，见 `_wait_replay_after_failover`）
    7b 重投同一批 tick（恢复后水印已回滚，需再顶一次）
    8  等新 checkpoint（300 s 设计值）
    9  等本批窗口落定 → 拍恢复后快照 → 判三条件
    ```

    ★ 为什么第 2 步要先「预热」一遍（impl-02d 实测）：幂等吸收发生在 Doris 的
    UNIQUE KEY 与 Flink 的 event_id 去重状态上。若这批 key **从未出现过**，
    第一次到达就得**建行**（窗口还要等水印才 fire），恢复后必然出现
    「第一次才落库」的新行——那不是幂等失败，是「数据本来就是新的」。
    预热让这批 key 先落定；第 5 步再投同一份字节时，它们要么命中去重状态
    （走 duplicate 通道），要么命中 Doris 唯一键（覆盖写），两表 count/sum 才
    真正「全等」。实测：预热 + 同字节重投 + kill ⇒ replay=263、counts 全等、
    DLQ=0。
    """
    checks: list[CheckResult] = []
    print("=== make verify-fault：故障恢复 + 幂等吸收证明 ===")

    # ★ 本轮的唯一标识：用来把 event_id 换成**跨轮不撞车**的一套（见 `_fixed_batch`）。
    #   不带它，第 2 轮的预热会被第 1 轮留下的 1 小时去重状态整批吸收，
    #   本批窗口里一行都长不出来 ⇒ 比对退化成 `0 == 0`。
    nonce = f"{int(time.time() * 1000):x}"
    print(f"[verify-fault] 本轮 nonce={nonce}")

    print("--- 1/9 等外部输入冻结（源端读入不再增长）")
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

    print(
        f"--- 2/9 准备并投喂固定批次（seed={DUMP_SEED}, {DUMP_SESSIONS} 会话）"
        "—— 预热，让这批 key 先落库并关窗口"
    )
    try:
        batch = _fixed_batch(nonce=nonce)
    except RuntimeError as exc:
        checks.append(
            CheckResult(
                Status.FAIL, "fault-feed-dump", str(exc)[:200], "可产出固定批次"
            )
        )
        return _finish(checks, report_dir)
    # ★ tick 的 event_time 由批次算出、**本轮固定**：预热与恢复后投的是同一批字节
    #   （否则第二次会以新 event_id 落新行 ⇒ counts 上涨 ⇒ 假 FAIL）。
    #   分区数从 topic 元数据读（不写死 6）：漏一个分区，那一边的水印就不动。
    partitions = _topic_partition_count() or 6
    print(f"    topic {FEED_TOPIC} 分区数={partitions}")
    tick_events = tick_events_for_batch(batch, nonce=nonce, partitions=partitions)
    tick_time = int(tick_events[0]["payload"]["event_time"])  # type: ignore[index]
    fed = _feed_batch(batch)
    # ★ 必须给 `at_least`：投递刚返回时源端通常**还没开始读**，读数与投递前相同，
    #   若只判「连续两次不变」会当场误判「已读完」（实测踩过：预热 263 条投完，
    #   read-records 仍是投递前的 268，下一轮才涨到 794）。
    drained = _wait_source_stable(at_least=(idle or 0) + max(fed, 0))
    checks.append(
        CheckResult(
            Status.PASS if fed > 0 and drained is not None else Status.FAIL,
            "fault-feed-dump",
            f"sent={fed} read-records={drained}",
            "投递成功且源端已读完",
        )
    )
    if fed <= 0 or drained is None:
        return _finish(checks, report_dir)

    # 本批覆盖的分钟窗口（比对范围）与 tick 的推进目标
    windows = scope_windows(batch)
    print(f"    本批窗口：{', '.join(windows)}；水印推进 tick event_time={tick_time}")

    print("--- 2b/9 投水印推进 tick，让本批窗口真正关闭（设计值，见 tick_event_time）")
    ticked = _feed_tick_verified(tick_events, when="预热后")
    checks.append(
        CheckResult(
            Status.PASS if ticked > 0 else Status.FAIL,
            "fault-watermark-tick",
            f"tick={ticked} 条 event_time={tick_time}",
            "每个分区一条 tick，把水印推过本批窗口",
        )
    )

    print(f"--- 3/9 等本批窗口静默落定（静默窗口 {QUIET_S} s，设计值）后拍基线")
    before = _wait_doris_settled(windows)
    if before is None:
        checks.append(
            CheckResult(
                Status.FAIL,
                "fault-baseline",
                "本批窗口内落库未达静默窗口/为空",
                "两表 count/sum 可读、已落定且非空",
            )
        )
        return _finish(checks, report_dir)
    print(f"    基线：{before}")
    checks.append(CheckResult(Status.PASS, "fault-baseline", str(before), "快照成功"))

    print("--- 4/9 等一次 checkpoint 完成（时序锚点）")
    anchor = _checkpoint_number()
    anchored = _wait_checkpoint_after(anchor) if anchor is not None else False
    # ★ 锚点要用**确认完成后的那一个编号**，不是探测前的编号。
    #   实机踩过：`_wait_checkpoint_after` 确认时编号已经从 90 走到 91，
    #   而第 5 步拿 91 与「探测前」的 90 比较 ⇒ 误判「已被 checkpoint 覆盖」，
    #   白丢一条本应 PASS 的 `fault-replay-window`。
    anchor = _checkpoint_number() if anchored else anchor
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

    print("--- 5/9 再投**同一份字节**，等源端读完，并确认 checkpoint 尚未前进")
    fed_replay = _feed_batch(batch)
    # 同上：必须等读数涨到「投递前 + 投递条数」，否则会把「还没开始读」当「读完了」。
    read_at_kill = _wait_source_stable(
        at_least=(drained or 0) + max(fed_replay, 0), timeout_s=60
    )
    checkpoint_before = _checkpoint_number()
    restored_before = _restored_count()
    print(
        f"    kill 前：checkpoint={checkpoint_before}（锚点 {anchor}）"
        f" restored={restored_before} read-records={read_at_kill}"
    )
    # ★ 若 checkpoint 已前进，这批 offset 已提交 ⇒ 恢复后不会重读 ⇒ 重放必为 0。
    #   这不是「幂等失败」，是**本轮时序没卡进窗口**，要如实报出来（含实际编号差）。
    in_window = (
        checkpoint_before is not None
        and anchor is not None
        and checkpoint_before <= anchor
    )
    checks.append(
        CheckResult(
            Status.PASS if fed_replay > 0 and in_window else Status.FAIL,
            "fault-replay-window",
            f"sent={fed_replay} checkpoint={checkpoint_before}（锚点 {anchor}）",
            "重投时 checkpoint 未前进（数据仍在途）",
        )
    )

    # 反例注入前的脏率快照（仅 corrupt 模式用）—— 记账要以它为**基线原点**，
    # 不能以「收尾时的当前值」为原点（那会洗白同窗口内的真实退化）。
    dirty_before_injection: DirtyRateSample | None = None
    injected = 0
    if corrupt:
        # ★ 反例证伪（票面验收第 7 条）：故意把**脏数据**投进去，让 DLQ 真的上涨。
        #   判据若仍然 PASS，说明它不是「在断言」而是恒真。
        #   这里必须走**与正常投递同一条路径**（同一 topic、同一分区键语义），
        #   否则证伪不成立：只测一个旁路通道证明不了主判据会 fail。
        from ecomgen.verify_runner import sample_dirty_rate

        dirty_before_injection = sample_dirty_rate()
        print(
            "--- 5b/9 ⚠️ 反例注入：投 "
            f"{CORRUPT_INJECT_COUNT} 条 schema_invalid 事件（缺 event_type）"
        )
        injected = _inject_dirty(CORRUPT_INJECT_COUNT)
        print(f"    已投脏数据 {injected} 条")
        checks.append(
            CheckResult(
                Status.PASS if injected == CORRUPT_INJECT_COUNT else Status.FAIL,
                "fault-counterexample-injected",
                f"injected={injected}",
                str(CORRUPT_INJECT_COUNT),
            )
        )

    print("--- 6/9 kill TaskManager 并等**真的**恢复")
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
    print(f"    等 RUNNING + restored 递增（超时 {TIMEOUT_RESTART_S} s，设计值）")
    running = _poll_job_recovered(restored_before)
    checks.append(
        CheckResult(
            Status.PASS if running else Status.FAIL,
            "fault-job-restarted",
            "已恢复（saw_down + restored 递增）"
            if running
            else f"超时 {TIMEOUT_RESTART_S} s",
            "先见 running<total，再回到全跑且 restored 递增",
        )
    )

    print("--- 7/9 等重放读完（failover 重置计数器 ⇒ 恢复后读数即重放条数）")
    after_records, counter_reset = (
        _wait_replay_after_failover(read_at_kill) if running else (None, False)
    )
    # ★ 口径（impl-02d 实机验证 + 本轮补机读断言）：failover 之后 `read-records`
    #   **从 0 重新计数**（实测 kill 前 526 → 恢复后稳定在 263，正是这批的条数）。
    #   因此「重放条数」就是恢复稳定后的读数本身，**不是**恢复后减 kill 前
    #   （旧口径 `after - before` 在计数器重置时恒为负 ⇒ 假 FAIL）。
    #   ★ 但「重置」这个前提**必须自己也是断言**：由 `_wait_replay_after_failover`
    #     验证 `min_seen < kill 前`，验证不到就整体 FAIL（否则「恢复后读数>0」
    #     在计数器不重置时恒真，判据又变回假断言）。
    replay = after_records if after_records is not None else 0
    replay_observed = replay > 0 and counter_reset
    lag_drained = _wait_lag_zero() if running else False  # 辅助信息，不参与判定
    checks.append(
        CheckResult(
            Status.PASS if replay_observed else Status.FAIL,
            "replay-observed",
            f"源端重读 {replay} 条（kill 前 {read_at_kill}），"
            f"计数器已重置={counter_reset}，lag 归零={lag_drained}",
            ">0 且观测到 failover 重置计数器（证明 kill 后确有重放）",
        )
    )

    # ★ 恢复后必须**再投一次同一批 tick**：failover 把水印随 source offset 一起
    #   回滚，恢复后的水印可能又退回批次末尾之下 ⇒ 已 fire 的窗口不会重 fire、
    #   没 fire 的更不会。同一批字节（同 event_id）⇒ 走唯一键/去重状态被吸收，
    #   counts 不变（这正是要证的），但水印被重新顶过去 ⇒ 恢复后该落库的行落库。
    if running:
        print("--- 7b/9 重投同一批水印 tick（恢复后水印已回滚，需再顶一次）")
        _feed_tick_verified(tick_events, when="恢复后")

    print(f"--- 8/9 等一次新 checkpoint（超时 {TIMEOUT_CHECKPOINT_S} s，设计值）")
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

    print(
        f"--- 9/9 等落定（本批 {len(windows)} 个窗口，静默 {QUIET_S} s 设计值）后拍快照并比对"
    )
    after = _wait_doris_settled(windows)
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

    if corrupt:
        # ★ 夹具脏数据必须**按注入条数**记账进脏率基线（impl-02e 收口实测的判据死锁）。
        #   不加这一步，反例注入的 schema_invalid 会永久顶高 DLQ offset，
        #   而基线只在 PASS 时前推 ⇒ 棘轮被夹具顶住后**再也回不去**，
        #   此后每次 `make verify` 的 dlq-rate-no-increase 都恒 FAIL。
        #   实机证据见 `absorb_fixture_dirty` 的 docstring（247→257→267→277 三连注入）。
        #
        #   ⚠️ 三条边界（code-review 指出后加固）：
        #     1. **只记账 injected 条**，不把基线重立到当前值 —— 否则同窗口内的
        #        真实退化会被洗白进安全上界，判据从「恒假」翻成「恒真」。
        #     2. 只在**确有注入**时记账（injected > 0）；注入失败就什么都不动。
        #     3. 只在本次**跑到收尾**时记账（after 已取到，说明流程完整）；中途
        #        异常退出的环境状态不可信，不该被当成新的安全上界。
        #   记账结果作为一条 CheckResult 落进 JSON 报告（spec §4.5 三层痕迹），
        #   不只是 print。
        from ecomgen.verify_runner import absorb_fixture_dirty

        plan = (
            absorb_fixture_dirty(
                dirty_before_injection,
                after.dlq_offset if after is not None else None,
                injected,
                report_dir,
            )
            if after is not None
            else None
        )
        if plan is not None:
            print(
                f"[verify-fault-corrupt] 反例夹具已记账进脏率基线："
                f"dlq {plan.absorbed.dlq_offset}/{plan.absorbed.input_events}"
                f"（观测增量 {plan.observed_delta}，注入 {plan.injected}，"
                f"可归因上限 {plan.attributable_ceiling}）"
            )
            checks.append(
                CheckResult(
                    Status.PASS if plan.attributable else Status.FAIL,
                    "fault-counterexample-absorbed",
                    f"观测增量 {plan.observed_delta}（注入 {plan.injected}）"
                    f" vs 可归因上限 {plan.attributable_ceiling}",
                    "夹具增量须可归因（<= 注入条数 × 重放倍数，设计值）",
                    None
                    if plan.attributable
                    else "增量超出上限：可能有**非夹具**的脏混入，"
                    "记账已含它，下次 verify 会据此判定",
                )
            )
        else:
            print(
                "[verify-fault-corrupt] ⚠️ 夹具未记账（注入=0 或流程未跑完）"
                "—— 下次 verify 可能误报 FAIL"
            )

    print("--- 写报告")
    report = _finish(checks, report_dir)
    if report.exit_code() != 0:
        _dump_fault_snapshot(
            report_dir,
            before,
            after,
            verdict,
            replay_observed,
            replay,
            rc,
            extra={
                "checkpoint_anchor": anchor,
                "checkpoint_at_kill": checkpoint_before,
                "restored_before_kill": restored_before,
                "read_records_at_kill": read_at_kill,
                "read_records_after_recovery": after_records,
                "counter_reset_observed": counter_reset,
                "replay_window_ok": in_window,
                "quiet_window_s": QUIET_S,
                "scope_windows": windows,
                "watermark_tick_event_time": tick_time,
                "run_nonce": nonce,
                "timeouts_s": {
                    "restart": TIMEOUT_RESTART_S,
                    "checkpoint": TIMEOUT_CHECKPOINT_S,
                    "lag_zero": TIMEOUT_LAG_ZERO_S,
                },
            },
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
    extra: dict[str, object] | None = None,
) -> None:
    """失败时把关键快照落进 artifacts/（票面验收第 7 条：供事后复盘）。

    verify 的失败痕迹是「原始响应全文」，fault 的失败痕迹是**三条件实测值**：
    基线、恢复后、裁决明细。缺了它，事后只能看一行 reason。

    `extra` 追加时序取证（锚点编号、kill 时编号、restored、两处 read-records、
    静默窗口与各超时值）——「重放=0」这类失败光看三条件分不清是
    「没卡进窗口」还是「判据读错字段」，必须把编号差一并留下。
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
    if extra:
        payload.update(extra)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(f"[verify-fault] 失败快照已写入 {path}")


def main(argv: list[str] | None = None) -> int:
    """CLI 入口：`uv run python -m ecomgen.fault [报告目录] [--corrupt]`。

    `--corrupt` 是**票面验收第 7 条**要求的反例模式：往主流里投脏数据，
    此时 `make verify-fault` 必须**退出非 0**（证明断言真的在断言）。
    它复用**同一条流程与同一套判据**——若另写一条「故意失败」的旁路，
    就证明不了真判据会 fail。
    """
    import argparse

    parser = argparse.ArgumentParser(
        prog="ecomgen.fault", description="verify-fault：故障恢复 + 幂等吸收证明"
    )
    parser.add_argument("report_dir", nargs="?", default="artifacts/verify")
    parser.add_argument(
        "--corrupt",
        action="store_true",
        help="反例模式：注入脏数据，期望退出非 0（票面验收第 7 条）",
    )
    parsed = parser.parse_args(argv)
    return run_verify_fault(parsed.report_dir, corrupt=parsed.corrupt).exit_code()


if __name__ == "__main__":
    raise SystemExit(main())
