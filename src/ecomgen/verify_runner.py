"""verify 检查执行器：真连容器/HTTP 做断言（spec.md §4.3、§4.4）。

**分层**：
- 本模块的每个 `check_*` 函数**只做一件事**：采集一个实测值，返回 `CheckResult`。
- 判定逻辑抽成纯函数（`check_*_from_*`），可单测；默认路径才碰真实环境。

P1 阶段的五项（spec §4.3 表）：**4 真 + 1 显式 SKIP**。
第三项「Paimon 五层有行」在 P1 **显式 SKIP**——不计通过也不计 fail。
**impl-03f（P2）起第三项已转真断言**：五层各 `COUNT(*)>0`（走 Flink SQL
探针 `sql/verify_paimon_layers.sql`，非 Doris 的 `total_record_count`），
状态参与退出码 —— spec §4.3 的 P2 行「五项真断言」。

附加断言（spec §4.3）：
- 漏斗单调（`cnt_inferred` 沿 step_no 非增）
- **脏率不升**（分子＝DLQ 总 offset，分母＝输入 topic 总 offset，与基线名次比较：
  取不到基线 ⇒ SKIP，**不是** PASS——旧实现「取到 offset 就 PASS」是恒真假通过）
- **`is_settled` 两态**（暂定 0 与落定 1 都必须 >0；只有落定 ⇒ FAIL）
- **探针对账**（impl-03f）：受控探针批 → 两轮逐分区踢水位 → Doris 落定行
  与同源公式复算**逐值严格相等**（is_settled=0 不判，spec §5.4）
- **F6 降级阶梯**（`docker inspect` 的 OOMKilled/exit 137 计数 ≥ 阈值 ⇒ FAIL；
  阈值是**设计值**，实测值写进报告 `degradation` 字段）

所有超时/阈值数字一律标 **设计值**，非官方保证（票面验收第 10 条）。
"""

from __future__ import annotations

import itertools
import json
import os
import re
import subprocess
import sys
import tempfile
import time
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

# 反例夹具的「可归因重放倍数」：注入的那批字节在 failover 后可能被重放，
# 于是同一份脏能进 DLQ 多次。取 3 = 1 次原始投递 + 至多 2 次重放。
# **设计值**，非官方保证；实测一轮是 2×（注入 5 → DLQ +10，报告 063129Z）。
ATTRIBUTABLE_REPLAY_FACTOR = 3

# ── topic 配置的期望值：唯一来源 gdd §A.2（与 init-kafka.sh 的两个默认值同源）──
TOPIC_RETENTION_MS = "604800000"  # 7 天（gdd §A.2 / REQ-F-MQ-03）
TOPIC_RETENTION_BYTES = "2147483648"  # 2 GB（gdd §A.2；按分区生效，见 gdd §D D3）

DORIS_DB = "ecom_ads"
# ADS 三张表：与 docker/doris/init.sql 的 DDL 逐字对齐（本判据只读，不改表）。
ADS_TABLES: tuple[str, ...] = (
    "ads_window_metric",
    "ads_funnel_step",
    "ads_late_expired_count",
)

# ── 第三项「Paimon 五层有行」的探针面（impl-03f 交付物 3）──────────────
# 层词汇表＝spec §5.3 的五值（ods|dwd|dim|dws|ads）；dim＝五张维表合计
# （与 replay_30_dim.sql 的 layer=dim rows= 同口径）。
PAIMON_LAYER_TABLES: tuple[str, ...] = ("ods", "dwd", "dim", "dws", "ads")
# 探针 SQL 经只读挂载 ./sql:/sql:ro 进容器（与 lake_assert.sh 同一执行面）。
PAIMON_LAYERS_CONTAINER = "eco-flink-jm"
PAIMON_LAYERS_SQL = "verify_paimon_layers.sql"
# 探针五条 SELECT（每层一条，含 SET/CATALOG 头）在批模式下实测 ~2 min 量级；
# 60 s 会掐断 ADS 那条最贵的扫描。**设计值**，非官方保证。
PAIMON_LAYERS_TIMEOUT_S = 300


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
    """1/5 Kafka topic 的 retention 与 gdd §A.2 一致。

    ★ 读法与施加方**必须读同一处**（impl-02c 修的就是这一点）：
    `docker/scripts/init-kafka.sh` 用 `kafka-configs.sh --alter --entity-type topics`
    写的是**动态主题配置**，所以判据也用 `kafka-configs.sh --describe` 读同一面。

    旧实现用 `kafka-topics.sh --describe` 读，其 `Configs:` 列在 Kafka 4.3.1
    **不保证**包含 retention —— P0 实测只有 `min.insync.replicas=1`，
    于是判据恒假 FAIL（施加明明成功，判据却永远看不到）。
    """
    rc, out = _run(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "kafka",
            "/opt/kafka/bin/kafka-configs.sh",
            "--bootstrap-server",
            "localhost:9092",
            "--describe",
            "--entity-type",
            "topics",
            "--entity-name",
            KAFKA_TOPIC,
        ]
    )
    configs = parse_dynamic_topic_configs(out) if rc == 0 else None
    return check_kafka_retention_from_configs(configs, raw=out)


def parse_dynamic_topic_configs(out: str) -> dict[str, str] | None:
    """解析 `kafka-configs.sh --describe --entity-type topics` 的**动态**配置。

    Kafka 4.3.1 实测格式：
    ```
    Dynamic configs for topic ecom.behavior.event.v1 are:
      retention.bytes=2147483648 sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:...}
      retention.ms=604800000 sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:...}
    ```
    没有 `Dynamic configs ... are:` 头 ⇒ 取不到，返回 None（**不**当「没有配置」
    静默通过）；头部存在但块内为空 ⇒ 返回 `{}`，由判定函数判 FAIL。
    """
    in_block = False
    configs: dict[str, str] = {}
    for line in out.splitlines():
        token = line.strip()
        if token.startswith("Dynamic configs"):
            in_block = True
            continue
        if not in_block or not token:
            continue
        name, sep, rest = token.partition("=")
        if not sep:
            continue
        configs[name] = rest.split()[0]
    return configs if in_block else None


def check_kafka_retention_from_configs(
    configs: dict[str, str] | None, raw: str = ""
) -> CheckResult:
    """retention 判据（纯函数，可单测）：两个键都必须与 gdd §A.2 全等。

    - 取不到动态配置 ⇒ FAIL（不是 SKIP：topic 存在与配置可读是本判据的前提）；
    - 任一键缺失或不等于期望值 ⇒ FAIL，`detail` 留原始响应供人对照；
    - 两者全等 ⇒ PASS。
    """
    name = "kafka-topic-retention"
    expected = (
        f"retention.ms={TOPIC_RETENTION_MS} 且 "
        f"retention.bytes={TOPIC_RETENTION_BYTES}（gdd §A.2）"
    )
    if configs is None:
        return CheckResult(Status.FAIL, name, "动态配置取不到", expected, raw)

    ms = configs.get("retention.ms")
    by = configs.get("retention.bytes")
    actual = f"retention.ms={ms or '缺失'} retention.bytes={by or '缺失'}"
    if ms == TOPIC_RETENTION_MS and by == TOPIC_RETENTION_BYTES:
        return CheckResult(Status.PASS, name, actual, expected)
    return CheckResult(Status.FAIL, name, actual, expected, raw or actual)


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


_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")
_TABLE_CELL_RE = re.compile(r"^\|\s*([^|]+?)\s*\|\s*([^|]+?)\s*\|$")


def parse_layer_rows_from_stdout(stdout: str) -> dict[str, int]:
    """从 sql-client 的 TABLEAU stdout 抽「层名 → 行数」（纯函数，可单测）。

    解析纪律与 `lake_assert.sh` 的 parse_stdout 同源（spec §5.5）：
    - 只认以 ``|`` 开头**且恰好两列**的表格行 —— 回显行以 ``>`` /
      ``Flink SQL>`` 开头，天然排除（sql-client 会逐字回显 SQL 源码含注释，
      注释里的 ``| ods | 999 |`` 字面量不会污染结果）；
    - 第一列必须是层词汇表（ods/dwd/dim/dws/ads）之一 ⇒ 表头
      （``layer_name``）、分隔行（``+---+---+``）天然不匹配；
    - 第二列去 ANSI 色后必须是纯数字，否则该行被忽略。

    解析不到任何层 ⇒ 返回 ``{}``，由判定函数按「层缺席」判 FAIL
    （03e F6 已证伪的假绿通道：缺席不等于通过）。
    """
    counts: dict[str, int] = {}
    for line in stdout.splitlines():
        match = _TABLE_CELL_RE.match(_ANSI_RE.sub("", line))
        if match is None:
            continue
        layer, value = match.group(1), match.group(2)
        if layer not in PAIMON_LAYER_TABLES or not value.isdigit():
            continue
        counts[layer] = int(value)
    return counts


def check_paimon_layers_from_counts(
    counts: dict[str, int] | None, raw: str = ""
) -> CheckResult:
    """五层行数判据（纯函数，可单测）：每层都必须被报告且 >0。

    - `counts is None`（探针跑不出来：容器缺失/超时/docker 层失败）⇒ **FAIL**
      —— P2 起这是真断言，「查不到」是失败，不是 P1 那种显式 SKIP；
    - 某层**缺席**（探针没输出该层，如中途 SQL 报错被 sql-client 中止）⇒ FAIL，
      记为「不可查」—— 判定面必须证明「每层都被报告过」，而不是
      「报告过的层都 >0」（03e F6 的同款假绿通道）；
    - 某层 = 0 ⇒ FAIL，`actual` 逐层点名（好层的状态不被掩盖）；
    - 五层齐且全 >0 ⇒ PASS。
    """
    name = "paimon-five-layers"
    expected = "五层各 COUNT(*)>0（走 Flink SQL，非 total_record_count）"
    if counts is None:
        return CheckResult(Status.FAIL, name, "探针执行失败", expected, raw)

    parts: list[str] = []
    for layer in PAIMON_LAYER_TABLES:
        value = counts.get(layer)
        if value is None:
            parts.append(f"{layer}=不可查 FAIL")
        elif value > 0:
            parts.append(f"{layer}={value} PASS")
        else:
            parts.append(f"{layer}={value} FAIL")

    actual = " ".join(parts)
    if any(
        counts.get(t) is None or (counts.get(t) or 0) <= 0 for t in PAIMON_LAYER_TABLES
    ):
        return CheckResult(Status.FAIL, name, actual, expected, raw or actual)
    return CheckResult(Status.PASS, name, actual, expected)


def collect_paimon_layer_counts() -> dict[str, int] | None:
    """在 flink-jm 容器里跑五层探针，返回「层名 → 行数」；执行失败 ⇒ None。

    执行面与 `lake_assert.sh` 同源：`./sql` 只读挂载为容器内 `/sql`
    （docker-compose.yml 的 ``- ./sql:/sql:ro``），`MSYS_NO_PATHCONV=1`
    防 Windows 宿主把 `/sql/...` 翻译成路径。⚠️ docker 退出码**不能**当
    成功判据（sql-client 报错仍退 0，spec §5.5）—— 只看解析出的层行数。

    ★★ 批探针与流作业**共用** session 集群的 slot 池（实机 2026-10-24）：
    本仓 TM 仅 4 slot、流作业 28 任务常态占满 ⇒ 探针以默认并行度 4 提交时
    等不到 slot，PendingSlotRequest 超时后批作业 FAILED 且
    `NoRestartBackoffTimeStrategy` 不重启 —— 第三项转真后的第一个实测教训。
    ⇒ 探针 SQL 里已 `SET 'pipeline.parallelism' = '1'`（见 sql/verify_paimon_layers.sql），
    单 slot 即可跑完；本函数不再额外改会话。
    """
    rc, out = _run(
        [
            "docker",
            "exec",
            "-i",
            PAIMON_LAYERS_CONTAINER,
            "/opt/flink/bin/sql-client.sh",
            "-f",
            f"/sql/{PAIMON_LAYERS_SQL}",
        ],
        timeout=PAIMON_LAYERS_TIMEOUT_S,
    )
    if rc != 0:
        return None
    return parse_layer_rows_from_stdout(out)


def check_paimon_layers() -> CheckResult:
    """3/5 Paimon 五层有行 —— **impl-03f（P2）起真断言**（spec §4.3）。

    P1 时期这里是显式 SKIP（不计通过不计 fail）；P2 按 spec §4.3 的
    「五项真断言」转真：五层各 `COUNT(*)>0`，走 Flink SQL 探针
    （`sql/verify_paimon_layers.sql`），**非** Doris 的 `total_record_count`
    （Paimon 系统表不在 Doris 支持列表内，票面 Notes 冻结）。
    """
    return check_paimon_layers_from_counts(collect_paimon_layer_counts())


def check_doris_ads_rows() -> CheckResult:
    """4/5 Doris ADS 真有行 —— 经 client 服务**逐表**查 COUNT(*)（票面 ③）。

    ★ impl-02c：旧实现把三张表捆成一个 PASS/FAIL，超期表为空时另外两张的
    好状态被连带掩盖。现在逐表查、逐表判，并在 `actual` 里逐表写
    `表名=行数 PASS/FAIL`，使「两张绿、一张红」在 stdout 与 JSON 报告里都可见。
    """
    counts: dict[str, int] = {}
    raw_parts: list[str] = []
    for table in ADS_TABLES:
        rc, out = _doris_scalar_runner(f"SELECT COUNT(*) FROM {DORIS_DB}.{table};")
        raw_parts.append(f"--- {table} (rc={rc}) ---\n{out}")
        value = parse_scalar(out) if rc == 0 else None
        if value is None:
            return check_doris_ads_rows_from_counts(None, raw="\n".join(raw_parts))
        counts[table] = value
    return check_doris_ads_rows_from_counts(counts, raw="\n".join(raw_parts))


def check_doris_ads_rows_from_counts(
    counts: dict[str, int] | None, raw: str = ""
) -> CheckResult:
    """逐表行数判据（纯函数，可单测）：每张表各自 >0 才算 PASS。

    - 查询失败/取不到 ⇒ FAIL（ADS 行数是 P1 的核心判据，不可 SKIP 掉）；
    - 任一表为 0 ⇒ FAIL，`detail` 保留原始响应、`actual` 已点名是哪张表；
    - 三张表都 >0 ⇒ PASS。
    """
    name = "doris-ads-rows"
    expected = "每张 ADS 表 COUNT(*)>0（逐表判定）"
    if counts is None:
        return CheckResult(Status.FAIL, name, "查询失败", expected, raw)

    parts: list[str] = []
    for table in ADS_TABLES:
        value = counts.get(table)
        if value is None:
            parts.append(f"{table}=不可查 FAIL")
        elif value > 0:
            parts.append(f"{table}={value} PASS")
        else:
            parts.append(f"{table}={value} FAIL")

    actual = " ".join(parts)
    if any(counts.get(t) is None or (counts.get(t) or 0) <= 0 for t in ADS_TABLES):
        return CheckResult(Status.FAIL, name, actual, expected, raw or actual)
    return CheckResult(Status.PASS, name, actual, expected)


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
#
# ── 探针对账（impl-03f 交付物 1，spec §5.2/§5.4 的键同构对账切面）──────
# 形态：附加断言（不动 P1 五项面——spec §4.3 P2 行「五项真断言」已由
# 五项本体满足，对账是六号位的**行为判据**）。跑一轮受控探针批
# （~4.5 min：200 s 落定等待 + 逐分区踢水位 + 60 s flush），判据面见
# reconcile_probe 模块头注（水位数学与分区 MIN 语义的实测定案）。


def check_recon_probe() -> CheckResult:
    """附加：Paimon↔Doris 探针对账（受控批，is_settled=1 逐值严格相等）。

    判据面＝reconcile_probe.run_probe_reconciliation（投批 → 两轮逐分区
    踢水位 → 落定行采集 → 同源公式复算 → 逐值比对）。失败形态含：
    投递失败 / Doris 读取失败 / 任一侧缺席 / 任一值不等——全部 FAIL
    （探针对账没有 SKIP 通道，缺席即失败）。
    """
    # 局部导入：探针面拉起 confluent_kafka，测试环境按需注入
    from ecomgen.reconcile_probe import run_probe_reconciliation

    return run_probe_reconciliation()


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


# 硬杀（SIGKILL / 掉电）留下的临时文件，超过这个年龄才允许被清掉。
# 同一个进程内的落盘是毫秒级的，故超龄窗口保证不会误删**当前**并发写者的新鲜临时文件。
# **设计值**，非官方保证。
STALE_BASELINE_TEMP_GRACE_S = 60.0  # 设计值


def _sweep_stale_baseline_temps(directory: Path) -> None:
    """清掉上一次**硬杀**（SIGKILL / 掉电）留下的临时文件。

    `try/finally` 抓不住 SIGKILL —— 那种死法下临时文件**必然**残留（finally 根本没机会
    执行）。故在每次落盘前顺手清一遍同前缀、且已超龄的临时文件：
    「临时文件不残留」这条验收因此在**正常路径、异常路径、硬杀路径**三种死法下都成立。
    """
    now = time.time()
    for candidate in directory.glob(f".{DIRTY_RATE_BASELINE}.*.tmp"):
        try:
            if now - candidate.stat().st_mtime < STALE_BASELINE_TEMP_GRACE_S:
                continue
            candidate.unlink()
        except OSError:
            # 清不掉（并发删、权限）不影响本次落盘：临时文件名本来就是唯一的。
            continue


def _current_umask() -> int:
    """读当前 umask。`os.umask` 只有**破坏性**读取，故读完立刻还原（CLI 单线程场景）。"""
    current = os.umask(0)
    os.umask(current)
    return current


def _desired_baseline_mode(path: Path) -> int:
    """目标文件应有的权限位，与施工前 `path.write_text` 的产物一致。

    施工前走 `open(path, "w")` ⇒ 新建文件拿到 `0o666 & ~umask`（本仓实测 0644）。
    而 `tempfile.NamedTemporaryFile` 固定建 **0600**，`os.replace` 会把 0600 一起搬过去
    —— 于是读者（面板同步脚本、可能以另一个 uid 跑）会**丢掉**它原本有的读权限。
    故覆盖落盘沿用旧文件的权限位，首次落盘补回 `open("w")` 的语义。
    """
    try:
        return path.stat().st_mode & 0o777
    except OSError:
        return 0o666 & ~_current_umask()


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
    """把脏率基线落盘（下一次运行与之比较）。写不了就抛 OSError，由调用方兜底。

    <h2>为什么必须是**原子替换**，不是先截断再写（impl-07c）</h2>
    这个文件是**闸门文件**：面板的基线行、以及脏率判据的读侧都依赖它。而基线是
    **自我维护**的（首跑判据 SKIP 时落盘、每次 PASS 前推、`--corrupt` 收尾吸收）
    ——写入频率是常规路径级别，不是「一年写一次」的风险。

    施工前的形态（`path.write_text`）以 `"w"` 打开目标文件，**此刻目标已被清零**，
    随后才往里写。若这段窗口里进程被 Ctrl-C / 被杀 / 掉电，文件就停在半截 JSON。
    而读侧把解析错误一律报成「基线文件不可解析」⇒ **所有**后续面板同步与脏率判据
    会一起指向一个误导性的方向（真因是写入被打断，不是文件「坏了」）。

    ⇒ 改为**同目录写临时文件，再 `os.replace` 原子替换**：读者在任意时刻要么看到
    完整的旧版、要么看到完整的新版，**永不**看到半截。

    <h2>为什么这三件事都不能省</h2>
    1. **临时文件与目标同目录**：`os.replace` 只在同一文件系统内保证原子，跨文件系统
       会退化成「先删后拷」——那就把本票要消灭的窗口又造回来了。
    2. **临时名带进程号 + 随机后缀**：固定名会在并发写时互相踩（同目录同时落盘虽
       当前不存在，但可执行面不止一个入口：`make verify` 与 `--corrupt` 收尾）。
    3. **先 `flush` + `fsync` 再替换**：否则「替换生效了而内容还没落盘」会留下一个
       合法的空/半文件——那也是一种半截。（**边界如实说**：本函数不 fsync 父目录，
       故这里保证的是**内容**先落盘，不是「整套替换在掉电后一定持久」；对 60 字节、
       每次 verify 都重写一次的基线，这个强度足够，不值得为它再加一次目录 fsync。）

    <h2>权限位（读者不改也得能读）</h2>
    `tempfile.NamedTemporaryFile` 固定建 **0600**，`os.replace` 会连同权限一起搬过去。
    而施工前的 `path.write_text` 走 `open(path, "w")` ⇒ 新建时是 `0o666 & ~umask`
    （本仓实测 **0644**）。若不管，首次落盘就会把基线从 0644 悄悄变成 0600，让本来
    读得到它的读者（面板同步脚本，可能以另一个 uid 跑）**丢掉读权限**——读者侧零改动
    这条验收就破了。故：覆盖落盘沿用旧文件的权限位，首次落盘补回 `open("w")` 的语义。

    写的内容与施工前**逐字一致**（同样的字段、同样的缩进、同样的换行）：本函数只改
    「怎么落盘」，不改基线的格式。

    <h2>残留</h2>
    **异常**（Ctrl-C / `SystemExit`）由 `finally` 清掉；**硬杀**（SIGKILL / 掉电）根本没
    机会跑 `finally`，故在下一次落盘前用 `_sweep_stale_baseline_temps` 清超龄残骸
    ——三种死法都不留残骸。
    """
    path = _baseline_path(report_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    _sweep_stale_baseline_temps(path.parent)
    payload = {
        "dlq_offset": sample.dlq_offset,
        "input_events": sample.input_events,
    }
    text = json.dumps(payload, ensure_ascii=False, indent=2) + "\n"
    mode = _desired_baseline_mode(path)

    temp_path: Path | None = None
    try:
        # newline 保持默认 None，与施工前的写盘形态同义（换行转换行为逐字一致）。
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{DIRTY_RATE_BASELINE}.{os.getpid()}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            temp_path = Path(handle.name)
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        # 先把权限位修正到「旧文件原本的样子」，再替换——替换后就不再有机会改了。
        os.chmod(temp_path, mode)
        os.replace(temp_path, path)
        temp_path = None  # 已被替换消费掉，不再需要清理
    finally:
        if temp_path is not None:
            # 失败/被打断时清掉临时文件（目标文件本身一个字节都没被碰过）。
            try:
                temp_path.unlink()
            except OSError:
                pass
    return path


@dataclass(frozen=True)
class FixtureAbsorption:
    """一次夹具记账的结果（含「多出来的脏」以便断言归因）。"""

    absorbed: DirtyRateSample
    observed_delta: int
    injected: int
    attributable_ceiling: int

    @property
    def attributable(self) -> bool:
        """观测到的 DLQ 增量是否**全部**可由夹具解释。"""
        return self.observed_delta <= self.attributable_ceiling


def plan_fixture_absorption(
    before: DirtyRateSample | None,
    after_dlq_offset: int | None,
    injected: int,
    input_events: int | None = None,
) -> FixtureAbsorption | None:
    """纯函数：算出「夹具记账后」的基线，并判定增量是否可归因。返回 None＝不可记账。

    <h2>为什么吸收的是**观测增量**而不是恰好 `injected` 条</h2>
    实机（报告 `063129Z`）：注入 **5** 条，DLQ 却涨了 **10** —— 因为注入发生在
    kill **之前**，这批字节在 failover 后被**重放**了一次，于是同一份脏进了两遍。
    `verify-fault` 的重放条数取决于时序（`replay=50`），所以夹具贡献是
    `injected × (1 + 重放次数)`，**不是**常数 `injected`。

    ⇒ 若死按 `injected` 减法，每轮会留下 `injected` 条无主增量，下一次
    `make verify` 必然**假 FAIL**。故改为：吸收**观测增量**，另立一条断言
    `增量 <= 可归因上限` —— 把「这个增量真的是夹具造成的」变成**被检查的命题**，
    而不是被默默假设掉的前提。

    可归因上限 = `injected × ATTRIBUTABLE_REPLAY_FACTOR`，
    factor 取 3（=1 次原始投递 + 至多 2 次重放），是**设计值**。
    """
    if before is None or injected <= 0:
        return None
    observed = after_dlq_offset if after_dlq_offset is not None else before.dlq_offset
    observed_delta = max(observed - before.dlq_offset, 0)
    ceiling = injected * ATTRIBUTABLE_REPLAY_FACTOR
    total = input_events if input_events is not None else before.input_events
    try:
        absorbed = DirtyRateSample(
            dlq_offset=before.dlq_offset + observed_delta, input_events=total
        )
    except ValueError:
        return None
    return FixtureAbsorption(
        absorbed=absorbed,
        observed_delta=observed_delta,
        injected=injected,
        attributable_ceiling=ceiling,
    )


def absorb_fixture_dirty(
    before: DirtyRateSample | None,
    after_dlq_offset: int | None,
    injected: int,
    report_dir: str = REPORT_DIR,
) -> FixtureAbsorption | None:
    """把**反例夹具**注入的脏数据记账进基线（不吞有机退化）。返回新基线或 None。

    <h2>为什么需要这个入口（impl-02e 收口实测的判据死锁）</h2>
    基线只在**判定通过**时前推（见 `check_dlq_rate_not_increased`）。这本身是对的：
    基线只能当「安全上界」，不能让一次失败把它稀释掉。

    但 `make verify-fault-corrupt`（票面验收第 7 条**要求**的反例模式）会往主流里
    投**真脏**事件，它们会被正确地判成 `schema_invalid` 进入 DLQ ⇒ DLQ offset 上涨。
    这批脏是**测试夹具**，不是系统退化；可基线不认这个区别，于是：

    ```
    05:02 corrupt 注入 → DLQ 247→257 → 基线停在 257（05:07 那次 PASS 前推）
    05:17 corrupt 注入 → DLQ 257→267
    05:49 corrupt 注入 → DLQ 267→277
    之后每次 make verify：0.006709（277/41287） vs 基线 0.006327（257/40620）⇒ 恒 FAIL
    ```

    实机证据（2026-10-22 06:03Z 收口首跑）：`FAIL dlq-rate-no-increase`。

    <h2>★★ 为什么必须按「注入条数」记账，而不是把基线重立到当前值 ★★</h2>
    第一版实现写成 `baseline := 当前实测值`，被 code-review 正确指出是**洗白**：
    那会把「本次注入窗口内积累的**一切**脏」都算成夹具 —— 若同时段有真实退化
    （重复被误判成脏、生成器注入异常），它们会被永久吸收进安全上界，
    于是基线只升不降、真实退化再也报不出来（判据从「恒假」翻成「恒真」）。

    ⇒ 口径落在 `plan_fixture_absorption`（纯函数，可单测）：吸收**观测增量**，
    并用 `增量 <= injected × 3` 的**可归因断言**兜住「这段窗口里有没有别的东西」。
    超出上限 ⇒ 调用方必须把记账结果标成**不可归因**并报 FAIL，
    绝不让有机退化悄悄进基线。

    分母取**当前**输入总数（夹具事件本身也计入输入，与判据分母口径一致）。

    仅由 `--corrupt` 分支在**确认注入成功**后调用；正常路径绝不走这里。
    """
    current = sample_dirty_rate()
    after_dlq = None if current is None else current.dlq_offset
    plan = plan_fixture_absorption(
        before,
        after_dlq,
        injected,
        input_events=None if current is None else current.input_events,
    )
    if plan is None:
        return None
    try:
        save_dirty_rate_baseline(plan.absorbed, report_dir)
    except OSError:
        return None
    return plan


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
    check_recon_probe,
)


def run_verify(report_dir: str = REPORT_DIR) -> VerifyReport:
    """跑完 P1 全部断言，写报告，返回结果。"""
    checks: list[CheckResult] = []
    print("=== make verify：分层判据（P2 起五项真断言，spec §4.3）===")
    for fn in P1_CHECKS:
        result = fn()
        checks.append(result)
        print(f"  {result.format_line()}")

    print("--- 附加断言（行为判据：漏斗/脏率/两态/探针对账）")
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
