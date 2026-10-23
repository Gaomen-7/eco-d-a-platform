"""编排链 SQL 的**跨语言常量守卫**（impl-03b code-review #1）。

**为什么需要这一片**：`sql/replay_20_dwd.sql` 里的 `-120000`（相对下界允许乱序幅度）
与 `+300000`（上界未来容忍度）是 `src/ecomgen/events.py` 两个 Python 常量的
**影子副本** —— SQL 侧无法 import Python，Python 侧也不会去读 SQL，两侧之间
此前**没有任何守卫**。任一侧改动即**静默漂移**：症状是 dt 分区或拒绝率错，
从日志上完全看不出「是哪个常量没跟上」。

本仓现成范式是 `tests/test_threshold_batch.py::test_max_late_constant_matches_injector`
（producer 冗余写死 `_THRESHOLD_MAX_LATE_MS`，用单测断言与 injectors 同值）。
本文件照抄该范式，对**反向**（SQL ← Python）再补一道。

覆盖：
1. 下界 `DEFAULT_ALLOWED_LATENESS_MS` / 上界 `_FUTURE_TOLERANCE_MS` 与 SQL 字面量相等；
2. `reject_reason` 取值域**恰为三值**且**不含 `duplicate`**（验收第 5 条）；
3. DWD 的 dt 走**事件时间**、拒绝表 dt 走**处理时间**（验收第 5 条的另一半）；
4. 零 `ADD JAR`（验收第 8 条：sql/ 内一条都没有）。
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from ecomgen.events import (
    _FUTURE_TOLERANCE_MS,
    DEFAULT_ALLOWED_LATENESS_MS,
)

_SQL_DIR = Path(__file__).resolve().parent.parent / "sql"
_DWD_SQL = _SQL_DIR / "replay_20_dwd.sql"
_DDL_SQL = _SQL_DIR / "replay_01_ddl.sql"


def _dwd_text() -> str:
    assert _DWD_SQL.is_file(), f"缺编排脚本：{_DWD_SQL}"
    return _DWD_SQL.read_text(encoding="utf-8")


def _ddl_text() -> str:
    assert _DDL_SQL.is_file(), f"缺 DDL 脚本：{_DDL_SQL}"
    return _DDL_SQL.read_text(encoding="utf-8")


# ── 1. 时间区间常量：SQL 字面量 == Python 常量 ────────────────────────────
def test_dwd_lower_bound_literal_matches_events_constant() -> None:
    """`event_time < batch_min_ms - 120000` 里的 120000 必须等于
    `events.DEFAULT_ALLOWED_LATENESS_MS`。

    这是 SRS §3.3 规则①的**相对下界**：下界 = 本批最早事件时间 − 允许乱序幅度。
    两侧不等时，生成器认为合法的行会被作业判 `time_out_of_range`
    （或反过来放过真正的超期行），而两边各自的单测都还是绿的。
    """
    text = _dwd_text()
    pattern = rf"batch_min_ms\s*-\s*({DEFAULT_ALLOWED_LATENESS_MS})\b"
    assert re.search(pattern, text), (
        f"replay_20_dwd.sql 里找不到 'batch_min_ms - {DEFAULT_ALLOWED_LATENESS_MS}'；"
        f"若故意改了 events.DEFAULT_ALLOWED_LATENESS_MS，SQL 必须同步改。"
    )


def test_dwd_upper_bound_literal_matches_events_constant() -> None:
    """`event_time > now_ms + 300000` 里的 300000 必须等于
    `events._FUTURE_TOLERANCE_MS`（上界：不晚于 now + 5 min）。"""
    text = _dwd_text()
    pattern = rf"now_ms\s*\+\s*({_FUTURE_TOLERANCE_MS})\b"
    assert re.search(pattern, text), (
        f"replay_20_dwd.sql 里找不到 'now_ms + {_FUTURE_TOLERANCE_MS}'；"
        f"若故意改了 events._FUTURE_TOLERANCE_MS，SQL 必须同步改。"
    )


def test_dwd_has_no_stale_shadow_constants() -> None:
    """反向守卫：SQL 里**不该**再出现别的「看起来像这两个常量」的毫秒字面量。

    只白名单这两个数：多出第三个 5~7 位毫秒字面量，大概率是有人改了
    Python 常量却把旧值留在 SQL 里（新值加旧值并存 ⇒ 判据恒真或恒假）。
    """
    text = _dwd_text()
    allowed = {
        str(DEFAULT_ALLOWED_LATENESS_MS),
        str(_FUTURE_TOLERANCE_MS),
        "3000",
        "1000",
    }
    found = set(re.findall(r"(?<![\w.])([1-9][0-9]{4,6})(?![\w.])", text))
    unexpected = found - allowed
    assert not unexpected, (
        f"replay_20_dwd.sql 出现未登记的毫秒字面量：{sorted(unexpected)}；"
        f"白名单={sorted(allowed)}。新常量请先加进本测试的白名单。"
    )


# ── 2. reject_reason 取值域恰为三值、且无 duplicate ──────────────────────
_REJECT_VALUES = ("schema_invalid", "value_out_of_range", "time_out_of_range")


def test_reject_reason_domain_is_exactly_three_values() -> None:
    """`replay_20_dwd.sql` 的 CASE 分支产出的取值集合**恰为三值**。

    直接抓 SQL 里所有单引号包裹的 reject 类原因码，与三值集合比**相等**：
    多一个（例如有人加了 `duplicate`）或拆了分支都会失败。
    """
    text = _dwd_text()
    produced = set(
        re.findall(r"'(schema_invalid|value_out_of_range|time_out_of_range)'", text)
    )
    assert produced == set(_REJECT_VALUES), (
        f"reject_reason 取值域 {sorted(produced)} ≠ 三值 {sorted(_REJECT_VALUES)}"
    )


def test_reject_reason_never_contains_duplicate() -> None:
    """验收第 5 条：三值里**没有** `duplicate` —— 重复走幂等吸收，不进拒绝表。

    断言的是「有任何地方把 duplicate 当拒绝原因」⇒ 源码层面即失败，
    不依赖跑起来看数据（那只是抽样，抽不到不等于没有）。
    """
    text = _dwd_text().lower()
    assert "'duplicate'" not in text, "replay_20_dwd.sql 把 duplicate 当成了拒绝原因"
    assert "then 'duplicate'" not in text


def test_reject_table_reason_comment_lists_three_values() -> None:
    """DDL 侧对 `reject_reason` 的注释也必须只列三值（文档与实现不得互相矛盾）。"""
    text = _ddl_text()
    comment = re.search(r"reject_reason\s+STRING COMMENT '([^']*)'", text)
    assert comment is not None, "replay_01_ddl.sql 缺 reject_reason 的 COMMENT"
    body = comment.group(1)
    for value in _REJECT_VALUES:
        assert value in body, f"reject_reason 注释漏了 {value}"
    assert "duplicate" not in body.lower(), "reject_reason 注释里出现了 duplicate"


# ── 3. 两种 dt 口径不得互换 ─────────────────────────────────────────────
def test_dwd_dt_uses_event_time_and_reject_dt_uses_processing_time() -> None:
    """DWD 的 dt 取**事件日**，拒绝表的 dt 取**摄入日（处理时间）**。

    这是形状表里最容易被「统一一下」搞反的一处：拒绝行的 event_time 本身不可信，
    拿它做分区＝让坏数据决定布局。两侧口径互换后行数、分区数**看起来都正常**，
    只有语义错了 ⇒ 必须在源码层钉住。
    """
    text = _dwd_text()
    # DWD：从 event_time 推导
    assert re.search(
        r"DATE_FORMAT\(TO_TIMESTAMP_LTZ\(\s*o\.event_time\s*,\s*3\s*\)\s*,\s*'yyyy-MM-dd'\s*\)",
        text,
    ), "DWD 的 dt 不是从 event_time 用 TO_TIMESTAMP_LTZ 推出来的"
    # 拒绝表：从 CURRENT_TIMESTAMP 推导（处理时间）
    assert re.search(
        r"DATE_FORMAT\(CURRENT_TIMESTAMP\s*,\s*'yyyy-MM-dd'\s*\)\s*AS dt",
        text,
    ), "拒绝表的 dt 不是从 CURRENT_TIMESTAMP（处理时间）推出来的"


# ── 4. 零 ADD JAR（验收第 8 条）────────────────────────────────────────
def test_no_add_jar_statement_anywhere_in_sql() -> None:
    """`sql/` 里**一条** `ADD JAR` 都没有：零业务 jar 的源码面判据。

    环境类 jar（paimon-hadoop-uber）由镜像构建期 `COPY` 进 `/opt/flink/lib`，
    归部署面（impl-03a），不是会话内 `ADD JAR` —— 两者不可混为一谈。
    """
    offenders = []
    for path in sorted(_SQL_DIR.glob("*.sql")):
        text = path.read_text(encoding="utf-8").upper()
        if re.search(r"^\s*ADD\s+JAR\b", text, re.MULTILINE):
            offenders.append(path.name)
    assert not offenders, f"出现 ADD JAR：{offenders}"


# ── 5. 每条层脚本都设了 dml-sync / UTC（否则行数与 dt 均不可信）──────────
@pytest.mark.parametrize("name", ["replay_10_ods.sql", "replay_20_dwd.sql"])
def test_layer_scripts_pin_timezone_and_sync(name: str) -> None:
    """层脚本必须钉 `table.local-time-zone=UTC` 与 `table.dml-sync=true`。

    前者是 dt 口径的前提（spec §5.2）；后者是「INSERT 后立刻查行数」成立的前提
    （§D5 实测：不设则 INSERT 异步提交，紧随的 SELECT 读到 0，日志行全是假 0）。
    """
    text = (_SQL_DIR / name).read_text(encoding="utf-8")
    assert "table.local-time-zone' = 'UTC'" in text, f"{name} 未钉 UTC"
    assert "table.dml-sync' = 'true'" in text, f"{name} 未钉 dml-sync"


# ── 6. ODS 层报告必须只统计本次落地分区（防「跑两次数字翻倍」）──────────
def test_ods_report_counts_only_the_current_landing_partition() -> None:
    """`layer=ods rows=` 必须是**本次写入条数**，不是整表行数。

    ★ 实机发现的缺陷（impl-03b，2026-10-23）：ODS 的 dt 是**落地日**，append 表的
    `INSERT OVERWRITE` 只改写本次 dt 分区（历史落地日的分区按设计保留）。于是
    隔天再跑一遍，整表变成 2097152 / 2 个分区，而本次只写了 1048576 条 ——
    日志行随之漂移，读者无法据此判断本次是否写全。

    ⇒ 判据钉两件事：报告语句里有 `dt =` 过滤，且过滤用的是
    `DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd')`（与写入侧的落地日同源）。
    """
    text = (_SQL_DIR / "replay_10_ods.sql").read_text(encoding="utf-8")
    report = text[text.index("layer=ods rows=") :]
    assert "FROM ods_behavior_event" in report, "层报告不再查 ods_behavior_event"
    assert re.search(
        r"WHERE\s+dt\s*=\s*DATE_FORMAT\(CURRENT_TIMESTAMP\s*,\s*'yyyy-MM-dd'\s*\)",
        report,
        re.IGNORECASE,
    ), "ODS 层报告未按本次落地日过滤 ⇒ 隔天重跑会翻倍"


def test_ods_insert_targets_current_landing_day() -> None:
    """写入侧的 dt 也必须是 `DATE_FORMAT(CURRENT_TIMESTAMP,'yyyy-MM-dd')`
    —— 否则上面那条「报告过滤」会与写入不同源，过滤出空分区或错分区。"""
    text = (_SQL_DIR / "replay_10_ods.sql").read_text(encoding="utf-8")
    assert re.search(
        r"DATE_FORMAT\(CURRENT_TIMESTAMP\s*,\s*'yyyy-MM-dd'\s*\)\s*AS dt",
        text,
    ), "ODS 写入侧 dt 不是从 CURRENT_TIMESTAMP 推的落地日"


# ── 7. DWD 读 ODS 必须限定本次落地日（否则隔天重跑翻倍、A1 破）──────────
def test_dwd_reads_only_the_current_ods_landing_partition() -> None:
    """`replay_20_dwd.sql` 读 ODS 的**两个**入口都必须限定落地日。

    ★ 实机发现的缺陷（impl-03b spec 轴审阅，2026-10-23）：只修了 ODS 的层报告过滤，
    而 DWD 的 `FROM ods_behavior_event` 读的是**整表** ⇒ 隔天重跑会把前一天的
    1 048 576 行再喂一遍，DWD 翻倍成 2 097 152，而 spec §5.4 的 A1 是**全局比对**
    （ods_distinct = dwd_distinct + reject_distinct）—— DWD 混进副本，等式当场破。

    判据钉三件事：
    ① 存在一个按落地日过滤的 ODS 作用域视图；
    ② 批次基准量（batch_min_ms / now_ms）从**该视图**取，不是整表；
    ③ 校验视图的源也必须是该视图，不是整表。
    """
    text = _dwd_text()
    # ① 作用域视图存在且带落地日过滤
    assert re.search(
        r"CREATE\s+TEMPORARY\s+VIEW\s+v_ods_scope\s+AS\s+SELECT\s+\*\s+FROM\s+ods_behavior_event\s+WHERE\s+dt\s*=\s*DATE_FORMAT\(CURRENT_TIMESTAMP",
        text,
        re.IGNORECASE,
    ), "缺按落地日过滤的 ODS 作用域视图（v_ods_scope）"
    # ② 批次基准量从作用域视图取
    assert re.search(r"FROM\s+v_ods_scope\s*;", text), "batch_ref 未从 v_ods_scope 取"
    # ③ 校验视图的源不是整表
    assert not re.search(r"FROM\s+ods_behavior_event\s+o\b", text), (
        "校验视图仍直接读整表 ods_behavior_event ⇒ 隔天重跑 DWD 会翻倍"
    )
    assert re.search(r"FROM\s+v_ods_scope\s+o\b", text), "校验视图未从 v_ods_scope 读"


def test_ods_and_dwd_share_one_landing_day_filter() -> None:
    """ODS 与 DWD 两侧的落地日过滤表达式必须**逐字相同**（同源）。

    两侧各写一个略有差异的表达式（例如一处 CURRENT_TIMESTAMP、一处 CURRENT_DATE）
    在跨 UTC 日边界的时刻会取到不同的日期 ⇒ ODS 报告与 DWD 入参错位，而这
    几乎不可能在人工测试里撞到。
    """
    pattern = r"DATE_FORMAT\(\s*CURRENT_TIMESTAMP\s*,\s*'yyyy-MM-dd'\s*\)"
    ods = (_SQL_DIR / "replay_10_ods.sql").read_text(encoding="utf-8")
    dwd = _dwd_text()
    ods_filter = re.findall(pattern, ods, re.IGNORECASE)
    dwd_filter = re.findall(pattern, dwd, re.IGNORECASE)
    assert ods_filter, "replay_10_ods.sql 没有落地日过滤"
    assert dwd_filter, "replay_20_dwd.sql 没有落地日过滤"
    # 各自都写成同一字面表达式（大小写不敏感归一后比较）
    norm = {f.lower().replace(" ", "") for f in ods_filter + dwd_filter}
    assert len(norm) == 1, f"两侧落地日过滤表达式不一致：{norm}"
