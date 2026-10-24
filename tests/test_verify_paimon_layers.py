"""verify 第三项「Paimon 五层有行」转真断言的判据面测试（impl-03f 交付物 3）。

**为什么这一片必须存在**（票面 AC 逐字）：
> `make verify` 第三项 `paimon-five-layers` 由 `SKIP` 转为**真断言**，
> 实测 `PASS`/`FAIL` 并**计入退出码**。

**判据形态**（spec §4.3 P2 行：五项全真）：
- 五层 ods/dwd/dim/dws/ads 各 `COUNT(*)>0`；
- **走 Flink SQL**，不是 Doris 的 `total_record_count`（票面 Notes：Paimon
  系统表不在 Doris 支持列表内）；
- 执行面与 `lake_assert.sh` 同源：`sql-client.sh -f` 报错仍退 0 ⇒ 唯一信号是
  stdout，解析规则＝只认 `|` 开头的表格行（spec §5.5）。

**测试策略**（与 tests/test_verify_checks.py 同构）：
- 判定逻辑抽成纯函数 `check_paimon_layers_from_counts`，注入假计数；
- TABLEAU 解析抽成 `parse_layer_rows_from_stdout`，注入假 stdout；
- 真实路径（连容器的 `check_paimon_layers`）不在单测内触碰。
"""

from __future__ import annotations

from ecomgen.verify import Status
from ecomgen.verify_runner import (
    PAIMON_LAYER_TABLES,
    check_paimon_layers_from_counts,
    parse_layer_rows_from_stdout,
)

# ══════════════════════════════════════════════════════════════════════
# 造 TABLEAU stdout 夹具：形状与真实 sql-client 输出同构
# ══════════════════════════════════════════════════════════════════════
# ★ 三个特征（impl-03e/03d 实测，缺一个夹具就失真）：
#   1. 结果表是 `| cell | cell |`，首尾各一个 `|`；
#   2. sql-client **逐字回显 SQL 源码**（含注释），回显行以 `Flink SQL> ` /
#      `> ` 开头 ⇒ 解析器只认 `|` 开头的行才不被回显带偏；
#   3. 输出里可能有 `[ERROR]` 行（spec §5.5：报错后 sql-client 中止整个脚本）。


def _render_probe_row(layer: str, rows: int | str) -> str:
    return f"| {layer} | {rows} |\n"


def _full_green_stdout() -> str:
    """五层全 >0 的探针输出（带回显行 + 表头，形状同真实输出）。"""
    return (
        "Flink SQL> \n"
        "> SELECT 'ods' AS layer_name, CAST(COUNT(*) AS STRING) AS rows\n"
        "> FROM ods_behavior_event;\n"
        "+------------+-------+\n"
        "| layer_name |  rows |\n"
        "+------------+-------+\n"
        "| ods        | 1048576 |\n"
        "+------------+-------+\n"
        "1 row in set (0.05 seconds)\n\n"
        "Flink SQL> \n"
        "> SELECT 'dwd' AS layer_name, CAST(COUNT(*) AS STRING) AS rows\n"
        "> FROM dwd_behavior_event;\n"
        "+------------+-------+\n"
        "| layer_name |  rows |\n"
        "+------------+-------+\n"
        "| dwd        | 1048576 |\n"
        "+------------+-------+\n"
        "1 row in set (0.05 seconds)\n\n"
        "Flink SQL> \n"
        "> SELECT 'dim' AS layer_name, CAST(COUNT(*) AS STRING) AS rows\n"
        "> FROM (SELECT COUNT(*) FROM dim_item UNION ALL …);\n"
        "+------------+-------+\n"
        "| layer_name |  rows |\n"
        "+------------+-------+\n"
        "| dim        | 105430 |\n"
        "+------------+-------+\n"
        "1 row in set (0.05 seconds)\n\n"
        "Flink SQL> \n"
        "> SELECT 'dws' AS layer_name, CAST(COUNT(*) AS STRING) AS rows\n"
        "> FROM dws_subject_window;\n"
        "+------------+-------+\n"
        "| layer_name |  rows |\n"
        "+------------+-------+\n"
        "| dws        | 1550285 |\n"
        "+------------+-------+\n"
        "1 row in set (0.05 seconds)\n\n"
        "Flink SQL> \n"
        "> SELECT 'ads' AS layer_name, CAST(COUNT(*) AS STRING) AS rows\n"
        "> FROM ads_window_metric;\n"
        "+------------+-------+\n"
        "| layer_name |  rows |\n"
        "+------------+-------+\n"
        "| ads        | 90348 |\n"
        "+------------+-------+\n"
        "1 row in set (0.05 seconds)\n\n"
    )


# ── 判定纯函数：五层各 COUNT(*)>0 ────────────────────────────────────


def test_all_layers_positive_passes() -> None:
    """正例：五层全 >0 ⇒ PASS（actual 逐层可见）。"""
    counts = dict.fromkeys(PAIMON_LAYER_TABLES, 100)
    result = check_paimon_layers_from_counts(counts)
    assert result.status is Status.PASS
    assert "ods=100" in result.actual
    assert "ads=100" in result.actual


def test_one_empty_layer_fails_naming_only_that_layer() -> None:
    """反例 1：某层 0 行 ⇒ FAIL，且点名是哪层（缺席不掩盖其它层的好状态）。"""
    counts = dict.fromkeys(PAIMON_LAYER_TABLES, 100)
    counts["dws"] = 0
    result = check_paimon_layers_from_counts(counts)
    assert result.status is Status.FAIL
    assert "dws=0 FAIL" in result.actual
    assert "ods=100 PASS" in result.actual


def test_counts_none_fails_not_skips() -> None:
    """反例 2：查询失败/取不到 ⇒ **FAIL**，不是 SKIP（否则又回 P1 的显式豁免）。

    P2 起这是五项真断言之一：探针跑不出来是失败，不是「场景未激活」。
    """
    result = check_paimon_layers_from_counts(None, raw="docker: no such container")
    assert result.status is Status.FAIL
    assert result.is_failure


def test_missing_layer_counts_fails() -> None:
    """反例 3：某层**整行缺席**（探针没输出该层）⇒ FAIL，不是被静默跳过。

    这是 03e F6 的同款假绿通道：判定面必须证明「每层都被报告过」，
    而不是「报告过的层都 >0」。
    """
    counts = dict.fromkeys(PAIMON_LAYER_TABLES, 100)
    del counts["ads"]
    result = check_paimon_layers_from_counts(counts)
    assert result.status is Status.FAIL
    assert "ads=不可查" in result.actual


def test_expected_layers_are_the_five_vocabulary() -> None:
    """词表钉死：五层 ods/dwd/dim/dws/ads（spec §5.3 的层词汇表）。"""
    assert PAIMON_LAYER_TABLES == (
        "ods",
        "dwd",
        "dim",
        "dws",
        "ads",
    )


# ── TABLEAU stdout 解析 ─────────────────────────────────────────────


def test_parse_reads_all_five_layers_from_green_stdout() -> None:
    """解析正例：从完整 TABLEAU 输出读出五层行数。"""
    counts = parse_layer_rows_from_stdout(_full_green_stdout())
    assert counts == {
        "ods": 1048576,
        "dwd": 1048576,
        "dim": 105430,
        "dws": 1550285,
        "ads": 90348,
    }


def test_parse_ignores_echoed_sql_lines() -> None:
    """回显行以 `>` / `Flink SQL>` 开头 ⇒ 不被当成数据（spec §5.5 解析纪律）。

    刻意构造：回显行里出现 `| ods | 999 |` 形的字面量 —— 若解析器不过滤
    回显前缀，就会把 999 读进去、覆盖真实行数。
    """
    stdout = (
        "Flink SQL> \n"
        "> -- 注释里写了 | ods | 999 | 字面量\n"
        "> SELECT 'ods' AS layer_name, CAST(COUNT(*) AS STRING) AS rows\n"
        "+------------+-------+\n"
        "| layer_name |  rows |\n"
        "+------------+-------+\n"
        "| ods        | 42 |\n"
        "+------------+-------+\n"
    )
    counts = parse_layer_rows_from_stdout(stdout)
    assert counts == {"ods": 42}


def test_parse_returns_empty_on_garbage() -> None:
    """非表格输出（如容器缺失的报错文本）⇒ 空计数 ⇒ 判定面 FAIL。"""
    assert parse_layer_rows_from_stdout("Error response from daemon") == {}
    assert parse_layer_rows_from_stdout("") == {}


def test_parse_strips_ansi_colors() -> None:
    """sql-client 会给输出上色；ANSI 序列嵌在单元格中间时也能读出数值。"""
    stdout = "| ods        | 1\x1b[0m048576 |\n"
    counts = parse_layer_rows_from_stdout(stdout)
    assert counts == {"ods": 1048576}


# ── 真实采集器的形状（不连集群，只钉契约）────────────────────────────


def test_check_paimon_layers_is_real_assertion_not_skip() -> None:
    """第三项的默认实现不再返回 SKIP（P2：转真断言、计入退出码）。

    这里 monkeypatch 采集函数，断言 check_paimon_layers **走它**并把
    结果原样折进 CheckResult —— 防止实现退回「打印一行 SKIP」的旧形态。
    """
    import ecomgen.verify_runner as vr

    captured: dict[str, object] = {}

    def fake_collect() -> dict[str, int]:
        captured["called"] = True
        return dict.fromkeys(PAIMON_LAYER_TABLES, 7)

    monkeypatch_target = vr.collect_paimon_layer_counts
    vr.collect_paimon_layer_counts = fake_collect  # type: ignore[assignment]
    try:
        result = vr.check_paimon_layers()
    finally:
        vr.collect_paimon_layer_counts = monkeypatch_target  # type: ignore[assignment]

    assert captured.get("called") is True
    assert result.status is Status.PASS
    assert result.name == "paimon-five-layers"


def test_p1_checks_third_entry_is_the_real_check() -> None:
    """P1_CHECKS 第三项仍是 check_paimon_layers（接线不换位）。"""
    from ecomgen.verify_runner import P1_CHECKS

    assert P1_CHECKS[2].__name__ == "check_paimon_layers"
    assert len(P1_CHECKS) == 5
