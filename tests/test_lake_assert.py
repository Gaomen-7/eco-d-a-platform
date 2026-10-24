"""`lake_assert.sh` 判定逻辑 + `assert_lake.sql` 输出语法的守卫（impl-03d）。

**为什么这一片必须存在**（票面 AC 逐字）：
> `lake_assert.sh` 的判定逻辑有**单元测试**（stdout 有 `[ERROR]` ⇒ 失败；
> 某哨兵缺 `ok=true` ⇒ 失败；全绿 ⇒ 通过）

**为什么判据不能靠退出码**（本票实机复核过的源码事实）：
`sql-client.sh -f` 语句报错后**仍以 0 退出**（`CliClient` 丢弃返回值、无
`System.exit`）。⇒ 判据只能落在 stdout 上，而 stdout 解析是**自研逻辑**
⇒ 它自己必须被测。

**测试策略：注入 stdout，不连集群。**
`lake_assert.sh --judge <file>` 是为此存在的：它只跑判定面、不看 docker 退出码
（视为 0）。这样单测**不需要** Flink / 容器 / 90 秒等待，且能构造任意失败形态
（真实集群上很难造「某条哨兵缺失」这种状态）。

★ 这些用例刻意覆盖**判据本身的缺陷形态** —— 本仓 impl-02e 记过教训：
「判据自身有缺陷、healthcheck 补成恒真」。故除了「全绿通过」，
每一个失败用例都对应一种**具体的假绿通道**。
"""

from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
_SCRIPT = _ROOT / "scripts" / "lake_assert.sh"
_ASSERT_SQL = _ROOT / "sql" / "assert_lake.sql"
_DDL_SQL = _ROOT / "sql" / "replay_01_ddl.sql"
_MAINT_SQL = _ROOT / "sql" / "replay_90_maintain.sql"
_REPLAY_SH = _ROOT / "scripts" / "lake_replay.sh"
_SQL_DIR = _ROOT / "sql"

_BASH = shutil.which("bash")


def _bash(*args: str) -> subprocess.CompletedProcess[str]:
    """跑 `lake_assert.sh` 的某个子命令（不走 docker 的那些）。"""
    assert _BASH, "本仓库的脚本执行面是 bash（Makefile 亦如此）"
    return subprocess.run(
        [_BASH, str(_SCRIPT), *args],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )


# ════════════════════════════════════════════════════════════════════════
# 造 stdout 夹具：形状必须与真实 sql-client 输出**同构**
# ════════════════════════════════════════════════════════════════════════
# ★ 真实输出的三个特征（本机实测，缺一个夹具就失真）：
#   1. 结果表是 `| cell | cell |`，首尾各一个 `|`；
#   2. **会回显 SQL 源码**，回显行以 `Flink SQL> ` 或 `> ` 开头；
#   3. 报错行形如 `[ERROR] Could not execute SQL statement. Reason:`。
#   夹具刻意**带上回显行**：`assert_lake.sql` 的注释里本身含 `ok=true` 字样，
#   回显后会出现"看起来像哨兵"的行 —— 解析器若不过滤 `>` 前缀就会误判。
_ALL_NAMES = [
    "A1_split_complete",
    "A3_late_bucket_split",
    "A3_late_flag_nonempty",
    "IDEMPOTENCY_fp_ready",
    "SNAP_within_upper_bound",
    "SNAP_empty_table_zero",
    "ZIP_interval_self_sync",
    "ZIP_audit_log_shows_change",
    "OPT_snapshot_retention",
]
_ALL_METRICS = {
    "metric.a2_dup_overshoot": "0",
    "metric.fp.rows": "1048576",
    "metric.fp.hash": "1126399221523001",
    "metric.fp.devent": "1048576",
    "metric.fp.dlag": "131267",
}


def _render_assertion(name: str, value: str) -> str:
    return (
        "Flink SQL> \n"
        "> SELECT '" + name + "' AS assertion,\n"
        ">        CASE WHEN 1 = 1 THEN 'ok=true' ELSE 'ok=false' END AS ok\n"
        "+-------------------+---------+\n"
        "|         assertion |      ok |\n"
        "+-------------------+---------+\n"
        f"| {name} | {value} |\n"
        "+-------------------+---------+\n"
        "1 row in set (0.10 seconds)\n\n"
    )


def _render_metric(name: str, value: str) -> str:
    return (
        "Flink SQL> \n"
        f"> SELECT '{name}' AS k, '{value}' AS v\n"
        "+--------------------------+------------------+\n"
        "|                        k |                v |\n"
        "+--------------------------+------------------+\n"
        f"| {name} | {value} |\n"
        "+--------------------------+------------------+\n"
        "1 row in set (0.10 seconds)\n\n"
    )


def _stdout(
    *,
    green: bool = True,
    missing: tuple[str, ...] = (),
    extra: tuple[str, ...] = (),
    metric_missing: tuple[str, ...] = (),
    error: bool = False,
) -> str:
    """构造一份 sql-client 形态的 stdout。"""
    parts = [
        "WARNING: Unknown module: jdk.compiler specified to --add-exports\n",
        "Flink SQL> [INFO] Execute statement succeeded.\n\n",
    ]
    for name in _ALL_NAMES:
        if name in missing:
            continue
        value = "ok=true" if green else "ok=false"
        parts.append(_render_assertion(name, value))
    for name in extra:
        parts.append(_render_assertion(name, "ok=true"))
    for name, value in _ALL_METRICS.items():
        if name in metric_missing:
            continue
        parts.append(_render_metric(name, value))
    if error:
        parts.append(
            "[ERROR] Could not execute SQL statement. Reason:\n"
            "org.apache.calcite.sql.validate.SqlValidatorException: "
            "No match found for function signature crc32(<CHARACTER>)\n"
        )
    parts.append("Shutting down the session...\ndone.\n")
    return "".join(parts)


@pytest.fixture()
def judge(tmp_path: Path):
    """`--judge <file>` 的便捷封装：返回 CompletedProcess。"""

    def _run(text: str) -> subprocess.CompletedProcess[str]:
        p = tmp_path / "stdout.txt"
        p.write_text(text, encoding="utf-8")
        return _bash("--judge", str(p))

    return _run


# ════════════════════════════════════════════════════════════════════════
# 1. 票面 AC 点名的三条
# ════════════════════════════════════════════════════════════════════════
def test_all_green_passes(judge) -> None:
    """票面 AC：「全绿 ⇒ 通过」。

    这是唯一一条**必须**绿的用例 —— 若它都过不了，脚本在正确数据上也不可用。
    """
    r = judge(_stdout(green=True))
    assert r.returncode == 0, r.stderr
    # `--judge` 注入模式的收尾文案与该模式的其它输出一致（不连集群，故不说"全部断言"）
    assert "通过（--judge 注入模式）" in r.stdout
    # 每条哨兵都必须被**逐条**报绿（不是只报一个总结）
    for name in _ALL_NAMES:
        assert f"✓ {name}" in r.stdout, f"没有逐条报绿：{name}"


def test_error_token_fails_even_when_all_sentinels_green(judge) -> None:
    """票面 AC：「stdout 有 `[ERROR]` ⇒ 失败」。

    ★ 构造的是**最恶劣的形态**：所有哨兵都 `ok=true`（说明"断言都过了"），
      但输出里有一条错误标记。真实场景对应「某条语句报错，但它后面那些
      哨兵的旧结果/上一条语句的结果仍在输出里」。
      ⇒ 判据必须**两个条件都要**：光看哨兵绿会漏掉这个错误。
    """
    r = judge(_stdout(green=True, error=True))
    assert r.returncode == 1
    assert "SQL 层错误标记" in r.stderr


def test_missing_sentinel_fails(judge) -> None:
    """票面 AC：「某哨兵缺 `ok=true` ⇒ 失败」。

    ★ 这条是**假绿的第二个通道**：只看"没有 ok=false"的话，
      一条语句**整段没跑**（缺整行）会得到"没有 ok=false ⇒ 通过"。
      故脚本硬编码期望清单、按名字逐条核，缺一条即失败。
    """
    r = judge(_stdout(green=True, missing=("A1_split_complete",)))
    assert r.returncode == 1
    assert "**没有出现**" in r.stderr
    assert "A1_split_complete" in r.stderr


def test_single_false_sentinel_fails(judge) -> None:
    """任一哨兵 `ok=false` ⇒ 失败（这条看似显然，但要与"缺整行"分开测）。"""
    text = _stdout(green=True).replace(
        "| A3_late_flag_nonempty | ok=true |", "| A3_late_flag_nonempty | ok=false |"
    )
    r = judge(text)
    assert r.returncode == 1
    assert "A3_late_flag_nonempty" in r.stderr
    assert "ok=false" in r.stderr


# ════════════════════════════════════════════════════════════════════════
# 2. A2 的特殊地位（票面 AC：只入日志、不作断言）
# ════════════════════════════════════════════════════════════════════════
def test_a2_value_never_affects_verdict(judge) -> None:
    """A2 的**数值多大都不影响成败** —— 票面 AC 的承重一条。

    做法：把 `metric.a2_dup_overshoot` 的值换成多个不同取值（含 0 与"很大"），
    判定结果必须**恒为通过**。若哪天有人把 A2 加进断言清单，这条会立刻红。
    """
    for value in ("0", "1", "1048576", "999999999"):
        text = _stdout(green=True).replace(
            "| metric.a2_dup_overshoot | 0 |", f"| metric.a2_dup_overshoot | {value} |"
        )
        r = judge(text)
        assert r.returncode == 0, f"A2={value} 竟然影响了判定：{r.stderr}"


def test_a2_metric_must_still_be_printed(judge) -> None:
    """A2 不判定，但**必须真的被打印**（否则"只入日志"变成"根本不记账"）。

    ⇒ 缺失度量行 ⇒ 失败。这与上一条不矛盾：上一条说"值不影响"，
    本条说"存在性要判"。两者合起来才是「只入日志」的完整含义。
    """
    r = judge(_stdout(green=True, metric_missing=("metric.a2_dup_overshoot",)))
    assert r.returncode == 1
    assert "metric.a2_dup_overshoot" in r.stderr


# ════════════════════════════════════════════════════════════════════════
# 3. 解析面的陷阱（本机实测过的两类）
# ════════════════════════════════════════════════════════════════════════
def test_echoed_source_lines_do_not_create_phantom_sentinels(judge) -> None:
    """**回显的 SQL 源码不得被当成哨兵**（实测陷阱）。

    sql-client 会逐字回显语句与注释。`assert_lake.sql` 的注释里本身就有
    `ok=true` 字样 ⇒ 若解析器不排除 `>` 前缀的回显行，会凭空多出哨兵
    ⇒ 脚本的「未登记哨兵」检查会**误红**。
    ★ 夹具里的 `_render_assertion` 已带 `> SELECT ... 'ok=true'` 回显行，
      故本用例只需断言"全绿仍然通过"—— 若解析器误收，就会报未登记哨兵。
    """
    text = _stdout(green=True) + (
        "> -- 说明：本行注释里写着 ok=true 这种东西\n"
        "> -- 还有错误标记字样也只在注释里\n"
    )
    r = judge(text)
    # 回显行不得变成哨兵：不该出现"未登记的哨兵"。
    assert "未登记的哨兵" not in r.stderr
    # 也不得因此判失败（回显行不是错误、不是断言）
    assert r.returncode == 0, r.stderr


def test_unregistered_sentinel_fails(judge) -> None:
    """SQL 里加了断言但脚本清单没跟上 ⇒ 必须红。

    ★ 为什么不静默接受：新断言的**名字**未登记时，脚本不知道它该不该绿
      ⇒ 若静默通过，「写了一条假断言」就永远无人发现。
      这条守卫让「加断言」与「登记清单」必须同时发生。
    """
    r = judge(_stdout(green=True, extra=("SOME_NEW_ASSERTION",)))
    assert r.returncode == 1
    assert "未登记的哨兵" in r.stderr
    assert "SOME_NEW_ASSERTION" in r.stderr


def test_all_false_still_fails(judge) -> None:
    """全体 `ok=false` ⇒ 失败（且是"逐个报出不匹配"而不是只报一条）。"""
    r = judge(_stdout(green=False))
    assert r.returncode == 1
    assert r.stderr.count("ok=false") >= len(_ALL_NAMES)


def test_ansi_colored_error_is_detected(judge) -> None:
    """**带 ANSI 色**的错误标记也要能识别（03b 已踩过的坑）。

    sql-client 给错误标记上色后原始字节是 `ESC[31;1m[ERROR]ESC[0m`；
    若上色码恰好落在字符之间、或有人改用 `sed` 粗剥，就可能漏判。
    本脚本先剥 ANSI 再找 ⇒ 两种情况都稳。
    """
    txt = _stdout(green=True, error=True).replace(
        "[ERROR] Could not execute", "\x1b[31;1m[ERROR]\x1b[0m Could not execute"
    )
    assert "\x1b" in txt, "夹具未真的注入 ANSI 码"
    r = judge(txt)
    assert r.returncode == 1
    assert "SQL 层错误标记" in r.stderr


def test_echoed_error_token_in_comment_does_fail_the_run(judge) -> None:
    """**回显注释里的错误标记会让整次判定失败** —— 这是刻意保留的**保守**行为。

    ★ 与上一条 `test_echoed_source_lines_do_not_create_phantom_sentinels` 配对：
      · 回显行里的 `ok=true` **不会**变成哨兵（解析只看 `|` 开头行）；
      · 但回显行里的错误标记**会**被判成错误（错误检测故意扫全文）。
    ★ 为什么不对称、为什么保守：真错误行与回显注释行在 stdout 上**无法可靠区分**
      （错误文本可能跨行、可能被 sql-client 改前缀）。宁可误红（人能一眼看出
      "这是注释里的字"），也不要漏掉一条真错误（漏掉就是假绿）。
      ⇒ 代价是 **SQL 文件里不许出现带方括号的错误标记**，
        由 `test_no_error_token_in_sql` 静态守住。
    """
    txt = _stdout(green=True) + "> -- 注释里写了 [ERROR] 三个字\n"
    r = judge(txt)
    assert r.returncode == 1
    assert "SQL 层错误标记" in r.stderr


# ════════════════════════════════════════════════════════════════════════
# 4. 用法面
# ════════════════════════════════════════════════════════════════════════
def test_unknown_flag_exits_2() -> None:
    """用法错误走 exit 2，与「断言失败」(1) / 「幂等破」(3) 分开。"""
    r = _bash("--no-such-flag")
    assert r.returncode == 2
    assert "未知参数" in r.stderr


def test_missing_input_file_exits_2(tmp_path: Path) -> None:
    r = _bash("--judge", str(tmp_path / "nope.txt"))
    assert r.returncode == 2
    assert "找不到输入文件" in r.stderr


def test_missing_fingerprint_file_exits_3(tmp_path: Path) -> None:
    """`--compare` 但指纹文件不存在 ⇒ **exit 3**（不是 0、也不是 1）。

    ★ 为什么不是 0：没有基线就断言"两次一致"是**谎报**。
    ★ 为什么不是 1：它不是断言失败，而是「没准备好比对」，归因不同
      （脚本文件头的退出码表把 3 单列出来，就是为了这个区分）。
    ★ 走 `--judge` 注入路径：不给会碰集群，让这条用例在任何机器上都可跑，
      且**不必等 90 秒**。--judge 模式下断言全绿后进入幂等比对分支，
      指纹文件缺失 ⇒ compare_fp 返回 3。
    """
    fp = tmp_path / "nope-fp.txt"
    p = tmp_path / "stdout.txt"
    p.write_text(_stdout(green=True), encoding="utf-8")
    r = _bash("--judge", str(p), "--compare", "--fingerprint-file", str(fp))
    assert r.returncode == 3, (
        f"指纹基线缺失时应 exit 3，实际 {r.returncode}\nstdout={r.stdout}\nstderr={r.stderr}"
    )
    assert "不存在" in r.stderr or "先跑一次" in r.stderr


def test_fingerprint_mismatch_exits_3(tmp_path: Path) -> None:
    """指纹与基线**不同** ⇒ exit 3（这是幂等判据本身的失败态）。"""
    fp = tmp_path / "fp.txt"
    fp.write_text("999 111 222 333\n", encoding="utf-8")  # 与夹具里的四件套都不同
    p = tmp_path / "stdout.txt"
    p.write_text(_stdout(green=True), encoding="utf-8")
    r = _bash("--judge", str(p), "--compare", "--fingerprint-file", str(fp))
    assert r.returncode == 3
    assert "幂等破" in r.stderr
    # 失败必须带**两侧**的值，否则无从归因
    assert "上一遍" in r.stderr and "本  遍" in r.stderr


def test_fingerprint_match_passes(tmp_path: Path) -> None:
    """指纹与基线**相同** ⇒ exit 0（幂等成立）。"""
    m = _ALL_METRICS
    fp = tmp_path / "fp.txt"
    fp.write_text(
        f"{m['metric.fp.rows']} {m['metric.fp.hash']} "
        f"{m['metric.fp.devent']} {m['metric.fp.dlag']}\n",
        encoding="utf-8",
    )
    p = tmp_path / "stdout.txt"
    p.write_text(_stdout(green=True), encoding="utf-8")
    r = _bash("--judge", str(p), "--compare", "--fingerprint-file", str(fp))
    assert r.returncode == 0, r.stderr
    assert "幂等" in r.stdout


def test_batch_check_detects_regeneration(tmp_path: Path) -> None:
    """`--assert-batch-unchanged` 在门槛批**变了**时必须退非 0。

    ★ 这条守的是幂等判据的**前提**：换了批，两遍"一致"毫无意义。
      重产入口可能来自环境变量（`FORCE_GEN=1`）⇒ 不能只靠"Makefile 没设它"。
    """
    baseline = tmp_path / "batch.sha256"
    baseline.write_text(
        "0000000000000000000000000000000000000000000000000000000000000000  x\n"
    )
    r = _bash("--assert-batch-unchanged", str(baseline))
    # 门槛批在 data/ 下（.gitignore 覆盖）；若本机没有它，用例只能验证"基线不符则失败"
    assert r.returncode == 1, (
        f"基线不符时应 exit 1，实际 {r.returncode}\nstdout={r.stdout}\nstderr={r.stderr}"
    )
    assert "被重产" in r.stderr


def test_batch_check_missing_baseline_exits_2(tmp_path: Path) -> None:
    r = _bash("--assert-batch-unchanged", str(tmp_path / "nope.sha256"))
    assert r.returncode == 2
    assert "找不到基线" in r.stderr


def test_value_flags_do_not_hang_without_value() -> None:
    """带值的参数**缺值**时必须 exit 2，**不得挂死**。

    ★ 本机实测过的真缺陷：`--fingerprint-file) FP_FILE="${2:-}"; shift 2 ;;`
      在只剩 1 个参数时 `shift 2` 失败且**不消耗参数** ⇒ `while [ $# -gt 0 ]` 永真
      ⇒ 死循环（`timeout 5` 返回 **124**）。
    ★ 用 `timeout` 而不是直接调用：万一回归，这条用例应当**失败**而不是
      把整个 pytest 挂住（超时 124 ≠ 2 ⇒ 断言红，且能定位）。
    """
    for flag in (
        "--fingerprint-file",
        "--judge",
        "--parse",
        "--assert-batch-unchanged",
    ):
        proc = subprocess.run(
            [_BASH, str(_SCRIPT), flag],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=20,  # 挂死会被这里打断 ⇒ 用例失败（TimeoutExpired）
            check=False,
        )
        assert proc.returncode == 2, (
            f"{flag} 缺值应 exit 2（不得挂死/不得静默跑集群），实际 {proc.returncode}"
        )


# ════════════════════════════════════════════════════════════════════════
# 5. SQL 与脚本清单的**静态一致性**（防止两边漂移）
# ════════════════════════════════════════════════════════════════════════
def _assert_sql_text() -> str:
    assert _ASSERT_SQL.is_file(), f"缺断言脚本：{_ASSERT_SQL}"
    return _ASSERT_SQL.read_text(encoding="utf-8")


def _script_text() -> str:
    assert _SCRIPT.is_file(), f"缺判定脚本：{_SCRIPT}"
    return _SCRIPT.read_text(encoding="utf-8")


def _strip_comments(text: str) -> str:
    """去掉 `--` 行注释（注释里刻意写满了反面说明，不能当代码判）。"""
    return "\n".join(line.split("--", 1)[0].rstrip() for line in text.splitlines())


def _sql_sentinel_names() -> list[str]:
    return re.findall(
        r"SELECT\s+'([A-Za-z0-9_.]+)'\s+AS\s+assertion",
        _strip_comments(_assert_sql_text()),
        re.IGNORECASE,
    )


def _sql_metric_names() -> list[str]:
    return re.findall(
        r"SELECT\s+'(metric\.[A-Za-z0-9_.]+)'\s+AS\s+k",
        _strip_comments(_assert_sql_text()),
        re.IGNORECASE,
    )


def _expected_list_from_script() -> list[str]:
    """从脚本的 `EXPECTED_ASSERTIONS=( … )` 取出硬编码清单。"""
    m = re.search(r"EXPECTED_ASSERTIONS=\((.*?)\n\)", _script_text(), re.DOTALL)
    assert m, "lake_assert.sh 里找不到 EXPECTED_ASSERTIONS 数组"
    return re.findall(r'"([A-Za-z0-9_.]+)"', m.group(1))


def _expected_metrics_from_script() -> list[str]:
    m = re.search(r"EXPECTED_METRICS=\((.*?)\n\)", _script_text(), re.DOTALL)
    assert m, "lake_assert.sh 里找不到 EXPECTED_METRICS 数组"
    return re.findall(r'"([A-Za-z0-9_.]+)"', m.group(1))


def test_expected_names_match_sql() -> None:
    """脚本硬编码的哨兵清单 == SQL 里 `AS assertion` 的名字集合。

    ★ 这条守卫的两面都重要：
      · SQL 加了断言、脚本没登记 ⇒ 运行时「未登记哨兵」会红（但那时才发现）；
      · 脚本登记了、SQL 里没了 ⇒ 运行时「没有出现」会红（这里是静态发现）。
      静态比对把这两类漂移提前到 `pytest` 阶段。
    """
    sql_names = _sql_sentinel_names()
    assert len(sql_names) == len(set(sql_names)), f"SQL 里哨兵名重复：{sql_names}"
    assert set(sql_names) == set(_expected_list_from_script()), (
        f"SQL 哨兵 {sorted(set(sql_names))} ≠ 脚本清单 "
        f"{sorted(set(_expected_list_from_script()))}"
    )


def test_expected_metrics_match_sql() -> None:
    """脚本核对的度量清单必须是 SQL 产出集合的**子集**，且 A2/指纹都在里面。

    ★ 为什么是子集而不是相等：`sql/assert_lake.sql` 还产出大量
      `metric.rows.*` / `metric.snap.*`（逐表规模与快照数，供人看）。
      它们**不需要**脚本逐条核对（脚本只关心"必须存在的关键度量"），
      但反过来——**脚本要求的每一条都必须真被 SQL 产出**——是硬要求。
    ★ 等式方向搞反会怎样：若要求相等，加一行 `metric.snap.dim_channel` 就得
      同步改脚本清单，纯属噪音；而若只要求"SQL 里有"，删掉 A2 也不会有测试红。
    """
    sql_metrics = set(_sql_metric_names())
    expected = set(_expected_metrics_from_script())
    missing = expected - sql_metrics
    assert not missing, f"脚本要求但这些度量 SQL 未产出：{sorted(missing)}"
    # A2 与指纹四件套必须在脚本清单里（票面 AC 与幂等判据的落点）
    assert "metric.a2_dup_overshoot" in expected
    for key in ("rows", "hash", "devent", "dlag"):
        assert f"metric.fp.{key}" in expected, f"指纹键 {key} 未进脚本清单"


def test_fingerprint_keys_are_all_produced_by_sql() -> None:
    """脚本里 `FP_KEYS` 的四个键，SQL 必须都产出对应度量行。

    ★ 若不产出：`read_fp_from_parsed` 会静默得到空串 ⇒ 指纹文件写成
      `"  "` ⇒ **两次都空 ⇒ 比对永远通过**。这是"幂等判据恒真"的典型通道。
    """
    m = re.search(r"FP_KEYS=\((.*?)\)", _script_text())
    assert m, "lake_assert.sh 里找不到 FP_KEYS"
    keys = re.findall(r"([A-Za-z0-9_]+)", m.group(1))
    assert keys, "FP_KEYS 为空"
    produced = set(_sql_metric_names())
    for key in keys:
        assert f"metric.fp.{key}" in produced, (
            f"FP_KEYS 里的 {key!r} 在 SQL 里没有对应的 metric.fp.{key} 度量行"
        )


def test_sentinel_names_fit_tableau_cell_width() -> None:
    """哨兵名必须**短到不被截断**（本机实测：TABLEAU 约 30 字符就渲染成 `…`）。

    ★ 这是本票实测抓到的静默失效：名字被截断后脚本按名核对会「没有出现」，
      而**真实原因**（太宽被截）从错误信息里完全看不出。
      ⇒ 把上限钉在 **26**（留 4 字符余量给 `-` 等），在 pytest 阶段就拦住。
    """
    too_long = [n for n in _sql_sentinel_names() if len(n) > 26]
    assert not too_long, f"以下哨兵名超过 26 字符，TABLEAU 会截断：{too_long}"


def test_no_error_token_in_sql() -> None:
    """`sql/` 下**每一个**脚本都不得含带方括号的错误标记字面量。

    ★ 原因：sql-client 会**逐字回显 SQL 源码（含注释）**，而两个判定面
      （`lake_replay.sh` 的 `run_sql`、`lake_assert.sh` 的错误检测）都在
      **全文**上找该标记 ⇒ 注释里写一个就会让一次**成功**的运行判成失败。
    ★ 这不是理论风险：本票实测踩了**两次** ——
      ① `sql/assert_lake.sql` 首版注释里引用了源码事实里的错误标记 ⇒ 断言必红；
      ② `sql/replay_90_maintain.sql` 首版同样 ⇒ `make replay` 在维护段报
         「SQL 报错」，而日志里真实内容只是回显的注释。
    ⇒ 覆盖面必须是**整个 sql/ 目录**，不能只管 assert_lake.sql：
      `lake_replay.sh` 对五层脚本用的是同一条判据。
    ★ 要引用这个标记时：去掉方括号（`ERROR`）或改写描述。
    """
    assert _SQL_DIR.is_dir(), f"缺 sql 目录：{_SQL_DIR}"
    offenders = []
    for path in sorted(_SQL_DIR.glob("*.sql")):
        text = path.read_text(encoding="utf-8")
        if re.search(r"\[ERROR\]", text):
            offenders.append(path.name)
    assert not offenders, (
        f"以下 SQL 文件含方括号错误标记字面量（回显后会被判成真错误）：{offenders}"
    )


def test_script_pins_container_and_sql_path() -> None:
    """脚本必须走既有约定：`eco-flink-jm` 容器 + `sql/assert_lake.sql` 路径。

    与 `scripts/lake_replay.sh` 用同一个容器（`LAKE_FLINK_CONTAINER` 可覆盖），
    否则两半会跑在两个 JVM/cgroup 上（impl-03c 的 OOM 事故就是这类混淆）。
    """
    text = _script_text()
    # ★ 只钉**语义**（哪个变量取哪个值），不钉引号风格/空白 —— 否则改个引号就红，
    #   而被钉的东西（同容器、同 SQL 路径）其实没变。
    assert re.search(r'CONTAINER="\$\{LAKE_FLINK_CONTAINER:-eco-flink-jm\}"', text), (
        "容器未与 lake_replay.sh 取同一个（eco-flink-jm，可用 LAKE_FLINK_CONTAINER 覆盖）"
    )
    assert re.search(r'SQL_FILE="sql/assert_lake\.sql"', text), (
        "断言脚本路径不是 sql/assert_lake.sql"
    )


# ════════════════════════════════════════════════════════════════════════
# 6. 交付面：维护段 / DDL 属性 / 编排接线
# ════════════════════════════════════════════════════════════════════════
def _ddl_text() -> str:
    assert _DDL_SQL.is_file(), f"缺 DDL：{_DDL_SQL}"
    return _DDL_SQL.read_text(encoding="utf-8")


def _table_blocks() -> dict[str, str]:
    """把 DDL 切成 `表名 -> 该表定义块（列+属性）`。"""
    text = _ddl_text()
    blocks: dict[str, str] = {}
    for m in re.finditer(
        r"CREATE TABLE IF NOT EXISTS (\w+)\s*\((.*?)\)\s*(?:PARTITIONED BY[^;]*?)?WITH\s*\((.*?)\);",
        text,
        re.IGNORECASE | re.DOTALL,
    ):
        blocks[m.group(1)] = m.group(2) + "\n" + m.group(3)
    return blocks


def test_every_table_pins_snapshot_retention() -> None:
    """**11 张表每一张**都必须显式写 `min=5` 与 `max=20`。

    ★ 为什么必须显式（票面 AC：`num-retained.min` 显式为 **5**）：
      官方默认是 **10**，而 10 **也落在 [5,20] 区间内** ⇒ 运行时快照数判据
      **看不出**有人漏写。只有静态守卫能拦。
    ★ 为什么 11 张都要：漏掉一张时，那张表的快照数会由 `time-retained`(1h) 单控
      ⇒ 长时间跑下来可以远超 20 ⇒ 这正是 `max=20` 要堵的"不受控"。
    """
    blocks = _table_blocks()
    assert len(blocks) == 11, (
        f"DDL 里应有 11 张表，实际 {len(blocks)}：{sorted(blocks)}"
    )
    missing = [
        name
        for name, block in blocks.items()
        if "'snapshot.num-retained.min' = '5'" not in block
        or "'snapshot.num-retained.max' = '20'" not in block
    ]
    assert not missing, f"以下表缺快照保留属性：{missing}"


def test_ddl_does_not_pin_time_retained() -> None:
    """`snapshot.time-retained` **不得**作为**表属性**写进 DDL —— 保持官方默认 1 h。

    票面 AC 逐字：「`time-retained` 保持 **1 h 默认**」。写上任何值都是改默认，
    且会与 gdd 的「实际快照数由 1 h 窗口决定」这一结论脱钩。
    ★ 判据是「属性键**没有**被赋值」，不是「文件里没有这个词」：
      注释里**必须**提到它（解释"为何不写"），那是文档而非配置。
      ⇒ 去掉注释后再找，且找的是 `'snapshot.time-retained' = '...'` 的形状。
    """
    code = _strip_comments(_ddl_text())
    assert not re.search(r"'snapshot\.time-retained'\s*=", code), (
        "replay_01_ddl.sql 给 snapshot.time-retained 赋了值（应保持官方默认 1h，不写）"
    )
    # 注释里必须说明"为何不写"（这是交付的文档面，不是装饰）
    assert "time-retained" in _ddl_text(), "DDL 注释里应说明 why 不写 time-retained"


def test_maintain_sql_calls_compact_and_expire_for_every_table() -> None:
    """维护段必须对**每一张表**都调 compact 与 expire（票面交付内容第 1 项）。"""
    assert _MAINT_SQL.is_file(), f"缺维护脚本：{_MAINT_SQL}"
    text = _MAINT_SQL.read_text(encoding="utf-8")
    code = _strip_comments(text)
    tables = sorted(_table_blocks())
    for t in tables:
        assert f"sys.compact(`table` => 'ecom_lake.{t}'" in code, (
            f"维护段没有对 {t} 调 sys.compact"
        )
        assert f"sys.expire_snapshots(`table` => 'ecom_lake.{t}'" in code, (
            f"维护段没有对 {t} 调 sys.expire_snapshots"
        )


def test_maintain_expire_uses_retain_max_20() -> None:
    """`retain_max => 20` 逐字（票面 AC：收的是**上界**）。

    ★ 同时钉住注释里"上界不是钉住"这句话**真的在**：票面 AC 要求
      「日志/注释里说清 `retain_max` 是上界不是钉住」。注释不是装饰，是交付项。
    """
    text = _MAINT_SQL.read_text(encoding="utf-8")
    assert "retain_max => 20" in text
    assert "上界" in text, "维护段注释未说明 retain_max 是上界"
    assert "钉住" in text, "维护段注释未说明它不是『钉住』"


def test_maintain_runs_expire_after_compact() -> None:
    """顺序必须是 **compact → expire**（先 expire 会让新压出的快照留在界外）。"""
    code = _strip_comments(_MAINT_SQL.read_text(encoding="utf-8"))
    first_compact = code.find("sys.compact(")
    first_expire = code.find("sys.expire_snapshots(")
    assert first_compact >= 0 and first_expire >= 0
    assert first_compact < first_expire, "expire 出现在 compact 之前（顺序反了）"
    # 最后一条 expire 也必须晚于最后一条 compact
    assert code.rfind("sys.compact(") < code.rfind("sys.expire_snapshots(")


def test_maintain_pins_batch_runtime_mode() -> None:
    """`compact_strategy => 'full'` **只支持 batch 模式**（官方逐字）⇒ 必须钉 batch。"""
    text = _MAINT_SQL.read_text(encoding="utf-8")
    assert "execution.runtime-mode' = 'batch'" in text, "维护段未钉 batch 模式"


def test_maintain_uses_stage_marker_not_layer_marker() -> None:
    """维护段的可观察性行必须用 `maintain rows=`，**不得**伪造 `layer=maintain`。

    ★ `layer=` 是 spec §5.3 的**层**词汇表（ods|dwd|dim|dws|ads），
      `LAYER=` 参数就是从它派生。造一个 `layer=maintain` 会让层列表出现
      一个永远选不中的标签（与拒绝表用 `dwd_reject` 的取舍同源）。
    """
    code = _strip_comments(_MAINT_SQL.read_text(encoding="utf-8"))
    assert "maintain rows=" in code, "维护段缺 maintain rows= 正向证据"
    assert "layer=maintain" not in code, "维护段伪造了 layer=maintain 标签"


def test_replay_sh_runs_maintain_only_on_full_replay() -> None:
    """`lake_replay.sh` 必须接上维护段，且**只在全量 replay** 时跑。

    ★ 为什么 LAYER 截断时不跑：维护段的语义是「这批五层数据落稳之后收快照」，
      数据不完整时压了只是白做（且 expire 会提前删掉可能还要用的快照）。
    """
    assert _REPLAY_SH.is_file()
    text = _REPLAY_SH.read_text(encoding="utf-8")
    assert 'run_sql maintain "replay_90_maintain.sql"' in text, (
        "lake_replay.sh 未调用维护段"
    )
    assert 'if [ "$STOP_AT" -lt 0 ]; then' in text, (
        "维护段未按「仅全量 replay」条件包裹"
    )
    assert "maintain rows=" in text, "维护段的 marker 未接线"


def test_a1_ods_side_filters_landing_day() -> None:
    """A1 的 ODS 侧必须限定**本次落地日**（与 `v_ods_scope` 同源）。

    ★ 这条守卫钉的是**一个真事故**而非风格：ODS 是 append + 落地日分区，跨天累积。
      不加过滤时，A1 的两侧数据范围不同源 —— 只要历史上落过**另一批**，
      `ods_distinct` 就会把两批并起来，而 DWD 只装本次落地日那一批 ⇒ A1 无缘无故红。
    ★ 反过来（加过滤）在「UTC 日边界刚过、本批还没重放」时会让 A1 **正确地红**
      （`ods_today = 0` 而 DWD 仍是上一批）。若是整表口径，两边都读上一批 ⇒ 假绿。
      「批次根本不对齐时通过」比红糟糕得多。
    """
    code = _strip_comments(_assert_sql_text())
    a1_block = code[code.index("A1_split_complete") :]
    a1_block = a1_block[: a1_block.index("A2") if "A2" in a1_block else 2000]
    # ★ 落地日取 `(SELECT MAX(dt) FROM ods_behavior_event)`，**不取**
    #   `DATE_FORMAT(CURRENT_TIMESTAMP,…)` —— 后者把判据绑死到"跑断言那天的 UTC 日"，
    #   而 replay 可能在前一天跑完 ⇒ 跨日后 A2 会静默变成 0（0 在日志里读起来
    #   正是"这批没有重复"，语义被悄悄换掉）。这是 code-review 的 G1。
    assert re.search(
        r"WHERE\s+dt\s*=\s*\(SELECT\s+MAX\(dt\)\s+FROM\s+ods_behavior_event\)",
        a1_block,
        re.IGNORECASE,
    ), "A1 的 ODS 侧未限定到最近一次落地批（会依赖历史落地批/或被钉到当天）"
    assert "CURRENT_TIMESTAMP" not in a1_block, (
        "A1 的 ODS 侧仍用 CURRENT_TIMESTAMP 取日期（跨 UTC 日会与 replay 的时间基不一致）"
    )


def test_a2_filters_landing_day() -> None:
    """A2 的 ODS 侧同理 —— 不过滤时报的是「跑过几次」而不是「这批有多少重复」。

    ★ 本票实测：首版不加过滤得到 **1 048 576**，正是前一天那一整个分区；
      加过滤后为 **0**（门槛批本身无重复）。同一个判据、天壤之别的数值，
      且**两个都能"看起来合理"** —— 只有钉住过滤条件才分得清。
    """
    code = _strip_comments(_assert_sql_text())
    idx = code.index("metric.a2_dup_overshoot")
    a2_block = code[idx : idx + 600]
    assert re.search(
        r"WHERE\s+dt\s*=\s*\(SELECT\s+MAX\(dt\)\s+FROM\s+ods_behavior_event\)",
        a2_block,
        re.IGNORECASE,
    ), "A2 的 ODS 侧未限定到最近一次落地批（会报成『跑过几次』）"
    assert "CURRENT_TIMESTAMP" not in a2_block, (
        "A2 的 ODS 侧仍用 CURRENT_TIMESTAMP 取日期"
    )
    # ★ A2 不作断言 ⇒ 它出错时没有哨兵会红，必须靠「比的是哪天」显式可读。
    assert "metric.rows.landing_day" in _strip_comments(_assert_sql_text()), (
        "缺 metric.rows.landing_day：A2 的 0 无法区分『真无重复』与『比错天』"
    )


def test_new_files_use_lf_line_endings() -> None:
    """本票新增/改动的文本文件必须是 **LF**（与仓内既有 9 个 SQL 脚本一致）。

    ★ 这是**施工中真踩到的**：改 `sql/replay_01_ddl.sql` 时用脚本重写，Python 在
      Windows 上以 `newline=None` 写文本文件 ⇒ 整篇 LF 被换成 **CRLF**。
      症状：`git diff --stat` 显示 **362 insertions / 305 deletions**（整文件重写），
      而真实改动只有几十行。审阅者会被这个假 diff 引向完全错误的地方，
      且真正的改动埋在里面看不见。
    ⇒ 判据只查**本票涉及的文件**，不是全仓（既有文件的历史状态不在本票职责内）。
    """
    files = [
        _ASSERT_SQL,
        _MAINT_SQL,
        _SCRIPT,
        _ROOT / "tests" / "test_lake_assert.py",
        _DDL_SQL,
        _REPLAY_SH,
        _ROOT / "Makefile",
        _ROOT / "sql" / "README.md",
        _ROOT / "GLOSSARY.md",
    ]
    offenders = []
    for path in files:
        assert path.is_file(), f"缺文件：{path}"
        if b"\r\n" in path.read_bytes():
            offenders.append(path.name)
    assert not offenders, (
        f"以下文件含 CRLF 行尾（本仓统一 LF；会造成整文件假 diff）：{offenders}"
    )


def test_snapshot_retention_literals_agree() -> None:
    """`5` / `20` 这三处字面量（DDL / ALTER / 断言期望）必须**一致**。

    ★ 三处都必要，无法合并成一处（理由见 `sql/assert_lake.sql` ⑦ 段的注释）：
      · `replay_01_ddl.sql` —— 管**新建**的表；
      · `replay_90_maintain.sql` —— 管**已存在**的表
        （`CREATE TABLE IF NOT EXISTS` 对老表一句不改）；
      · `sql/assert_lake.sql` —— 是**判据**（读集群 `$options`，不读 DDL 文本）。
    ★ 风险所在：只改一处时，**另外两处会静默地按旧值工作** ——
      DDL 与 ALTER 不一致时，新建表与老表拿到不同属性（集群内部就分裂了）；
      而断言若停在旧值，它会**红**（这一半是好事），但如果有人"为了让它绿"
      去改断言而不改 DDL，就变成了判据追着实现跑。
    ⇒ 本条让「改一个必须改三个」在 pytest 阶段就现形。
    """
    ddl_code = _strip_comments(_ddl_text())
    maint_code = _strip_comments(_MAINT_SQL.read_text(encoding="utf-8"))
    assert_code = _strip_comments(_assert_sql_text())

    ddl_min = set(re.findall(r"'snapshot\.num-retained\.min'\s*=\s*'(\d+)'", ddl_code))
    ddl_max = set(re.findall(r"'snapshot\.num-retained\.max'\s*=\s*'(\d+)'", ddl_code))
    maint_min = set(
        re.findall(r"'snapshot\.num-retained\.min'\s*=\s*'(\d+)'", maint_code)
    )
    maint_max = set(
        re.findall(r"'snapshot\.num-retained\.max'\s*=\s*'(\d+)'", maint_code)
    )
    assert_min = set(
        re.findall(
            r"'snapshot\.num-retained\.min'\s*AND\s*`value`\s*=\s*'(\d+)'", assert_code
        )
    )
    assert_max = set(
        re.findall(
            r"'snapshot\.num-retained\.max'\s*AND\s*`value`\s*=\s*'(\d+)'", assert_code
        )
    )

    assert ddl_min == {"5"}, f"DDL 的 min 不止一个值：{ddl_min}"
    assert ddl_max == {"20"}, f"DDL 的 max 不止一个值：{ddl_max}"
    assert maint_min == ddl_min, f"维护段的 min {maint_min} ≠ DDL {ddl_min}"
    assert maint_max == ddl_max, f"维护段的 max {maint_max} ≠ DDL {ddl_max}"
    assert assert_min == ddl_min, f"断言的 min {assert_min} ≠ DDL {ddl_min}"
    assert assert_max == ddl_max, f"断言的 max {assert_max} ≠ DDL {ddl_max}"


def test_markers_are_not_satisfiable_by_echo() -> None:
    """层/维护段的 marker 判据**不得**被 sql-client 回显的注释满足（假绿通道）。

    ★ 这是本票实测抓到的第二个假绿通道（第一个是「缺哨兵整行」）：
      sql-client 会**逐字回显 SQL 源码（含注释）**，而各层脚本的注释里就写着
      marker 字面量。实测维护段：输出里 **3 处回显 + 1 处结果** ⇒
      把结果行删掉，旧的全文 `grep -q` **仍然通过**
      ⇒ 正向证据退化成「文件被回显过」，而 marker 的全部意义就是
      「absence of error != success」。
    ★ 判据分两层：
      ① 脚本侧必须锚在**表格行**上（`^\\|` 起头）；
      ② SQL 注释里**不得**出现「看起来像结果」的 marker 形状（`marker=数字`），
         因为那种形状一旦被将来某个更宽松的判据捞到，就会重演这个洞。
    """
    script = _REPLAY_SH.read_text(encoding="utf-8")
    # ① marker 检查必须锚表格行。用"行内容断言"而不是正则转义 ——
    #    后者在本文件里已经因为反斜杠层级写错过一次（守卫自己写错比没有更糟）。
    marker_check = [
        line
        for line in script.splitlines()
        if "marker" in line and "grep" in line and not line.lstrip().startswith("#")
    ]
    assert marker_check, "lake_replay.sh 里找不到 marker 的判定语句"
    anchored = [line for line in marker_check if "|" in line and "^" in line]
    assert anchored, (
        "lake_replay.sh 的 marker 判据未锚定表格行（需要 ^| 形状）"
        f"⇒ 会被回显的注释满足；实际语句：{marker_check}"
    )
    # ② 各层 SQL 的**注释行**里不得出现「marker=数字」形状。
    #    代码里出现 `CONCAT('layer=ods rows=', …)` 是**产出**，必须允许；
    #    故只查以 `--` 开头的行。
    offenders = []
    for path in sorted(_SQL_DIR.glob("*.sql")):
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.lstrip().startswith("--"):
                continue
            if re.search(r"(layer=[a-z]+|dwd_reject|maintain)\s+rows=\d+", line):
                offenders.append(f"{path.name}: {line.strip()[:80]}")
    assert not offenders, (
        "以下 SQL 注释里出现了「marker=数字」形状（会被宽松判据当结果）："
        + str(offenders)
    )


def test_judge_rejects_green_where_one_sentinel_missing(judge) -> None:
    """回归守卫：删掉**任何一条**哨兵都失败（逐个试，不是抽样一条）。"""
    for name in _ALL_NAMES:
        r = judge(_stdout(green=True, missing=(name,)))
        assert r.returncode == 1, f"删掉 {name} 竟然通过"
        assert name in r.stderr


def test_judge_rejects_green_where_one_sentinel_false(judge) -> None:
    """回归守卫：任意一条 `ok=false` 都失败（逐个试）。"""
    for name in _ALL_NAMES:
        text = _stdout(green=True).replace(
            f"| {name} | ok=true |", f"| {name} | ok=false |"
        )
        r = judge(text)
        assert r.returncode == 1, f"{name} 变 false 竟然通过"


def test_assert_sql_pins_required_session_settings() -> None:
    """断言脚本必须钉 batch / UTC / dml-sync（与其它层脚本同一口径）。"""
    text = _assert_sql_text()
    assert "execution.runtime-mode' = 'batch'" in text
    assert "table.local-time-zone' = 'UTC'" in text
    assert "table.dml-sync' = 'true'" in text


def test_assert_sql_does_not_use_crc32() -> None:
    """**不得**用 `crc32` —— Flink 2.2 SQL 里没有这个函数（本机实测）。

    逐字报错：`SqlValidatorException: No match found for function signature
    crc32(<CHARACTER>)`。spec §5.4 的伪代码里有它，但**伪代码不能执行**。
    ★ 这条守卫的价值：`crc32` 写进去后 sql-client **仍退 0** ⇒ 只有本哨兵
      （`IDEMPOTENCY_fp_ready`）会变红，而它红得像个数据问题、不像语法问题。
      静态守卫让它一眼可见。
    """
    code = _strip_comments(_assert_sql_text())
    assert not re.search(r"\bcrc32\s*\(", code, re.IGNORECASE), (
        "assert_lake.sql 用了 crc32（Flink 2.2 没有该函数）"
    )


def test_assert_sql_hash_code_is_cast_before_sum() -> None:
    """`HASH_CODE` 必须**先 CAST 成 BIGINT 再 SUM**。

    ★ `HASH_CODE` 返回 INT；百万行求和会**溢出 INT**。溢出后的值在两次跑之间
      仍**稳定** ⇒ 指纹比对照样通过，但校验和能力已经废了。
      「稳定但无效」是最难发现的一类判据缺陷，故在源码层钉住。
    """
    code = _strip_comments(_assert_sql_text())
    # 每个 SUM(...HASH_CODE...) 的形状都必须是 SUM(CAST(HASH_CODE(...) AS BIGINT))
    assert re.search(r"SUM\s*\(\s*CAST\s*\(\s*HASH_CODE\s*\(", code, re.IGNORECASE), (
        "缺 SUM(CAST(HASH_CODE(...) AS BIGINT)) 的形状"
    )
    assert not re.search(r"SUM\s*\(\s*HASH_CODE\s*\(", code, re.IGNORECASE), (
        "出现 SUM(HASH_CODE(...)) 未先 CAST ⇒ INT 溢出"
    )
