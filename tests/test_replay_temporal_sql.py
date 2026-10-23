"""DIM 拉链 + DWS 区间 join + ADS 键同构的**跨层守卫**（impl-03c）。

本票把 30/40/50 三个 SQL 脚本从无到有落盘，其中三处约定一旦写错都**不会报错**，
只会静默产出错数：

1. **拉链区间约定**：`valid_to_ms = 下一版 change_time − 1`（闭区间），与 join 侧
   `BETWEEN valid_from_ms AND valid_to_ms` 是一对。若一侧改成开区间或忘了 `− 1`，
   边界那一毫秒的事件会**漏命中**（少算）或**双命中**（多算）。
2. **时态 join 走 batch 区间 join**：写成 lookup join / `FOR SYSTEM_TIME AS OF` /
   temporal 提示词，语法都合法、作业也跑得动，但 lookup join 是 processing-time，
   回放必错（票面 Notes 逐字禁止）。
3. **ADS 键与 Doris 同构**：Paimon ADS 的键列名/含义与 Doris
   `ecom_ads.ads_window_metric`（UNIQUE KEY `(window_start, metric_name, dim_key)`）
   必须逐列对得上，否则 impl-03f 的逐值对账**配不上键**，而两边各自的单测都是绿的。

本文件只做**静态形状守卫**（读 SQL 文本 / 读 Doris DDL 文本），不连集群 ——
真跑通与逐值正确由 `make replay` + impl-03d 的断言负责。
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

_SQL_DIR = Path(__file__).resolve().parent.parent / "sql"
_DIM = _SQL_DIR / "replay_30_dim.sql"
_DWS = _SQL_DIR / "replay_40_dws.sql"
_ADS = _SQL_DIR / "replay_50_ads.sql"
_DDL = _SQL_DIR / "replay_01_ddl.sql"
_DORIS_INIT = Path(__file__).resolve().parent.parent / "docker" / "doris" / "init.sql"


def _text(path: Path) -> str:
    assert path.is_file(), f"缺编排脚本：{path}"
    return path.read_text(encoding="utf-8")


def _strip_comments(text: str) -> str:
    """去掉 `--` 行注释，避免注释里的反面说明被当作真实语句判中。

    ★ 必须做这一步：本仓 SQL 的注释刻意写满了「为什么不用 lookup join」之类的
    反面说明，直接用原文匹配会**把注释当代码**，守卫变成假阴性来源。
    """
    return "\n".join(line.split("--", 1)[0].rstrip() for line in text.splitlines())


def _norm(text: str) -> str:
    """归一空白 + 大小写，便于跨排版比较。"""
    return re.sub(r"\s+", " ", text).lower()


# ── 1. 拉链：区间闭合约定自洽（写侧 − 1 ↔ 读侧 BETWEEN）─────────────────
def test_zipper_valid_to_is_next_change_time_minus_one() -> None:
    """`valid_to_ms` 必须是「下一版 change_time − 1」，且与 `BETWEEN`（闭）成对。

    这一毫秒之差决定边界事件的归属。**写侧与读侧必须成对**：
    · `− 1` + 闭区间 `BETWEEN` ⇒ 边界那一毫秒恰归**新版本**，两版不重叠、不留空隙；
    · 若改成「valid_to_ms = 下一版起点」再配开区间，是另一种自洽写法；
    · 但**混搭**（`− 1` 配开区间 / 不 `− 1` 配闭区间）会让边界那一毫秒
      **两个版本都不命中或都命中**。

    ★ 本仓选前者（[05] Q10 签字口径，与生产 SCD2 惯例一致）。本条只钉**写侧形状**；
    读侧 `BETWEEN` 的闭区间形状由下一条 test_dws_join_is_a_batch_range_join 钉。
    两边分开钉是刻意的：只钉一边时，另一半改成开区间不会有任何测试变红。
    """
    code = _strip_comments(_text(_DIM))
    # 形状：LEAD(change_time) OVER (...) - 1  （OVER 子句里含括号，故不能只 [^)]*）
    assert re.search(
        r"LEAD\s*\(\s*change_time\s*\)\s*OVER\s*\([^)]*\)\s*-\s*1",
        code,
        re.IGNORECASE,
    ), "valid_to_ms 未写成 LEAD(change_time) OVER (...) - 1（边界那一毫秒的归属会错）"
    assert "9999999999999" in code, "缺「仍有效」哨兵值 9999999999999"


def test_dws_join_is_a_batch_range_join_not_a_lookup_join() -> None:
    """DWS 必须用**闭区间 BETWEEN** 的 batch 区间 join；且**不得**出现 lookup/temporal。

    票面 Notes 逐字禁止 lookup join（processing-time，回放必错），也禁止
    `FOR SYSTEM_TIME AS OF` 与 temporal 提示词。这些写法语法合法、作业能跑，
    只有静态守卫能拦住「有人图省事改回 lookup」。
    """
    norm = _norm(_strip_comments(_text(_DWS)))

    # ① 区间 join 的硬形状：等值键 + BETWEEN valid_from_ms AND valid_to_ms
    assert re.search(r"between\s+d\.valid_from_ms\s+and\s+d\.valid_to_ms", norm), (
        "DWS 缺 ON ... BETWEEN d.valid_from_ms AND d.valid_to_ms 的区间条件"
    )
    assert re.search(r"on\s+d\.item_id\s*=\s*w\.item_id", norm), (
        "DWS 缺 ON d.item_id = w.item_id 的等值键"
    )

    # ② 反面：lookup join / temporal / 系统时间 / 排序压缩
    for forbidden in ("lookup", "for system_time as of", "temporal", "sequence.field"):
        assert forbidden not in norm, f"DWS 出现了被禁止的时态写法：{forbidden!r}"


def test_dws_covers_exactly_three_subject_types() -> None:
    """`subject_type` 的取值域**恰为** user / item / category 三个（票面验收第 4 条）。

    DWS 的键是 (dt, subject_type, subject_id, window_start)；多一类或少一类都会让
    ADS 的 pv/uv/gmv 汇总口径漂移，而表上看不出「少了哪一类」。
    """
    code = _strip_comments(_text(_DWS))
    written = set(re.findall(r"'\s*(\w+)\s*'\s*AS\s+subject_type", code, re.IGNORECASE))
    assert written == {"item", "user", "category"}, (
        f"subject_type 取值域不是恰三值：{sorted(written)}"
    )


def test_dws_window_start_is_a_dwd_derived_minute_floor() -> None:
    """`window_start` 必须是 `(event_time / 60000) * 60000` 的窗口下界。

    这是实时侧 `(eventTime / WINDOW_SIZE_MS) * WINDOW_SIZE_MS`（60_000）的
    离线影子副本；改成别处推导（例如直接用 event_time）会让 DWS/ADS 的
    `window_start` 与 Doris 实时口径**不可比**，对账（03f）当场失效。
    """
    norm = _norm(_strip_comments(_text(_DWS)))
    assert re.search(r"event_time\s*/\s*60000\s*\)\s*\*\s*60000", norm), (
        "DWS 的 window_start 不是 (event_time / 60000) * 60000"
    )


def test_dws_does_not_filter_dwd_by_landing_day() -> None:
    """DWD 是**整表 OVERWRITE**，DWS 读它时**不得**加落地日过滤。

    ★ 这是与本仓其它层相反的一条，写反了会**读空**：DWD 的 `dt` 是**事件日**
    （不是落地日），拿 `CURRENT_TIMESTAMP` 去过滤它等于筛掉所有历史事件。
    `replay_20_dwd.sql` 必须过滤（它读 append 的 ODS），`replay_40_dws.sql`
    必须**不过滤**（它读 OVERWRITE 的 DWD）—— 这一对差异是最容易写反的地方。
    """
    code = _strip_comments(_text(_DWS))
    assert not re.search(
        r"FROM\s+dwd_behavior_event\s+WHERE\s+dt\s*=", code, re.IGNORECASE
    ), "DWS 给 DWD 加了落地日过滤 ⇒ DWD 的 dt 是事件日，会读空"
    assert not re.search(r"CURRENT_TIMESTAMP", code, re.IGNORECASE), (
        "DWS 出现 CURRENT_TIMESTAMP ⇒ 疑似把落地日过滤写到了 DWD 上"
    )


# ── 2. ADS：键与 Doris 同构 ─────────────────────────────────────────────
def test_ads_key_columns_are_isomorphic_with_doris() -> None:
    """ADS 的键列必须与 Doris `ads_window_metric` 的 UNIQUE KEY 逐列对得上。

    票面验收第 5 条。Paimon 侧多一个分区列 `dt`（= 窗口起点日，可由
    `window_start` 唯一确定），这是**同构**而非不一致 —— 守卫同时钉住
    「去分区列后键集合相同」与「多出来的那一列确实是 dt」两件事。
    """
    ddl = _text(_DDL)
    paimon = re.search(
        r"CREATE TABLE IF NOT EXISTS ads_window_metric\s*\((.*?)\)\s*PARTITIONED BY\s*\(\s*(\w+)\s*\)",
        ddl,
        re.IGNORECASE | re.DOTALL,
    )
    assert paimon, "DDL 里找不到 ads_window_metric 的分区主键表定义"
    columns_blob, partition_col = paimon.group(1), paimon.group(2)
    pk = re.search(r"PRIMARY KEY\s*\(([^)]*)\)", columns_blob, re.IGNORECASE)
    assert pk, "ads_window_metric 缺 PRIMARY KEY"
    paimon_keys = {c.strip().lower() for c in pk.group(1).split(",")}

    doris = _text(_DORIS_INIT)
    doris_m = re.search(r"UNIQUE\s+KEY\s*\(([^)]*)\)", doris, re.IGNORECASE)
    assert doris_m, "Doris init.sql 里找不到 ads_window_metric 的 UNIQUE KEY"
    doris_keys = {c.strip().strip("`").lower() for c in doris_m.group(1).split(",")}

    # ① 去除分区列后，两组键**完全相同**
    assert paimon_keys - {partition_col.lower()} == doris_keys, (
        f"Paimon ADS 键（去分区列）{sorted(paimon_keys - {partition_col.lower()})} "
        f"≠ Doris UNIQUE KEY {sorted(doris_keys)}"
    )
    # ② 多出来的那一列确实是分区列，且是窗口起点日
    assert partition_col.lower() == "dt", (
        f"ADS 多出的键列不是 dt 分区列：{partition_col!r}"
    )


def test_ads_metric_names_match_doris_metric_domain() -> None:
    """ADS 写入的 `metric_name` 取值必须落在 Doris 侧同一指标域内（pv/uv/gmv）。"""
    code = _norm(_strip_comments(_text(_ADS)))
    names = set(re.findall(r"'\s*(pv|uv|gmv)\s*'", code))
    assert names == {"pv", "uv", "gmv"}, (
        f"ADS 写入的 metric_name 不是恰三值：{sorted(names)}"
    )


def test_ads_aggregates_three_dws_subjects_into_one_key() -> None:
    """三行 DWS（item/user/category）必须**显式合并**成一行 ADS，不能各写一次。

    ADS 的键里没有 `subject_type` ⇒ 三条 DWS 若各自写一次 `dim_key='ALL'` 的行，
    会同键互相覆盖，落成「最后一个 subject 赢」= **静默数据丢失**。
    守卫钉住：`dim_key='ALL'` 的写入必须是对 DWS 的 `GROUP BY` 汇总。
    """
    code = _norm(_strip_comments(_text(_ADS)))
    assert "'all'" in code, "ADS 缺 dim_key='ALL' 的全量口径行"
    assert re.search(r"from\s+dws_subject_window", code), "ADS 全量口径未读 DWS"
    assert re.search(r"group\s+by\s+dt\s*,\s*window_start", code), (
        "ADS 全量口径未按 (dt, window_start) 汇总 ⇒ 三条 DWS 会互相覆盖"
    )
    # ★★ 必须只取**一条 1:1 subject**，不能把三条相加 —— 实机实测三倍计数
    #    （SUM(pv)=1409772 vs DWD page_view=469924，恰 3.0×）。
    #    三条 subject 的 join 都是 1:1 覆盖事件集，相加即三倍。
    assert re.search(r"where\s+subject_type\s*=\s*'user'", code), (
        "ADS 全量口径未取单一 1:1 subject（user）⇒ 三 subject 相加会三倍计数"
    )


def test_ads_channel_slice_reads_dwd_not_dws() -> None:
    """渠道切分必须回 DWD 取数 —— DWS 的键里**没有** channel 列。

    ★ 这条容易被「优化」掉：看到 DWS 已经有 pv/uv/gmv，就想按渠道切一刀。
    但 DWS 的 subject 键是 (subject_type, subject_id)，渠道根本不在键里；
    硬塞会破坏 §5.2 的 DWS 形状，只能从 DWD 重新按 (窗口 × 渠道) 聚合。
    """
    code = _norm(_strip_comments(_text(_ADS)))
    assert "channel=" in code, "ADS 缺渠道切分（dim_key='channel=…'）"
    assert re.search(r"from\s+dwd_behavior_event", code), "ADS 渠道切分未从 DWD 取数"
    # 渠道聚合必须带 GROUP BY（否则每行一个窗口外键，写进 ADS 会撞主键）
    assert re.search(r"group\s+by[^;]*channel", code), "ADS 渠道切分未按渠道分组"


# ── 3. 迟到计数：只存计数，不存明细 ──────────────────────────────────
def test_late_event_count_table_stores_only_counts() -> None:
    """`dwd_late_event_count` 必须**只存计数**（票面验收第 6 条）。

    明细留在 DWD（`is_late` 列）；这张表若是把明细再抄一遍，既浪费又把
    「迟到多少」变成「迟到哪些」，A3 的 `sum(cnt) == count(*) WHERE is_late=1`
    也会失去意义。
    """
    code = _norm(_strip_comments(_text(_ADS)))
    assert "dwd_late_event_count" in code, "ADS 层未写 dwd_late_event_count"
    tail = code.split("dwd_late_event_count")[1][:900]
    assert re.search(r"count\s*\(\s*\*\s*\)", tail), "迟到计数不是 COUNT(*)"
    for detail_col in ("event_id", "user_id", "item_id", "raw_payload"):
        assert detail_col not in tail, f"迟到计数表写入了明细列 {detail_col!r}"


# ── 3.5 同一张表**只许被一条** INSERT 写（实机事故换来的守卫）──────────
# ★★ 这条守卫的来历（impl-03c，2026-10-23 实机事故）：
#   ADS 有两个切面（`dim_key='ALL'` 与 `channel=…`）写**同一张主键表**。
#   首版①用 `INSERT OVERWRITE`、②用 `INSERT INTO` ⇒ 换更小跨度重跑时②的旧渠道行残留；
#   修法把②也改成 `INSERT OVERWRITE` ⇒ ②执行时把①刚写的 **全部 ALL 行抹掉**：
#     修复前 rows=90 348 ；两条 OVERWRITE 时 rows=77 436
#     差额 12 912 == 当时 `dim_key='ALL'` 的全部行数
#   ⚠️ 两次都是**五层 exit 0、marker 齐全**，只有数字错 —— marker 从不等于断言。
#   `INSERT OVERWRITE` 是**整表**语义：一张表要么由一条语句写全，要么谁都写不全。
def _insert_targets(text: str) -> list[str]:
    """抽出（去注释后的）所有 `INSERT [OVERWRITE|INTO] <表>` 的目标表名。"""
    code = _strip_comments(text)
    return [
        m.group(1).lower()
        for m in re.finditer(
            r"\bINSERT\s+(?:OVERWRITE|INTO)\s+(\w+)", code, re.IGNORECASE
        )
    ]


def test_no_sql_file_writes_a_table_more_than_once() -> None:
    """同一个 SQL 文件里，一张表**不得**被两条 INSERT 写。

    这是上面那段事故的最小不变式：只要一张表在同一个文件里被写两次，
    后一条 `INSERT OVERWRITE` 就会把前一条的结果整表抹掉（`INSERT INTO`
    则会在换跨度重跑时留下上批次残行）—— 两种都**不报错**。
    多切面必须并进**同一条** INSERT 的 `UNION ALL`。
    """
    offenders: list[str] = []
    for path in sorted(_SQL_DIR.glob("*.sql")):
        targets = _insert_targets(_text(path))
        dupes = {t for t in targets if targets.count(t) > 1}
        if dupes:
            offenders.append(f"{path.name}: {sorted(dupes)}")
    assert not offenders, (
        "以下文件对同一张表写了多条 INSERT（后一条 OVERWRITE 会抹掉前一条）："
        + "；".join(offenders)
    )


def test_ads_writes_both_slices_in_one_insert_statement() -> None:
    """ADS 的两个切面必须在**同一条** INSERT 里（票面事故的直接回归守卫）。

    不只钉「一张表一条 INSERT」：还要钉**两个切面真的都在那一条里** ——
    否则有人可以把渠道切面整段删掉，「一条 INSERT」依然成立而数据少一个面。
    """
    code = _strip_comments(_text(_ADS))
    inserts = list(
        re.finditer(
            r"\bINSERT\s+(?:OVERWRITE|INTO)\s+ads_window_metric\b",
            code,
            re.IGNORECASE,
        )
    )
    assert len(inserts) == 1, f"ADS 主表被 {len(inserts)} 条 INSERT 写（应恰 1 条）"
    # ★ 断言范围是**整个文件**而不是那条 INSERT 的文本：切面可以在它上游的
    #   临时视图里并好（当前实现就是 `v_metric_src` 的 UNION ALL + 一次 CROSS JOIN），
    #   只钉 INSERT 语句本身会把这种（更好的）写法误判成缺切面。
    assert "'ALL'" in code, "ADS 缺全量切面（dim_key='ALL'）"
    assert "channel=" in code, "ADS 缺渠道切面（dim_key='channel=…'）"
    assert re.search(r"\bunion\s+all\b", code, re.IGNORECASE), (
        "两个切面未并进同一条源（UNION ALL）⇒ 必有一面会被摊成第二条 INSERT"
    )


# ── 4. 零业务 jar（票面验收第 8 条）──────────────────────────────────
# ★ 这一条**不在这里**重复实现：tests/test_replay_sql.py::test_no_add_jar_statement_anywhere_in_sql
#   已经 glob 整个 sql/ 目录并正则匹配 `^\s*ADD\s+JAR\b`（含本票三份新脚本）。


# ── 5. 三份脚本的公共外观（与本仓既有脚本一致）─────────────────────────
# ★ 只钉 **本票新增的那一条**（batch runtime mode）：UTC / dml-sync 已由
#   tests/test_replay_sql.py::test_script_pins_session_settings 对 sql/*.sql
#   全量守住（它 glob 整个目录）⇒ 这里再断言一遍是重复守卫，删掉。
#   batch 模式**不能**删：流模式下同一条区间 join 语义不同（时态维会退化成
#   lookup），而它是本票时态语义的前提，既有守卫不认识这条。
@pytest.mark.parametrize(
    "name", ["replay_30_dim.sql", "replay_40_dws.sql", "replay_50_ads.sql"]
)
def test_new_layers_pin_batch_runtime_mode(name: str) -> None:
    """三层新脚本必须钉 **batch runtime mode**（时态区间 join 的语义前提）。"""
    text = _text(_SQL_DIR / name)
    assert "execution.runtime-mode' = 'batch'" in text, f"{name} 未钉 batch 模式"
