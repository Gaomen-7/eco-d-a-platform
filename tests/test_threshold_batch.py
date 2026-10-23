"""门槛批（impl-03b §E5 F5）的生成器单测：条数 / dt 分区 / 三通道 / 跨度归一化。

判据来源：票面 §E5 F5 明列的四条 —— 恰好 N 条、dt 恰 3 个、三通道全非空、
`3d` 与 `72h` 归一化等价、非整天输入报错 —— 再加「同 seed 可复现」。

**为什么必须用 plan 驱动真生成器**（而不是只验 `plan_threshold_batch` 的算术）：
门槛批的三个数是**互相耦合**的 —— 条数由 `max_events` 定、跨度由
`watermark_step_ms` 定，而迟到注入器还会把 `event_time` 往回拨最多
300 s（`injectors._EXPIRED_MAX_MS`）。票面记的「坑③」正是：基座压在
00:00:00Z 时 dt 会出 **5** 个分区而不是 3 个。只验推导函数的返回值发现不了这类
耦合错误 —— 必须照 `cli._params` 的用法产一遍，再按 UTC 日真数分区。

**为什么单测用 2000 条而不是 1048576 条**：判据是**形状类**（条数相等、分区个数、
通道非空），与规模无关；1M 条要 45 s，会把整套单测拖成分钟级。真批次由
`make replay` 产，不归单测管。
"""

from __future__ import annotations

import itertools
import json
from dataclasses import replace
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from ecomgen.cli import _parse_logical_span, main
from ecomgen.injectors import _EXPIRED_MAX_MS
from ecomgen.producer import (
    _THRESHOLD_MAX_LATE_MS,
    Event,
    EventBatch,
    ProducerParams,
    ThresholdBatchPlan,
    event_dt,
    plan_threshold_batch,
    produce_batch,
)

# 固定 seed（票面实机用的就是它），使全部断言**确定**、失败可逐条复现。
_SEED = 20241022
# 单测规模：判据与规模无关，取 2000 条（生成 < 0.1 s）。
_N_EVENTS = 2_000
_DAY_MS = 86_400_000


def _batch_from_plan(
    n_events: int = _N_EVENTS, seed: int = _SEED
) -> tuple[ThresholdBatchPlan, EventBatch]:
    """用 plan 驱动生成器，**装配走 `ProducerParams.from_plan`**（与 CLI 同一入口）。

    ★ 为什么不再在这里手装 ProducerParams（impl-03b code-review）：本文件的注释
      原本自己写着「测例应尽量少抄 CLI 的装配」，却又逐字段抄了一遍。抄两遍的后果是
      真实存在的：`from_plan` 加/改一个 plan 派生字段时，这里不同步就会产出**另一种**
      批次，而单测用的正是这份错的装配，于是它验的还是自己 ⇒ 照样全绿。
      现在装配只有一处实现，测试与 CLI 走同一个函数，抄无可抄。

    会话数由 `plan.carrier_sessions` 给（每会话 ≈2.2 条、多 20% 余量）：条数由
    `max_events` **精确截断**，会话数只负责「造够」—— 这正是票面 E7 第 9 条
    「`--sessions` ≠ 条数」的落地方式。
    """
    plan = plan_threshold_batch(n_events)
    return plan, produce_batch(ProducerParams.from_plan(plan, seed=seed))


def _dt_of(event: Event) -> str:
    """事件时间的 UTC 日（`yyyy-MM-dd`）—— 与 DDL 的 dt 口径同源。

    ★ 换算走 `producer.event_dt`（唯一权威），本文件**不再**自写一份：
      测试若自带口径，口径漂移时会用自己的错误换算验自己的产出，两边一起错仍全绿。
    """
    return event_dt(int(event.record["event_time"]))


def _dt_partitions(events: list[Event]) -> list[str]:
    return sorted({_dt_of(event) for event in events})


# ── 1. 恰好 N 条 ────────────────────────────────────────────────────────
def test_threshold_batch_has_exactly_n_events() -> None:
    """判据①：产出条数**恰好**等于请求条数，不是「差几条」。

    为什么不能用「产完再裁」的直觉：票面 §P2 坑① 实测（seed=7 / 1075 条取前 1000）
    `expired` 从 22 掉到 **0** —— 迟到事件在后移发送序里排在尾部，
    `events[:N]` 取到的几乎全是正常事件。所以条数必须由「产够即停」
    （`max_events`）保证，且它按**条**停、不受「每会话 1~4 条」的波动影响。
    """
    plan, batch = _batch_from_plan()

    assert plan.n_events == _N_EVENTS
    assert len(batch.events) == _N_EVENTS, (
        f"请求 {plan.n_events} 条，实产 {len(batch.events)} 条"
    )


def test_max_events_counts_events_not_sessions() -> None:
    """`max_events` 的语义是「按条停」：会话数给多少都不改变产出条数。

    票面 E7 第 9 条记的坑：`--sessions 10000` 实产 **22 289** 条
    ⇒ 想要定条数只能用 `--events`。这里把「会话数浮动」摊开测：
    会话数 ×1 与 ×10（远多于够用）都必须停在 N。
    """
    plan = plan_threshold_batch(600)
    base = ProducerParams.from_plan(plan, seed=_SEED)
    produced = []
    for n_sessions in (300, 3_000):
        # 只改「载体会话数」这一个字段：其余 plan 派生值仍由 from_plan 统一给。
        batch = produce_batch(replace(base, n_sessions=n_sessions))
        produced.append(len(batch.events))

    assert produced == [plan.n_events, plan.n_events]


# ── 2. dt 恰 3 个分区 ──────────────────────────────────────────────────
def test_threshold_batch_spans_exactly_three_dt_partitions() -> None:
    """判据②：dt 恰 3 个分区，且恰为 plan 声明的 `min_dt..max_dt` 且逐日连续。

    这条是票面「坑③」的回归守卫：超期档最多把 `event_time` 往回拨 300 s，
    基座若压在 00:00:00Z，最早一批落到**前一天**、末尾又顶上第 4 天 ⇒ 5 个分区。
    `plan_threshold_batch` 把 `epoch_base_ms` 前移 300 s 正是为此 ——
    断言「恰好且连续」才能同时挡住「多一个前一天」和「少一个空分区」两种坏法。
    """
    plan, batch = _batch_from_plan()
    partitions = _dt_partitions(batch.events)

    assert len(partitions) == plan.expected_dt_count == 3, (
        f"dt 分区数 {len(partitions)} ≠ 期望 {plan.expected_dt_count}：{partitions}"
    )
    assert partitions[0] == plan.min_dt
    assert partitions[-1] == plan.max_dt

    days = [date.fromisoformat(item) for item in partitions]
    assert all(
        (later - earlier).days == 1 for earlier, later in itertools.pairwise(days)
    ), f"分区不是逐日连续的：{partitions}"


def test_event_time_stays_inside_the_declared_span() -> None:
    """`event_time` 严格落在 `[epoch_base_ms, epoch_base_ms + 3 天)` 内。

    dt 分区数是这个区间断言的**结果**，不是独立巧合：只要每条事件都在区间内，
    3 个分区就是必然的。分开断言是为了让失败点指向「区间越界」而不是「数错了分区」。
    """
    plan, batch = _batch_from_plan()
    times = [int(event.record["event_time"]) for event in batch.events]
    lower = plan.epoch_base_ms
    upper = plan.epoch_base_ms + 3 * _DAY_MS

    assert min(times) >= lower, f"最早事件 {min(times)} 早于基座 {lower}"
    assert max(times) < upper, f"最晚事件 {max(times)} 越过上界 {upper}"
    assert max(times) - min(times) > 2.5 * _DAY_MS, (
        "跨度不足 2.5 天 ⇒ 3 个分区里必有空分区或分布失真"
    )


def test_three_dt_partitions_hold_across_seeds() -> None:
    """dt = 3 不是「挑中了那个 seed」的运气：换 8 个 seed 仍恰 3 个分区。

    计划的推导只依赖 `n_events` 与 `span_days`，与 seed 无关；但迟到偏移量
    是 seed 决定的随机量，所以「与 seed 无关」这件事必须被证明，而不是声称。
    """
    plan = plan_threshold_batch(1_000)
    for seed in (1, 2, 3, 5, 7, 11, 42, 2026):
        batch = produce_batch(ProducerParams.from_plan(plan, seed=seed))
        partitions = _dt_partitions(batch.events)
        assert partitions == [plan.min_dt, "2024-01-02", plan.max_dt], (
            f"seed={seed} 的 dt 分区跑偏：{partitions}"
        )


# ── 3. 三通道全非空 ────────────────────────────────────────────────────
def test_threshold_batch_keeps_all_three_channels_non_empty() -> None:
    """判据③：`normal` / `late_correctable` / `late_expired` 三通道都 > 0，且计数自洽。

    为什么定条数之后还要再断一次通道：坑① 的症状正是「链路看着通、超期通道被裁成 0」
    ⇒ `dwd_late_event_count` 与 A3 迟到对账全成 0 行。通道非空必须是
    **定条数这条路径**上的断言，不能只在「产满再裁」的旧路径上过一遍。
    三通道互斥且穷尽（gdd Q21），故三项之和必须等于总条数 —— 这条同时挡住
    「计数漏了一个通道」与「一条事件被算进两个通道」。
    """
    _, batch = _batch_from_plan()
    normal = batch.count_channel("normal")
    correctable = batch.count_channel("late_correctable")
    expired = batch.count_channel("late_expired")

    assert normal > 0 and correctable > 0 and expired > 0, (
        f"通道有空的：normal={normal} correctable={correctable} expired={expired}"
    )
    assert normal + correctable + expired == len(batch.events)


def test_threshold_batch_channel_ratios_match_injector_params() -> None:
    """三通道比例贴近注入器参数（默认 3% 可修正 / 2% 超期）。

    票面只要求「非空」，但**非空**与**比例对**是两件事：若偏移或通道判定被改坏，
    可能出现「只有 1 条超期」也能过非空断言的情况。给一个宽区间（比例的
    一半到两倍）即可挡住数量级错误，又不与随机波动打架。
    """
    _, batch = _batch_from_plan()
    total = len(batch.events)

    assert 0.01 <= batch.count_channel("late_correctable") / total <= 0.06
    assert 0.005 <= batch.count_channel("late_expired") / total <= 0.045


# ── 4. 3d 与 72h 归一化等价 ────────────────────────────────────────────
@pytest.mark.parametrize(
    ("text", "expected_days"),
    [
        ("3d", 3),
        ("72h", 3),
        ("1440m", 1),
        ("86400s", 1),
        ("48h", 2),
        ("7d", 7),
        (" 3D ", 3),  # 大小写与前后空白都归一到同一口径
        # 单位前的空格被容忍（int("3 ") 成立）—— 这是宽松而非错误：
        # 结果仍是「整数 + 单位」，且归一化到同一个天数。
        ("3 d", 3),
    ],
)
def test_logical_span_normalises_to_days(text: str, expected_days: int) -> None:
    """单位只影响书写，结果一律归一到**天**（门槛批按天配额）。"""
    assert _parse_logical_span(text) == expected_days


def test_logical_span_3d_equals_72h() -> None:
    """判据④：`3d` 与 `72h` 归一化**等价**（票面明列）。

    为什么要单列一条：两者若不等，`--logical-span 72h` 会算出不同步长 ⇒
    同一批数据的 dt 分区数与 `3d` 不同，而命令行看起来只是写法不同。
    """
    assert _parse_logical_span("3d") == _parse_logical_span("72h") == 3


def test_logical_span_defaults_to_three_days_when_absent() -> None:
    """不给 `--logical-span`（None 或空串）⇒ 票面默认 3 天，而不是报错。"""
    assert _parse_logical_span(None) == 3
    assert _parse_logical_span("") == 3
    assert _parse_logical_span("   ") == 3


# ── 5. 非法输入报错 ────────────────────────────────────────────────────
@pytest.mark.parametrize("text", ["36h", "3600s", "18h", "90m", "90000s"])
def test_logical_span_rejects_non_whole_day_spans(text: str) -> None:
    """判据⑤：非整天输入必须报错（dt 按 UTC 日分区，半天跨度语义不清）。

    拒绝而不是静默取整：取整会让「dt 恰 N 个分区」的判据与用户写的跨度脱节，
    且失败是**静默**的 —— 正是本仓反复记过的「恒真判据」病根。
    """
    with pytest.raises(ValueError, match="整天"):
        _parse_logical_span(text)


@pytest.mark.parametrize(
    "text", ["3x", "3", "d", "h", "1e3h", "1.5d", "0d", "-1d", "dd"]
)
def test_logical_span_rejects_malformed_input(text: str) -> None:
    """非法单位 / 缺数字 / 小数 / 非正数一律 `ValueError`（不猜、不兜底）。"""
    with pytest.raises(ValueError):
        _parse_logical_span(text)


def test_logical_span_error_messages_carry_the_input() -> None:
    """错误信息必须带上原始输入，否则使用者无从判断是哪个参数写错了。"""
    with pytest.raises(ValueError, match="36h"):
        _parse_logical_span("36h")


# ── 6. plan_threshold_batch 的推导本身 ─────────────────────────────────
def test_plan_declares_three_partitions_and_a_shifted_base() -> None:
    """`expected_dt_count == span_days`，且基座**前移**了最大迟到量（300 s）。

    基座前移量是坑③ 的修法本身，必须钉住：少了它，最早一批迟到事件的
    `event_time` 会落到前一天；多了它，末条离 00:00 太远、3 天铺不满。
    """
    plan = plan_threshold_batch(_N_EVENTS, span_days=3)

    assert plan.expected_dt_count == 3
    assert plan.epoch_base_ms == 1_704_067_200_000 + _THRESHOLD_MAX_LATE_MS
    assert plan.min_dt == "2024-01-01"
    assert plan.max_dt == "2024-01-03"


def test_plan_step_times_n_events_covers_the_usable_span() -> None:
    """步长 × 条数 ≈ 可用跨度（扣掉前移量与末尾余量），且步长 ≥ 1 ms。

    这是 `span_used // n_events` 的算术不变量：若步长按「请求条数」而不是
    「实际条数」算（票面坑②），跨度会 ×2.2，本条能直接抓到。
    """
    n_events = 5_000
    plan = plan_threshold_batch(n_events, span_days=3)
    usable = 3 * _DAY_MS - _THRESHOLD_MAX_LATE_MS - 1_000

    assert plan.watermark_step_ms >= 1
    covered = plan.watermark_step_ms * n_events
    assert covered <= usable, "步长 × 条数 越过了可用跨度"
    assert usable - covered < n_events, "差额应小于一条事件的步长（整除的余数）"


def test_max_late_constant_matches_injector() -> None:
    """`producer._THRESHOLD_MAX_LATE_MS` 必须与 `injectors._EXPIRED_MAX_MS` 相等。

    `producer.py` 里这份是**冗余写死**的（避免反向 import 内部常量），
    源码注释明确要求「在单测里断言两者相等」—— 否则一边改了另一边不改，
    dt 分区会静默跑偏。
    """
    assert _THRESHOLD_MAX_LATE_MS == _EXPIRED_MAX_MS == 300_000


@pytest.mark.parametrize(
    ("n_events", "span_days"),
    [(0, 3), (-5, 3), (_N_EVENTS, 0), (_N_EVENTS, -1)],
)
def test_plan_rejects_non_positive_inputs(n_events: int, span_days: int) -> None:
    """条数与跨度必须为正：`ValueError` 而不是产出畸形 plan。"""
    with pytest.raises(ValueError):
        plan_threshold_batch(n_events, span_days)


def test_plan_rejects_batches_that_cannot_fit_the_span() -> None:
    """条数多到步长 < 1 ms 时必须报错，而不是算出 step=0 让事件时间重叠。"""
    with pytest.raises(ValueError, match="步长"):
        plan_threshold_batch(10**12, span_days=3)


# ── 7. CLI 端到端（--events / --logical-span）───────────────────────────
def test_dump_with_events_writes_exactly_n_lines(tmp_path: Path) -> None:
    """CLI 走 `--events` 路径时，落盘行数 == N，dt 恰 3 个。

    单测驱动的是 `main(argv)` 这个纯函数入口（不读 `sys.argv`），
    所以不必 patch 全局状态；`tmp_path` 保证不落大文件（本文件约 0.6 MB）。
    """
    out = tmp_path / "raw.jsonl"
    manifest = tmp_path / "manifest.json"

    code = main(
        [
            "dump",
            "--seed",
            str(_SEED),
            "--events",
            str(_N_EVENTS),
            "--logical-span",
            "3d",
            "--out",
            str(out),
            "--manifest",
            str(manifest),
        ]
    )

    assert code == 0
    lines = out.read_text(encoding="utf-8").splitlines()
    assert len(lines) == _N_EVENTS

    records = [json.loads(line) for line in lines]
    partitions = {
        datetime.fromtimestamp(int(r["event_time"]) / 1000, tz=UTC).strftime("%Y-%m-%d")
        for r in records
    }
    assert sorted(partitions) == ["2024-01-01", "2024-01-02", "2024-01-03"]

    data = json.loads(manifest.read_text(encoding="utf-8"))
    assert data["total_events"] == _N_EVENTS
    assert data["late_correctable"] > 0
    assert data["late_expired"] > 0


def test_dump_prints_parseable_shape_line(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """dump 的完成行必须可直接解析出条数与 dt 分区数（`make replay` 要读它）。

    票面 §P2 记的格式是 `[ecomgen] dump 完成：N 条 dt=[..] dt_count=K`。
    只断 `dt_count=3` 与 `2000 条` 这两个子串 —— 完整格式的措辞可以微调，
    但「条数」与「分区数」这两个判据必须留在输出里。
    """
    out = tmp_path / "raw.jsonl"
    assert (
        main(
            [
                "dump",
                "--seed",
                str(_SEED),
                "--events",
                str(_N_EVENTS),
                "--logical-span",
                "3d",
                "--out",
                str(out),
            ]
        )
        == 0
    )

    printed = capsys.readouterr().out
    assert f"{_N_EVENTS} 条" in printed
    assert "dt_count=3" in printed
    assert "late_expired=" in printed


def test_dump_rejects_non_whole_day_span(tmp_path: Path) -> None:
    """非法跨度在 CLI 面非 0 退出，且**一个文件都不产出**（不能留半成品）。"""
    out = tmp_path / "x.jsonl"
    code = main(
        [
            "dump",
            "--seed",
            "1",
            "--events",
            "100",
            "--logical-span",
            "36h",
            "--out",
            str(out),
        ]
    )

    assert code != 0
    assert not out.exists()


# ── 8. 可复现性 ────────────────────────────────────────────────────────
def test_threshold_batch_is_reproducible_for_same_seed() -> None:
    """判据⑥：同 seed 两次生成 ⇒ 条数与 `event_id` 序列一致（`REQ-F-GEN-06`）。

    比较 `event_id` 序列而不是整行 JSON：`event_id` 由派生 rng 逐条取 16 字节，
    是整个序列的指纹 —— 任何一处提前/滞后消费随机数都会让它错位。
    """
    _, first = _batch_from_plan()
    _, second = _batch_from_plan()

    assert len(first.events) == len(second.events)
    assert [e.record["event_id"] for e in first.events] == [
        e.record["event_id"] for e in second.events
    ]


def test_dump_output_is_byte_identical_for_same_seed(tmp_path: Path) -> None:
    """落盘字节级一致：`--seed` 决定内容（文件哈希可作为判据）。"""
    argv = [
        "dump",
        "--seed",
        str(_SEED),
        "--events",
        str(1_000),
        "--logical-span",
        "3d",
    ]
    first = tmp_path / "a.jsonl"
    second = tmp_path / "b.jsonl"

    assert main([*argv, "--out", str(first)]) == 0
    assert main([*argv, "--out", str(second)]) == 0
    assert first.read_bytes() == second.read_bytes()


def test_dump_3d_and_72h_produce_identical_files(tmp_path: Path) -> None:
    """`--logical-span 3d` 与 `72h` 产出**逐字节相同**（归一化等价在 CLI 面的终点）。

    比 `_parse_logical_span` 的返回值更硬：等价性必须一路传导到步长、
    再到落盘内容，否则「函数相等但参数装配不等」仍会漏。
    """
    out_3d = tmp_path / "3d.jsonl"
    out_72h = tmp_path / "72h.jsonl"

    assert (
        main(
            [
                "dump",
                "--seed",
                str(_SEED),
                "--events",
                "1000",
                "--logical-span",
                "3d",
                "--out",
                str(out_3d),
            ]
        )
        == 0
    )
    assert (
        main(
            [
                "dump",
                "--seed",
                str(_SEED),
                "--events",
                "1000",
                "--logical-span",
                "72h",
                "--out",
                str(out_72h),
            ]
        )
        == 0
    )

    assert out_3d.read_bytes() == out_72h.read_bytes()


# ── 10. plan → params 的装配只有一处（from_plan）──────────────────────
def test_from_plan_is_the_only_plan_to_params_mapping() -> None:
    """`ProducerParams.from_plan` 必须把 plan 的**每个**派生值都搬进参数。

    ★ 回归守卫（impl-03b code-review）：`watermark_step_ms` / `max_events` /
    `epoch_base_ms` / `carrier_sessions` 四个值此前在 CLI 与测试里各装一遍。
    漂移的症状是「实际条数 < 请求条数」而单测仍绿（测试抄的是同一份错装配）。
    这条断言把四者的**映射关系**钉死在 from_plan 上。
    """
    plan = plan_threshold_batch(1_234)
    params = ProducerParams.from_plan(plan, seed=99)

    assert params.seed == 99
    assert params.max_events == plan.n_events
    assert params.watermark_step_ms == plan.watermark_step_ms
    assert params.epoch_base_ms == plan.epoch_base_ms
    assert params.n_sessions == plan.carrier_sessions


def test_from_plan_produces_exactly_the_planned_count() -> None:
    """from_plan 装出来的参数必须真的产够 N 条（装配对了才谈得上「恰好 N 条」）。

    与上面那条分开：上面验**字段映射**，这条验**端到端后果**。两者都必要 ——
    字段映射对但 carrier 会话数不够时，产出的条数会短，而字段断言仍然通过。
    """
    plan = plan_threshold_batch(800)
    batch = produce_batch(ProducerParams.from_plan(plan, seed=7))

    assert len(batch.events) == plan.n_events


def test_plan_carrier_sessions_has_headroom_for_average_session_size() -> None:
    """载体会话数必须留有余量：每会话平均 ≈2.2 条，故 N 条只需 ≈0.46N 个会话。

    取 0.6N（2.0 分母 + 1.2 余量）是**单向**余量 —— 少了造不满 N 条、多了只是空转。
    断言下界与上界都在合理区间，挡住「把余量改没了」的回归。
    """
    plan = plan_threshold_batch(10_000)

    assert plan.carrier_sessions >= 10_000 / 2.2, "载体会话数不足以造满 N 条"
    assert plan.carrier_sessions == max(1, int(10_000 / 2.0 * 1.2))
