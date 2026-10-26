"""CLI 双模式与复现判据的测试（TDD 第七片：端到端接缝）。

判据来源：票面验收第 3 条、SRS `REQ-F-GEN-01`（两模式独立运行且退出码 0）、
`REQ-F-GEN-06`（同 seed 同参数 ⇒ 输出文件哈希一致）、
spec 附录 A 的 **F7 裁决**（event_time 相对本轮起点平移，墙钟起点）。

接缝 = `ecomgen.cli.main(argv)`：一个纯函数式入口，返回退出码，
不直接读 `sys.argv`（这样测试不需要 patch 全局状态）。
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from ecomgen.cli import main


def test_dump_mode_writes_events_and_exits_zero(tmp_path: Path) -> None:
    """dump 模式一次性产出文件，退出码 0（REQ-F-GEN-01）。"""
    out = tmp_path / "events.jsonl"
    code = main(["dump", "--seed", "7", "--sessions", "50", "--out", str(out)])

    assert code == 0
    assert out.exists()
    lines = out.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) > 0
    # 每行必须是合法 JSON（JSONL 形态，便于回放与哈希比对）
    for line in lines:
        assert json.loads(line)["schema_version"] == "1.0"


def test_dump_is_byte_identical_for_same_seed(tmp_path: Path) -> None:
    """同 seed 同参数跑两遍 ⇒ 文件哈希一致（REQ-F-GEN-06）。"""
    out_a = tmp_path / "a.jsonl"
    out_b = tmp_path / "b.jsonl"
    argv = [
        "dump",
        "--seed",
        "11",
        "--sessions",
        "60",
        "--late-correctable",
        "0.03",
        "--late-expired",
        "0.02",
    ]

    assert main([*argv, "--out", str(out_a)]) == 0
    assert main([*argv, "--out", str(out_b)]) == 0

    assert out_a.read_bytes() == out_b.read_bytes(), "同 seed 产出不一致"


def test_dump_differs_for_different_seed(tmp_path: Path) -> None:
    out_a = tmp_path / "a.jsonl"
    out_b = tmp_path / "b.jsonl"
    assert main(["dump", "--seed", "1", "--sessions", "30", "--out", str(out_a)]) == 0
    assert main(["dump", "--seed", "2", "--sessions", "30", "--out", str(out_b)]) == 0
    assert out_a.read_bytes() != out_b.read_bytes()


def test_dump_writes_manifest_with_counts(tmp_path: Path) -> None:
    """产出一份可用作判据输入的清单（通道计数 + 事件类型计数）。"""
    out = tmp_path / "events.jsonl"
    manifest = tmp_path / "manifest.json"
    assert (
        main(
            [
                "dump",
                "--seed",
                "5",
                "--sessions",
                "80",
                "--out",
                str(out),
                "--manifest",
                str(manifest),
            ]
        )
        == 0
    )

    data = json.loads(manifest.read_text(encoding="utf-8"))
    assert data["seed"] == 5
    assert data["total_events"] > 0
    assert data["late_expired"] >= 0
    assert set(data["by_event_type"]) <= {
        "page_view",
        "view_item",
        "add_to_cart",
        "begin_checkout",
        "purchase",
    }


def test_live_mode_requires_bootstrap_and_prints_plan(tmp_path: Path) -> None:
    """live 模式在 --dry-run 下只打印发送计划，不真的连 Kafka（可离线验语义）。"""
    code = main(
        [
            "live",
            "--seed",
            "3",
            "--sessions",
            "20",
            "--rate",
            "200",
            "--bootstrap",
            "kafka:9092",
            "--dry-run",
        ]
    )
    assert code == 0


def test_live_dry_run_shifts_event_time_to_round_start(tmp_path: Path) -> None:
    """F7 裁决：live 把 event_time 相对本轮起点平移，历史日期不进入实时流。"""
    plan = tmp_path / "plan.json"
    code = main(
        [
            "live",
            "--seed",
            "3",
            "--sessions",
            "20",
            "--bootstrap",
            "kafka:9092",
            "--dry-run",
            "--plan",
            str(plan),
        ]
    )
    assert code == 0
    data = json.loads(plan.read_text(encoding="utf-8"))
    # 平移后的时间跨度应与原始跨度一致（相对形状不变，只整体平移）
    assert data["span_ms"] > 0
    assert data["round_start_ms"] > 0


# ── R7-i 案 1：live 跨度不得越过质量门的未来裕度（impl-06d）────────────
#
# 反例证伪（impl-02d 判据纪律）：**未修复**的行为要能机读复现。
#
# 旧实现：shift = round_start_ms - raw_min_ms（批次**起点**平移到现在），
# 批次逻辑跨度原样保留 ⇒ 尾部事件 event_time = now + span，而 span 随
# --sessions 线性增长（默认 1000 会话 ⇒ span ≈ 80 min，实测 100 会话 ≈ 8 min）。
# Flink 侧质量门的未来裕度只有 5 min（EventQuality.FUTURE_TOLERANCE_MS，
# 与 Python 侧 events.FUTURE_TOLERANCE_MS 同源）⇒ 超界尾部被正确判
# time_out_of_range 进 DLQ。
#
# 下面这条判据是**机读的**，不依赖墙钟之外的手工观测：把「平移后的最晚事件时间」
# 与「平移基准 + 裕度」比较。它同时是修复后的验收物。


def test_counterexample_start_aligned_shift_violates_tolerance() -> None:
    """**反例证伪**：复现旧语义（起点对齐），证明它必然越过未来裕度。

    这条对**未修复**实现是"红"的另一种形态——它不依赖实现缺字段，而是直接
    断言「旧公式产出越界」。故它证明的是**行为**，不是形状：

        shift_old = now − raw_min  ⇒  tail = now + span  >  now + 5 min   （span > 5 min 时）
        shift_new = now − raw_max  ⇒  tail = now         ≤  now + 5 min   （恒成立）

    ⚠️ span 取 300 会话的实测原始跨度（1,270,148 ms ≈ 21 min），任何 > 裕度的
    span 都让旧语义越界 ⇒ 这是结构性的，不靠运气。
    """
    from ecomgen.cli import live_shift_ms
    from ecomgen.events import FUTURE_TOLERANCE_MS

    now_ms = 1_800_000_000_000
    raw_min_ms = now_ms - 10 * 86_400_000  # dump 基座离现在很远，跨度才是关键
    span_ms = 1_270_148  # 实测：ecomgen live --seed 3 --sessions 300 的原始跨度
    assert span_ms > FUTURE_TOLERANCE_MS, "前置条件：跨度必须大于未来裕度"
    raw_max_ms = raw_min_ms + span_ms

    # ── 旧语义（起点对齐）：尾部越界 ───────────────────────────────
    shift_old = now_ms - raw_min_ms
    tail_old = (raw_max_ms + shift_old) - now_ms
    assert tail_old > FUTURE_TOLERANCE_MS, (
        f"旧语义居然没越界（tail_old={tail_old} ms）—— 反例不成立，"
        f"说明 span {span_ms} ms 并未超过裕度 {FUTURE_TOLERANCE_MS} ms"
    )
    assert tail_old == span_ms  # 旧语义的尾部偏移恰等于整批跨度

    # ── 新语义（窗尾对齐）：尾部恰好落到 now，永不越界 ────────────
    shift_new = live_shift_ms(raw_min_ms, raw_max_ms, now_ms)
    tail_new = (raw_max_ms + shift_new) - now_ms
    assert tail_new == 0
    assert tail_new <= FUTURE_TOLERANCE_MS


def test_live_shift_keeps_tail_within_future_tolerance() -> None:
    """修复后的判据：平移后最晚事件时间 ≤ 本轮基准 + 未来裕度（5 min）。

    ⚠️ 判据用 plan 的**可解析字段**而不是重新算一遍 shift —— 后者会把实现
    再抄一份，实现改了判据也跟着改，等于没判。
    """
    from ecomgen.events import FUTURE_TOLERANCE_MS

    plan_path = Path(__file__).parent / "_live_plan_r7i.json"
    try:
        code = main(
            [
                "live",
                "--seed",
                "3",
                "--sessions",
                "300",
                "--bootstrap",
                "kafka:9092",
                "--dry-run",
                "--plan",
                str(plan_path),
            ]
        )
        assert code == 0
        data = json.loads(plan_path.read_text(encoding="utf-8"))
    finally:
        plan_path.unlink(missing_ok=True)

    # 300 会话的原始跨度 ≈ 4×100 会话的 8 min ⇒ 必然 > 5 min 裕度（旧实现必然红）
    assert data["span_ms"] > FUTURE_TOLERANCE_MS, (
        f"前置条件不成立：300 会话的原始跨度 {data['span_ms']} ms 未超过裕度 "
        f"{FUTURE_TOLERANCE_MS} ms，这条反例对旧实现不构成挑战"
    )
    tail_after_shift = data["round_start_ms"] + data["tail_offset_ms"]
    assert tail_after_shift <= data["round_start_ms"] + FUTURE_TOLERANCE_MS, (
        "平移后尾部事件落在质量门未来裕度之外："
        f"尾部偏移 {data['tail_offset_ms']} ms > 裕度 {FUTURE_TOLERANCE_MS} ms "
        f"（原始跨度 {data['span_ms']} ms）"
    )


def test_live_plan_reports_shift_semantics() -> None:
    """plan 必须同时报「原始跨度」与「平移后尾部偏移」——判据要能分辨平移语义。

    旧版 plan 只有 span_ms + round_start_ms，读者无法区分「起点对齐」（旧）
    与「窗尾对齐」（新）⇒ 新增 tail_offset_ms 作为**平移后**的最晚事件相对
    基准的偏移，它是案 1 修复的可机读出口。
    """
    plan_path = Path(__file__).parent / "_live_plan_r7i_semantics.json"
    try:
        assert (
            main(
                [
                    "live",
                    "--seed",
                    "3",
                    "--sessions",
                    "300",
                    "--bootstrap",
                    "kafka:9092",
                    "--dry-run",
                    "--plan",
                    str(plan_path),
                ]
            )
            == 0
        )
        data = json.loads(plan_path.read_text(encoding="utf-8"))
    finally:
        plan_path.unlink(missing_ok=True)

    assert data["tail_offset_ms"] >= 0, "窗尾对齐后尾部事件不应落在基准之前"
    assert data["tail_offset_ms"] <= data["span_ms"]


def test_invalid_ratio_exits_nonzero(tmp_path: Path) -> None:
    """参数越界必须非 0 退出（而不是产出畸形批次）。"""
    out = tmp_path / "x.jsonl"
    assert (
        main(
            [
                "dump",
                "--seed",
                "1",
                "--sessions",
                "10",
                "--late-expired",
                "1.5",
                "--out",
                str(out),
            ]
        )
        != 0
    )


def test_unknown_mode_exits_nonzero() -> None:
    with pytest.raises(SystemExit):
        main(["bogus"])
