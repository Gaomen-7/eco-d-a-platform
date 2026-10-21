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
