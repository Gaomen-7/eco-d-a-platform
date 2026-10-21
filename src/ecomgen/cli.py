"""生成器 CLI：`dump` / `live` 双模式（gdd.md §1.1、票面 ①）。

**为什么 `main(argv)` 是纯函数**：退出码即判据。把 `sys.argv` 的读取留在
`__main__` 之外，测试就能直接调用 `main([...])` 断言退出码与产出，
不必 patch 全局状态、也不必真起 Kafka。

两种模式的分工（F3 / F7 裁决的落点）：
- `dump`：一次性把事件写成 JSONL + 清单。**内容**由 `--seed` 完全决定，
  哈希判据针对这个文件，所以字节必须稳定。
- `live`：按 `--rate` 逐条发往 Kafka。**节奏**由墙上时钟控制，但**内容**与
  dump 同源；`--dry-run` 只打印发送计划（含 F7 的整体平移结果），不连 Kafka。

F7 裁决：live 把事件时间**相对本轮起点整体平移**——历史日期不进入实时流，
窗口由 Flink 水印按平移后的时间关闭。
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from ecomgen.producer import EventBatch, ProducerParams, produce_batch

# live 模式的平移基准取「本轮启动时刻」，故必须在运行时读取墙上时钟。
# dump 模式**不读**，因此 dump 产出与运行时刻无关（可复现）。


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ecomgen",
        description="电商用户行为数据生成器（live/dump 双模式）",
    )
    sub = parser.add_subparsers(dest="mode", required=True)

    def add_common(p: argparse.ArgumentParser) -> None:
        p.add_argument("--seed", type=int, required=True, help="随机种子（同参同产出）")
        p.add_argument("--sessions", type=int, default=1_000, help="会话数")
        p.add_argument(
            "--rate",
            type=int,
            default=200,
            help="目标速率 events/s（仅影响 live 节奏）",
        )
        p.add_argument(
            "--late-correctable", type=float, default=0.03, help="可修正迟到比例"
        )
        p.add_argument("--late-expired", type=float, default=0.02, help="超期迟到比例")
        p.add_argument("--chaos-ratio", type=float, default=0.0, help="混沌跳步比例")
        p.add_argument("--users", type=int, default=100_000, help="用户池规模")
        p.add_argument("--items", type=int, default=2_000, help="商品池规模")

    p_dump = sub.add_parser("dump", help="一次性产出 JSONL 文件")
    add_common(p_dump)
    p_dump.add_argument("--out", required=True, help="输出 JSONL 路径")
    p_dump.add_argument("--manifest", default=None, help="输出清单 JSON 路径（可选）")

    p_live = sub.add_parser("live", help="按速率发往 Kafka")
    add_common(p_live)
    p_live.add_argument(
        "--bootstrap", default="kafka:9092", help="Kafka bootstrap servers"
    )
    p_live.add_argument("--topic", default="ecom.behavior.event.v1", help="目标 topic")
    p_live.add_argument(
        "--dry-run", action="store_true", help="只打印发送计划，不连 Kafka"
    )
    p_live.add_argument("--plan", default=None, help="把发送计划写成 JSON（可选）")

    return parser


def _params(args: argparse.Namespace) -> ProducerParams:
    return ProducerParams(
        seed=args.seed,
        n_sessions=args.sessions,
        rate_per_sec=args.rate,
        late_correctable=args.late_correctable,
        late_expired=args.late_expired,
        chaos_ratio=args.chaos_ratio,
        n_users=args.users,
        n_items=args.items,
    )


def _manifest(batch: EventBatch, params: ProducerParams) -> dict[str, Any]:
    """清单：让 verify 能拿「期望脏数 / 期望迟到数」做独立对照。"""
    by_type: dict[str, int] = {}
    for event in batch.events:
        key = str(event.record["event_type"])
        by_type[key] = by_type.get(key, 0) + 1

    return {
        "seed": params.seed,
        "sessions": params.n_sessions,
        "total_events": len(batch.events),
        "late_correctable": batch.count_channel("late_correctable"),
        "late_expired": batch.count_channel("late_expired"),
        "by_event_type": by_type,
        "schema_version": "1.0",
    }


def _dump(args: argparse.Namespace) -> int:
    params = _params(args)
    batch = produce_batch(params)

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    # newline="\n" 固定行尾：否则 Windows/WSL 差异会让「同 seed 同哈希」假失败
    with out.open("w", encoding="utf-8", newline="\n") as handle:
        for event in batch.events:
            handle.write(event.to_json())
            handle.write("\n")

    if args.manifest:
        manifest_path = Path(args.manifest)
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        manifest_path.write_text(
            json.dumps(
                _manifest(batch, params), ensure_ascii=False, indent=2, sort_keys=True
            )
            + "\n",
            encoding="utf-8",
        )

    print(f"[ecomgen] dump 完成：{len(batch.events)} 条 → {out}")
    print(
        f"[ecomgen] 通道：late_correctable={batch.count_channel('late_correctable')} "
        f"late_expired={batch.count_channel('late_expired')}"
    )
    return 0


def _live(args: argparse.Namespace) -> int:
    params = _params(args)
    batch = produce_batch(params)

    # F7 裁决：整体平移到「本轮起点」，历史日期不进入实时流。
    # dump 的基座是固定常量（_EPOCH_BASE_MS），live 把它换成此刻。
    raw_times = [int(e.record["event_time"]) for e in batch.events]
    span_ms = max(raw_times) - min(raw_times) if raw_times else 0
    round_start_ms = int(time.time() * 1000)

    plan = {
        "mode": "live",
        "seed": params.seed,
        "bootstrap": args.bootstrap,
        "topic": args.topic,
        "rate_per_sec": params.rate_per_sec,
        "total_events": len(batch.events),
        "span_ms": span_ms,
        "round_start_ms": round_start_ms,
        "late_correctable": batch.count_channel("late_correctable"),
        "late_expired": batch.count_channel("late_expired"),
        "dry_run": bool(args.dry_run),
    }

    if args.plan:
        plan_path = Path(args.plan)
        plan_path.parent.mkdir(parents=True, exist_ok=True)
        plan_path.write_text(
            json.dumps(plan, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    if args.dry_run:
        print(
            f"[ecomgen] live --dry-run：{len(batch.events)} 条，跨度 {span_ms} ms，"
            f"起点平移至 {round_start_ms}，rate={params.rate_per_sec}/s"
        )
        return 0

    return _send_to_kafka(
        batch, params, args, round_start_ms, min(raw_times) if raw_times else 0
    )


def _send_to_kafka(
    batch: EventBatch,
    params: ProducerParams,
    args: argparse.Namespace,
    round_start_ms: int,
    raw_min_ms: int,
) -> int:
    """真发：按速率逐条投递。分区键取 `user_id` 保证每用户有序（gdd Q12）。"""
    try:
        from confluent_kafka import Producer
    except ImportError:
        print("[ecomgen] ✗ 缺少 confluent-kafka", file=sys.stderr)
        return 2

    producer = Producer({"bootstrap.servers": args.bootstrap, "client.id": "ecomgen"})
    shift = round_start_ms - raw_min_ms

    sent = 0
    interval = 1.0 / params.rate_per_sec
    next_at = time.monotonic()
    for event in batch.events:
        record = dict(event.record)
        record["event_time"] = int(record["event_time"]) + shift
        payload = json.dumps(
            record, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        )
        producer.produce(
            topic=args.topic,
            key=str(record["user_id"]).encode(),
            value=payload.encode("utf-8"),
        )
        sent += 1

        # 按速率节流：**只控制节奏，不改内容**（F3 裁决）
        next_at += interval
        sleep_for = next_at - time.monotonic()
        if sleep_for > 0:
            time.sleep(sleep_for)
        else:
            next_at = time.monotonic()
            producer.poll(0)

    producer.flush(30)
    print(f"[ecomgen] live 完成：已投递 {sent} 条 → {args.bootstrap}/{args.topic}")
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    try:
        if args.mode == "dump":
            return _dump(args)
        if args.mode == "live":
            return _live(args)
    except ValueError as exc:
        # 参数越界：非 0 退出（不能产出畸形批次）
        print(f"[ecomgen] ✗ 参数非法：{exc}", file=sys.stderr)
        return 2

    print(f"[ecomgen] ✗ 未知模式：{args.mode}", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
