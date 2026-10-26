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

from ecomgen.dims import build_dimension_stream, count_by_kind, write_dimension_stream
from ecomgen.events import FUTURE_TOLERANCE_MS
from ecomgen.jsonl import write_jsonl
from ecomgen.producer import (
    EventBatch,
    ProducerParams,
    event_dt,
    plan_threshold_batch,
    produce_batch,
)
from ecomgen.world import build_world

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
    # ★ 维度变更流**另落一个文件**（impl-03c）：混进事件文件会改动事件字节，
    #   把 03b 已锁定的门槛批字节判据一起推翻。给定即产出，不给则不产（向后兼容）。
    p_dump.add_argument(
        "--dim-out",
        default=None,
        help="维度变更流输出 JSONL 路径（可选；拉链维表 dim_* 的输入）",
    )
    # ★ 门槛批（impl-03b）：`--events` 与 `--logical-span` 是**条数与跨度**的表达，
    #   与 P1 的 `--sessions`（会话数）是两个不同用途的参数
    #   （spec §5.1：批次密度 ≈4 ev/s 与 REQ-NF-PERF-03 的 200 ev/s **不同源**）。
    #   给了 `--events` 时以它为准：条数定死，步长由跨度/条数回算
    #   （推导见 producer.plan_threshold_batch，两个坑都在那里记着）。
    p_dump.add_argument(
        "--events",
        type=int,
        default=None,
        help="目标事件条数（门槛批：1048576）。给定即按条数定产，忽略 --sessions 的条数含义",
    )
    p_dump.add_argument(
        "--logical-span",
        default=None,
        # ⚠️ 示例里**不要**写 3600s：跨度会被归一到**整天**（dt 按 UTC 日分区），
        #   3600s 会在 _parse_logical_span 里报错。旧 help 曾举这个例子，
        #   与 :152-157 的整天校验自相矛盾（impl-03b code-review 指出）。
        help="逻辑跨度，须为整天（如 3d / 72h / 1440m / 86400s）。给定即按跨度定步长，使 dt 恰为跨度的天数个分区",
    )

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
    common: dict[str, Any] = {
        "seed": args.seed,
        "rate_per_sec": args.rate,
        "late_correctable": args.late_correctable,
        "late_expired": args.late_expired,
        "chaos_ratio": args.chaos_ratio,
        "n_users": args.users,
        "n_items": args.items,
    }
    if getattr(args, "events", None) is not None:
        # ★ 门槛批路径的装配**只有一处**：ProducerParams.from_plan
        #   （impl-03b code-review：三个 plan 派生值此前在这里与测试里各装一遍，
        #   是 Data Clumps；漂移的症状是「实际条数 < 请求条数」而单测仍绿）。
        span_days = _parse_logical_span(getattr(args, "logical_span", None))
        return ProducerParams.from_plan(
            plan_threshold_batch(args.events, span_days), **common
        )
    return ProducerParams(n_sessions=args.sessions, **common)


_SPAN_UNIT_MS: dict[str, int] = {
    "s": 1_000,
    "m": 60_000,
    "h": 3_600_000,
    "d": 86_400_000,
}


def _parse_logical_span(text: str | None) -> int:
    """把 `3d` / `72h` / `86400s` 解析成**天数**（门槛批按天配额，故归一到天）。

    只接受整单位：门槛批的跨度语义是「几天分区」，`1.5d` 这种会落到半天，
    与「dt 恰 N 个分区」的判据纠缠不清，直接拒绝比默默取整好。
    """
    if text is None:
        return 3  # 票面默认：3 天跨度
    raw = str(text).strip().lower()
    if not raw:
        return 3
    unit = raw[-1]
    if unit not in _SPAN_UNIT_MS:
        raise ValueError(f"--logical-span 单位须为 s/m/h/d，实得 {text!r}")
    try:
        amount = int(raw[:-1])
    except ValueError as exc:
        raise ValueError(
            f"--logical-span 须为整数+单位（如 3d），实得 {text!r}"
        ) from exc
    if amount <= 0:
        raise ValueError(f"--logical-span 须为正，实得 {text!r}")
    total_seconds = amount * _SPAN_UNIT_MS[unit] // 1_000
    if total_seconds % 86_400 != 0:
        raise ValueError(
            f"--logical-span 须为整天（dt 按 UTC 日分区），实得 {text!r} "
            f"= {total_seconds} s"
        )
    return total_seconds // 86_400


def _dt_of(record: dict[str, Any]) -> str:
    """事件时间的 UTC 日期（`yyyy-MM-dd`）—— 与 DDL 的 dt 口径同源。

    换算本体在 `producer.event_dt`（**唯一权威**）：此前这里与测试各抄一份，
    口径漂移时测试会拿错误口径验自己的产出、两边一起错。
    """
    return event_dt(int(record["event_time"]))


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
    # ★ 维度变更流要**同一份世界**（同一 seed 派生）——重建一次而不是让
    #   produce_batch 回传：build_world 是纯函数（同 seed 同产出），重建不改变
    #   任何事件字节，却能让「维度与事件同源」这件事在调用点显式可见。
    world = build_world(params.seed, n_users=params.n_users, n_items=params.n_items)

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    # newline="\n" 固定行尾：否则 Windows/WSL 差异会让「同 seed 同哈希」假失败
    #
    # ★ 投递偏移 `_src_offset`（impl-03b，2026-10-22）—— 这是**传输元数据**，
    #   不是事件字段，故以 `_` 前缀与 SRS §3.3 的业务字段区分开。
    #   为什么必须落进文件：离线重放的「迟到」定义是**到达序**语义 ——
    #   迟到 ⟺ 事件时间 < 到此行为止已见过的最大事件时间（Flink 水印只由到达序推进）。
    #   要算这个就必须知道**行序**，而 Flink batch 下 `ROW_NUMBER() OVER ()`
    #   不可用（实测 ValidationException: "window rank function requires order by
    #   clause with non-constant fields"）。`_src_offset` 提供了真实的、非常量的序。
    #   gdd Q25 本就要求 ODS 保留 offset 类元数据，这里是它在文件侧的落点。
    # ★ 落盘走 `ecomgen.jsonl.write_jsonl`（impl-03c）：事件流与维度变更流必须
    #   共用**同一份**「键序固定 / 无空格 / \n / 末尾换行」实现 —— 两侧各写一遍
    #   json.dumps 时，任一侧漏掉 sort_keys 就会让「同 seed 同字节」按文件类型分叉，
    #   而两份文件各自的单测都还是绿的。
    write_jsonl(
        out,
        (
            {"_src_offset": offset, **event.record}
            for offset, event in enumerate(batch.events)
        ),
    )

    # ★ 维度变更流（impl-03c）：窗口 [最早事件时刻, 最晚事件时刻) 由**已产出的事件批次**
    #   反推，而不是由计划参数猜——「每个对象至少一个覆盖全批次的初始版本」因此由构造
    #   成立（事件落在区间内），时态 join 不会漏行。空批次直接跳过（没有时刻可覆盖）。
    if getattr(args, "dim_out", None) and batch.events:
        event_times = [int(e.record["event_time"]) for e in batch.events]
        dim_rows = build_dimension_stream(
            world,
            start_ms=min(event_times),
            end_ms=max(event_times) + 1,
        )
        write_dimension_stream(Path(args.dim_out), dim_rows)
        counts = count_by_kind(dim_rows)
        print(
            f"[ecomgen] 维度变更流：{len(dim_rows)} 行 "
            f"({', '.join(f'{k}={v}' for k, v in sorted(counts.items()))}) → {args.dim_out}"
        )

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

    # ★ 可解析的完成行（impl-03b）：`make replay` 与判据要能直接读条数与 dt 范围，
    #   而不是再从文件里数一遍。dt 范围由**事件时间的 UTC 日**算出，
    #   与 DDL 的 dt 口径同源（ODS 用落地日、DWD 用事件日，此处报的是事件日）。
    dt_values = sorted({_dt_of(e.record) for e in batch.events})
    dt_range = f"{dt_values[0]}..{dt_values[-1]}" if dt_values else "-"
    print(
        f"[ecomgen] dump 完成：{len(batch.events)} 条 "
        f"dt=[{dt_range}] dt_count={len(dt_values)} → {out}"
    )
    print(
        f"[ecomgen] 通道：late_correctable={batch.count_channel('late_correctable')} "
        f"late_expired={batch.count_channel('late_expired')}"
    )
    return 0


def live_shift_ms(raw_min_ms: int, raw_max_ms: int, now_ms: int) -> int:
    """live 的平移量：把批次**窗尾**对到 `now_ms`（R7-i 案 1）。

    抽成纯函数是为了让「平移语义」可被判据直接断言——判据若重新算一遍
    `now - max`，就是抄实现，实现改错了判据也跟着错（impl-02d 判据纪律）。
    """
    return now_ms - raw_max_ms


def _live(args: argparse.Namespace) -> int:
    params = _params(args)
    batch = produce_batch(params)

    # F7 裁决：整体平移使历史日期不进入实时流；**平移基准是窗尾**（R7-i 案 1 修复）。
    # dump 的基座是固定常量（_EPOCH_BASE_MS），live 把它换成本轮时刻。
    #
    # ★★ 为什么是「窗尾对齐 now」而不是「窗首对齐 now」（impl-06d，R7-i 案 1）★★
    # 旧实现 shift = round_start_ms − raw_min_ms：批次**起点**落到现在，逻辑跨度
    # 原样保留 ⇒ 尾部事件 et = now + span。而 span 随 --sessions 线性增长
    # （实测 100 会话 ≈ 8 min、默认 1000 会话更甚、300 会话实测 1,270,148 ms ≈ 21 min），
    # 质量门的未来裕度只有 5 min（设计值，events.FUTURE_TOLERANCE_MS 与
    # EventQuality.FUTURE_TOLERANCE_MS 同源）⇒ 合规参数下的尾部必然被判
    # time_out_of_range 进 DLQ —— 即「生成器自己产出质量门拒收的事件」。
    # 窗尾对齐后：整批落在 [now − span, now]，尾部永不超过 now，故永不超界；
    # span 与迟到相对形状**完全不变**（仍由 --seed 决定，可复现）。
    raw_times = [int(e.record["event_time"]) for e in batch.events]
    span_ms = max(raw_times) - min(raw_times) if raw_times else 0
    round_start_ms = int(time.time() * 1000)
    raw_min_ms = min(raw_times) if raw_times else round_start_ms
    raw_max_ms = max(raw_times) if raw_times else round_start_ms
    shift_ms = live_shift_ms(raw_min_ms, raw_max_ms, round_start_ms)

    # 由**平移后的时间**派生，而不是由公式断言 shift 正确（判据不能抄实现）。
    tail_offset_ms = (raw_max_ms + shift_ms) - round_start_ms

    # 硬断言：越界即拒绝发送（而不是产出一批注定被质量门拒收的事件）。
    # 窗尾对齐下它恒成立，保留它是防止日后有人把基准改回「窗首」而不改这条。
    if tail_offset_ms > FUTURE_TOLERANCE_MS:
        print(
            f"[ecomgen] ✗ live 平移后尾部超界：tail_offset_ms={tail_offset_ms} ms "
            f"> 质量门未来裕度 {FUTURE_TOLERANCE_MS} ms（设计值；span={span_ms} ms）",
            file=sys.stderr,
        )
        return 2

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
        # ★ 判据出口（impl-06d）：平移**后**尾部相对基准的偏移。
        #   旧版 plan 只有 span_ms/round_start_ms，读者无法分辨「起点对齐」与
        #   「窗尾对齐」——两者在旧字段上恒等。
        "tail_offset_ms": tail_offset_ms,
        "shift_ms": shift_ms,
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
            f"[ecomgen] live --dry-run：{len(batch.events)} 条，原始跨度 {span_ms} ms，"
            f"窗尾平移至 {round_start_ms}（尾部偏移 {tail_offset_ms} ms "
            f"≤ 裕度 {FUTURE_TOLERANCE_MS} ms，设计值），rate={params.rate_per_sec}/s"
        )
        return 0

    return _send_to_kafka(batch, params, args, shift_ms)


def _send_to_kafka(
    batch: EventBatch,
    params: ProducerParams,
    args: argparse.Namespace,
    shift_ms: int,
) -> int:
    """真发：按速率逐条投递。分区键取 `user_id` 保证每用户有序（gdd Q12）。"""
    try:
        from confluent_kafka import Producer
    except ImportError:
        print("[ecomgen] ✗ 缺少 confluent-kafka", file=sys.stderr)
        return 2

    producer = Producer({"bootstrap.servers": args.bootstrap, "client.id": "ecomgen"})
    shift = shift_ms

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
