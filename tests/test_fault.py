"""verify-fault 判据的测试（TDD 第十片：故障恢复与幂等吸收证明）。

判据来源：spec.md §4.4（全参数化流程图逐行）、票面验收第 6/7 条。
接缝 = `ecomgen.fault` 的**纯判据函数**：给定 kill 前后的快照，判定是否通过。

★ 关键反例保护（票面明确要求）：**条数一致但 DLQ 上升 ⇒ fail**。
若把重复误当脏，重放 5 条合法事件就能把脏率从 0% 推到 50%（gdd 原型实测），
所以这条判据必须真的能证伪。

★ 第三条子判据是 `replay_observed`（源端重读条数 >0），**不是**
「重复通道计数上升」——后者在实机上已被证伪（impl-02d），理由见
`ecomgen.fault.judge_recovery` 的 docstring。
"""

from __future__ import annotations

import pytest

from ecomgen.fault import (
    ALLOWED_LATENESS_MS,
    WINDOW_MS,
    Baseline,
    FaultVerdict,
    baseline_looks_populated,
    batch_end_ms,
    batch_window_literals,
    build_tick_events,
    judge_recovery,
    scoped_snapshot_sql,
    tick_event_time,
    tick_events_for_batch,
)


def _baseline(**overrides: object) -> Baseline:
    base: dict[str, object] = {
        "window_metric_count": 100,
        "window_metric_sum": 5000,
        "funnel_step_count": 40,
        "funnel_step_sum": 800,
        "dlq_offset": 10,
        "duplicate_count": 100,
    }
    base.update(overrides)
    return Baseline(**base)  # type: ignore[arg-type]


def test_recovery_passes_when_counts_equal_dlq_flat_and_replay_seen() -> None:
    """正常恢复：两表 count/sum 与基线全等、DLQ 增量为 0、源端确有重读。"""
    before = _baseline()
    after = _baseline()
    verdict = judge_recovery(before, after, replay_observed=True)

    assert isinstance(verdict, FaultVerdict)
    assert verdict.passed
    assert verdict.exit_code() == 0


def test_recovery_fails_when_row_count_changed() -> None:
    """条数变了 ⇒ 幂等吸收没做到。"""
    verdict = judge_recovery(
        _baseline(), _baseline(window_metric_count=101), replay_observed=True
    )
    assert not verdict.passed


def test_recovery_fails_when_sum_changed() -> None:
    """和变了（即使条数没变）⇒ 值被重复累加，同样不通过。"""
    verdict = judge_recovery(
        _baseline(), _baseline(window_metric_sum=5100), replay_observed=True
    )
    assert not verdict.passed


def test_recovery_fails_when_dlq_increased() -> None:
    """★ 反例保护：**条数一致但 DLQ 上升** ⇒ fail（重复被误当脏）。"""
    verdict = judge_recovery(
        _baseline(), _baseline(dlq_offset=15), replay_observed=True
    )
    assert not verdict.passed
    assert verdict.exit_code() == 1
    assert "dlq" in verdict.reason.lower() or "脏" in verdict.reason


def test_recovery_fails_when_no_replay_observed() -> None:
    """源端没有重读 ⇒ 没证据表明「确实发生了重放」（票面验收第 6 条）。

    ★ 这条同时是「kill 没命中在途数据」的保护：此时 count/sum 就算全等，
    也不能算通过——那只是「什么都没发生」。
    """
    verdict = judge_recovery(_baseline(), _baseline(), replay_observed=False)
    assert not verdict.passed
    assert "重放" in verdict.reason


def test_verdict_states_both_conclusions_in_stdout_lines() -> None:
    """stdout 必须**明示**两条结论（票面验收第 6 条）。"""
    verdict = judge_recovery(_baseline(), _baseline(), replay_observed=True)
    text = "\n".join(verdict.format_lines())

    assert "重放证据" in text
    assert "DLQ 增量==0" in text or "DLQ 增量 == 0" in text


def test_baseline_rejects_negative_values() -> None:
    import pytest

    with pytest.raises(ValueError):
        Baseline(
            window_metric_count=-1,
            window_metric_sum=0,
            funnel_step_count=0,
            funnel_step_sum=0,
            dlq_offset=0,
            duplicate_count=0,
        )


# ── impl-02d 实机反证回归（这些用例锁住「卡了整夜」的那几个坑）──────────


def test_funnel_sum_change_alone_fails_even_if_counts_equal() -> None:
    """漏斗行的**和**变了也要 fail（旧版把 count 与 sum 捆在同一条 SQL 里）。

    实机教训（impl-02c）：`ads_funnel_step` 的 `cnt_inferred` 会被迟到修正
    原地改写，`COUNT(*)` 不动而 `SUM(cnt_inferred)` 变——只看 count 会漏。
    """
    verdict = judge_recovery(
        _baseline(), _baseline(funnel_step_sum=801), replay_observed=True
    )
    assert not verdict.passed


def test_duplicate_count_change_does_not_decide_pass_or_fail() -> None:
    """`duplicate_count` 只作旁证，**不参与判定**。

    实机反证（impl-02d）：同一份字节重投 + kill 命中在途数据时，
    两表 count/sum 全等、DLQ 不动，而 `duplicate_absorbed` **完全没动**（115→115）。
    反过来，在流重复会让它上升而业务行不动。两种情况都不该由它决定裁决。
    """
    up = judge_recovery(
        _baseline(duplicate_count=100),
        _baseline(duplicate_count=160),
        replay_observed=True,
    )
    assert up.passed

    down = judge_recovery(
        _baseline(duplicate_count=100),
        _baseline(duplicate_count=90),
        replay_observed=True,
    )
    assert down.passed


def test_replay_observed_is_the_only_source_of_replay_evidence() -> None:
    """「确有重放」只能来自 `replay_observed`（源端重读条数 >0）。

    即使 count/sum 全等、DLQ 也不动，只要 `replay_observed=False` 就必须 fail ——
    否则「什么都没发生」会被当成「幂等吸收成功」（旧版就是这样恒假通过的）。
    """
    verdict = judge_recovery(_baseline(), _baseline(), replay_observed=False)
    assert not verdict.passed
    assert verdict.exit_code() == 1
    assert "重放" in verdict.reason


def test_verdict_detail_reports_all_measured_values() -> None:
    """失败快照要能复盘：detail 必须带上三条件的**实测值**（票面验收第 7 条）。"""
    verdict = judge_recovery(
        _baseline(),
        _baseline(window_metric_count=101, dlq_offset=12),
        replay_observed=False,
    )
    assert "window_metric count 100→101" in verdict.detail
    assert "dlq 10→12" in verdict.detail
    assert "duplicate 100→100" in verdict.detail


def test_stdout_states_dlq_conclusion_even_when_dlq_rose() -> None:
    """stdout 的 DLQ 结论行必须能表达「不为 0」，不能只印真值。"""
    verdict = judge_recovery(
        _baseline(), _baseline(dlq_offset=11), replay_observed=True
    )
    text = "\n".join(verdict.format_lines())
    assert "DLQ 增量!=0" in text
    assert "两表 count/sum 与基线全等：是" in text
    assert "不通过" in text


# ── 水印推进 tick（impl-02d 第四轮实机定位的第 7 个判据缺陷）─────────


def test_tick_event_time_closes_every_batch_window() -> None:
    """tick 的 event_time 必须让水印越过**所有**批次窗口的 `windowEnd`。

    实测根因（2026-10-22 12:00，`/tmp/span2.log` + Doris `updated_at`）：
    批次 event_time 压到 `[now-90s, now-40s]` 后，作业的水印被
    `forBoundedOutOfOrderness(120s)` 压在「最大事件时间 − 120 s」，
    所以**批次自己的窗口永远等不到关闭**；`Doris 已静默 110 s` 是
    「什么都还没发出来」的假落定。直到**下一轮**的批次把水印推上去，
    上一轮的窗口行才在 `04:01:06` 一次性落库（正是 span2 结束后）。
    该断言锁住「tick − 120 s ≥ windowEnd」这条充分条件。
    """

    for batch_max in (1792641215373, 1792641654076, 1792641000000):
        window_end = (batch_max // WINDOW_MS + 1) * WINDOW_MS
        tick = tick_event_time(batch_max)
        assert tick - ALLOWED_LATENESS_MS >= window_end


def test_tick_is_never_a_future_event() -> None:
    """tick 必须是**过去**事件：未来事件会命中 `time_out_of_range` 进 DLQ。

    批次被压在 `now − 200 s`；最坏情况下批次最大值刚好落在分钟边界前一刻
    ⇒ `windowEnd ≈ now − 140 s` ⇒ `tick = windowEnd + 120 s + 5 s ≈ now − 15 s`。
    即整轮**不依赖任何未来事件**，tick 绝不会自己变成脏数据。
    """

    now = 1792641654076
    end = batch_end_ms(now)
    assert end == now - 200_000
    # 最坏情形：批次最大值就在 end（= 分钟边界前的任意位置）
    # tick ∈ (end+125 s, end+185 s] ⇒ 离 now 还有 15~75 s，恒为过去时间
    tick = tick_event_time(end)
    assert tick < now
    assert now - tick <= 90_000


def test_tick_event_time_covers_the_worst_case_minute_boundary() -> None:
    """批次最大值正好压在分钟边界上时，tick 仍必须越过那一整个窗口。"""

    boundary = 1792641600000  # 恰好是 2026-10-22 04:00:00（分钟边界）
    tick = tick_event_time(boundary)
    # 边界值本身属于 04:00 窗口 ⇒ windowEnd = 04:01:00
    assert (tick // WINDOW_MS) * WINDOW_MS - ALLOWED_LATENESS_MS >= boundary + WINDOW_MS


def test_tick_events_cover_every_partition_exactly_once() -> None:
    """每个分区各一条 —— 水印取**分区最小值**，少一个分区就推不动。"""

    events = build_tick_events(
        nonce="run-abc", tick_time_ms=1792641634076, partitions=6
    )

    assert [e["partition"] for e in events] == list(range(6))
    ids = [e["payload"]["event_id"] for e in events]
    assert len(set(ids)) == 6
    assert all("run-abc" in i for i in ids)
    assert {e["payload"]["event_type"] for e in events} == {"page_view"}


def test_tick_events_have_all_required_fields_and_are_valid() -> None:
    """tick 必须是**合法**事件：它只用来推水印，不能变成 DLQ 里的脏数据。

    缺任一必填字段都会被 `EventQuality` 判 `schema_invalid`（DLQ 增量判据立即被
    自己污染），所以这里逐字段校验，并锁住 `event_time == tick_time_ms`。
    """

    payload = build_tick_events(nonce="n1", tick_time_ms=1792641634076, partitions=1)[
        0
    ]["payload"]

    assert payload["event_time"] == 1792641634076
    for field in ("event_id", "user_id", "item_id", "category_id", "event_type"):
        assert payload[field], field
    assert payload["schema_version"] == "1.0"


def test_tick_payload_bytes_are_stable_for_repeats() -> None:
    """同一轮内重复投 tick 必须**逐字节相同**（否则第二遍会新增行而不是被吸收）。

    喂 tick 的时机不止一次（关窗口、恢复后再关窗口），若两次字节不同，
    第二次会以新 event_id 落新行 ⇒ 两表 count 凭空上涨 ⇒ 假 FAIL。
    """

    first = build_tick_events(nonce="run-abc", tick_time_ms=1792641634076, partitions=3)
    second = build_tick_events(
        nonce="run-abc", tick_time_ms=1792641634076, partitions=3
    )
    assert first == second


def test_build_tick_events_from_batch_is_pure_round_trip() -> None:
    """从批次的 event_time 明细直接算出 tick —— 不留「谁记得传哪个最大值」的坑。"""

    batch = [
        {"event_time": 1792641000000},
        {"event_time": 1792641050000},
        {"event_time": 1792640999999},
    ]
    events = tick_events_for_batch(batch, nonce="n2", partitions=2)

    tick_time = events[0]["payload"]["event_time"]
    assert all(e["payload"]["event_time"] == tick_time for e in events)
    # 最晚事件的 min 边界 ⇒ windowEnd = 1792641060000
    assert tick_time - 120_000 >= 1792641060000


# ── 比对口径：只比「本批窗口内的业务行」（impl-02d 第四轮，解跨轮残留）─────


def test_batch_window_literals_are_utc_minute_buckets() -> None:
    """窗口字面量必须是 **UTC 分钟桶**，与 Doris 里 `window_start` 的存法逐字一致。

    实机读到的行是 `2026-10-22 03:53:00` 这种形状（= epoch/60000 后按 UTC 格式化），
    所以这里也按 UTC 生成；若按本地时区（+08:00）生成，SQL 会**一行都匹配不到**，
    scoped 快照恒为空 ⇒ 判据退化成 `0 == 0` 的恒真断言。
    """
    literals = batch_window_literals(
        [{"event_time": 1792641654076}, {"event_time": 1792641654076 + 60_000}]
    )
    assert literals == ["2026-10-22 04:00:00", "2026-10-22 04:01:00"]


def test_scoped_snapshot_sql_covers_only_batch_windows_and_business_rows() -> None:
    """scoped 快照必须同时满足三件事：限窗口、排心跳、排重复通道。

    - **限窗口**：解「跨轮残留」（上一轮的窗口行在别的分钟，不该参与比对）；
    - **排心跳**：`provisional_emitted` 是处理时间心跳，墙钟一走就长新行；
    - **排重复通道**：`duplicate_absorbed` 正是本判据**期望上升**的那一侧，
      把它算进「业务行全等」会让断言自相矛盾。
    """
    sql = scoped_snapshot_sql(["2026-10-22 03:52:00", "2026-10-22 03:53:00"])

    assert "window_start IN ('2026-10-22 03:52:00', '2026-10-22 03:53:00')" in sql
    assert "provisional_emitted" in sql
    assert "duplicate_absorbed" in sql
    assert "ecom_ads.ads_window_metric" in sql
    assert "ecom_ads.ads_funnel_step" in sql


def test_scoped_snapshot_sql_refuses_empty_window_set() -> None:
    """空窗口集 ⇒ 直接报错，绝不生成一条「匹配不到任何行」的 SQL。"""
    with pytest.raises(ValueError):
        scoped_snapshot_sql([])


def test_baseline_looks_populated_rejects_empty_scope() -> None:
    """基线的非空护栏：scoped 快照全 0 ⇒ 不能当基线（否则 `0 == 0` 恒真）。"""
    assert baseline_looks_populated(_baseline())

    empty = _baseline(
        window_metric_count=0,
        window_metric_sum=0,
        funnel_step_count=0,
        funnel_step_sum=0,
    )
    assert not baseline_looks_populated(empty)
