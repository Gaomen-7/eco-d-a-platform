# impl-02: P1 实时闭环 — 链路 + 判据 + `make init` 四段

Type: task
Status: ready-for-agent
Blocked by: impl-01

**What to build:** 一条能看见的实时链路，加一套骗不过去的判据。跑完本票，`make verify` 在 P1 阶段以「四真断言 + 一显式 SKIP」退出 0，`make verify-fault` 能在 kill TaskManager 后从 checkpoint 恢复并证明确有重放被幂等吸收。

四件事合在一票，因为它们互相是对方的判据前提：

**① 生成器 `live`/`dump` 双模式** — 五类事件（`page_view`/`view_item`/`add_to_cart`/`begin_checkout`/`purchase`）漏斗递减，维护商品/品类/地域/渠道维度数据，统一 UTC epoch 毫秒。`--seed` 同参数同产出。`--late-correctable`（默认 3%，落 2 分钟允许延迟**内**、可修正）与 `--late-expired`（默认 2%，**超期**只计数）两个注入器，偏移一律**相对水印**而非墙上时钟计算。`--chaos-ratio`（默认 0）开启后 `cnt_observed` 可非单调。

**② Flink → Doris 实时链路** — 窗口聚合落 SQL 段、窗口修正落 DataStream 段（Flink SQL 无 allow-lateness、`allowedLateness` 是 DataStream-only，这是官方能力强制）。`ads_window_metric` 以 `(window_start, metric_name, dim_key)` 唯一键承载暂定/落定/修正三态（`is_settled` 0/1、`revision` 递增）。漏斗行 `purchase` 迟到 ≤120 s 修正已落定窗口并使 `revision`+1，超期只计数绝不修正。UV 走精确滚动去重。DLQ 只摘真脏（`schema_invalid`/`value_out_of_range`/`time_out_of_range`），重复走幂等吸收通道**不进脏率分子**。

**③ `make init` 从「打印待补」补成真的一条命令** — 四段冻结：默认 profile `up -d`；`--profile doris up -d --wait`；`run --rm client` 跑 Doris 初始化；Superset `db upgrade` → `fab create-admin` → `superset init`。同时把 Doris 断言改经 `client` 发起（FE→BE 第二跳是容器地址，宿主 follow 不到），并查 ADS 真正**有行**而非只查 FE 健康。

**④ 两套判据进码** —
- `make verify`：五项分层进码，第三项「Paimon 五层有行」在 P1 阶段**显式 SKIP，不计通过也不计 fail**（结构上堵死今天打 PENDING 却假通过的路径）；另加 lag 断言、脏率不升、漏斗单调（`cnt_inferred` 沿 `step_no` 1→4 非增，`cnt_observed` 不押单调）；退出前写 JSON 报告进 `artifacts/verify/`（目录进 `.gitignore`），FAIL 时快照原始响应全文。
- `make verify-fault`：输入冻结 → 等 lag==0 → 快照两表 count/sum 基线与 kill 前 DLQ 快照 → kill TM → 轮询 `/jobs/overview` 到 RUNNING（超时 120 s，**设计值**）→ 等 lag==0 且此后完成一次 checkpoint（超时 300 s，**设计值**）→ 两表 count/sum 与基线全等、重复通道计数**上升**、DLQ 增量为 0。条数一致但 DLQ 上升即 fail（重复被误当脏的反例保护）。

**Blocked by:** impl-01

**Status:** ready-for-agent

## Acceptance criteria

- [ ] `make init` 在已有制品的机器上跑完后，`make verify` 退出码 0、`docker compose ps` 全 `healthy`；`make init` 输出里没有「待补」字样
- [ ] Doris 断言经 compose 网内 `client` 发起，且断言内容是「ADS 表有行」而非「FE 健康」
- [ ] 设 `--late-correctable 0.03 --late-expired 0.02` 后，实时侧窗口修正段确有迟到事件落入、超期段确有迟到计数条目
- [ ] `purchase` 迟到 ≤120 s 的事件使对应分钟漏斗客数被修正且 `ads_funnel_step.revision` +1；迟到 >120 s 的事件只增加计数，`revision` 不变
- [ ] `make verify` 在 P1 阶段打印五项逐条 `PASS/FAIL/SKIP <名> <实测> vs <期望>`，第三项显示 SKIP 且退出码仍为 0；运行一次能观察到「窗口关闭（暂定→落定）与迟到修正时刻 `revision`+1」
- [ ] `make verify-fault` 全流程可在超时内完成，退出码 0，且 stdout 明示「重复通道计数上升」与「DLQ 增量==0」两条结论
- [ ] 故意改坏一个输入让 DLQ 上升时，`verify-fault` 退出码非 0（证断言真的在断言，不是恒真）
- [ ] `artifacts/verify/verify-report-<UTC时间戳>.json` 逐断言落状态；人为制造一次 FAIL 时，同目录出现原始响应快照全文
- [ ] 脏率分子只含 DLQ 增量；同一批数据跑两遍时重复通道计数上升而脏率不升
- [ ] 所有超时数字（120 s / 300 s）标注 `设计值`，未标为官方保证

## Notes

- **强序**：本票通过后 P2（impl-03）才开——spec §4.3 明写「P2 只在 P1 实时四项通过后开」。
- **别重做**：`make init` 的第①②③段（Doris profile、初始化脚本）P0 已建成并跑通（commit `4a4759a`），本票只补第④段 Superset 引导与断言改道。
- **不重跑破坏性实测**：`down -v` 会销毁 P0 已验证的卷态成果；L0 零状态实测在 impl-05。
- 官方硬约束（Flink SQL 无迟到侧输出、SQL 侧无 allow-lateness、Group Window Aggregation 在 2.2 官方标 deprecated）已归档在 spec §4.6，是**能力强制**不是选型偏好——遇阻不要试图绕开，走 [09] Q10 兜底。