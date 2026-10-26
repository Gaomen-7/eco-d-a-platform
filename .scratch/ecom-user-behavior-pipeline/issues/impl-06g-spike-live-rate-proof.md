# impl-06g: `make spike` 真发 — ≥1000 events/s 的一手速率证据

Type: task
Status: resolved
Blocked by: None（可立即开工；真发需栈在跑）

**What to build:** 让 `make spike` 从「打印一个 dry-run 计划」变成**真的把尖峰流量打进
Kafka**，并留下「实测速率 ≥1000 events/s」的一手证据。

**Blocked by:** None（可立即开工）

## 为什么这是唯一的"功能面"缺口

[SRS.md](../SRS.md) 有两条硬要求：

- `REQ-F-GEN-04`（**SHOULD**）：大促尖峰模式，短时速率 **≥1000 events/s**，
  判据「尖峰模式触发期间实测速率 ≥1000/s」
- `REQ-NF-PERF-03`（**MUST**）：默认 200 events/s；尖峰模式 ≥1000 events/s

而现在 `Makefile` 的 `spike:` 目标只做：

> `ecomgen live --seed 1 --sessions 5000 --rate 1000 … --dry-run`，
> 并打印「⚠️ 真发需 compose 网内可达 kafka:9092」

⇒ **速率判据从未被实测过**。这不是文档问题，是**没有任何一手证据**说它达标。
其余功能票（P1 实时闭环 / P2 五层 / 看板 / 终验）全部结案，这是唯一剩下的
「要求写了、没验过」的功能条目。

## 一手事实（写票/施工前须知道）

- 生成器的速率节奏由墙上时钟控制（`--rate` **仅影响 live 节奏**，内容与 seed 绑定、
  同参同产出）。已实测的反例教训：[producer.py](../../src/ecomgen/producer.py)
  注释记着「请求条数 ≠ 实际产出条数」（`n_sessions=10000` 实产 22,289 条），
  故**不能用"计划条数 ÷ 1 s"充当速率证据**。
- impl-06d 之后 live 平移改「窗尾对齐 now」并带硬断言（尾部偏移 > 未来裕度即拒绝
  发送、退出码 2）。尖峰跑必须走**合规参数**，否则会被自己的断言拒发。
- `make verify` 的 `dlq-rate-no-increase` 判据对**真脏**敏感。尖峰若触发
  `time_out_of_range`，会让后续 `make verify` 变红——这是**已知耦合**，
  跑之前要记录 DLQ 基线偏移，跑完判断增量归因（真脏 vs 夹具脏）。

## Acceptance criteria

- [x] `make spike` 默认路径**真发**（不再 dry-run），且可用一个参数退回 dry-run 便于无栈环境预演
- [x] 实测速率有**一手测量口径**：发送侧记录 起止墙钟 + 实际发出条数，速率 = 条数 / 秒，
      且**不使用计划条数**当分子
- [x] 实测速率 ≥1000 events/s，数字与口径入票（含采样窗口、条数、耗时、退化情况）
- [x] 尖峰期间链路不崩：Flink 作业保持 RUNNING、无 failover、消费组 lag 在尖峰后回落
- [x] 尖峰对 `dlq-rate-no-increase` 的影响被**归因并记录**（若产生真脏，说明来源与恢复办法）
- [x] README 与 Makefile 的 `spike` 描述改为「已实测 ≥1000/s（日期/数字）」，并与 [impl-06f-1](impl-06f-1-readme-reality-alignment.md) 的「尚未真发」措辞同步替换
- [x] 判据进码：新增一条可机读断言（pytest 或脚本），使「速率 ≥1000/s」不是只活在本票文字里
- [x] 跑完把 DLQ 基线按既有流程复位，`make verify` 回到绿

## Notes

- **不是压测票**：目标是证明 SRS 的 ≥1000/s 达标，不做容量拐点/长时间稳定性。
- 若实测**达不到** 1000/s：**不要**调低判据。可拆副票（生成器节奏 / Kafka 生产端
  批量 / 宿主 IO），并在本票如实登记实测天花板与瓶颈证据。
- 资源上限受 HLD §4 约束（声明 11.5 GiB / 实测目标 ≤8 GB）——尖峰不得为此调 mem_limit。
- 破坏面：真发会污染 `ads_window_metric` 与 DLQ。跑前记录基线，跑后按既有流程归因复位。

## Answer（2026-10-26）

**已实测达标：1011.8 events/s（11134 条 / 11004 ms，含 flush），四项判据全 PASS、退出码 0；`make verify` 尖峰后 10 PASS / 0 FAIL / 0 SKIP 回绿。** SRS `REQ-F-GEN-04` / `REQ-NF-PERF-03` 的尖峰判据首次有一手证据。

### 实测记录（终轮，2026-10-26T15:07Z，报告 `artifacts/spike/spike-report-20261026T150703Z.json`）

| 项 | 实测值 | 口径 |
| --- | --- | --- |
| 速率 | **1011.8 events/s** | 11134 条实际 produce / 首条 produce→flush 完成的墙钟 11004 ms（含 flush，保守口径；分子不是计划条数） |
| 尖峰规模 | 11134 条（seed=1, sessions=5000） | 采样窗口 = 整个发送批次（11.0 s） |
| 期间存活 | 8 个采样全 RUNNING、异常条目零新增 | 监控线程 1 s 采样 `/jobs/overview` + `/jobs/<jid>/exceptions`（期间证据非端点差分） |
| lag 回落 | 尖峰后 44 s 归零（上限 180 s，设计值） | `ecom-p1-consumer` 消费组六分区 LAG 之和 |
| DLQ 增量 | **0（无真脏）** | 窗尾对齐下 event_time ≤ now，质量门上界（now+5 min）不触发——impl-06d 语义的实机验证 |
| verify | 10 PASS / 0 FAIL / 0 SKIP，退 0（`verify-report-20261026T151354Z.json`） | 尖峰后直接跑，`dlq-rate-no-increase` PASS 且基线正常前推 14911/77664 |

### 交付物（全部进码，判据不活在本票文字里）

- `src/ecomgen/spike.py`（新，652 行）：编排 + 四条机读判据（`rate_result` ≥1000 / `liveness_result` / `lag_settle_result` / `dlq_delta_result`），报告落 `artifacts/spike/spike-report-*.json`；前置 RUNNING 检查（无作业退 2，不代拉栈）。
- `src/ecomgen/cli.py`：`SendMeasurement`（一手口径的纯数据模型：实际 produce 条数 / 含 flush 单调墙钟，空批次速率无定义非 0）；`measured_send` 机读块写回 live plan；`_send_to_kafka` 返回 `(rc, measurement)`。
- `Makefile` `spike:`：默认真发（`python -m ecomgen.spike`），`make spike DRY_RUN=1` 退回 `--dry-run` 预演；bootstrap 修正为 `127.0.0.1:29092`（HOST listener，旧注释指 9092 是 P0 已证伪的宿主不可达路）。
- `tests/test_spike.py`（新）：26 条离线单测——阈值边界（999.9 FAIL/1000 PASS）、期间监控缺失 FAIL、空批次速率无定义、DLQ 归因证据面、采集面解析纯函数、分窗节流语义。全套 338+26=364 passed（`tests/` 全量）、ruff check/format PASS、basedpyright 0 error。

### 过程发现：逐条 pacing 的结构性欠冲（票面 Notes 预判分支的实锤）

首轮真发（同参数）实测 **969.2 events/s ⇒ FAIL**——差 31 到阈值。归因：旧 `_send_to_kafka` 逐条按 `i/rate` 目标时间线 `time.sleep`，每条 ~32 µs 的 sleep 超时 × 11134 条 = 354 ms 纯损耗，**结构性无法追回**（sleep 超时只增不减）⇒ 实测速率永远低于 `--rate`，≥1000/s 按该调度**原理性不可达**——不是 Kafka/链路瓶颈（同轮 liveness/lag/DLQ 全 PASS，Kafka 轻松吸干）。

按票面 Notes「可拆副票（生成器节奏）」处理：修在**生成器节奏面**（本票范围内，无需另开票）——`_window_quota_exhausted` 分窗信贷节流（每 1 s 窗口 front-load 份额，发满睡到窗边界）：`--rate` 语义从「逐条时间线」改「持续目标速率」。内容与 seed 绑定不变（F3 裁决：只控制节奏不改内容；节流仍由墙钟控制）。首轮 969.2 的失败证据保留在 `spike-report-20261026T145449Z.json`（终轮报告之外的对照物）。

### 破坏面处置

- `ads_window_metric`：尖峰 11134 条实时聚合正常写入（Doris 三表行数在 verify 中 PASS），无污染迹象——这正是实时链路的正常业务面。
- DLQ：增量 0，无需基线复位；`dlq-rate-no-increase` 在尖峰后 verify 里直接 PASS（0.191994 vs 基线 0.271960），基线按「判定通过时前推」纪律走到 14911/77664。**未动** `dirty-rate-baseline.json`。
- 资源上限未动（HLD §4）：全程无 OOM（`degradation-ladder` PASS，oom_killed=0）。
