# impl-06d: R7-i 三案修复——live 跨度 · 冷启重放 · 水位劫持

Type: task
Status: resolved
Blocked by: —（impl-05 已结清；实施会触碰 impl-02 已验收的 P1 链路，判据纪律见 Notes）

> ✅ **2026-10-25 用户批准开票**（裁决：R7-i 三条修复候选**一张票收口**——三案同源：重放与水位，一起验收）。
> 出处：HLD §8-R7-i（两级连锁，impl-05 终验实测登记）；票面 [impl-05](impl-05-final-acceptance.md) §4。

**What to build:** 消掉「作业冷启动 = 全量重放 + 水位被 topic 内未来事件钉死 ~2 h」的行为——修复后，P1 作业冷启动只消费未消费的增量（committed offsets 生效），live 生成不再产出越过质量门裕度的未来事件，被拒事件不再抬水位。三案一起落地：

1. **live 跨度超界**：`ecomgen live` 平移逻辑改「窗尾对齐 now」（或加 span ≤ 未来裕度的硬断言），使合规参数下不再产出 `time_out_of_range` 的尾部事件。
2. **冷启全量重放**：source 初始化 `OffsetsInitializer.earliest()` → `committedOffsets(OffsetResetStrategy.EARLIEST)`，作业重提只读消费组 committed 之后的增量。
3. **时间戳分配先于质量门**：重审分配与门次序——被质量门拒绝的事件不得参与水位计算（否则一条未来脏数据即可劫持全链 ~2 h）。

**验收标准**

- [x] 三案各自的**反例证伪**先行（沿 impl-02d 判据纪律）：未修复行为可机读复现，修复后反例转绿（`scripts/_before_r7i.sh` 在旧语义 shim 上 4 条红：案2 B / 案3 A 劫持 7,201,000 ms / 案3 B / 案3 时钟；`make r7i-falsify` 退出码 0）
- [x] 作业重提：committed offsets 之后的增量消费（topic 既有数据**不再**全量重放，numRecordsOut 与增量相符）（投入 96 条 ⇒ 源算子 read-records = 96，非 topic 全量 54,066）
- [x] 人工注入一条未来事件后重提作业：水印不被钉在未来（探针/真实事件照常落 Doris）（注入 et=now+2h 后 ahead_of_now ≈ −243 s；DLQ 只 +1 条；`recon-offline-mirror-doris` 逐值全等）
- [x] `ecomgen live` 默认与合规参数下，DLQ 不再出现 `time_out_of_range` 增量（保留 chaos 注入的设计脏）（窗尾对齐后尾部偏移恒 0 ≤ 裕度 300,000 ms 设计值）
- [x] 回归面全绿：`make verify` 两连绿 + `verify-fault` 反例按预期失败 + 全量 pytest（verify 两轮 10/10；`verify-fault` 12/12 PASS；`verify-fault-corrupt` 12 PASS/2 FAIL 退出码 1 = 按预期失败；pytest 314 passed）
- [x] HLD §8-R7-i 标已结案（处置列回填修法与实证），运维口径段随修复失效部分划除（HLD 处置列 + boundary §11.4 划除线）
- [x] 全部本地提交，未推送（`40e4227` 及本轮 review 修订，均未推送）

## Notes

- **改的是 impl-02/02b 已验收面**：`GateSegment.java` source 初始化与时间戳分配属 P1 判据覆盖的链路——每案独立小步提交，先证伪后修复，不许顺手重构。
  - **执行口径（2026-10-26 code-review 后登记）**：三案代码以**单个 commit** 收口（`40e4227`），与「每案独立小步提交」字面不符——用户批准开票时的裁决即「三案同源、一张票收口」，且本仓 impl-05/06 系列一贯按「一票一 commit」落地；「先证伪后修复」已逐案满足（反例侧脚本与判据先红、修复后转绿）。此条作为纪律例外记档，不改历史。
- 修复后**首次重提仍会全量重放一次**（旧 committed offsets 语义切换前的最后一批）——该次重放按 R7-i 运维口径执行（确认 topic 无未陈化未来事件），不算回归失败。
- 三案若实证后出现耦合冲突（如 committedOffsets 与幂等 checkpoint 语义冲突），允许拆副票，但须先在票面登记拆分理由。

---

## 进度（2026-10-26，**结案**）

> 本票三案全部落地并真机终判通过。取证明细见
> [evidence/impl-06d/00-PROGRESS.md](../evidence/impl-06d/00-PROGRESS.md)。

### 一、三案代码 + 判据（全部绿）

**案 1：live 跨度超界 —— 已修，判据绿**

- 反例证伪先行：`tests/test_cli.py` 新增两条判据。
  - `test_live_shift_keeps_tail_within_future_tolerance`：未修复实现因缺
    `tail_offset_ms` 而红（红已复现）。
  - `test_counterexample_start_aligned_shift_violates_tolerance`：**纯行为反例**——
    旧公式 `shift = now − raw_min` 的尾部偏移**恒等于整批跨度**（300 会话实测
    span = 1,270,148 ms ≈ 21 min），新公式 `shift = now − raw_max` 的尾部偏移**恒为 0**。
    这条不依赖实现缺字段，故证的是行为不是形状。
- 修法：`cli.live_shift_ms(raw_min, raw_max, now)` 改「窗尾对齐 now」；plan 新增
  `tail_offset_ms` / `shift_ms` 可机读出口；加**硬断言**（尾部偏移 > 5 min 裕度 ⇒
  拒绝发送，退出码 2）。
- 实测出口（`--seed 42 --sessions 40 --dry-run --plan`）：`96 条，原始跨度 245,483 ms，
  窗尾平移至 …，尾部偏移 0 ms ≤ 裕度 300,000 ms`；plan 的 `tail_offset_ms = 0`。
- 未来裕度提为公开名 `events.FUTURE_TOLERANCE_MS`（与 Flink 侧
  `EventQuality.FUTURE_TOLERANCE_MS` 同源），旧私有名保留为别名。

**案 2：冷启全量重放 —— 已修，判据绿 + 实机实证**

- 修法：新增 `SourceTimePolicy.startingOffsets()` =
  `OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST)`；
  `GateSegment.mainSource` 改用它。
- **实机实证（决定性，2026-10-26 结案轮复测）**：作业 RUNNING、消费组已追平后投入
  **恰好 96 条**新事件，源算子 `read-records = 96`（逐 subtask 38/6/9/43 求和）——
  **不是** topic 全量 54,066。旧实现下该值必等于 topic 全量。
- 反例侧：旧 shim 的 `earliest()` 在同一判据上返回 `EARLIEST_OFFSET(-2)` ⇒ 红。
- 票面 Notes 的「首次重提仍会全量重放一次」也已实证：删组后重提
  `read-records = 53,862`（≈ topic 全量）；此后 committed 追平、再投 96 条增量时 = 96。

> ⚠️ **判据层口径差（已登记，勿重踩）**：flink-connector-kafka 5.0.0-2.2 的
> `committedOffsets()` 是 `ReaderHandledOffsetsInitializer(COMMITTED_OFFSET = -3, …)`，
> `getPartitionOffsets` 对每个分区**原样返回 -3**（javap 实证）；真正的「committed 优先」
> 在 **reader 侧**（`KafkaPartitionSplitReader.parseStartingOffsets` 对 -3 单独分支）。
> ⇒ 判据打三层可判别事实（reset 策略 == EARLIEST / 决策器 != -2 / 对照组 earliest() == -2），
> 再加真机 `read-records` 增量实证。

**案 3：时间戳分配先于质量门 —— 已修，判据绿 + 真机终判通过**

- 修法：新增 `SourceTimePolicy.GatedWatermarks`（实现 `WatermarkGenerator`）：
  `onEvent` 先过质量门，**被拒事件不参与 max 更新**；`GateSegment.mainSource` 用
  `WatermarkStrategy.forGenerator(...)` 接入。
- 初值逐字照抄内置 `BoundedOutOfOrdernessWatermarks`（`Long.MIN_VALUE + outOfOrderness + 1`）：
  首版写成 `Long.MIN_VALUE + 1` 会**下溢成未来**，判据
  `checkInitialWatermarkStaysAtLowerBound` 已把这次回归钉死。
- 反例证伪：同一 harness 在旧语义 shim 上测出**劫持幅度 7,201,000 ms ≈ 2.0 h**
  （与 impl-05 §4 实测 +2.1 h 同量级）；修复后水位纹丝不动。
- **真机终判（本轮到手）**：注入 et = now + 2 h 的脏事件 `r7i-future-*` 后，源算子水位
  `ahead_of_now_ms = −242,724 … −263,724`（**仍在墙钟后侧**，未被钉到未来）；
  DLQ 只 +1 条且 `reason=time_out_of_range`（分区 1，offset 2411→2412）；
  `make verify` 两连绿，`recon-offline-mirror-doris` 探针键集**逐值全等**（PASS）。

### 二、本轮（结案轮）额外挖出并修掉的一条真缺陷

**★ 质量门基准时钟被冻结在提交时刻**——首版 `GateSegment.mainSource` 里
`final long nowMs = SourceTimePolicy.nowMs()` 只取一次，作业 RUNNING 超过 5 min
裕度后**一切真实事件都被判 `time_out_of_range`**。

- 实测形态：04:02 提交的作业在 04:29 / 04:35 / 04:44 三轮探针批**全部进 DLQ**，
  Doris 只剩 `provisional_emitted` 行（`recon-offline-mirror-doris` 连续三轮 FAIL，
  期望=8 实测=None）——与 R7-i 的「静默误丢」**同形**，只是诱因换成了「时钟停在提交时刻」。
  这也解释了本轮开头「复跑探针仍 FAIL」的归因：**不是案 3 没修好，是时钟错了**。
- 修法：`GatedWatermarks` 持 `Clock`（`System::currentTimeMillis` 生产 / 固定值判据），
  改后 `make verify` 立刻 10/10 绿。
- 判据：新增 `R7iFalsification#checkProductionClockIsLive`（拿生产工厂造生成器、两次读时钟
  必须观察到推进；冻结快照的实现两次读数相同 ⇒ 红）。

### 三、回归面（全绿）

| 判据 | 结果 |
|---|---|
| `make r7i-falsify` | 退出码 0（案 1 3 条 pytest + 案 2 三层 + 案 3 四条 javac/java 断言） |
| `scripts/_before_r7i.sh`（反例侧） | 按预期红（案2 B / 案3 A 劫持 7,201,000 ms / 案3 B / 案3 时钟，共 4 条） |
| `make verify` 两连绿 | **10/10 PASS，退出码 0**（两轮均绿） |
| `make verify-fault` | **12/12 PASS，退出码 0**（首跑因 `eco-client` 残留容器环境坑恒挂，重建 client 后过——见 §三注） |
| `make verify-fault-corrupt` | **按预期失败**（12 PASS / 2 FAIL，退出码 1——判据确实在断言） |
| 全量 `pytest` | **314 passed** |
| `make lint`（ruff check + format --check） | 全过 |
| `make typecheck`（basedpyright） | errors=0 warnings=6 files=14 ⇒ PASS |

### 四、HLD 与运维口径回填

- **HLD §8-R7-i 处置列已回填**：三案修法 + 实证数字 + 「下文运维口径随之失效」；
  并新增两条修复期环境事实——④**作业 jar 必须放 `/opt/flink/usrlib/`**
  （放 `lib/` 会 `LinkageError: loader constraint violation … committedOffsets`，
  因 shade jar 含 `org/apache/kafka/**` 而 `flink-dist-2.2.1.jar` 不含 kafka 类）、
  ⑤**质量门时钟不得冻结在提交时刻**（即 §二 那条）。
- **`reproducibility-boundary.md` §11.4 的 R7-i 运维口径已划除**（`--sessions ≤ 40`、
  「冷启动前确认 topic 无未陈化未来事件」两条随修复失效），改为指向本票实证。

### 五、运行态与提交

- 分支 `feat/ecom-pipeline-impl`，**全部本地提交，未推送**。
- 最终 jar：`flink/target/ecom-flink-0.1.0-SNAPSHOT.jar`（sha256 `9031125a…`（code-review 修订后终版）），
  JM/TM 两端 `/opt/flink/usrlib/ecom-flink.jar` sha256 与之一致。
- 运行态：作业 `104c837b…` RUNNING；消费组 `ecom-p1-consumer` 六分区 lag=0。
- 取证脚本：`scripts/r7i-falsify.sh`、`scripts/_before_r7i.sh`、
  `scripts/_inject_future.py`、`evidence/impl-06d/probe_job.py`（新增 `watermarks` 模式）。