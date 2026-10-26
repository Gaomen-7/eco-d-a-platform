# impl-06d 实施进度（2026-10-26，暂停点）

> 本条是**施工进度**记录，不是结案。三案代码与判据已落地，但真机终判、
> HLD 回填、全量回归与提交尚未完成（见 §二）。

## 一、已完成（三案代码 + 判据）

### 案 1：live 跨度超界 —— 已修，判据绿

- **反例证伪先行**：`tests/test_cli.py` 新增两条判据。
  - `test_live_shift_keeps_tail_within_future_tolerance`：未修复实现因缺
    `tail_offset_ms` 而红（红已复现）。
  - `test_counterexample_start_aligned_shift_violates_tolerance`：**纯行为反例**，
    把旧公式 `shift = now − raw_min` 与新公式 `shift = now − raw_max` 并排断言 ——
    旧式尾部偏移**恒等于整批跨度**（300 会话实测 span = 1,270,148 ms ≈ 21 min），
    新式尾部偏移**恒为 0**。这条不依赖实现缺字段，故证的是行为不是形状。
- **修法**：`cli.live_shift_ms(raw_min, raw_max, now)` 改「窗尾对齐 now」；plan 新增
  `tail_offset_ms` / `shift_ms` 两个可机读出口；加**硬断言**：尾部偏移 > 未来裕度
  （5 min）时拒绝发送（退出码 2）。
- **实测出口**（`--seed 42 --sessions 40`）：
  `96 条，原始跨度 245483 ms，窗尾平移至 …, 尾部偏移 0 ms ≤ 裕度 300000 ms`。
- 未来裕度提为公开名 `events.FUTURE_TOLERANCE_MS`（与 `EventQuality.FUTURE_TOLERANCE_MS`
  同源），保留旧私有名为别名。

### 案 2：冷启全量重放 —— 已修，判据绿 + 实机实证

- **修法**：新增 `SourceTimePolicy.startingOffsets()` =
  `OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST)`；
  `GateSegment.mainSource` 改用它。
- **实证（决定性）**：作业 RUNNING、消费组已追平后，投入**恰好 96 条**新事件
  （`ecomgen live --sessions 40`），源算子 `read-records = 96` —— **不是** topic
  全量 54,030。旧实现下该值必等于 topic 全量（impl-05 实测两次独立提交均 = 53,789）。
- **反例侧**：`_before_shim` 的 `earliest()` 在同一判据上返回 `EARLIEST_OFFSET(-2)` ⇒ 红。

> ⚠️ **一处判据层修正（结案时需复核）**：首版把案 2 判据放在
> `PartitionOffsetsRetriever` 桩上求「起始 == committed」，**实机证明该层求不出** ——
> flink-connector-kafka 5.0.0-2.2 的 `committedOffsets()` 是
> `ReaderHandledOffsetsInitializer(COMMITTED_OFFSET = -3, …)`，`getPartitionOffsets`
> 对每个分区**原样返回 -3**（javap 实证）；真正的「committed 优先」在 **reader 侧**
> （`KafkaPartitionSplitReader.parseStartingOffsets` 对 -3 单独分支；
> `KafkaPartitionSplit.COMMITTED_OFFSET` 常量实证）。⇒ 判据改为打三层可判别事实
> （reset 策略 == EARLIEST / 决策器 != -2 / 对照组 earliest() == -2），再加真机
> `read-records` 增量实证。**这条口径差若不登记，下个会话会重复踩。**

### 案 3：时间戳分配先于质量门 —— 已修，判据绿；真机终判未到手

- **修法**：新增 `SourceTimePolicy.GatedWatermarks`（实现 `WatermarkGenerator`）：
  `onEvent` 先过质量门，**被拒事件不参与 max 更新**；`GateSegment.mainSource` 用
  `WatermarkStrategy.forGenerator(...)` 接入。质量门判定与 `ValidateAndDedup` 共用
  **同一个 instant**（`EventQuality.reason(e, nowMs)` 新增显式时钟重载），避免两处
  各读一次墙钟得出不同结论。
- **反例证伪**：`R7iFalsification#checkRejectedEventDoesNotAdvanceWatermark` 在旧语义
  shim 上测出**劫持幅度 7,201,000 ms ≈ 2.0 h**（与 impl-05 §4 实测 +2.1 h 同量级），
  修复后水位纹丝不动。
- **本轮自查发现并修掉的一处真缺陷**：首版初值写成 `Long.MIN_VALUE + 1`，减
  outOfOrderness 后**下溢成极大的正数** ⇒ 作业冷启第一步水位就跳到未来，
  **恰好复现 R7-i 的「静默误丢」形态**。已按内置实现（javap 实证）改为
  `Long.MIN_VALUE + outOfOrderness + 1`，并加判据 `checkInitialWatermarkStaysAtLowerBound`
  把这次回归钉死。

## 二、结案轮（2026-10-26）补齐的四项

1. **案 3 真机终判 —— 到手**。重启 JM/TM 使新 jar 生效、重提作业（`104c837b…`）→
   注入 et = now + 2 h 的 `r7i-future-*` → 源算子水位 `ahead_of_now_ms = −242,724`
   （**仍在墙钟后侧**）、DLQ 只 +1 条（`time_out_of_range`）→ `make verify` 两连绿，
   `recon-offline-mirror-doris` **逐值全等 PASS**。
2. **未来脏事件的口径**：本轮**保留**在 topic 里做冒烟（正是它证明案 3 生效）；
   陈化后无害这条由「水位不越过墙钟 + 质量门按到达时刻判」两条保证，不再需要单独口径。
3. **回归面全绿 —— 已跑**：`make r7i-falsify` 0；`_before_r7i.sh` 必红（4 条）；
   `make verify` 两连绿 10/10；全量 pytest 314 passed；lint/typecheck 全过。
4. **HLD / 运维口径 —— 已回填**（§8-R7-i 处置列 + `reproducibility-boundary.md` §11.4 划除）。
5. **提交** —— 全部本地提交，未推送。

## 三、结案轮新挖出的真缺陷（★ 记档，勿重踩）

### 质量门基准时钟被冻结在提交时刻

首版 `GateSegment.mainSource`：`final long nowMs = SourceTimePolicy.nowMs()` 只取一次。
作业 RUNNING 超过 5 min 未来裕度后，**一切真实事件都被判 `time_out_of_range`**。

- 现象：04:29 / 04:35 / 04:44 三轮探针批全进 DLQ，Doris 只剩 `provisional_emitted`；
  `recon-offline-mirror-doris` 连续 FAIL（期望=8 实测=None）。
- 归因：**不是案 3 没修好，是时钟错了**——上一会话把这轮 FAIL 记成「初值缺陷未修」，
  实际初值缺陷当时已修，真正的原因在时钟。
- 修法：`GatedWatermarks` 持 `Clock`（生产 `System::currentTimeMillis` / 判据固定值）。
- 判据：新增 `checkProductionClockIsLive`（生产工厂造的生成器，两次读时钟必须推进）。

### 环境坑：`eco-client` 残留容器导致 Doris 快照恒 None

`docker ps -a` 显示 `eco-client  Exited (255) 3 days ago`，其网络已不存在 ⇒
`docker start` 报 `network … not found` ⇒ `fault._ensure_client()` 恒 False ⇒
**所有 Doris 快照恒 None**（`verify-fault` 在「等本批窗口落定」一步恒挂 300 s）。
处置：`docker compose --profile doris up -d client` 重建（**不要** `docker start`）。

## 四、当前工作区与运行态

- 分支 `feat/ecom-pipeline-impl`，全部本地提交，未推送。
- 最终 jar sha256 `0fee2170f30a612133ab8c3ca5989612c1dd15de30e1f652e902d8c754a8a51e`，
  JM/TM 两端 `/opt/flink/usrlib/ecom-flink.jar` 一致。
- 作业 `104c837b…` RUNNING；消费组 `ecom-p1-consumer` 六分区 lag=0。
