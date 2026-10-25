# impl-06d: R7-i 三案修复——live 跨度 · 冷启重放 · 水位劫持

Type: task
Status: ready-for-agent
Blocked by: —（impl-05 已结清；实施会触碰 impl-02 已验收的 P1 链路，判据纪律见 Notes）

> ✅ **2026-10-25 用户批准开票**（裁决：R7-i 三条修复候选**一张票收口**——三案同源：重放与水位，一起验收）。
> 出处：HLD §8-R7-i（两级连锁，impl-05 终验实测登记）；票面 [impl-05](impl-05-final-acceptance.md) §4。

**What to build:** 消掉「作业冷启动 = 全量重放 + 水位被 topic 内未来事件钉死 ~2 h」的行为——修复后，P1 作业冷启动只消费未消费的增量（committed offsets 生效），live 生成不再产出越过质量门裕度的未来事件，被拒事件不再抬水位。三案一起落地：

1. **live 跨度超界**：`ecomgen live` 平移逻辑改「窗尾对齐 now」（或加 span ≤ 未来裕度的硬断言），使合规参数下不再产出 `time_out_of_range` 的尾部事件。
2. **冷启全量重放**：source 初始化 `OffsetsInitializer.earliest()` → `committedOffsets(OffsetResetStrategy.EARLIEST)`，作业重提只读消费组 committed 之后的增量。
3. **时间戳分配先于质量门**：重审分配与门次序——被质量门拒绝的事件不得参与水位计算（否则一条未来脏数据即可劫持全链 ~2 h）。

**验收标准**

- [ ] 三案各自的**反例证伪**先行（沿 impl-02d 判据纪律）：未修复行为可机读复现，修复后反例转绿
- [ ] 作业重提：committed offsets 之后的增量消费（topic 既有数据**不再**全量重放，numRecordsOut 与增量相符）
- [ ] 人工注入一条未来事件后重提作业：水位不被钉在未来（探针/真实事件照常落 Doris）
- [ ] `ecomgen live` 默认与合规参数下，DLQ 不再出现 `time_out_of_range` 增量（保留 chaos 注入的设计脏）
- [ ] 回归面全绿：`make verify` 两连绿 + `verify-fault` 反例按预期失败 + 全量 pytest
- [ ] HLD §8-R7-i 标已结案（处置列回填修法与实证），运维口径段随修复失效部分划除
- [ ] 全部本地提交，未推送

## Notes

- **改的是 impl-02/02b 已验收面**：`GateSegment.java` source 初始化与时间戳分配属 P1 判据覆盖的链路——每案独立小步提交，先证伪后修复，不许顺手重构。
- 修复后**首次重提仍会全量重放一次**（旧 committed offsets 语义切换前的最后一批）——该次重放按 R7-i 运维口径执行（确认 topic 无未陈化未来事件），不算回归失败。
- 三案若实证后出现耦合冲突（如 committedOffsets 与幂等 checkpoint 语义冲突），允许拆副票，但须先在票面登记拆分理由。
