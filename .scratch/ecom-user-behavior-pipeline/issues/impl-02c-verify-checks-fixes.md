# impl-02c: 修 kafka-retention 与 doris-ads-rows 两条判据自身的缺陷

> GitHub: https://github.com/Gaomen-7/eco-d-a-platform/issues/3
> 父票：impl-02（本地票 `.scratch/ecom-user-behavior-pipeline/issues/impl-02-p1-realtime-closure.md`）

Type: task
Status: ready-for-agent
Blocked by: None（可与 impl-02a 并行开工；但转 PASS 需等 impl-02b）

**What to build:** 修掉 `make verify` 里两条**与超期阻断无关**的判据缺陷，让判据本身可信——现在它在做「假失败」。

## 背景：本票的两个 FAIL 都是判据自身的错，不是系统的错

已实跑 `make verify` 复现（WSL 下执行）：

```
PASS=5  FAIL=2  SKIP=2  ⇒ 退出码 1
  FAIL kafka-topic-retention  retention 不匹配 vs retention.ms=604800000
  FAIL doris-ads-rows         有表为空或不可查 vs >0
```

### 缺陷 1：retention 施加与读取用了两套工具，读的不是同一份配置

- `docker/scripts/init-kafka.sh` 用 `kafka-configs.sh --alter --add-config "retention.ms=...,retention.bytes=..."` **确实施加了** retention（Kafka 4.3.1 的 `kafka-topics.sh --alter` 已移除，这是 P0 已验证的正确做法）。
- 但 `check_kafka_topic()` 用 `kafka-topics.sh --describe` 读取，该输出实测只有 `Configs: min.insync.replicas=1`，**不含** `retention.ms`。
- 实测 `kafka-configs.sh --describe` 对该 topic 返回 `Dynamic configs for topic ... are:`（空）——需一并查清 retention 究竟落在何处（动态配置 vs 静态配置），再定判据读法。
- ⇒ 判据与施加方必须读同一处，否则是恒假。

### 缺陷 2：`doris-ads-rows` 把三张表捆成一个 PASS/FAIL

脚本查三张表，任一为 0 则整项 FAIL。超期表空表时，另外两张表（254 行 / 30 行，均真实有数据）的好状态被**连带掩盖**。这与判据「逐断言落状态」的要求相悖：应逐表独立出状态，让「两张绿、一张红」可见。

## Acceptance criteria

- [ ] `kafka-topic-retention` 在 retention **正确施加**的前提下判 PASS；先独立证明 retention 真的在 Kafka 侧生效（读施加它的那个配置面），再改判据
- [ ] 该判据的**反例**成立：把 retention 改成错值时判 FAIL（有单测或实机反例证据，不是只测正例）
- [ ] `doris-ads-rows` 拆成逐表断言，三张表各自独立落 PASS/FAIL/实际行数，超期表为 0 时另两张仍显示绿
- [ ] `make verify` 退出码在超期表仍为 0 的情况下，只因超期表而失败（能说清「还差哪一项」）
- [ ] 判据的期望值来源可追溯（指向 gdd §A.2 或 spec 条目），不是硬编码字面量
- [ ] 相关单测补齐；全量 pytest 仍全绿

## Notes

- **本票不动 Flink/Doris 运行时**，只改判据与其读取脚本，因此可与 impl-02a/02b 并行。
- 预期本票完成后 `make verify` 仍退出非 0（超期表仍空），那是 impl-02b 的活——不要为了「让它变绿」而放宽超期表的期望。
- `verify_runner.py` 里 `check_degradation_ladder` 返回 `(CheckResult, dict)`，与 `P1_EXTRA_CHECKS` 的签名不兼容而被单独调用，构造 `VerifyReport` 前必须先取 degradation——顺序坑已踩过，改这块时勿破坏。