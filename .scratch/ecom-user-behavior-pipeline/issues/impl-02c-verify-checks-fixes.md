# impl-02c: 修 kafka-retention 与 doris-ads-rows 两条判据自身的缺陷

> 父票：impl-02（本地票 `.scratch/ecom-user-behavior-pipeline/issues/impl-02-p1-realtime-closure.md`）

Type: task
Status: resolved（2026-10-21）

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

- [x] `kafka-topic-retention` 在 retention **正确施加**的前提下判 PASS；先独立证明 retention 真的在 Kafka 侧生效（读施加它的那个配置面），再改判据
- [x] 该判据的**反例**成立：把 retention 改成错值时判 FAIL（有单测或实机反例证据，不是只测正例）
- [x] `doris-ads-rows` 拆成逐表断言，三张表各自独立落 PASS/FAIL/实际行数，超期表为 0 时另两张仍显示绿
- [x] `make verify` 退出码在超期表仍为 0 的情况下，只因超期表而失败（能说清「还差哪一项」）
- [x] 判据的期望值来源可追溯（指向 gdd §A.2 或 spec 条目），不是硬编码字面量
- [x] 相关单测补齐；全量 pytest 仍全绿

## Answer（2026-10-21 实机取证与修法）

### 缺陷 1：retention 判据读错了配置面 —— 两个错叠在一起

**错一（读法）**：`init-kafka.sh` 用 `kafka-configs.sh --alter --entity-type topics`
写的是**动态主题配置**；判据却用 `kafka-topics.sh --describe` 读，其 `Configs:` 列在
Kafka 4.3.1 实测只输出 `min.insync.replicas=1` ⇒ **恒假 FAIL**。

实机证据（施加方与判据读了两份不同的东西）：

```
$ kafka-topics.sh --describe --topic ecom.behavior.event.v1
Topic: ... Configs: min.insync.replicas=1          ← 判据读这里

$ kafka-configs.sh --describe --entity-type topics --entity-name ecom.behavior.event.v1
Dynamic configs for topic ecom.behavior.event.v1 are:        ← 为空
```

**错二（施加生效性）**：当时动态配置面确实是**空的**——不是判据读法的锅，而是
topic 在 `kafka-init` 之后被**重建**过。铁证是 TopicId 对不上：

| 来源 | TopicId |
|---|---|
| `docker logs eco-kafka-init` 里的 final state | `oo5nJ7cGR-uOMKowE90ckQ` |
| 当前 topic | `QctlI2_OS9WP6btYGaCE1w` |

⇒ 重建后的新 topic 继承 broker 默认，**没有**主题级 retention 覆盖。
重新走既有初始化路径（`docker compose run --rm kafka-init`）后：

```
Dynamic configs for topic ecom.behavior.event.v1 are:
  retention.bytes=2147483648 ... synonyms={DYNAMIC_TOPIC_CONFIG:retention.bytes=...}
  retention.ms=604800000 ... synonyms={DYNAMIC_TOPIC_CONFIG:retention.ms=...}
```

### 修法（`src/ecomgen/verify_runner.py`）

1. `check_kafka_topic()` 改读 `kafka-configs.sh --describe --entity-type topics
   --entity-name <topic>` —— **与施加方同一配置面**。
2. 拆成「解析」与「判定」两个纯函数：
   - `parse_dynamic_topic_configs()`：只认 `Dynamic configs ... are:` 块内的
     `键=值`；**没有块头 ⇒ 返回 `None`（取不到）**，不把「没配置」静默当空。
   - `check_kafka_retention_from_configs()`：`retention.ms` 与 `retention.bytes`
     **两个键都必须全等**才算 PASS（只对一项不算过）。
3. 期望值来自 `TOPIC_RETENTION_MS = "604800000"` / `TOPIC_RETENTION_BYTES = "2147483648"`，
   注释指向 **gdd §A.2 / REQ-F-MQ-03**，与 `init-kafka.sh` 的默认值同源。
4. 取不到动态配置时判 **FAIL 而非 SKIP**：topic 存在且配置可读是本判据的前提，
   SKIP 会重新打开「恒真假通过」的口子。

### 缺陷 2：`doris-ads-rows` 三表捆成一条

改为**逐表查、逐表判**（`check_doris_ads_rows` + 纯函数
`check_doris_ads_rows_from_counts`），`actual` 里逐表写 `表名=行数 PASS/FAIL`：

```
PASS doris-ads-rows ads_window_metric=772 PASS ads_funnel_step=103 PASS
     ads_late_expired_count=157 PASS vs 每张 ADS 表 COUNT(*)>0（逐表判定）
```

超期表为 0 时同一行会显示 `ads_window_metric=772 PASS … ads_late_expired_count=0 FAIL`，
另两张的好状态不再被掩盖，也直接回答了「还差哪一项」。
查询失败判 FAIL（ADS 行数是 P1 核心判据，不 SKIP 掉）。

`docker/scripts/doris-assert.sh` 保持原样——它仍服务 `make init` 第四段，
并额外断言 `dynamic_partition_enable`；verify 不再借道它。

### 反例证据（判据真的在断言）

`tests/test_verify_checks.py` 新增 10 条，其中 6 条是**反例**：

| 反例 | 期望 |
|---|---|
| `retention.ms=60000` | FAIL，`actual` 里能看到错值 |
| 只有 `retention.ms`、缺 `retention.bytes` | FAIL（不是「一项对就算过」） |
| 动态配置取不到（命令失败） | FAIL（不是 SKIP） |
| 块头存在但块内为空（retention 未施加） | FAIL |
| `ads_late_expired_count=0` | FAIL，且另两张仍显示 `PASS` 与实际行数 |
| Doris 查询失败 | FAIL，`detail` 保留原始输出 |

### 验收实测

```
$ make verify
PASS kafka-topic-retention retention.ms=604800000 retention.bytes=2147483648 vs ...
PASS flink-job-running RUNNING vs jobs[].state==RUNNING
SKIP paimon-five-layers P1 未开离线分层 vs ...
PASS doris-ads-rows ads_window_metric=772 PASS ads_funnel_step=103 PASS
                    ads_late_expired_count=157 PASS vs 每张 ADS 表 COUNT(*)>0（逐表判定）
PASS superset-health OK vs OK
PASS funnel-cnt-inferred-monotone [30, 28, 25, 20] vs 沿 step 1→4 非增
PASS dlq-rate-no-increase ...
PASS is-settled-two-states pending=69 settled=703 vs ...
PASS degradation-ladder oom_killed=0（阈值 2，设计值） vs ...
=== verify 完成：PASS=8 FAIL=0 SKIP=1 ⇒ 退出码 0 ===
```

- 全量 pytest：**98 passed**（原 88 + 新增 10）
- `make lint` PASS、`make typecheck`（errors=0）

### 遗留提醒

topic 被重建会**静默丢掉**主题级 retention。当前补救路径是重跑 `kafka-init`；
由于 `make init` 的第一段依赖 compose 的 `kafka-init` 服务，正常流程不会漏。
若将来有人手工删建 topic，`make verify` 的 `kafka-topic-retention` 会立刻 FAIL 点名，
这正是本判据现在应有的行为。

## Notes

- **本票不动 Flink/Doris 运行时**，只改判据与其读取脚本，因此可与 impl-02a/02b 并行。
- 预期本票完成后 `make verify` 仍退出非 0（超期表仍空），那是 impl-02b 的活——不要为了「让它变绿」而放宽超期表的期望。
- `verify_runner.py` 里 `check_degradation_ladder` 返回 `(CheckResult, dict)`，与 `P1_EXTRA_CHECKS` 的签名不兼容而被单独调用，构造 `VerifyReport` 前必须先取 degradation——顺序坑已踩过，改这块时勿破坏。