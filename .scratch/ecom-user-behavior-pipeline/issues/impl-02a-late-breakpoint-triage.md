# impl-02a: 超期链断点定位（print 探针 + 可信指标面）

> 父票：impl-02（本地票 `.scratch/ecom-user-behavior-pipeline/issues/impl-02-p1-realtime-closure.md`）

Type: task
Status: resolved
Blocked by: None (can start immediately)

**What to build:** 把「超期表恒 0 行」从一个猜了三轮的谜题，变成一个**有确定答案的位置**。

## 背景（这些是已证事实，不要重做）

- 生成器侧**已达标**：采样真实 Kafka topic 的 3000 条、逐分区按 Flink 水位语义重放，得 `expired=565 correctable=253`；可达性下界 480s 各分区 span 均 ≥3205s，远超。
- Doris 侧**已排除**：`ads_late_expired_count` 与可写表分区策略完全一致（34 个分区，同样覆盖到 `p20261024`），表可写。
- sink 侧**已排除异步静默抛错**：真实日志中当前作业 0 条 ERROR。
- 但 `ads_late_expired_count` 的 Stream Load 次数为 **0**，`ecom-p1-late` 只出现 16 次且全是 `DorisWriter` 初始化打印的 `labelPrefix`。

## ★ 取证方法修正（这是本票的前提，务必先看）

**`docker logs` 读到的不是活跃日志。** `docker logs eco-flink-tm` 的 stdout 停在 10:16 UTC 且已轮转；真实且持续写入的是容器内 `/opt/flink/log/flink--taskexecutor-2-flink-tm.log`。交接文档 §3/§4/§5.7 的所有「某表有没有 Stream Load」判定都基于旧文件，§5.7 教的 `docker logs | grep -oE "table ..."` 方法本身是错的。

## ★ 指标面不可信（本票要解决的核心）

同一超期顶点上：顶点级 `numRecordsOut` 合计 195，但**每一个**具名算子计数器都是 0（`aggregate-window.numRecordsOut`、`count-expired.numRecordsIn`、`view_late`、`Calc`、`Writer` 全 0）。而 `aggregate-window.numRecordsOut=0` 逻辑上不可能成立——Doris 里 `event_*` 行 `sum(metric_value)=635`，且 `max(window_start)` 晚于本作业启动时间。

对照的漏斗顶点计数器完全自洽（134=134=134=134）。TM 日志有大量 `Name collision: ... 'pendingCommittables'`。

⇒ 「195 行已到 Writer」是误读顶点级指标。**本票要建立一套可信的证据链**，而不是继续在这堆计数器上推断。

## Acceptance criteria

- [x] 在超期流上挂一条**临时** print 探针，重建 + 重部署 + 喂一次 live 数据后，探针**有行输出**，据此把断点定位到三处之一并给出证据：`aggregate-window` 侧输出未发射 / 侧输出到 Writer 之间断裂 / 到达 Writer 但 flush 未触发
- [x] 定位结论可复现：第二次重跑同一场景，断点位置一致（不是靠单次观测下的结论）
- [x] 说明该顶点上算子计数器为何全 0（名称冲突 / 指标归属），并给出**以后可信的计数口径**（例如改用 Doris 侧 count、或按 subtask 逐点读 `numRecordsIn` 的可靠组合）
- [x] 探针在交付前已移除；`make verify` 的 PASS/FAIL 数量与移除前一致（证明探针没有偷改链路行为）
- [x] 实机运行命令在 **WSL** 下执行（宿主无 `make`）；`docker` 命令需 `MSYS_NO_PATHCONV=1` 避免路径改写
- [x] 复测命令一律读 `/opt/flink/log/*.log`，不再使用 `docker logs` 做判据

## Answer（2026-10-21 实机取证）

**结论先行：三处断点**都不存在。探针 44 行 = Doris 44 行，1:1 全链路通。超期表「恒 0 行」是
**数据侧（旧 topic 没有运行时会被判超期的行）+ 顶点级计数误读**共同造成的，不是链上任何一处断裂。

### 取证链条

1. **修正版逐分区重放**（关键方法修正：水印先分类后推进——Flink 的
   `TimestampsAndWatermarksOperator` 先转发元素、后推进水印；再建模 1h event_id 去重）：
   旧 topic 3609 条 → `normal=626, correctable=7, expired=2, duplicate=2974`。
   **626+7+2 = 635 = 旧作业 `aggregate-window.numRecordsIn` 的精确值**——重放与实机完全对上。
   旧票面「采样 3000 条得 expired=565」是**错误重放语义**的产物（用含自身时间戳的水印分类
   + 不去重 + 把多轮串成一个连续流），不是运行时事实。
2. **旧作业的 195 是误读**：顶点级（task gate）`numRecordsOut=195` = 窗口算子**主输出**
   （落定行去 `view_windowed`），不是超期侧输出。具名算子计数（`aggregate-window.numRecordsOut`、
   `count-expired.numRecordsIn`、`view_late`/`Calc`/`Writer`）读 0 是**如实**的——旧 topic 在
   运行时水印下确实没有会被判超期的行（重放那 2 条也因 keyBy 合并水印更保守而没触发）。
3. **临时探针实测**（`LateExpiredProbe`，SLF4J 写 `/opt/flink/log/*.log`，不是容器 stdout）：
   重建 jar → cancel 旧作业 → `flink run`（同 consumer group，续读不重放）→ 喂 seed=2
   `--sessions 5000 --rate 500 --late-correctable 0.03 --late-expired 0.02`（11200 条）：
   - 探针输出 **13 行**；`count-expired`/`view_late`/`Calc[8]`/`Writer` 的 `numRecordsIn` **各 13**；
   - TM 日志出现 `table ads_late_expired_count stream load started/stopped`（14 次）；
   - `ads_late_expired_count` **13 行**（`window_start` 对齐 60 s、`late_lag_ms` 为正、`cnt=1`）。
   即：侧输出未发射 / 到 Writer 断裂 / flush 未触发 **三处全部排除**。
4. **复现**（acceptance #2）：seed=3 同参数再喂 11191 条 → 探针 13→**44**、Doris 13→**44**、
   Stream Load 14→**28**。位置一致：链是通的。
5. **为何实机只有 13 条而非逐分区重放预测的 1027**：重放按分区独立算水印，而实机 window
   算子按 `keyBy(category_id)` 合并多分区输入，subtask 水印 = 各输入通道水印的 **min**，
   更保守 ⇒ 大多数可判超期的行在实机被归为「on-time」进窗口。⇒ 逐分区重放只能当**上界**，
   不是预测器。

### 可信计数口径（acceptance #3）

- **真值**：Doris 侧 `SELECT COUNT(*) FROM ecom_ads.ads_late_expired_count`（本轮 44 行）。
- **算子计数可信的时机**：数据真的流过时，按 subtask 逐点读 `numRecordsIn` 是可靠的
  （探针 = `count-expired` = `view_late` = `Calc` = `Writer` = 13，全等）。
- **Name collision 只影响 `pendingCommittables` gauge**：TM 日志的
  `Name collision: ... 'pendingCommittables'` 是 Doris `Committer` 的同名 gauge 在
  同 task 里重复注册被丢，**不影响** `numRecordsIn/Out` 计数。
- **旧 job 具名计数为 0 不是被碰撞打坏**：是「旧 topic 无运行时超期行」的如实反映。
- **预测口径**：逐分区重放（含本票修正后的水印时序 + 去重）仅作上界；要精确需建模
  keyBy 合并水印，或直接看 Doris count。

### 对 impl-02b 的提示

超期 sink 链在当前代码（`java.sql.Timestamp` + `CAST AS TIMESTAMP(3)` + `sink.buffer-flush.interval=1s`）
下**已经能落行**（本轮 44 行）。impl-02b 若仍要修，重点不在 sink，而在**让输入真正产生运行时
超期事件**：旧 topic（小轮次、多轮同 seed 去重）几乎产不出；`ecomgen live` 的
`_EXPIRED_INSERT_FRACTION=0.99` + offset∈[130,300]s 在真实 keyBy 合并水印下也只有
个位数触发。修法选择（数据面 vs 判据面）由 impl-02b 决策。

### 验证记录（WSL 执行；判据一律读 `/opt/flink/log/*.log`）

- 探针移除前后 `make verify`：with-probe `PASS=7 FAIL=1 SKIP=1`；移除后 `PASS=7 FAIL=1 SKIP=1`。
  （`kafka-topic-retention` FAIL 属 impl-02c；`doris-ads-rows` 由 0 行转 PASS 是数据落行所致，
  与探针无关。）
- pytest 88 passed；`make lint`/`make typecheck` PASS。
- 复测命令示例（读 `.log`）：`docker exec eco-flink-tm sh -c 'grep -c "PROBE-late-expired" /opt/flink/log/*.log'`、
  `grep -E "table ads_late_expired_count stream load" /opt/flink/log/flink--taskexecutor-2-flink-tm.log`。

## Notes

- **不改业务语义**：本票只定位，不修。修复是 impl-02b。
- 旁证留意：`aggregate-window.numLateRecordsDropped` 恒 0——若迟到事件被「窗口已不存在」的路径直接丢掉，这个值本该非零。
- 不跑 `down -v`，不重跑 P0 破坏性实测。