# impl-02a: 超期链断点定位（print 探针 + 可信指标面）

> GitHub: https://github.com/Gaomen-7/eco-d-a-platform/issues/1
> 父票：impl-02（本地票 `.scratch/ecom-user-behavior-pipeline/issues/impl-02-p1-realtime-closure.md`）

Type: task
Status: ready-for-agent
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

- [ ] 在超期流上挂一条**临时** print 探针，重建 + 重部署 + 喂一次 live 数据后，探针**有行输出**，据此把断点定位到三处之一并给出证据：`aggregate-window` 侧输出未发射 / 侧输出到 Writer 之间断裂 / 到达 Writer 但 flush 未触发
- [ ] 定位结论可复现：第二次重跑同一场景，断点位置一致（不是靠单次观测下的结论）
- [ ] 说明该顶点上算子计数器为何全 0（名称冲突 / 指标归属），并给出**以后可信的计数口径**（例如改用 Doris 侧 count、或按 subtask 逐点读 `numRecordsIn` 的可靠组合）
- [ ] 探针在交付前已移除；`make verify` 的 PASS/FAIL 数量与移除前一致（证明探针没有偷改链路行为）
- [ ] 实机运行命令在 **WSL** 下执行（宿主无 `make`）；`docker` 命令需 `MSYS_NO_PATHCONV=1` 避免路径改写
- [ ] 复测命令一律读 `/opt/flink/log/*.log`，不再使用 `docker logs` 做判据

## Notes

- **不改业务语义**：本票只定位，不修。修复是 impl-02b。
- 旁证留意：`aggregate-window.numLateRecordsDropped` 恒 0——若迟到事件被「窗口已不存在」的路径直接丢掉，这个值本该非零。
- 不跑 `down -v`，不重跑 P0 破坏性实测。