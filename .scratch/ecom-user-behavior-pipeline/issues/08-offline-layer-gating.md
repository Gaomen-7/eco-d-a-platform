# 08 · 离线分层放在哪：P2 是“P1 之后”，还是与 P1 并列的另一条时间线

Type: grilling
Status: open
Blocked by: 04
Created: 2026-10-20
Ticket: 08
Source: map.md → SRS §7 / FSR §9 / gdd §1.4 / HLD §2.2

## Question

`SRS §7` 与 `FSR §9` 都把离线分层排在 P2（P1 之后），但**没有任何一条说明 P1 与 P2 之间是强序还是可并行**。实际上两者在资源和接缝上互相咬：

- **共享同一批生成器代码**（`--seed`、会话轨迹、注入器）与同一套超集 schema；
- **共享 Flink 集群**（P1 的流作业常驻 + P2 的批作业，TaskManager 槽位只有 4）；
- **共享 Doris**（P1 写 `ecom_ads`；P2 落 Paimon，但 P2 的 ADS 是否也要写 Doris？`REQ-F-LAKE-05` 说「供看板与 OLAP 消费」，而看板直连的是 `ecom_ads`——两条 ADS 是不是同一个地方）；
- **共享 `make replay` 与 `make verify` 的判据**（`verify` 六项里有「Paimon 五层有行」）。

要裁决的：

1. **强序还是并列**：P2 的离线链路能否在 P1 未完成前开（例如先只跑 ODS→DWD？），或者说「P1 成立」的定义里就已经含「Paimon 五层有行」这一项——若是，P2 事实上被拉进了 P1。
2. **门槛**：`REQ-NF-CAP-05` 要求「离线分层最小可跑数据量 ≥100 万事件」。这 100 万从哪来——`dump` 文件、Kafka 重读（D3 说不能超 6h）、还是 `replay` 常数倍？谁负责产出这批数据、它是否要进 git（C5 说仓库零原始数据）。
3. **槽位与内存的互相挤占**：P2 批作业启动时，`mem_limit` 与 TM 槽位已经按 P1 分配。要不要在 compose 里给 P2 一组独立的 `flink-batch` jobmanager/taskmanager profile？（这会改动 HLD §4.2 的预算表，须先获批。）
4. **触发面**：`REQ-F-LAKE-07` 冻结的是「`make replay` 可完整跑完五层」。`replay` 是**一个目标跑五层**，还是 Makefile 依赖关系链（`ods` → `dwd` → `dim` → `dws` → `ads`）？后者会让「层间定序」变得可观察，但会新增目标名——`REQ-F-ORCH-03` 只列了八个。

## 约束

- HITL。**不新增常驻容器**（HLD §4.1 与天花板），若提议新 profile，要同时给出预算变化与回滚。
- 不改八个冻结目标名；新增目标要走 `REQ-F-ORCH-03` 的冻结项变更并记在 Answer 的「需你批准」一栏。
- 一切「P2 能不能今晚就跑」的结论都要落到**实测**，不许写「按设计应当可以」。

## Answer

<!-- 待解。解析步骤：Skill "grilling" + "domain-modeling" → Status: resolved → 往 map.md 追加，
     并把 F11 / F12 按产出毕业成票 -->