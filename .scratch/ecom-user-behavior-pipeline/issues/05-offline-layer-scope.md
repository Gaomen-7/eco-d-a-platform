# 05 · 离线分层的范围：P2 与 P1 的先后、门槛、触发面，以及五层内部的“形状”

Type: grilling
Status: open
Blocked by: 02
Blocked by (note): `02` 指**地图票 D4（已落子）**。
Created: 2026-10-20（原 08 + 10 合并）
Ticket: 05
Source: map.md · SRS §7 / FSR §6 /F §9 / gdd §1.4 / D6.1 / D7 · HLD §2.2 / §4.2

## Question

`SRS §7` 与 `FSR §9` 都把离线分层排在 P2（P1 之后），但**没有任何一条说明两者是强序还是可并行**，而它们在资源与接缝上互相咬：同一批生成器代码、同一套超集 schema、同一个 Flink 集群（4 个槽位）、同一个 Doris（P1 写 `ecom_ads`，而 `REQ-F-LAKE-05` 的 ADS 也要「供看板消费」——两条 ADS 是不是同一个地方）、同一组 `make replay` / `make verify` 判据（`verify` 里就有「Paimon 五层有行」）。

**上半：P2 在路线图里的位置**

1. **强序还是并列**：P2 能否在 P1 未完成前开（先只跑 ODS→DWD）？还是「P1 成立」的定义里已含「Paimon 五层有行」，P2 事实上被拉进 P1？
2. **门槛**：`REQ-NF-CAP-05` 要「≥100 万事件」。这批数据从哪来——`dump` 文件、Kafka 重读（gdd D3 说不能超 6h）、还是 `replay`？谁产出、是否进 git（C5 仓库零原始数据）。
3. **槽位与内存**：P2 批作业启动时，`mem_limit` 与 TM 槽位已按 P1 分配。要不要给 P2 一组独立的 `flink-batch` profile（会改动 HLD §4.2 的预算表 → 须先获批）。
4. **触发面**：`REQ-F-LAKE-07` 冻结的是「`make replay` 可完整跑完五层」。`replay` 是一个目标跑五层，还是 Makefile 依赖链（`ods→dwd→dim→dws→ads`）？后者可观察但新增目标名，而 `REQ-F-ORCH-03` 只列了八个。

**下半：五层内部的形状**（`FSR §6` 定了 append/主键，`gdd D6.1` 定了维表字段，但 DDL 形状从未写出）

5. **分区键**：ODS/DWD 按 `dt`？`hour`？不分区？Paimon 对「分区键与主键的先后关系」有无官方约束（这类约束在 Doris 侧已经踩过一次，见 HLD §7.3 U-ν Bug G）。
6. **bucket**：`bucket=-1`（动态）还是固定数？`file://` 单机、GB 级数据量下官方建议是什么。
7. **拒绝表形态**：`dwd_behavior_event_reject` 用 append 还是主键？若 DWD 是 Paimon 主键表，`event_id` 相同的两次到达会**覆盖**——迟到标记 `is_late` / `late_lag_ms` 就被后到者改写，这正确吗？
8. **拉链时间源**：`dim_item` 的 `start_date`/`end_date` 取事件时间还是处理时间？变更流喂进来时取自生成器的哪个字段（`gdd D6.1` 只说「拉链」，未说时间源）。
9. **判据的可执行形态**：`gdd D6.1` 的「无重叠无空洞」怎么写断言（逐 `item_id` × `event_time` 全对比？抽样？何时修）；`gdd §1.4` 的不变量「ODS = DWD + 拒绝 + 迟到标记差异」里，「迟到标记差异」不是可加项，精确等式待写。
10. **快照与保留**：`snapshot.num-retained.min=5` 是**下限**（gdd D7），与「快照过期才物理删除」的交互是什么；`REQ-F-LAKE-08` 的验收判据措辞该由本票还是 [01] 收口（当前约定：[01] 收口口径，本票给形状）。

## 约束

- HITL。先要**形状**（每层一行的决策表 + 需官方确认的约束清单），不要一次写五层全 DDL。
- 官方约束必须引原文；没给就标「官方未给出」。Paimon 侧拿不到一手证据的，标「待 [08] 确认」。
- **不新增常驻容器**；若提议新 profile，同时给出预算变化与回滚。
- 不改 `ecom_lake` 与 `ods_*/dwd_*/dim_*/dws_*/ads_*` 表名前缀（SRS §3.4 冻结）；新增 Makefile 目标须走冻结项变更并在 Answer 单列「需你批准」。

## Answer

<!-- 待解。解析步骤：Skill "grilling" + "domain-modeling" → Status: resolved → 往 map.md 追加 -->
