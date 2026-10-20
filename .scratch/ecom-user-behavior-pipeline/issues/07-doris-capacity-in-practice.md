# 07 · Doris 容量与可证性：30 天动态分区在这台机器上到底能怎么证

Type: research
Status: open
Blocked by: 02
Blocked by (note): `02` 指**地图票 D4（已落子）**。
Created: 2026-10-20（原 07；并入原 07 的「保留策略可证性」与 map 迷雾 F10 的同类问题）
Ticket: 07
Source: map.md · SRS `REQ-F-OLAP-03` / `REQ-F-MQ-03` · HLD §7.2 G-D8 · HLD §8-R5 · gdd D3 / D8

## Question

`REQ-F-OLAP-03` 是 MUST：DWS/ADS 按分区保留 **30 天**，判据「分区/保留策略配置可见」。gdd D8 与 HLD §7.2 G-D8 已把它升级为「行为可证」，并列出三个先决条件（FE 总开关 `dynamic_partition_enable`、`start=-30`、`create_history_partition=true` + `time_unit`/`end`/`prefix`），本仓 `docker/doris/init.sql` 已按此落地。但**没有文档回答**：

1. **默认值的一手出处**：`dynamic_partition.start` 默认到底是不是 `-2147483648`？请给官方配置页 / 源码逐字，并核对 **4.1.x** 上的实际默认（此参数历史上有变动记录）。
2. **调度与粒度**：`time_unit=DAY` 时，`start=-30` 是「第 31 天触发删除」还是要多等一个调度周期？官方给出的调度间隔是多少。
3. **删除是不是物理且立即**：DROP PARTITION 在 BE 上是否异步回收、有没有 trash/回收站类参数，对磁盘（`REQ-NF-CAP-02` ≤20 GB）意味着什么。
4. **开发机上的可证性（核心）**：30 天窗口内删不到东西 ⇒ 判据不可能靠等待通过。官方有没有合法手段「加快时钟」（例如 `time_unit=HOUR` + 小时级 `start`）？若有，本 MUST 可行为可证；若没有，只能写成「配置 + 演练」。
5. **同类项对照**：Kafka 的「7 天或 2 GB 取小」实际保留 ≈6h（gdd D3）。它和 30 天分区是不是同一类「保留策略在开发机上不产生行为」的问题？两者的可证性差异在哪。
6. **成本侧**：Doris BE compaction 与 Paimon compaction 在 `file://` 上的开销，与磁盘 20 GB 预算（HLD §8-R5）会不会相撞——官方给了数字就引，没给就明说没给。

## 约束

- 只读调研，一手 URL 为准；官方未给出的**必须写「官方未给出」**。
- **不要**在真机上调 partition 或删数据——只查官方语义。需要实机验证时另开 task 票并先获批。
- 结论要能直接回答：`REQ-F-OLAP-03` 的判据写成 **「配置 + 小时级演练」**，还是 **「等到第 31 天实测」**。

## Answer

<!-- 待解。解析步骤：Skill "research" → Status: resolved → 往 map.md 追加 -->
