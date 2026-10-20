# 07 · Doris 动态分区策略的真相：30 天保留在开发机上是“静默无动作”还是有实际成本

Type: research
Status: open
Blocked by: 04
Created: 2026-10-20
Ticket: 07
Source: map.md → Not yet specified F10 / SRS `REQ-F-OLAP-03` / HLD §7.2 G-D8 / HLD §8-R5

## Question

`REQ-F-OLAP-03` 是 MUST：DWS/ADS 按分区保留 **30 天**，判据是「分区/保留策略配置可见」。gdd D8 与 HLD §7.2 G-D8 已经把它升级为「行为可证」，并列出了三个先决条件（`dynamic_partition_enable` 的 FE 总开关、`start=-30`、`create_history_partition=true` + `time_unit`/`end`/`prefix`）。本仓的 `docker/doris/init.sql` 已按此落地。但**没有一个上游文档回答**：

1. **官方对 `dynamic_partition.start` 的默认值到底是多少**——HLD 引的是 `-2147483648`，gdd D8 也是；请给出一手出处（官方配置页/源码行），并确认 **4.1.x** 上的实际默认是否与文档一致（历史上该参数有变动记录）。
2. **默认分区粒度**：`time_unit=DAY` 时，一张按天分区的表，`start=-30` 意味着「第 31 天触发删除」还是要多等一个调度周期？官方给出的调度周期（默认多久扫一次）是多少。
3. **删除是物理且立即的吗**：Doris 的 DROP PARTITION 在 BE 上的回收是否异步、有没有 `trash` / 回收站（官方有 `trash` 相关参数吗），对**磁盘占用**（`REQ-NF-CAP-02` ≤20 GB）意味着什么。
4. **开发机上的实际行为**：30 天内根本删不到东西——这意味着 `REQ-F-OLAP-03` 的「行为可证」在 P1 阶段**不可能通过等待 30 天**来证。官方有没有「把 `time_unit` 设成 `HOUR`、把 `start` 设成小时级」这种**加快时钟**的合法手段？如果有，可证性就成立；如果没有，此 MUST 就只能是「配置 + 单元演练」。
5. `REQ-F-MQ-03`（Kafka「7 天或 2 GB 取小」）的实际生效窗 ≈6h（gdd D3）与本项是否同属「保留策略在开发机上不生效」这一类——若是，请一并指出两者的可证性差异。

## 约束

- 只读调研，一手 URL 为准；官方未给出的**必须写「官方未给出」**。
- **不要**在真机上调 partition 或删数据——这条票只查官方语义。若需要实机验证，另开 task 票并先获批。
- 结论要能直接回答一个二选一：`REQ-F-OLAP-03` 的判据应当写成 **「配置+小时级演练」**，还是 **「等到第 31 天实测」**。

## Answer

<!-- 待解。解析步骤：Skill "research" → Status: resolved → 往 map.md 追加 -->