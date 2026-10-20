# 07 · Doris 容量与可证性：30 天动态分区在这台机器上到底能怎么证

Type: research
Status: resolved
Blocked by: 02
Blocked by (note): `02` 指**地图票 D4（已落子）**。
Created: 2026-10-20（原 07；并入原 07 的「保留策略可证性」与 map 迷雾 F10 的同类问题）
Resolved: 2026-10-20（产物：[`research/07-doris-dynamic-partition-retention.md`](../research/07-doris-dynamic-partition-retention.md)）
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

**全文取证见 [`research/07-doris-dynamic-partition-retention.md`](../research/07-doris-dynamic-partition-retention.md)（783+ 行，S1–S25 全部一手引用）。此处只落结论。**

**证据规则（本票最大的方法论产出）**：本仓跑的是 4.1.5 二进制 ⇒ **默认值/阈值一律以 `apache/doris` tag `4.1.5-release` 源码为权威**。本轮在 **6 个配置项**上发现 4.x 文档页与出厂源码不一致（其中 `dynamic_partition_enable` 官方**两个文档页之间就先自相矛盾**）——HLD §8-R7「文档说应该怎样 ≠ 实机实际怎样」的教训**前移到了文档站自身**。

**逐问裁决**：

1. **默认值出处**：`dynamic_partition.start` 默认 `-2147483648`＝「历史分区不删除」——✅ **本仓正确**（文档+源码一致；源码 `DynamicPartitionProperty.MIN_START_OFFSET = Integer.MIN_VALUE`，删除路径以精确相等短路）。⚠️ 但 **`dynamic_partition_enable` 官方默认 `false` 是错的**：4.1.5 源码 = **`true`**（2.1/3.0/4.1.5 三个 tag 都是 `true`，`conf/fe.conf` 未设该项 ⇒ 生效值 `true`）⇒ **gdd D8 的「第三个前置条件」不成立**；`init.sql:16` 幂等冗余，且官方明写 `ADMIN SET FRONTEND CONFIG` **FE 重启即失效**（要持久须进 `fe.conf`）。⚠️ `max_dynamic_partition_num` 4.1.5 源码 = **20000** 不是 500 ⇒ `init.sql:18-19` 若启用是**收紧**不是放宽。真正的前置条件是**表级四项**：enable / 显式 start / time_unit+end+prefix / create_history_partition。
2. **调度与粒度**：`dynamic_partition_check_interval_seconds = 600 s`（官方给出，文档+源码一致）⇒ 删除判定每天边界对齐一次、执行最坏多等一个 10 分钟周期——**附加等待只有 ≤600 s，不是问题**。精确语义（源码推导）：`start=-30` ⇒ **保留恰好 30 个历史日分区，被删的是第 31 老的那个**；`create_history_partition=true` ⇒ **建表当场即建 34 个分区（30 历史+今+3 未来）**。
3. **物理与立即性**：DROP 走非 force ⇒ **进 catalog recycle bin**；BE 文件经 GC 异步回收。4.1.5 源码 `trash_file_expire_time_sec = 0`（文档页却写 86400——第 3 处冲突），且源码注释自承「只是建议值，磁盘不足可不遵守」⇒ **「分区删除⇒磁盘 T 内释放」官方未给出**。
4. **可证性（核心）**：**官方给出了合法加速手段**——`time_unit=HOUR` 是文档+源码双重支持的正规粒度（分区名 `p<yyyyMMddHH>`），HOUR+`start=-2` ⇒ **≤1200 s（2 个调度周期）内可观测真实删除**。硬约束：DATE/DATEV2 与整数分区列**官方 DdlException 拒绝 HOUR** ⇒ 演练表只能建在 DATETIMEV2 分区列上（`ads_window_metric`/`ads_late_expired_count` 的键型可用；`ads_funnel_step` 的 `session_date DATE` **不可用**，漏斗表自身的 30 天保留**没有**小时演练路径）。`time_zone` 只能平移边界相位、不能加速；`reserved_history_periods` 只能多留不能少留。「用 HOUR 做测试」官方未推荐 ⇒ 演练是本仓基于官方能力的推导用法，判据必须诚实分两段。
5. **同类项对照**：**同一类问题，可证性差一个量级**——Kafka 的 2 GB 是**体积**上限（给定速率下小时级可达 ⇒ 可写行为判据）；Doris 的 30 天是**时间**上限（与写入量无关、31 天内必不可观测 ⇒ 不能靠等）。⚠️ 附带证伪两条：本仓 gdd D3 所引逐字句「A segment will be deleted whenever either of these criteria are met」在 Kafka 4.3 官方站**未逐字命中**（官方逐字在 Log 实现页 "a segment that is eligible for deletion due to either policy will be deleted" + `cleanup.policy` 条目）；且 `retention.bytes` 官方明写**按分区生效** ⇒ 6 分区下主题级实为 12 GB ⇒ **gdd D3 的「≈6 h」须按配在哪一层重算（可能是 ≈36 h）**，连同 `file.delete.delay.ms=60 s`（删文件还要再等 1 分钟）一并交 [09]/实机自查。
6. **成本侧**：Doris BE compaction 的**耗时/IO/单版本写放大：官方未给出**；但本轮取到**唯一一个官方磁盘系数**——env-checking 页「BE = 总量 ×3× **1.4**」，×1.4 逐字为 **"40% reserved space for background compaction"**（口径：integrated+3 副本；本仓 1 副本 ⇒ ×3 不适用、×1.4 可单独套用——推导）。Paimon `file://` compaction 成本数字仍**官方未给出**（与 08a §4.4 一致）；官方 dev/test 磁盘下限 FE 10 GB+/BE 50 GB+ 本轮**升格为一手 URL**（`doris.apache.org/docs/4.x/install/preparation/env-checking/`）。⇒ ≤20 GB 与 compaction 会不会相撞：**无官方数字可判，只能 P1/P2 实测定**（维持 §8-R5「未实测即未通过」）。

**票面核心问题的答案**：`REQ-F-OLAP-03` 判据写 **「配置 + 小时级演练」**，不是「等第 31 天」——三层措辞（配置层：`SHOW DYNAMIC PARTITION TABLES` + 当场 34 个日分区；行为层：独立 HOUR 演练表 ≤1200 s 内分区真实消失；诚实条款：**不声称**已验证 30 天边界本身，登记为后续实机任务）见 research 文件 §7.2。

**连带产物**：① gdd D8 / HLD G-D8 / canonical-truth E 需按本节修订（见 research §7.3）；② gdd D3 需按第 5 条重算（research §7.4）；③ 后续实机 task 票三张待开（HOUR 演练实机验证 / DROP 后磁盘回落实测 / compaction×20 GB 对撞实测），**均须先获批**。
