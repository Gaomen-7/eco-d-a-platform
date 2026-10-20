# 10 · 离线 schema 与演化策略：五层 DDL 的形状、分区键与“主键表 vs append 表”的落地

Type: grilling
Status: open
Blocked by: 08
Created: 2026-10-20
Ticket: 10
Source: map.md → gdd §1.4 / FSR §6 / gdd D6.1 / gdd D7

## Question

`FSR §6` 把五层形态定了（ODS append、DWD/DIM/DWS/ADS 主键表），`gdd §1.4` 补了「ODS 只落原样字节」「离线三通道与实时同构」，`gdd D6.1` 补了维表字段与拉链。但**没有任何地方写出 Paimon 的实际 DDL 形状**：

1. **分区键**：ODS/DWD 按什么分区（`dt` 天？`hour`？不分区）？主键表的分区键与主键必须是什么关系（Paimon 有「分区键必须是主键前缀」一类的官方约束吗？请引原文——这一条在 Doris 那边（gdd §1.5 / U-ν Bug G）已经踩过一次）。
2. **bucket 数**：`bucket=-1`（动态）还是固定数？在 `file://` 单机、5 GB 级数据量下，官方对 bucket 的建议是什么。
3. **主键表 vs append 表的边界**：`dwd_behavior_event_reject` 是 append 还是主键？重复事件「由 DWD `event_id` 主键天然吸收」——但如果 DWD 是 Paimon 主键表，`event_id` 相同的两次到达会**覆盖**，这对「迟到标记 `is_late` / `late_lag_ms`」意味着后到者改写先到者的标记，正确吗？
4. **拉链的事实表结构**：`dim_item` 的 `start_date`/`end_date` 是**事件时间**还是**处理时间**？变更流喂进来时，一条变更的生效时间取自生成器的哪个字段（`gdd D6.1` 只说「拉链」，未说时间源）。
5. **快照与 schema 演进**：与 [02] 对齐——Paimon 加列后旧数据读出来的默认值行为（官方原文），以及 `snapshot.num-retained.min=5` 与「快照过期才物理删除」的交互。

## 约束

- HITL。先要**形状**，不要一次写五层全 DDL；本票的产物是「每层一行的表设计决策表 + 需要官方确认的约束清单」。
- 官方给的约束（如主键/分区关系）必须引原文；官方没给就标「官方未给出」。
- 不改 `ecom_lake` 库名与 `ods_*` / `dwd_*` / `dim_*` / `dws_*` / `ads_*` 表名前缀（SRS §3.4 冻结）。

## Answer

<!-- 待解。解析步骤：Skill "grilling" + "domain-modeling" → Status: resolved → 往 map.md 追加 -->