# 01 · 唯一真相源：REQ 冲突的裁决口径 + 收口后文档长什么样

Type: grilling
Status: open
Blocked by: 02
Blocked by (note): `02` 指**地图票 D4「配置确定性与可复现＝立项基石」**（已落子，见 [map.md](../map.md) 的 Decisions so far），不是本票自身——本地后端的 `Blocked by:` 只认票号，故在此标注。
Created: 2026-10-20（原 03；2026-10-20 由 11 票精简为 9 票时并入 02·schema 版本化与 10·离线 schema 演化）
Ticket: 01
Source: map.md · gdd §D / HLD §7 / FSR R1 / SRS §6

## Question

本仓的权威链是**单向**的（SRS → gdd → HLD），**没有反向收口通道**：SRS 与 FSR「一个字都不改」，gdd 的 D1–D15 与 HLD 的 D-A…D-G 只是登记，等一个**永远不会自己发生**的 `spec.md` 会话。同一个数在三个文件里有三个值：

| 冲突 | 上位文件说 | 下位文件已证 |
| --- | --- | --- |
| **A** | `REQ-F-LAKE-08`：快照数**受控为 5** | gdd D7：5 是**下限**；HLD §8 引用该证伪 |
| **B** | `REQ-F-RT-03`：UV 用**近似算法** + 误差量化 | gdd D9：前提不成立 → 改**精确** + 一致性校验 |
| **C** | `REQ-F-RT-04`：迟到走**侧输出** | gdd D11：Flink SQL 无此能力 → 强制混合路线（D11/D12 本身待一手证实） |
| **D** | `REQ-F-GEN-07`：`--late-ratio` 默认 5% | gdd §1.1：拆成 `--late-correctable 3%` + `--late-expired 2%` |
| **E** | `REQ-F-OLAP-03`：30 天保留 | gdd D8 + HLD §7.2 G-D8：还须 FE 级 `dynamic_partition_enable` |
| **F** | `REQ-NF-REL-03` + `REQ-F-OLAP-02`：两道独立防线 | gdd D1：一层主防 + 一层兜底 |
| **G** | `REQ-F-VERIFY-04`：重复算脏 | gdd D2：重复不是脏 |
| **H** | `REQ-F-LAKE-09` 与规则①：公开数据集 100% 被判脏 | gdd D4：规则①改**相对下界**（`【按推荐】`，未落死） |
| **I** | `REQ-NF-CAP-01` / FSR §2：≤8 GB | HLD §4.2 实测：声明上限 10.5 GiB（全起） |
| **J** | `REQ-F-MQ-03`：7 天或 2 GB 取小 | gdd D3：实际保留窗 ≈6h |
| **K** | `REQ-F-VERIFY-01`：五项烟测 | HLD §3.3-6：`lint`/`typecheck` 另设目标 |
| **L** | `REQ-F-ORCH-05`：named volume | gdd F16 / HLD §8-R7-e：挂载 ≠ 数据落进去，须显式 `KAFKA_LOG_DIRS` |

本票**也承接**两件原先拆出去、本质属于同一问题（「收口后的文档长什么样」）的「形状」决策：

1. **事件 schema 的版本化铁律**：`schema_version` 只有字面值 `1.0`，没有机制——谁改、加字段算不算 minor、生成器 / DWD 清洗 / DLQ 校验三处对版本号的理解是否必须一致、跨版本从 checkpoint 恢复是禁止还是「缺省值 + 标记」。
2. **离线 schema 与演化策略**：五层 DDL 的形状——分区键（`dt`？`hour`？Paimon 对「分区键与主键的先后关系」有无官方约束）、`bucket=-1` 还是固定数、`dwd_behavior_event_reject` 是 append 还是主键（后者会让迟到标记 `is_late` 被后到者覆盖）、`dim_item` 拉链的时间源是事件时间还是处理时间。

## 要裁决的元规则

1. **谁有限制权**：物理/官方 API 限制（C、E、L 类）可以**直接覆盖** SRS 的 MUST，还是必须走冻结项变更？「判据升级」（B、G 类：把「配置可见」改成「行为可证」）算不算改需求？
2. **「按推荐」还算不算已决**：Q24–Q31 已在 2026-10-19「全部按推荐裁决」，但 D4（相对下界）这类**改动 SRS 语义**的条目，是否要在路线图里**重新确认一次**？
3. **收口顺序**：二十来条偏离里，哪些必须在 P1 施工**前**收口、哪些可拖到 P2、哪些只留登记不收。
4. **收口产物是几件**：一份 `spec.md`，还是「SRS 修订页 + gdd 补遗 + HLD 修订页」三件套？
5. **schema 版本号与 DDL 形状**：上面第 1、2 项的具体口径（写到哪一行、谁能改、恢复语义）。

## 约束

- HITL。**不要**自己替 SRS 改字——本票产出的是**口径**，收口动作另行施工。
- 上位文件「一个字都不改」的政策**可以被推翻**，但要你明说。
- 涉及 Paimon/Flink 官方行为的部分，拿不到一手证据就写「待 [08] 确认」，不要自己发明。

## Answer

<!-- 待解。解析步骤：Skill "grilling" + "domain-modeling" → Status: resolved → 往 map.md 的 Decisions so far 追加一行 -->
