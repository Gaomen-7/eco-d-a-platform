# 03 · REQ 冲突的裁决口径：SRS、GDD、HLD 三层谁该让路，以及 D1–D15 的收口顺序

Type: grilling
Status: open
Blocked by: 04
Created: 2026-10-20
Ticket: 03
Source: map.md → Decisions D4（“不为文档说错了开票”政策的例外裁决权）/ gdd §D / HLD §7 / FSR R1 / SRS §6

## Question

本仓的权威链是**单向**的（SRS → gdd → HLD），**没有反向收口通道**：SRS 与 FSR「一个字都不改」，gdd 的 D1–D15 与 HLD 的 D-A…D-G 都只是「登记」，等一个**永远不会自己发生**的 `spec.md` 会话。后果是同一个数在三个文件里有三个值：

| 冲突 | 上位文件说 | 下位文件已证 | 谁在替它说话 |
| --- | --- | --- | --- |
| **A** | `REQ-F-LAKE-08`：快照数**受控为 5** | gdd D7：5 是**下限**；HLD §8 引用该证伪 | **没人**（map-dag 缺口 #1） |
| **B** | `REQ-F-RT-03`：UV 用**近似算法** + 误差量化 | gdd D9：前提不成立 → 改**精确** + 一致性校验 | gdd |
| **C** | `REQ-F-RT-04`：迟到走**侧输出** | gdd D11：Flink SQL 无此能力 → 强制混合路线 | gdd（且 gdd D11/D12 本身待证，见 [02] 型 research 票） |
| **D** | `REQ-F-GEN-07`：`--late-ratio` 默认 5% | gdd §1.1：拆成 `--late-correctable 3%` + `--late-expired 2%` | gdd |
| **E** | `REQ-F-OLAP-03`：30 天保留 | gdd D8 + §7.2 G-D8：还须 FE 级 `dynamic_partition_enable` | HLD（已落地到 init.sql） |
| **F** | `REQ-NF-REL-03` + `REQ-F-OLAP-02`：两道独立防线 | gdd D1：一层主防 + 一层兜底 | gdd |
| **G** | `REQ-F-VERIFY-04`：重复算脏 | gdd D2：重复不是脏 | gdd |
| **H** | `REQ-F-LAKE-09` 与规则①：公开数据集 100% 被判脏 | gdd D4：规则①改**相对下界** | gdd（`【按推荐】`，未落死） |
| **I** | `REQ-NF-CAP-01` / FSR §2：≤8 GB | HLD §4.2 实测：声明上限 10.5 GiB（全起） | HLD（披露为**口径差**） |
| **J** | `REQ-F-MQ-03`：7 天或 2 GB 取小 | gdd D3：实际保留窗 ≈6h | gdd |
| **K** | `REQ-F-VERIFY-01`：五项烟测 | HLD §3.3-6：`lint`/`typecheck` 另设目标 | HLD（设计面，不漂移） |
| **L** | `REQ-F-ORCH-05`：named volume | gdd F16 / HLD §8-R7-e：挂载 ≠ 数据落进去，须显式 `KAFKA_LOG_DIRS` | 已施工 |

这张票要你裁决**元规则**，不是逐条裁决内容：

1. **谁有限制权**：物理/官方 API 限制（C、E、L 类）可以**直接覆盖** SRS 的 MUST，还是必须走冻结项变更？「实现判据升级」（B、G 类：把「配置可见」改成「行为可证」）算不算改需求？
2. **「按推荐」还算不算已决**：gdd 的 Q24–Q31 已在 2026-10-19「全部按推荐裁决」，但 D4（相对下界）这类**改动 SRS 语义**的条目，是否需要在路线图里被**重新确认一次**？还是视为已决、只做施工？
3. **收口顺序**：D1–D15 + HLD D-A…D-G 这 20 来条里，哪些必须在 P1 施工**前**收口、哪些可以拖到 P2、哪些根本不收（只留登记）？
4. **`spec.md` 是单文件还是多文件**：目的地说「spec-ready」——收口产物是一份 `spec.md`，还是「SRS 修订页 + gdd 补遗 + HLD 修订页」三件套？
5. **issue 后端**：`.scratch/<feature>/` 既放设计链又放票，票的 `Status:` 行与 triage 五角色并存——是否需要在票文件里禁用 triage 标签（`wayfinder:` 系列是唯一标签）？

## 约束

- HITL。**不要**自己替 SRS 改字——本票的产出是**口径**，收口动作另行施工。
- 上位文件「一个字都不改」的既有政策**可以被这张票推翻**——但推翻要你明说。

## Answer

<!-- 待解。解析步骤：Skill "grilling" + "domain-modeling" → 逐条裁决 → Status: resolved → 往 map.md 追加 -->