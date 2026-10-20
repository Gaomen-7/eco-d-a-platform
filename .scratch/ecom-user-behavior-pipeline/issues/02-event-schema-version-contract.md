# 02 · 事件 schema 的版本化铁律与演进演练做到什么程度

Type: grilling
Status: open
Blocked by: 03, 04
Created: 2026-10-20
Ticket: 02
Source: map.md → Not yet specified F1 / gdd F5 / gdd D5 / SRS §3.3

## Question

现在全仓对 `schema_version` 只有一个字面值（`1.0`），**没有一个机制**：谁改、怎么改、旧数据怎么办、作业从 checkpoint 恢复时版本不一致会怎样。而 `gdd F5` 已经把「schema 变更导致恢复失败」写成了登记在册的失败模式，只留了「预留 `ext_field` 列；或从 ODS 重放」两条逃生口，**验收判据是「schema 演进演练可跑通」——没人是它的主语**。

要点：

1. **谁有权改**：`schema_version` 的递增规则（加字段算不算 minor？删字段呢？）；生成器、DWD 清洗、DLQ 校验三处对同一版本号的理解是否必须一致。
2. **兼容的边界**：加可选字段（后向兼容）与改语义（如 `event_time` 秒→毫秒）分别走哪条路；Paimon 主键表加列对历史分区的影响（官方行为需引原文）。
3. **恢复语境的约束**：从 checkpoint 恢复时，Flink 的 state schema 与事件 schema 是两件事——这张票只需要你裁决**两者对齐的口径**（例如：状态恢复时读到的旧字段一律按「缺省值 + 标记」处理，还是禁止跨版本恢复）。
4. **演练的最小形态**：`REQ-F-VERIFY-04`/`gdd F5` 的「演练」到底做一次什么动作、留下什么痕迹（拒绝表计数？一个 `$schemas` 断言？一次成功恢复的日志行？）。

## 约束

- HITL：需要你裁决，不接受「按推荐」而无你回复的答案（本仓最大坑，HLD §0.3）。
- 不重开事件字段表本身（`SRS §3.3` 冻结）；也不扩五类枚举（`gdd D5` 已否决扩枚举）。
- 涉及 Paimon/Flink 官方行为的部分，若一时拿不到一手证据，**写成「待 01 确认」**，不要自己发明。

## Answer

<!-- 待解。解析步骤：先 Skill "grilling" + "domain-modeling"，逐条问用户，落裁决 → Status: resolved → 往 map.md 追加 -->
