# 05 · 漏斗与 UV 的窗口语义：两条正交时间线怎么在同一个 Doris 表里活着

Type: grilling
Status: open
Blocked by: 04, 08
Blocked by (note): `04` 指地图票 D4「配置确定性与可复现」（已落子）；`08` 是真实票号 [08 离线分层的门槛](08-offline-layer-gating.md)——实时先钉边界，离线再定门槛。上一版注释误把 `08` 当成 D4 的别名，已修正。
Created: 2026-10-20
Ticket: 05
Source: map.md → GLOSSARY「转化漏斗（步进式）」「滚动 UV」/ gdd §1.3-5/6 / gdd §1.5

## Question

`GLOSSARY.md` 立了一条硬区分：**漏斗的 1 分钟窗口只决定「每分钟统计多少条步进」**，漏斗本身按 `user_id` 的步进推进，与窗口正交；滚动 UV 则是「最近 1 小时内的去重用户数」。于是同一个 `ecom_ads` 库里至少有三个时间轴同时在跑：

| 量 | 时间轴 | 状态规模 | 谁在算 |
| --- | --- | --- | --- |
| 每分钟 PV / GMV / 品类·地域分布 | 1 分钟滚动窗口 | 窗口态 | Flink SQL 窗口聚合 |
| 滚动 UV（最近 1h） | 1 小时滚动 | 键控去重 ~MB 级 | Flink SQL / DataStream |
| 四步漏斗 | 会话（30 min 空闲切断）+ 窗口只做客数 | 键控状态 TTL 32 min | DataStream（`autofill` + 双列并陈） |

关键未决：

1. **多行语义**：`revision` 递增的覆盖写发生在谁身上？只有窗口聚合行，还是漏斗行也可能被迟到事件修正（如果一条 `purchase` 迟到 90s，它算不算「修正」了某分钟的漏斗客数）？
2. **提前发射**（gdd §1.3-4，`【已确认 Q13】`）：每 10s 发射的「暂定值」与 1 分钟窗口的落定值，在 Doris 里是**同一张表的同一行**（靠 `revision`/状态列区分），还是**两张表**？两张表的代价是唯一键不同，D1 的「一层主防」前提要不要重证。
3. **UV 与漏斗的对账**：`gdd §1.4` 要「实时滚动 UV 与离线精确 UV **逐窗口相等**」——但离线是 Paimon 批，窗口切分口径必须与实时完全一致才算得出相等；这个口径写在哪里、谁维护。
4. **混沌/跳步**：`--chaos-ratio` 触发的补记会让 `funnel_cnt_inferred` 与观测值分离，而这两列的 monotonic 约束**不同**（观测可非单调、推断必须单调）。看板同时展示两列时，唯一键还包不包 `revision`。

## 约束

- HITL。可以直接起一个 **AFK 原型**（单文件 HTML，照 `prototypes/funnel.prototype.html` 的方式跑断言）来把「1h UV × 1min 窗口 × 30min 会话」叠加起来看——但原型是手段，裁决仍是你。
- 不改 `GLOSSARY.md` 已定义的概念（漏斗、UV、落定/暂定、会话、补记）；有冲突就指出并在 Answer 里提议修订词条。

## Answer

<!-- 待解。解析步骤：Skill "prototype"（可先）→ Skill "grilling" + "domain-modeling" → Status: resolved → 往 map.md 追加 -->