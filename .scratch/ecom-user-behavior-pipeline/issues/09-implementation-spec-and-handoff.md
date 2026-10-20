# 09 · 施工规格与交接：19 条已决怎么变成下一会话能直接照做的 spec.md

Type: grilling
Status: claimed
Blocked by: 01, 03, 04, 05, 06, 07, 08
Blocked by (note): 这是全图的**汇聚节点**——它要把其余八张票的答案压成一份可施工的规格，所以它被所有票阻塞。
Created: 2026-10-20（新增；原「目的地＝spec.md」在 D4 收窄为「spec-ready 路线图」后，落地动作需要一张票）
Ticket: 09
Source: map.md · Destination / Notes · `docs/agents/issue-tracker.md` · 三份 `.scratch/handoff/`

## Question

地图的 Destination 是「解到 frontier 为空时**唯一**还站着的规划产物」。现在还没人回答：**那个产物长什么样、放在哪、怎么被下一个会话消费。**

1. **形态**：单份 `spec.md`？还是「`spec.md` + 三份交接归档 + 一张票索引」？`SRS.md` 的文件名约定是「不使用 `spec.md`」（SRS 头注），所以 `spec.md` 这个名字会不会与它冲突。
2. **内容边界**：只写「已决的施工口径」，还是连「怎么验证每一步」一起写（即把 [03] 的断言、[05] 的形状、[06] 的判据表都嵌进去）？后者会让 spec 变厚，但省掉命名来回翻票。
3. **票与 spec 的关系**：解完的票保留原文（作为决策档案）还是折叠成 spec 的一节？本地 markdown 后端里「一张票一个文件」的约定要不要给已解票加 `Status: folded` 之类的新状态。
4. **交接仪式**：本仓已有 `handoff/` 惯例（三份 P0 交接）。spec 完成后要不要照同样格式产一份 `handoff/YYYYMMDD-...-spec-ready.md`，指向 spec 与若干「下一步可开工」的入口。
5. **入场判据**：其余票全部 resolved 才能开这张；如果有票被判定 out of scope，spec 里怎么写「此处不做、理由是什么」。
6. **本票自己算不算在 frontier 里**：它是汇聚节点，不是决策节点——若它永远是该验算的最后一张，那就应当明确「它 resolved ⇔ 地图完成」。

## 约束

- HITL。**不在这里重开任何已决问题**；若发现某张票的答案互相矛盾，写进「冲突待回票」，不要就地改。
- 不推送、不改 `SRS.md`（收口语义归 [01]，落地动作归这里）。

## Answer

<!-- 待解。解析步骤：Skill "grilling" → Status: resolved → 地图完成，往 map.md 追加最后一条 Decision，并把 frontier 标为「空」 -->
