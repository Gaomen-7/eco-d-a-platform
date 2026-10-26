# impl-06h: 脏率基线「来源与写者」文档-实现偏差收口

Type: task
Status: ready-for-human（核心是用户裁决 A/B；裁决后的机械落笔可转 agent）
Blocked by: None（可立即裁决）

**What to build:** 面板侧文档三处声称脏率基线「＝ verify-fault 输入冻结时的 DLQ 快照」
「写者只有 verify-fault」「不存在就先跑 make verify-fault 立基线」；代码实况是
**普通 `make verify-fault` 从不写基线文件**。需用户裁决以哪侧为准，再机械收口。

**出处:** [impl-06f-3](impl-06f-3-glossary-three-terms.md) 施工中按「以代码为准」复核
面板口径时发现（该票 Notes 预设的「实现与 panel 口径不一致 → 登记新票」情形，2026-10-26）。

## 冲突明细（2026-10-26 查证）

| # | 文档陈述 | 位置 | 代码实况 |
| --- | --- | --- | --- |
| 1 | 「基线文件的写者只有 verify-fault（票面 ⑤）」 | `docker/scripts/superset-dirty-rate-sync.sh:16` | `save_dirty_rate_baseline` 全仓仅三处调用（`src/ecomgen/verify_runner.py:749/774/793`）：`absorb_fixture_dirty`（`--corrupt` 收尾）、`check_dlq_rate_not_increased` 首跑 SKIP 写 + PASS 前推——全部在 `make verify` / `verify-fault --corrupt` 路径；**普通 verify-fault 零调用** |
| 2 | 「基线 = verify-fault 输入冻结时的 DLQ 快照」 | 同脚本 :7；[impl-04](impl-04-dashboard-delivery.md) 票面 ⑤；[map.md](../map.md) D11 ④（[03] Q7 裁决语义） | 基线实况＝「最近一次 verify PASS 时的当前采样」或「corrupt 吸收后的记账值」，随运行**前推漂移**；「verify-fault 输入冻结快照」只在首跑立基线那一刻与其重合 |
| 3 | 失败提示「先跑 make verify-fault（首跑落盘基线）」 | 同脚本 :33；`docker/scripts/superset-provision.sh:114` | 首跑落盘的是 `make verify`（判据 SKIP+写）；普通 verify-fault 落不了盘——提示把人引向一条**不会生效**的命令 |

## 裁决面（二选一）

- **A（推荐）：实现为准，改文档**——实现侧语义经 impl-02e 收口验收并有守卫测试固化
  （棘轮：只在 PASS 前推；吸收：观测增量 ≤ 注入×3 可归因；防洗白：不得重立到当前值）。
  机械部分：改 sync 脚本 :7/:16/:33 与 provision :114 的口径（含「先跑 make verify 立基线」）；
  map.md D11 ④ 属已裁**意图**（[03] Q7），按 GLOSSARY「事实 / 意图」分档改写需用户签字——
  本票裁决即签字；impl-04 票面为已结案历史，按 06f-2 惯例只加指顶行的备注、不改 Answer。
- **B：决策为准，改实现**——让基线真正由 verify-fault 输入冻结时立、只由 verify-fault 写。
  代价：推翻 impl-02e 收口的棘轮语义，牵动 `tests/test_verify_gaps.py` 守卫与 GLOSSARY
  新词条「脏率基线」（词条须同步修订）。

## Acceptance criteria

- [ ] 用户裁决 A/B 写进本票 Answer（意图类改写需用户明示，不接受 agent 代裁）
- [ ] 按裁决完成对应侧收口：A＝面板侧脚本三处 + 决策记录修订；B＝实现 + 守卫测试 + GLOSSARY 词条
- [ ] 面板侧 grep「输入冻结」「写者只有 verify-fault」按裁决后口径清零或改准
- [ ] `make lint` / `make typecheck` 不回归（A 路零代码改动，跑通即收口）

## Notes

- GLOSSARY「脏率基线」词条已按实现口径落笔（impl-06f-3，2026-10-26）；**不随本票预翻**
  ——裁决 B 才需修订词条。
- 面板**行为**不受影响：provision 在基线缺席时跳过不阻塞，基线总会被任何一次
  `make verify` 立起——偏差的危害是误导性注释与失败提示（文档层），不是行为缺陷。
