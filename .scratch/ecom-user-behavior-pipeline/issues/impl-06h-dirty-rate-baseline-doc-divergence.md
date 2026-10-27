# impl-06h: 脏率基线「来源与写者」文档-实现偏差收口

Type: task
Status: resolved（2026-10-27 收口：用户裁决 A「实现为准，改文档」；10 处落笔——面板侧脚本 6 处 + map D11 ④ + spec §6.4（新发现）+ impl-04 与 06 两张已结案票指顶备注；零代码改动，lint/typecheck/superset 交付测试全绿）
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

- [x] 用户裁决 A/B 写进本票 Answer（意图类改写需用户明示，不接受 agent 代裁）
      ★ 2026-10-27 13:07 用户在本会话裁决问句中明选 **A：实现为准，改文档**（推荐项）——map.md D11 ④ 的意图类改写以 Answer「裁决记录」节为签字
- [x] 按裁决完成对应侧收口：A＝面板侧脚本三处 + 决策记录修订
      ★ 实落 **10 处**：票面预列 6 处（sync 原 :7/:16/:33 + provision 原 :104/:114）＋ map.md D11 ④ ＋ spec.md §6.4（**施工中新发现**，票面冲突表未列）＋ sync 原 :58 Δ<0 提示（**连带**）＋ impl-04 与 06-dashboard-delivery 两张已结案票指顶备注（后者为嫁接**源头**，**施工中新发现**）；明细见 Answer 落笔清单
- [x] 面板侧 grep「输入冻结」「写者只有 verify-fault」按裁决后口径清零或改准
      ★ `docker/scripts/` 零残留（唯一命中 provision :105 为「原口径已改写」引述句）；`.scratch` 剩余命中逐条归置见 Answer「面板侧 grep 归置」（流程步骤正确用法 / 历史取证存档 / 引述句三类）
- [x] `make lint` / `make typecheck` 不回归（A 路零代码改动，跑通即收口）
      ★ 2026-10-27 WSL 实跑：lint PASS（All checks passed + 128 files already formatted）；typecheck PASS（errors=0 warnings=6 files=15）；另跑 `tests/test_superset_delivery.py` **6 passed**（改动的两个 .sh 在其锁定清单内，均不涉基线文案断言）

## Notes

- GLOSSARY「脏率基线」词条已按实现口径落笔（impl-06f-3，2026-10-26）；**不随本票预翻**
  ——裁决 B 才需修订词条。
- 面板**行为**不受影响：provision 在基线缺席时跳过不阻塞，基线总会被任何一次
  `make verify` 立起——偏差的危害是误导性注释与失败提示（文档层），不是行为缺陷。

## Answer（2026-10-27 收口）

**一句话**：用户裁决 **A（实现为准，改文档）**——「基线＝verify-fault 输入冻结快照」「写者只有
verify-fault」「先跑 verify-fault 立基线」三族口径按代码实况（`make verify` 棘轮：首跑判据
SKIP 落盘、PASS 前推、`--corrupt` 收尾吸收；普通 `verify-fault` 零写）全部改准；零代码改动，
lint / typecheck / superset 交付测试全绿。

### 裁决记录（签字）

2026-10-27 13:07（Asia/Shanghai）用户在本会话裁决问句中明选 **A：实现为准，改文档**（推荐项）。

- 本节即 map.md D11 ④ 意图类改写的**签字**（[03] Q7 裁决语义按「事实 / 意图」分档由本票裁决改写）。
- B（改实现）不采：impl-02e 收口的棘轮 / 吸收 / 防洗白语义与 `tests/test_verify_gaps.py`
  守卫维持原状；GLOSSARY「脏率基线」词条（impl-06f-3 落笔，本就是实现口径）按票面 Notes
  「不随本票预翻」的预设**零改动**。

### 落笔清单（10 处＝票面预列 6 + 连带 1 + 施工中新发现 3）

| # | 位置 | 改动 | 归类 |
|---|---|---|---|
| 1 | `docker/scripts/superset-dirty-rate-sync.sh`（原 :7-8 注释） | 基线来源：「= verify-fault 输入冻结时的 DLQ 快照，票面 ⑤ 逐字」→「`make verify` 维护的棘轮上界：首跑判据 SKIP 落盘、PASS 前推、`--corrupt` 收尾吸收（impl-06h 裁决 A 按实现改写）」 | 票面冲突 2 |
| 2 | 同脚本（原 :16 注释） | 「基线文件的写者只有 verify-fault（票面 ⑤）」→「写者是 make verify 判据（首跑 SKIP 写、PASS 前推）与 `--corrupt` 收尾的吸收函数；普通 verify-fault 不写基线」 | 票面冲突 1 |
| 3 | 同脚本（原 :33 失败提示） | 「先跑 make verify-fault（首跑落盘基线）」→「先跑 make verify（首跑判据 SKIP 时落盘基线）」 | 票面冲突 3 |
| 4 | 同脚本（原 :58 Δ<0 提示） | 「重跑 make verify-fault 立新基线」→「删基线文件后重跑 make verify 立新基线（首跑 SKIP 重立）」 | **连带**（票面未列，同类误导） |
| 5 | `docker/scripts/superset-provision.sh`（原 :104 step 注释） | 「面板的基线行从 verify-fault 快照来」→「从 dirty-rate-baseline.json 来——make verify 维护的棘轮上界」 | 连带 |
| 6 | 同脚本（原 :114 跳过提示） | 「先跑 make verify-fault 立基线」→「先跑 make verify 立基线（首跑判据 SKIP 时落盘）」 | 票面冲突 3 |
| 7 | `map.md` D11 ④ | 「基线＝verify-fault 输入冻结的 DLQ 快照（[03] Q7）」→ 棘轮口径，并指明 [03] Q7 的「kill 前 DLQ 快照」是流程内负断言基线且明写「无需历史基线文件」——原句属嫁接，经本票裁决 A 改写 | 票面 A 路预列（裁决即签字） |
| 8 | `spec.md` §6.4 | 「基线＝verify-fault 输入冻结快照（kill 前 DLQ 计数）」→ 棘轮口径 + 改写出处注 | **施工中新发现**（票面冲突表未列；spec 是施工蓝图，不改会继续复制偏差） |
| 9 | [impl-04](impl-04-dashboard-delivery.md) 票面 ⑤ | 指顶备注（06f-2 惯例：blockquote ★、原文保留、指向本票） | 票面 A 路预列 |
| 10 | [06-dashboard-delivery.md](06-dashboard-delivery.md) Q4 | 同上指顶备注 | **施工中新发现**——嫁接**源头**（[03] Q7 本身写「无需历史基线文件」，是 06 票面把 kill 前快照嫁接成面板基线来源；票面冲突表未列） |

### 复核证据（2026-10-27，WSL 实跑）

```
$ make lint                 # ruff check + format --check
All checks passed! + 128 files already formatted  ⇒ lint: PASS
$ make typecheck            # basedpyright
errors=0 warnings=6 files=15                     ⇒ typecheck: PASS
$ .venv/bin/python -m pytest tests/test_superset_delivery.py -q
6 passed in 7.48s           # 改动的两个 .sh 在其锁定清单内（MSYS_NO_PATHCONV / -u / 顺序，均不涉基线文案）
```

### 面板侧 grep 归置（验收第 3 条）

| 范围 | grep「输入冻结｜写者只有 verify-fault｜verify-fault 立基线｜先跑 make verify-fault」 | 归置 |
|---|---|---|
| `docker/scripts/`（面板侧本体） | 唯一命中 provision :105——「原口径经 impl-06h 裁决 A 改写」引述句 | 改准 ✓ |
| `.scratch` 设计链 | map / spec 已改写；impl-04、06 两票原文保留＋★ 指顶备注；impl-06f-3 与本票命中为**取证历史**（偏差登记本身）；gdd / spec §4 / 03 / impl-02 / impl-02d 的「输入冻结」全是 verify-fault 流程步骤**正确用法**（等外部输入冻结 → lag==0 → …） | 改准 ✓ |
| `src/`（代码） | `fault.py` / `verify_runner.py` 的「输入冻结」为流程注释正确用法 | 不动 ✓ |

`save_dirty_rate_baseline` 全仓三处调用（`verify_runner.py:749/774/793`）零改动；GLOSSARY「脏率基线」
词条零改动（A 路径预设不动）；HLD / gdd / README 经 grep 复核无同类口径（HLD 命中全为 JDK / 内存 /
观测性基线等无关词；gdd「快照基线」是 verify-fault 流程内用法）。

### 边界（刻意不动）

- [03] Q7 原文（`03-semantics-and-assertions.md` Q7 行）：写的是「负断言基线＝kill 前 DLQ 快照……
  **无需历史基线文件**」——本身无偏差，保留原文。
- impl-06f-3 Answer 的偏差登记表：历史取证，保留原文。
- impl-dag 的 06h 行与 Frontier 已同步结案注（2026-10-27）。
