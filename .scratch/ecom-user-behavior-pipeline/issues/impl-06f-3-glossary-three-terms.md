# impl-06f-3: GLOSSARY 三词补登记 — 脏率 · 反例夹具 · 脏率基线

Type: task
Status: resolved（2026-10-26 收口：三词条按代码口径落地，判据全绿；施工中发现面板侧基线口径偏差 → 新票 impl-06h）
Blocked by: None（可立即开工）

**What to build:** 把三个**已在代码与票面里稳定使用、却没进术语表**的词收进
根目录 `GLOSSARY.md`，让后来者不必再从实现里反推语义。

**Blocked by:** None（可立即开工）

## 欠账出处

[impl-02e](../issues/impl-02e-p1-acceptance-closeout.md) §遗留提醒 记着：
code-review S4 转出的 domain-modeling 欠账——「脏率」「反例夹具」「脏率基线」
三个词未进 `GLOSSARY.md`，建议在 impl-03 开工前补一条，「避免下游票各自造词」。
impl-03 已结案而此账仍未销。

## 三词现在的实现口径（写词条时**以代码为准**，不得臆造）

| 词 | 现状口径（可复核） | 复核位置 |
| --- | --- | --- |
| **脏率** | 分子**只含真脏**（`dlq_offset / input_events`），**重复不进分子**；重复另走「重复吸收」通道计数 | `src/ecomgen/verify_runner.py` 的脏率采样与比较函数；`GLOSSARY.md` 现有「重复吸收」词条 |
| **脏率基线** | 落盘快照（`dirty-rate-baseline.json`），首次运行 SKIP 并写基线，此后每次**真比**；**只在判定通过时前推** | 同文件的基线与比较函数 |
| **反例夹具** | `verify-fault --corrupt` 这类**故意制造脏数据**的注入器；它是**唯一**允许故意造脏的入口，其脏量必须被收尾处的吸收函数吃掉，否则会把基线顶死 | `src/ecomgen/fault.py` 的 `absorb_fixture_dirty` 调用点；impl-02e 已登记「新注入路径必须同样调用」的前提 |

## Acceptance criteria

- [x] `GLOSSARY.md` 新增三条词条，术语名逐字为「脏率」「反例夹具」「脏率基线」
      ★ 已落地：「脏率」→ §事件处置（紧跟「重复通道」之后）；「反例夹具」「脏率基线」→ §读者与验证（「烟测」之后）
- [x] 每条词条的定义与上表口径一致，且**不引入代码里没有的说法**
      ★ 逐句溯源见 Answer 溯源表（`DirtyRateSample` / `check_dlq_rate_not_increased` / `absorb_fixture_dirty` / `_inject_dirty` / impl-02e 遗留提醒）；唯一叙事性表述「面板口径争议的根」逐字取自本票验收第 3 条原文
- [x] 「脏率」词条必须显式写明「**重复不计入分子**」这条易错点（这是当初造成面板口径争议的根）
      ★ 已写：「**重复不计入分子**——重复是合法重放，走**重复通道**幂等吸收、单独计数，不产生 DLQ 行；这条易错点是当初面板口径争议的根」
- [x] 「反例夹具」词条必须写明它与「脏率」的耦合风险（夹具脏若不吸收会顶死基线）
      ★ 已写：「⚠️ 与『脏率』的耦合风险：夹具脏会实打实进 DLQ，必须在收尾处被吸收函数记账进**脏率基线**，否则基线被顶得只升不降（顶死）」；并按 impl-02e 登记的前提写明「新增故意造脏路径必须同样调用吸收（守卫测试锁着现有调用点）」
- [x] 词条风格与本文件既有条目一致（不新增分类标题、不重排既有内容）
      ★ 零新增分类标题、零重排；`git diff --stat`＝23 insertions / 1 deletion——唯一「删除」行是 EOF 换行伪影（原文件无尾换行符，追加必须给末行补换行），既有词条一字未动
- [x] `make lint` / `make typecheck` 不受影响（本票不改代码；若有文档测试引用 GLOSSARY，须仍通过）
      ★ 2026-10-26 WSL 实跑：`pytest tests/test_lake_assert.py` **43 passed**（含引用 GLOSSARY.md 的 `test_new_files_use_lf_line_endings`）；`make lint` **PASS**（124 files already formatted）；`make typecheck` **PASS**（errors=0 warnings=6 files=14）

## Notes

- **只增不改**：不重写 GLOSSARY 既有词条，不动「重复吸收」那条。
- 若发现代码口径与词条应写的口径**冲突**（例如实现与 panel 口径不一致），
  **不要在词条里调和**：如实写实现口径，并登记为新票。

## Answer（2026-10-26 收口）

**一句话**：三词条按代码口径落地（零代码改动、只增不改），三项判据全绿；
impl-02e code-review S4 转出的 GLOSSARY 欠账就此销账。施工中按 Notes 复核面板口径时
发现「脏率基线」的文档-实现偏差，按票面要求**未在词条里调和**，登记新票
[impl-06h](impl-06h-dirty-rate-baseline-doc-divergence.md)。

### 落点与逐句溯源（口径出处均为代码，非臆造）

| 词条 | 落点 | 关键口径的代码出处 |
|---|---|---|
| 脏率 | §事件处置（「重复通道」后） | `DirtyRateSample`（分子＝dlq_offset / 分母＝input_events）、`check_dlq_rate_not_increased_result`（「重复通道不进分子」docstring 逐字）、`superset-dirty-rate-sync.sh`（面板分子分母同口径） |
| 反例夹具 | §读者与验证（「烟测」后） | `_inject_dirty`（缺 `event_type` ⇒ schema_invalid、走主流同一条路径）、impl-02e 遗留提醒（「`--corrupt` 是唯一会故意制造脏数据的入口」）、`tests/test_verify_gaps.py:270`（守卫测试锁 `absorb_fixture_dirty` 调用点） |
| 脏率基线 | §读者与验证（「反例夹具」后） | `check_dlq_rate_not_increased`（首跑 SKIP 且写基线 / 只在 PASS 时前推）、`absorb_fixture_dirty`（吸收观测增量、防洗白——「基线重立到当前值＝洗白」取自其 docstring）、`save_dirty_rate_baseline` 全仓仅三处调用 |

### 复核证据（2026-10-26，WSL 实跑）

```
$ .venv/bin/python -m pytest tests/test_lake_assert.py -q
43 passed in 21.81s        # 含 test_new_files_use_lf_line_endings（GLOSSARY.md 在其清单内）
$ make lint                 # ruff check + format --check
lint: PASS（124 files already formatted）
$ make typecheck            # basedpyright
errors=0 warnings=6 files=14  ⇒ typecheck: PASS
```

`git diff --stat GLOSSARY.md`＝23 insertions / 1 deletion；那 1 行「删除」是 EOF 换行伪影
（原文件无尾换行符，追加必须补），既有词条与四个分类标题零改动。

### Notes 触发：文档-实现偏差 → 新票 impl-06h

按「以代码为准」复核面板口径时，发现**普通 `make verify-fault` 从不写基线文件**
（`save_dirty_rate_baseline` 的调用点全在 `make verify` 判据与 `--corrupt` 收尾吸收），
而面板侧三处文档相反声称：

| # | 文档陈述 | 位置 |
|---|---|---|
| 1 | 「基线文件的写者只有 verify-fault（票面 ⑤）」 | `docker/scripts/superset-dirty-rate-sync.sh:16` |
| 2 | 「基线 = verify-fault 输入冻结时的 DLQ 快照」 | 同脚本 :7；impl-04 票面 ⑤；map.md D11 ④（[03] Q7 裁决语义） |
| 3 | 失败提示「先跑 make verify-fault（首跑落盘基线）」 | 同脚本 :33；`docker/scripts/superset-provision.sh:114` |

处置：词条如实写实现口径（「基线的写者只有 verify 判据与反例夹具的吸收函数」），
不在词条里调和；登记 [impl-06h](impl-06h-dirty-rate-baseline-doc-divergence.md)
（`ready-for-human`：D11 ④ 属已裁意图，改文档需用户签字；改实现则推翻 impl-02e 收口的
棘轮语义）。本票词条不随该裁决预翻——若裁决为改实现，词条须另开修订。
