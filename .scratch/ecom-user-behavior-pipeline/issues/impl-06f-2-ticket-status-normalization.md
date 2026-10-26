# impl-06f-2: 票面状态归正 — 顶行 Status 唯一，过期接手清单退场

Type: task
Status: resolved（2026-10-26 收口：预核实三处 + 施工中新发现第 4 处镜像矛盾全部归正；剩余 39 个 `- [ ]` 逐条保留理由见 Answer）
Blocked by: None（可立即开工）

**What to build:** 让每个票面文件的**状态陈述只有一个来源**。现在有票面顶部写着
`resolved`，正文中段却还留着 `**Status:** ready-for-agent`；也有票面的「接手清单」
停在票据已结案的当时，7 个未勾选复选框会让下一个接手者以为活还没干。本票把这些
矛盾清掉，**不重写任何历史取证内容**。

**Blocked by:** None（可立即开工）

## 已核实的三处矛盾（2026-10-26 查证）

| # | 位置 | 现状 | 问题 |
| --- | --- | --- | --- |
| 1 | [impl-02](../issues/impl-02-p1-realtime-closure.md) 正文中段 | 顶行 `Status: resolved（2026-10-22…）`，中段仍有 `**Status:** ready-for-agent` | 全仓 grep `^Status:` 与正文 `**Status:**` 会读出两个相反的答案 |
| 2 | [impl-03](../issues/impl-03-p2-offline-layering.md) 正文中段 | 同上：顶行 resolved，中段 `**Status:** ready-for-agent` | 母票已由 03f 收口，中段那行是拆票前的原票残留 |
| 3 | [impl-03b](../issues/impl-03b-lake-ddl-and-ods-dwd-replay.md) §P5 / §E5 | 两段「接手清单」共 7 个 `- [ ]`（`replay_20_dwd.sql`、`LAYER=` 开关、拒绝旁路、`check-ignore` 复验、生成器单测、票面第 8 条改写、收口） | 这 7 项**已由后续施工全部兑现**，但复选框没勾、也没标注「已被哪一节兑现」 |
| 4 | [impl-04](../issues/impl-04-dashboard-delivery.md) 顶行（**施工中新增发现**） | 顶行 `Status: in-progress`，正文 `**Status:** done（2026-10-25 收口…）`、Progress「2026-10-25 下午会话，收口」、impl-dag「✅ 已结清（2026-10-25）」 | 与 #1/#2 相反方向的同一病：顶行是收口时忘改的残留。非重判状态——结案证据三处在册，故归本票而非 impl-03g |

## Acceptance criteria

- [x] impl-02 中段那行 `**Status:** ready-for-agent` 改为指向顶行的收口状态（例如「已由 [impl-02e](impl-02e-p1-acceptance-closeout.md) 收口，见顶行」），不留相反陈述
      ★ 已改：「已由 impl-02e 收口（2026-10-22，票面 10 条验收全勾）；状态陈述以文件顶行为准」
- [x] impl-03 中段同处理：改为「已由 [impl-03f](impl-03f-paimon-doris-reconciliation-and-verify.md) 收口，见顶行」
      ★ 已改：「已由 impl-03f 收口（2026-10-24 22:00，六张副票全结、10 条验收全勾）；状态陈述以文件顶行为准」
- [x] impl-03b 的 §P5 / §E5 两段清单：每项标注**已在何处兑现**（章节号或下游票），或在段首统一加一句「以下清单已于 <日期> 全部兑现，勾选状态见 §收口」并逐项指向证据
      ★ 两法并用：两段段首各加归正声明（注明 2026-10-26、impl-06f-2），11 个复选框（§P5 4 个 + §E5 7 个）逐项回勾并标注兑现章节（§A·1/§A·4/§A·5/§A·6/§A·7/§A·12/§P0-2/§P0-3/§P0-4/§P0-6/§P0-7 及下游票 03c/03f）
- [x] 全仓 `.scratch/ecom-user-behavior-pipeline/issues/` 内，凡文件顶部 `Status:` 为 resolved/done 者，正文**不得**再出现声称其它状态的 Status 行
      ★ 施工后 grep `\*\*Status:\*\*`：impl-01/02/03/04/05 五处正文状态行全部与顶行同向（01/05 原本一致未动；02/03/04 为本次归正）；其余 33 份票面无正文状态行
- [x] 全仓剩余 `- [ ]` 逐条给出保留理由（真未做），写进本票 Answer 的清单里
      ★ 39 个，逐条见 Answer「剩余 39 个 `- [ ]` 保留理由」表（impl-03g 6 / impl-03h 6 / impl-06f 5 / impl-06f-3 6 / impl-06f-4 7 / impl-06g 8 / impl-04 1）
- [x] 全仓 `## Comments` 段逐份查看：凡内容已被 `## Answer` 覆盖者，在段首加一行
      「以下评论为当日讨论存档，结案结论见 §Answer」；**不删除**评论原文
      ★ **判据空转成立**：grep `^## Comments` 与关键词「Comments/评论」在全仓 issues/ 零命中（唯一命中是本票自己的验收文字）——各票讨论写在 Progress/Answer 区，无一票使用 Comments 段，无可操作对象，如实记空转
- [x] `issues/impl-dag.md` 与上述状态一致（该图的票面表不含矛盾项）
      ★ 06f-2 行加 ✅ 结案注（含第 4 处发现、11 框回勾、Comments 空转、39 框保留清单指针）；票面表逐行核对无矛盾项。DAG Frontier 里 impl-03「已结清」与 03e `ready-for-human` 的措辞张力归 impl-03h 验收第 4 条，本票不抢
- [x] **（施工中新增）** impl-04 顶行过期 `in-progress` 归正为 `resolved（2026-10-25 收口…）`，正文 `**Status:** done` 行同步改为指顶行
      ★ 结案证据三处在册（正文状态行 / Progress 收口实录 / DAG「✅ 已结清（2026-10-25）」），顶行是收口时忘改的残留——归本票而非 impl-03g（后者职责是「经复核改判状态」，此处无改判）

## Notes

- **只有两处允许"改字"**：状态行、清单段。**已结案的 `## Answer` 一律不改**。
- 若某张票的状态本就该改（例如复核后从 ready-for-human 变 resolved），
  **不在本票做**——那属 [impl-03g](impl-03g-falsification-signoff.md) 的职责。
- 判定「已兑现」必须有证据：指向具体章节、命令输出或下游票的验收行，
  不接受「应该是做了吧」。

## Answer（2026-10-26 收口）

**一句话**：5 处归正全部落地（预核实 3 处 + 施工中新发现第 4 处镜像矛盾 impl-04），
全部是「改状态行 / 清单段」——**已结案票的 `## Answer` 与历史取证正文零改动**；
施工后全仓 grep 零残留，剩余 39 个 `- [ ]` 逐条保留理由见下。

### 归正清单（5 处）

| # | 文件 | 改动 | 兑现依据 |
|---|---|---|---|
| 1 | [impl-02](impl-02-p1-realtime-closure.md) | 中段 `**Status:** ready-for-agent` → 「已由 impl-02e 收口…状态陈述以文件顶行为准」 | 顶行 resolved（2026-10-22 经 02e）；10 条验收全勾；02e Answer「票面 10 条验收逐条核对」 |
| 2 | [impl-03](impl-03-p2-offline-layering.md) | 中段 `**Status:** ready-for-agent` → 「已由 impl-03f 收口…状态陈述以文件顶行为准」 | 顶行 resolved（2026-10-24 22:00）；10 条验收全勾；03f §3.6 证据索引 |
| 3 | [impl-04](impl-04-dashboard-delivery.md)（**施工中新增发现**） | 顶行 `in-progress` → `resolved（2026-10-25 收口…）`；正文 `**Status:** done` 行同步改指顶行 | 结案证据三处在册：正文状态行 / Progress「2026-10-25 下午会话，收口」/ DAG「✅ 已结清（2026-10-25）」——顶行是收口时忘改的残留，非重判 |
| 4 | [impl-03b](impl-03b-lake-ddl-and-ods-dwd-replay.md) §P5/§E5 | 11 个 `- [ ]` 全部回勾 + 行内兑现证据 + 两段段首归正声明 | 逐项指向 §A·1 / §A·4 / §A·5 / §A·6 / §A·7 / §A·12 / §P0-2 / §P0-3 / §P0-4 / §P0-6 / §P0-7 及下游票 03c / 03f；F5 单测今日复跑 49 passed（见下） |
| 5 | [impl-dag.md](impl-dag.md) | 06f-2 行加 ✅ 结案注 | 本节 |

**impl-03b 清单的「已兑现」取证口径**：每条标注均指向票内既有实机证据章节或下游票验收行，
另对 F5（生成器单测）取了**当日新鲜证据**——

```
$ wsl -e bash -lc 'cd "/mnt/f/Anaconda practice project/eco-a-d-platform" && \
    .venv/bin/python -m pytest tests/test_threshold_batch.py -q'
49 passed in 9.54s        # 2026-10-26 本会话实跑
```

点名五类覆盖逐类 grep 核对仍在（恰好 N 条 / dt 恰 3 个 / 三通道非空 / 3d≡72h / 非整天报错
各有对应 test 函数）。P0-4 记「50 例全绿」为当时会话口径：该文件自 `89c40a6` 提交后
无任何改动（`git log -- tests/test_threshold_batch.py` 单一提交），今日 49 passed 即在册状态，
差 1 例属当时记录口径、非后续漂移。

### 归正边界（两处刻意不动）

- **impl-dag Frontier 里 impl-03「已结清（03a–03f 全勾）」与 impl-03e `ready-for-human`
  的措辞张力**：归 [impl-03h](impl-03h-mother-ticket-final-reconcile.md) 验收第 4 条
  （「不再出现与 03e 状态冲突的全结措辞」），本票不抢——本票判据限定「票面表」，
  票面表逐行核对无矛盾项。
- **impl-03e 的 `ready-for-human` → `resolved`**：人工签字，归 [impl-03g](impl-03g-falsification-signoff.md)。

### 全仓复核（施工后，2026-10-26）

| grep | 结果 |
|---|---|
| `^Status:`（顶行） | 38 处：角色词与正文陈述一致，无「resolved 顶行 + 正文 ready」残留 |
| `\*\*Status:\*\*`（正文状态行） | 仅 impl-01/02/03/04/05 五处，全部与顶行同向（01/05 原本一致未动，02/03/04 为本次归正） |
| `^\s*- \[ \]`（未勾复选框） | 39 处，全部在 7 张未结票 + 1 处有意留空（见下表） |
| `^## Comments` | **零命中**——判据空转成立：全仓无一票使用 Comments 段（讨论进 Progress/Answer），故「段首加存档声明」无可操作对象；关键词「Comments/评论」亦仅命中本票自身文字 |

### 剩余 39 个 `- [ ]` 保留理由（逐条）

> 判定基准：票顶 `Status:` 仍为 `ready-for-agent`（未开工/待签字）＋ DAG Frontier
> 「功能票全结，剩余 8 张」＋ 各票验收零勾选——三处互证「真未做」。

**impl-03g（6 条）—— 人工签字票未执行，agent 不代签（本票 Notes 明文）：**

| 复选框 | 保留理由 |
|---|---|
| 逐行复核总账 12 行（破坏→期望红→实际→回滚绿四段齐全） | 人手复核未做；总账在 `evidence/impl-03e/falsification_summary.md`，尚未有人逐行看过 |
| 两个特例（A2 负向 RC=0、F4 RC=3）单独判读 | 同上——特例判读是签字票的核心工作量，未动笔 |
| 交叉核对 ≥2 个反例证据文件与总账一致 | 抽查动作未执行 |
| 复核结论写入 impl-03e 的 Answer 新节 | 依赖上面三条先做完 |
| impl-03e 的 `Status:` 置 `resolved` | 本票 Notes 明写「不在本票做」；03e 顶行至今 `ready-for-human` 与此一致 |
| impl-dag 的 impl-03 段同步「03e 已签字」 | 签字未发生，DAG 不能预写 |

**impl-03h（6 条）—— blocked by impl-03g（签字链未走，终核无从开工）：**

| 复选框 | 保留理由 |
|---|---|
| 母票 10 条验收逐条重新确认（证据位置 + 今日是否仍成立） | 03g 未签，终核按强序排在签字之后 |
| 依赖部署面条目在 06e 口径变更后确认仍成立 | 同上；06e 判据 ④ 已改口径（usrlib），母票验收第 ③ 类条目须复核但尚未做 |
| 母票正文状态改为「六张副票全结（含 03e 签字）」 | 03g 未签，不能写「含签字」 |
| impl-dag impl-03 段落与 Frontier「全结」措辞归正 | 即上文「归正边界」第一条，03h 自己的活 |
| 证据失效则登记新票不勾销 | 复核未发生，无从触发 |
| 收口结论写进母票 Answer 末尾新节 | 全部前序未做 |

**impl-06f（5 条）—— 母票索引票，等四切片子票收口后才能勾（现 1/2 已结、3/4 未结）：**

| 复选框 | 保留理由 |
|---|---|
| 四张切片各自验收勾满且证据可一条命令复现 | 06f-3 未开工、06f-4 未实测 |
| README 不再含与代码矛盾的「现状」句 | 06f-1 已归正六处，但最终判定权在母票收口时整体复核，尚未做 |
| 全部票面文件顶部 `Status:` 唯一 | 本票（06f-2）已把已知矛盾清零，但该条的**终核**（全仓再扫一遍）归母票收口 |
| 未兑现复选框只剩「真未做」且写明理由 | 本票 Answer 已交 39 条清单；母票收口时以此为底再核 |
| 端口遗留以「已知限制」写进 README 与 impl-03a §C | 06f-4 收尾门实测（端口 curl）未跑，实测前不能落「已知限制」的最终形态 |

**impl-06f-3（6 条）—— ready-for-agent 未开工（GLOSSARY 三词条尚未落笔）：**

| 复选框 | 保留理由 |
|---|---|
| GLOSSARY 新增三条词条（脏率/反例夹具/脏率基线） | 零施工，词条未写 |
| 定义与上表口径一致、不引入代码里没有的说法 | 依赖上一条 |
| 「脏率」写明「重复不计入分子」 | 依赖上一条 |
| 「反例夹具」写明与脏率的耦合风险 | 依赖上一条 |
| 词条风格与既有条目一致 | 依赖上一条 |
| `make lint` / `make typecheck` 不受影响 | 收口复核动作，未到 |

**impl-06f-4（7 条）—— 收尾门实机实测未跑（本票收口后其 blockedBy 06f-1+06f-2 全解，但实测本身仍未做）：**

| 复选框 | 保留理由 |
|---|---|
| 不设 override 跑 `deploy_surface_check.sh` 全 11 条原始输出入票 | 实机实测未跑 |
| 11 条全 PASS 则销掉 impl-03a §C 冻结端口遗留项 | 依赖上一条结果 |
| FAIL 逐条归因（判据/宿主/真缺陷）不得 override 掩盖 | 依赖实测发生 FAIL 才触发 |
| `make init` → `make verify` 实测结果入票 | 实机实测未跑 |
| 冻结端口逐个 `curl` 实测表入票 | 未跑；WinNAT 排除段动态漂移（06f-1 已记），收尾门前须实测 |
| 跑完把 Flink 作业恢复 RUNNING | 依赖实测先跑 |
| 本票结论写进 impl-dag Frontier | 依赖实测结论 |

**impl-06g（8 条）—— ready-for-agent 未开工（`REQ-F-GEN-04` ≥1000 events/s 从未实测，`make spike` 仍 dry-run）：**

| 复选框 | 保留理由 |
|---|---|
| `make spike` 默认路径真发 + 参数退回 dry-run | 代码改动未做（当前默认 dry-run） |
| 一手测量口径（起止墙钟 + 实际发出条数，不用计划条数当分子） | 实测未跑 |
| 实测速率 ≥1000 events/s 数字与口径入票 | 同上——这是全仓唯一从未实测的功能条目（DAG 61 行） |
| 尖峰期间链路不崩（RUNNING/无 failover/lag 回落） | 实测未跑 |
| 尖峰对 `dlq-rate-no-increase` 影响归因记录 | 实测未跑 |
| README 与 Makefile 的 spike 描述改「已实测」 | 须先有实测数字，06f-1 的「尚未真发」措辞与它同步替换 |
| 判据进码（可机读断言） | 实测与断言都未做 |
| 跑完 DLQ 基线复位、`make verify` 回绿 | 尖峰后置动作，未到 |

**impl-04（1 条）—— 已结案票面上唯一有意留空的框：**

| 复选框 | 保留理由 |
|---|---|
| 录制 GIF（窗口关闭跳变 + 修订变色） | 票面明写「GIF 属浏览器录制步骤，留评审者」；数据层对偶证据（暂定→落定跳变、rev 递增的 updated_at 时序）已取得入票。这是**给评审者的复现材料**，不是未完成的施工项 |

### 复核命令（任何人可重跑）

```bash
cd .scratch/ecom-user-behavior-pipeline/issues
grep -n '^Status:' *.md | grep -v 'resolved\|done\|ready-for-agent\|ready-for-human\|in-progress'   # 应空
grep -n '\*\*Status:\*\*' *.md                        # 五处，全部与顶行同向
grep -c '^\s*- \[ \]' *.md | grep -v ':0'             # 39 处，全在未结票 + impl-04 的 1 处留空
grep -n '^## Comments' *.md                           # 空
```
