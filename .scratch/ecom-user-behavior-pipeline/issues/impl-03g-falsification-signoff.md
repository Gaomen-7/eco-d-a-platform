# impl-03g: impl-03e 复核签字 — 把「待人复核」收成 resolved

Type: task
Status: resolved（2026-10-27 复核签字收口：总账实 13 行逐行四段齐全、两特例判读成立、抽查 4 组证据零偏差；impl-03e 已收 resolved）
Blocked by: None（可立即开工）

**What to build:** 把 [impl-03e](../issues/impl-03e-lake-assert-counterexample-falsification.md)
从 `ready-for-human` 收成 `resolved`。它**没有施工量**——反例早就跑完了，
证据也在，缺的只是「有人对着总账复核一遍并签字」这一步。

**Blocked by:** None（可立即开工）

## 为什么它还开着

impl-03e 的 `Status:` 逐字是：
「ready-for-human（2026-10-24 全部反例跑完待人复核；F1–F6 + 5 附加组全证伪，
总账见文末 Progress §9 与 `evidence/impl-03e/falsification_summary.md`）」。

⇒ 它是一张**已完成但未签字**的票。impl-03 母票却在 2026-10-24 已被 03f 以
「六张副票全结」收口 ⇒ 母票与副票状态**不自洽**（impl-03d 的断言在 03e 签字前
按 03d/03e 的切分原则**不算数**）。本票把这个矛盾消掉。

## 复核对象（总账里 12 行，含 2 个特例）

`evidence/impl-03e/falsification_summary.md` 记录了：

- **F1**（A1 分流完备）、**F2a/F2b**（A3 迟到对账两侧）、**F3a/F3b**（快照保留区间）、
  **F5**（有 `[ERROR]` 时哨兵全绿仍须判失败）、**F6**（哨兵缺行也判失败）、
  **SNAP_empty**、**ZIP_interval**、**ZIP_audit**、**fp_clear**（空 DWD 连带红）——
  均为 **RC=1** 且指出具体哨兵
- **特例 1：A2 负向 = RC=0**（造重复后仍绿，日志出现 `metric.a2_dup_overshoot = 1`）——
  这是**设计如此**：A2 只入日志不作断言（承认恒真），不是漏判
- **特例 2：F4（幂等）= RC=3**——幂等走 `--compare` 路径，退出码语义与哨兵面不同

## Acceptance criteria

- [x] 逐行复核总账 12 行，每行确认「破坏动作 → 期望红 → 实际红/绿 → 回滚绿」四段齐全
  —— 总账实有 **13** 个数据行（票面「12 行」口径差 1，如实登记、移交 impl-03h 措辞归正）；13 行逐行四段齐全，明细见 impl-03e Answer 节核对表
- [x] **两个特例**（A2 负向 RC=0、F4 RC=3）单独给出「为什么这不是漏判」的判读，不能含糊带过
  —— A2：spec §5.4 逐字「恒真不作断言」，双向要点＝不误报（9 哨兵全绿 RC=0）＋有记账（`a2_dup_overshoot=1`）；F4：幂等走 `--compare` 指纹路径，RC=3 是 03d 定义的专用码，9 哨兵全绿恰是预期。判读全文见 impl-03e Answer 节
- [x] 交叉核对：至少抽查 2 个反例的**证据文件**（如 `f1_break.sql` / `f1_rollback.sql`）与总账描述一致
  —— 实查 **4 组**（F1 夹具对、F4 夹具对、A2 夹具对、F5 judge 输出），与总账逐字零偏差
- [x] 复核结论写入 impl-03e 的 `## Answer`（新增一节，**不改写既有 Progress §9**）
  —— `## Answer（2026-10-27 人工复核签字，impl-03g 执行）` 已新增于 Progress 之前，Progress §9 一字未动
- [x] impl-03e 的 `Status:` 置为 `resolved`，并保留「反例全部证伪」的结论句
  —— 已置 `resolved`（含 2026-10-27 签字收口实录），「F1–F6 + 5 附加组全证伪」结论句原样保留
- [x] `issues/impl-dag.md` 的 impl-03 段落同步：写明 03e 已由人工复核签字
  —— 三处同步：副票表 03g 行结案注记、Frontier 待办由 2 张收 1 张、impl-03 内部 Frontier 的 03e 条目补签字句

## Notes

- **本票零代码改动**：不重跑反例、不改 `lake_assert.sh`、不改证据文件。
  若复核中发现某行证据**确实不成立**，**不要**就地修补——开新票登记缺口。
- 复核者须能回答：「如果明天有人把 A1 的判定改坏，这套断言会不会抓到？」
  抓不到就说明 03e 的结论要打折，如实写。
