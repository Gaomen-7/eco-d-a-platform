# impl-03h: impl-03 母票终核收口 — 10 条验收逐条对账 + 签字链补全

Type: task
Status: resolved（2026-10-27 终核收口：母票 10 条验收逐条对账今日仍成立——`make verify` 10P0S0F 退 0、`lake_assert.sh --compare` 指纹逐字回基线 `…001`、`git check-ignore` 复跑命中；签字链接入、DAG 措辞归正含 12/13 行口径差；未发现失效证据）
Blocked by: impl-03g

**What to build:** 对 impl-03 母票的 10 条验收做**最后一次逐条对账**，确认每条的
证据都还站得住（不是当初站得住），把 03e 的人工签字接进收口链，并把 DAG 与母票
措辞调整到「不再有任何票名义未结」。

**Blocked by:** impl-03g

## 为什么要再核一遍

母票 [impl-03](../issues/impl-03-p2-offline-layering.md) 的 10 条验收已在 2026-10-24
由 impl-03f 全勾。但收口**之后**又发生了两件事，使「全结」这个说法需要重新确认：

1. **impl-03e 至今名义未结**（`ready-for-human`）——而 03d 的断言"在 03e 证伪前
   不算数"是 03d/03e 拆票的**立票原则**。⇒ 母票"六张副票全结"中的一张其实没结。
   本票的前置 [impl-03g](impl-03g-falsification-signoff.md) 负责把它收掉。
2. **部署面判据口径已变**（[impl-06e](impl-06e-deploy-check-usrlib.md)，2026-10-26）：
   判据 ④ 从「Paimon jar 在两容器 lib + sha256」改为「作业 jar 在 `/opt/flink/usrlib/`」，
   原断言下移为 4d。母票验收中有依赖「部署面」措辞的条目，须确认表述仍然成立。

## Acceptance criteria

- [x] 母票 10 条验收**逐条**重新确认，每条给出「证据位置 + 今日是否仍成立」两栏
  —— 母票新增 `## Answer（2026-10-27 impl-03h 终核收口）` 逐条表：#4/#5/#6/#7正/#8/#9 今日实跑复证（verify 10P0S0F 退 0、assert --compare 指纹回基线、git check-ignore 命中 .gitignore:10），#1 部分复跑、#2/#3 DDL 零漂移＋哨兵复绿、#7 反例与 #10 凭未被覆盖的入库证据
- [x] 其中依赖部署面的条目，确认在 [impl-06e](impl-06e-deploy-check-usrlib.md) 口径变更后仍成立
  —— 10 条中无逐字引用旧判据 ④ 的条目；正文 ③ 的 Paimon connector 断言＝06e 下移后的 4d（语义未丢、钉死值未变）；依赖部署面的 #1/#8 今日已在现行部署面实跑复绿；部署面自身 06i 结案 11/11 PASS
- [x] 母票正文状态改为「六张副票全结（含 03e 人工签字，见 [impl-03g](impl-03g-falsification-signoff.md)）」
  —— 母票顶行 `Status:` 与正文 `**Status:**` 两处均已改写为该措辞（附 03h 终核指针）
- [x] `issues/impl-dag.md`：impl-03 段落与 Frontier 更新，不再出现与 03e 状态冲突的「全结」措辞
  —— 五处归正：副票表 03g/03h 行结案注记、impl-03 已结清条目补签字链、Frontier「全部结案、无剩余待办」、impl-03 内部 Frontier 补终核句、「总账 12 行 → 实 13 行」口径差归正
- [x] 若复核发现任何一条**证据已失效**（例如某脚本被改坏），登记新票并**不勾销**该条
  —— 无触发：10/10 证据均站得住（关键脚本 `lake_assert.sh`/`verify_runner.py`/DDL 自各自收口提交起 git 零漂移），无需登记缺口票
- [x] 收口结论写进母票 `## Answer` 末尾的新节（既有正文与 03f 收口节不改写）
  —— 新节已落母票文末（母票原无 Answer 节，本次新建）；原票正文、AC 清单、Notes 与 03f 收口记录一字未动

## Notes

- **这是对账票，不是施工票**：不允许顺手改 `sql/`、脚本或断言。
- 「证据还站得住」的判定标准：能**重新跑一遍**或能指向一份**未被覆盖的**取证文件；
  「当初跑过」本身不算。
- 若时间/环境不允许重跑某条，就写「未能复跑，依据为 <文件>」，不要写「已验证」。
