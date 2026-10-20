# 09 · `verify` 与 `verify-fault` 的判据强度：退出码写到哪一层才算“可机读”

Type: grilling
Status: open
Blocked by: 02, 04
Created: 2026-10-20
Ticket: 09
Source: map.md → Not yet specified F8 / SRS §3.1.8 / gdd §1.8 / HLD §6 / gdd F1

## Question

`REQ-F-VERIFY-01` 是五项烟测、`REQ-F-VERIFY-03` 是故障注入，但两份文档对「判据」的表述密度差异巨大：`gdd §1.8` 给了五项各自的确切断言（`jobs[].state == "RUNNING"`、`/health` 纯文本 `OK`、`count(*)` 而非 `total_record_count`…），而 **`verify-fault` 只给了「条数一致」与「脏率不上升」两句自然语言**。同时 `gdd §1.3` 又新增了四条「行为可证」判据，`gdd §1.4` 新增了一条「不变量断言」——它们**是否进入 `verify` / `verify-fault` 的退出码**，没人说。

要裁决的：

1. **退出码的语义分层**：`verify` 退出 0 表示「五项全过」，还是「五项 + 幂等 + lag + 不变量 + 脏率」全过？后者会让 `verify` 变成「一次跑完全部验收」，与 `REQ-F-VERIFY-01` 的字面范围不符。
2. **`verify-fault` 的机器判据**：kill TM → 等作业 RUNNING → 比对 Doris `count(*)`。但「等多久」、`count(*)` 比对的是**哪张表**（是全部 ADS 表，还是窗口表）、「脏率不上升」的分子分母各取什么，都要落成脚本可执行的形式。
3. **反向断言要不要**：`gdd §1.8` 有一条「反例保护：若 kill 后条数一致但脏率飙升，判为不通过」——这是一个**负断言**，脚本里怎么写（先算基线脏率？基线从哪来？）。
4. **`lint` / `typecheck` 的位置**：HLD §3.3-6 明确它们**不混入** `verify`，但 `make verify` 的 CI 化（如果将来有）要不要一个总入口 `make check`。
5. **fail 时的产出**：`verify` 失败时，断言结果落在哪（stdout 行？JSON 文件？Flink REST 原始响应快照？）——「失败必须有痕迹」（横切关注点 2）。

## 约束

- HITL。**不改变八个冻结目标名**；若要加 `make check`，走冻结项变更并在 Answer 里单列「需你批准」。
- 每条裁定都要给**脚本可执行的形式**（精确到命令 + 判定的解析方式），否则等于没定（HLD §0.4）。
- 与 [02]（schema 契约）有交叠的部分（DLQ 原因码怎么判），写「依赖 02」。

## Answer

<!-- 待解。解析步骤：Skill "grilling" + "domain-modeling"（断言表）→ Status: resolved → 往 map.md 追加 -->