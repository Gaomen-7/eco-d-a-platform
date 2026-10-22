# impl-03d: 断言执行面 `lake_assert.sh` + A1/A2/A3 + 幂等 + 快照区间 + 维护段

> 父票：impl-03
> 规格落点：spec §5.3（`replay_90_maintain` / `assert_lake`）、§5.4（离线断言）、§5.5（断言执行面）

Type: task
Status: ready-for-agent
Blocked by: impl-03c

**What to build:** 把「断言」这件事从「相信 `sql-client.sh` 的退出码」换成**哨兵 SQL + stdout 解析**：每条断言写成 `SELECT '<名>' AS assertion, COUNT(*)>0 AS ok FROM …` 一行，`scripts/lake_assert.sh` 判定「无 `[ERROR]` **且** 每条哨兵 `ok=true` 字符串出现」。跑完本票，`assert_lake.sql` + `lake_assert.sh` 在一个真实 `replay` 上跑绿。

## 交付内容

1. **`replay_90_maintain.sql`**：`sys.compact(...)` ×N ＋ `sys.expire_snapshots(retain_max=>20)` ×N。
2. **`sql/assert_lake.sql`**：五层行数 + 断言集，全部写成哨兵 SELECT。
3. **`scripts/lake_assert.sh`**：解析 stdout，「无 `[ERROR]` 且每条哨兵 `ok=true`」⇒ 退 0，否则退非 0。
4. **断言集**：

| # | 断言 | 判据 |
| --- | --- | --- |
| **A1 分流完备** | `ods_distinct = dwd_distinct + reject_distinct` | **全局比对**（ODS 无事件日分区，降为全局粒度，可判性不变） |
| **A2 重复记账** | `count(*) − count(DISTINCT event_id)` | **只入日志、不作断言**（承认恒真；它是「为什么迟到标记差异不可加」的实例） |
| **A3 迟到对账** | `count(*) FROM dwd WHERE is_late=1` ＝ `sum(cnt) FROM dwd_late_event_count` | 「只存计数」的唯一用途 |
| **幂等** | 同批 dump 重放两遍，DWD `count(*)` 与逐列校验和 `SUM(crc32(concat_ws('|', event_id, is_late, late_lag_ms)))` 两次一致 | `REQ-F-VERIFY-02` 的离线版 |
| **快照 [5,20]** | `SELECT count(*) FROM <t>$snapshots` 落 **[5,20]** | min=5 显式；max=20 把「受控」变成有限数；`time-retained` 保持 1 h 默认 |
| **拉链两条** | 当前快照的区间自洽（普通 SELECT）＋「变更发生过」（`t$audit_log`） | 批读会丢 DELETE ⇒ 分两条走 |

## 断言执行面（这是本票的技术核心）

**源码事实**：`sql-client.sh -f` 语句报错后**仍以 0 退出**（`CliClient` 丢弃返回值、无 `System.exit`）⇒ **退出码不可用** ⇒ 只能靠哨兵 + stdout 解析。实现时不要试图「让 sql-client 退非 0」，那条路官方**没有**（见下方登记项）。

## Acceptance criteria

- [ ] `lake_assert.sh` 在真实 `replay` 之后**判定通过并退 0**（A1/A3/快照/拉链/`$audit_log` 全绿）
- [ ] `lake_assert.sh` 在哨兵面**被人为破坏**时退非 0（此条是**接线自检**，完整反例证伪归 03e —— 本票只需证明一次失败路径能走通，不要求穷举）
- [ ] A1 实测成立：`ods_distinct = dwd_distinct + reject_distinct`（全局粒度）
- [ ] A2 **只入日志、不作断言**（日志里有重复率数值，但 `lake_assert.sh` 不因它退非 0）
- [ ] A3 实测成立：DWD `is_late=1` 行数 == `dwd_late_event_count` 的 `sum(cnt)`
- [ ] **幂等**：同批 dump 跑两遍后，DWD `count(*)` 与逐列 crc32 校验和**两次完全一致**
- [ ] 快照 `count(*)` 实测落在 **[5,20]** 区间内（逐表查 `$snapshots`，留下数值）
- [ ] `num-retained.min` 显式为 **5**；`max-retained` **20**；`time-retained` 保持 **1 h 默认**
- [ ] 拉链区间校验走**当前快照的普通 SELECT**（不依赖批读）；「变更发生过」走 `t$audit_log` 且能查到记录
- [ ] `replay` 末尾的 `sys.expire_snapshots(retain_max=>20)` 收的是**上界**，日志/注释里说清「`retain_max` 是上界不是钉住」
- [ ] `lake_assert.sh` 的判定逻辑有**单元测试**（stdout 有 `[ERROR]` ⇒ 失败；某哨兵缺 `ok=true` ⇒ 失败；全绿 ⇒ 通过）

## Notes

- **本票不穷举反例**。「每条断言都能被证伪」是 03e 的独立职责 —— 这是刻意切开的：impl-02e 的 Answer 记录过一次教训（判据自身有缺陷、healthcheck 补成恒真），把「写断言」和「证伪断言」放进同一张票，容易两头都绿。
- **幂等不进每次 `verify`**：全量重放两遍是重操作，由本票的离线幂等断言 ＋ impl-02 的 `verify-fault` 恢复重放共同覆盖（spec §8.2 已定）。
- **官方未给出的，按 spec §8.3 登记、不臆造**：
  - **Flink SQL 断言失败 → 非零退出码机制 —— 源码显示不存在**（这是本票的直接成因，不是缺陷）。
  - `sys.compact` 对**主键表**的逐句适用声明。
  - Paimon `file://` compaction 成本数字。
- **不实现**（spec §8.2）：排序 compaction · 幂等进 `verify` · 「离线重算覆盖实时行」。
- Doris 侧的 `ads_window_metric` 对账**不在本票**，归 03f；本票只判 Paimon 侧自洽。
- 本票**不改** `verify_runner.py` 的第三项（仍 SKIP），转真断言归 03f。