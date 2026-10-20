# 08 · Paimon / Flink 用法的一手事实：SQL-only 可行性、系统表、迟到侧输出、MoW 部分列更新

Type: research
Status: open
Blocked by: 02
Blocked by (note): `02` 指**地图票 D4（已落子）**。
Created: 2026-10-20（原 01；并入原 02 下挂的两条 research——Flink SQL 迟到侧输出、Doris MoW 部分列更新）
Ticket: 08
Source: map.md · SRS `REQ-F-LAKE-06/07/08`、`REQ-F-RT-04` · gdd D7 / D11 / D12 / §1.4 / §1.5

## Question

此前所有关于 Paimon 与 Flink/Doris 能力的断言都是**文档级**的，没有一条经过实机或逐字原文核对。这张票要四组一手事实（每组给逐字原文 + URL；官方没写就明说）：

**A · Paimon 2.1.0 × Flink 2.2.1 的 SQL 能力**
1. `paimon-flink-2.2-2.1.0.jar` 上 `INSERT INTO`（主键表 upsert）、`MERGE INTO`、`CREATE TABLE ... WITH ('snapshot.num-retained.min'='5')`、`SELECT * FROM <table>$snapshots` 各自的支持声明；`file://` warehouse 有无额外限制。
2. 系统表 `$snapshots` / `$files` / `$schemas` 的**列名与语义**（用于把 `REQ-F-LAKE-08` 写成可执行 SQL）；「快照过期才物理删除」的交互。
3. 「catalog 名」与「库名」的官方语义：`ecom_lake` 在 Paimon 里是 catalog 还是 database？`REQ-F-LAKE-06`「单一 catalog（库名 `ecom_lake`）」的措辞落在哪一边。
4. `file://` 上 compaction 的官方做法（`paimon compact` CLI / Flink 侧 action），以及官方给没给成本数字。

**B · Flink SQL 2.2 的迟到事件能力**
5. 「Flink SQL 没有迟到侧输出」是否仍然成立（FLINK-10031 / FLINK-20527 现状）——这条若被证伪，gdd D11 的「强制混合路线」就不再是硬约束。
6. `table.exec.emit.early-fire.*` 与 **Window TVF 不兼容**、以及 Group Window Agg 的 `@Experimental` 状态（FLINK-29692）在 2.2 上的现状——这决定 gdd D12 的「提前发射」还用不用得起。

**C · Doris 的覆盖写能力**
7. UNIQUE KEY MoW 表的 `partial_columns=true` 语义：能不能只更新「本轮触发的步数」列而保留其他列；默认 `INSERT INTO` 是整行 UPSERT 的官方原文。
8. Doris Stream Load 2PC 与 label 去重的分工原文（gdd D1 引用过一版，需复核链接是否仍有效）。

**D · 副作用面**
9. 上面 1–8 里，哪些结论会**改变已有设计**（例如 B5 若被证伪，作业形态可回到纯 SQL；C7 若成立，漏斗表可以窄行写入）。逐条写「影响哪张票」。

## 约束

- 只读调研；**不动仓库任何文件**，产出写进本票 `## Answer`（长文另起 `research/*.md` 并在此链接）。
- 每条结论带一手 URL；**不得把设计值当官方保证**（HLD §0.3）。
- 不重启选型论证——Paimon 2.1.0、Flink 2.2.x、Doris 4.1.5 都是冻结锚点，本票只看**怎么用**。

## Answer

<!-- 待解。解析步骤：Skill "research"（issue subagent 可并行四组）→ Status: resolved → 往 map.md 追加 -->
