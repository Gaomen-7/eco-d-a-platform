# 01 · Paimon 2.1.0 在 Flink 2.2.1 上到底能不能纯 SQL 跑完五层

Type: research
Status: open
Blocked by: 04
Created: 2026-10-20
Ticket: 01
Source: map.md → Not yet specified F13 / Decisions D4

## Question

`REQ-F-LAKE-07` 要求「离线分层由 **Flink 批模式 + Makefile 触发**，无调度器」，`gdd` §1.4 追加了「快照数、`$schemas`、行数统统走 **Flink SQL 系统表**，不走 Doris」。此前所有关于 Paimon 的断言都是**文档级**的，没有一条经过实机。

这张票要的是**一手事实**，不是设计：

1. Paimon `2.1.0` 的 `paimon-flink-2.2-2.1.0.jar` 在 **Flink 2.2.1** 上跑 `INSERT INTO`（主键表 upsert）、`MERGE INTO`、`CREATE TABLE ... WITH (snapshot.num-retained.min=5)`、`SELECT * FROM <table>$snapshots` 这四类语句，**分别有什么官方支持声明**（逐字引用 + URL），以及在 `file://` warehouse 上是否受额外限制。
2. `REQ-F-LAKE-08` 要「定期 compaction 合并小文件」。官方在 **`file://`** 上跑 compaction 的方式（`paimon compact` CLI？Flink 侧 `compact` action？）与**成本**（每次 compaction 的 IO/时间量级，官方如果给了数字就引数字，没给就明说没给）。
3. **表名与 catalog 名**：`ecom_lake` 作为 catalog 名与「库名」的官方语义区别（Paimon 的 catalog 带 database 层，SRS §3.4 冻结的是「库名 `ecom_lake`」还是 catalog 名？）——给出官方定义原文，并指出 SRS/gdd 在哪一处措辞含混。
4. 系统表 `$snapshots` / `$files` / `$schemas` 的**列名与语义**（逐字），用于把 `REQ-F-LAKE-08` 的判据写成可执行 SQL。

## 约束

- 只读调研；**不动仓库任何文件**，产出写进本票的 `## Answer`（长文另起 `research/paimon-sql-2.2.md` 并在此链接）。
- 每条结论带一手 URL；官方没给的**必须写「官方未给出」**，不许估算（HLD §0.3）。
- 不要重启 HLD/GDD 的选型论证——Paimon 2.1.0 是冻结锚点，这张票只看**怎么用**。

## Answer

<!-- 待解。解析步骤：调用 Skill "research"（可起 subagent 并行）→ 追加答案 → Status: resolved → 往 map.md 的 Decisions so far 追加一行 -->
