# 08 · Paimon / Flink 用法的一手事实：SQL-only 可行性、系统表、迟到侧输出、MoW 部分列更新

Type: research
Status: resolved
Resolved: 2026-10-20
Blocked by: 02
Blocked by (note): `02` 指**地图票 D4（已落子）**。
Created: 2026-10-20（原 01；并入原 02 下挂的两条 research——Flink SQL 迟到侧输出、Doris MoW 部分列更新）
Ticket: 08
Source: map.md · SRS `REQ-F-LAKE-06/07/08`、`REQ-F-RT-04` · gdd D7 / D11 / D12 / §1.4 / §1.5
Long-form: [research/08a-paimon-flink-sql.md](../research/08a-paimon-flink-sql.md) · [research/08b-flink-sql-late-and-early-fire.md](../research/08b-flink-sql-late-and-early-fire.md) · [research/08c-doris-mow-partial-columns.md](../research/08c-doris-mow-partial-columns.md)

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

**状态：resolved（2026-10-20）**。四组一手事实全部取得，逐字原文与 URL 落在三份长文：

| 分组 | 文件 | 覆盖问题 |
| --- | --- | --- |
| A · Paimon 2.1.0 × Flink 2.2 | [research/08a-paimon-flink-sql.md](../research/08a-paimon-flink-sql.md)（1416 行） | 1、2、3、4、8 |
| B · Flink SQL 2.2 迟到与提前发射 | [research/08b-flink-sql-late-and-early-fire.md](../research/08b-flink-sql-late-and-early-fire.md) | 5、6、7 |
| C · Doris 覆盖写与 2PC | [research/08c-doris-mow-partial-columns.md](../research/08c-doris-mow-partial-columns.md) | 7、8 |

**版本锚点**：Paimon 引用一律取 **`paimon.apache.org/docs/2.1/`** 版本固定文档（对应 tag `release-2.1.0`，commit `3a5ffe7`），配置默认值另取该 tag 的 `docs/generated/core_configuration.html`；Flink 侧取 `nightlies.apache.org/flink/flink-docs-release-2.2/` 与 `apache/flink` `release-2.2` 分支源码；Doris 侧取 `doris.apache.org/docs/4.x/`（各页自带 last-updated）。访问日期 2026-10-20。**未使用本仓 `research-notes.md` 作为证据**（它是二次汇编，不是一手）。

### 结论速查（9 问）

| # | 问 | 一手结论 |
| --- | --- | --- |
| 1 | Paimon × Flink SQL 能力 | `paimon-flink-2.2-2.1.0.jar` **官方在 artifact 表中明列**（Flink 2.2 在 2.1.0 支持矩阵 1.16–1.20 与 2.0–2.2 内）。`INSERT INTO` 主键表 upsert ✅。**`MERGE INTO` ❌——Paimon 2.1.0 无 Flink 标准 MERGE 语句**（Flink 2.2 官方语句清单亦无）；官方等价物是 **`CALL sys.merge_into(...)` procedure**（仅主键表）与 action jar。`WITH ('snapshot.num-retained.min'='5')` ✅（**默认 10**）。`$snapshots` ✅。`file://` **无额外 jar、无额外依赖、无限制条款**。 |
| 2 | 系统表 | 引擎列表逐字为 **"Flink, Spark, Trino and StarRocks"** ⇒ **不含 Doris**（D7 成立）。`$schemas` 列名是 `schema_id \| fields \| partition_keys \| primary_keys \| options \| comment \| update_time`——**没有 `schema` JSON 列**。`total_record_count` 原文逐字命中本仓说法。⚠️ **`$files`/`$snapshots` 官方并未给出完整列名清单**（只有示例 SQL 用到的列 + "Additional columns include…" + snapshot spec 的 camelCase 字段表；snake_case↔camelCase 映射**官方未给出**）⇒ 本仓 `research-notes.md` 那份 `file_id`/`delete_count` 清单**不是官方逐字列表**（官方小表写 `deleteRowCount`）。 |
| 3 | catalog vs database | 官方层级是 **catalog → database → table**；`warehouse` 是 **catalog 级**属性，表默认落 catalog 的 `default` database。⇒ **`ecom_lake` 官方对应 database，不是 catalog**；`REQ-F-LAKE-06`「单一 catalog（库名 `ecom_lake`）」**一个词混说了两件事**（见 §D-3）。 |
| 4 | `file://` 上 compaction | 官方入口只有 **`CALL sys.compact(...)` / `sys.compact_database(...)`** 与 **`paimon-flink-action-2.1.0.jar` + `flink run compact`**。**没有** `$compaction` 系统表；**没有**独立于 action jar 的 `paimon compact` CLI。**成本数字（耗时/IO/内存/合并文件数）官方未给出**。 |
| 5 | Flink SQL 有无迟到侧输出 | **断言成立**：FLINK-10031（Status Open、Resolution Unresolved、Updated 10/Feb/25）逐字 *"The Table API & SQL drop late events right now."*；FLINK-20527（Open/Unresolved）*"how to add a side output for flink sql watermaker?"*。2.2 官方文档**无任何 SQL 级迟到侧输出机制**；release notes 亦无相关条目。★ 源码级铁证：`WindowAggOperator` javadoc 逐字 *"late elements (elements belong to emitted windows) will be simply dropped."*（附 metric `numLateRecordsDropped`）。⇒ **gdd D11 的「混合路线」不是选型偏好，是官方能力强制**。 |
| 6 | early-fire / Window TVF | 限制的**确切出处已定位到源码**：`WindowUtil.checkEmitConfiguration` 抛 `"Currently, window table function based aggregate doesn't support early-fire and late-fire configuration 'table.exec.emit.early-fire.enabled' and 'table.exec.emit.late-fire.enabled'."`，由 `StreamPhysicalWindowAggregate.translateToExecNode()` 调用。5 个配置项**全部 `@Experimental`**（注释：*"its removal will be consistently monitored and addressed through FLINK-29692"*）；FLINK-29692 **Open/Unresolved**。⚠️ **比 gdd D12 现措辞更强的一条**：唯一支持 early-fire 的 **Group Window Aggregation 在 Flink 2.2 官方文档中被标 `deprecated`**（*"Warning: Group Window Aggregation is deprecated."*），源码注释更写 *"In the long future, [[StreamPhysicalGroupWindowAggregate]] will be dropped."*；而推荐的 Window TVF 聚合官方明写 *"do not emit intermediate results but only a final result"*。★ 另：**SQL 侧连 allow-lateness 都没有**（FLINK-24000 Closed/Later，逐字 *"Currently, Window aggregate does not support allow-lateness like Group Window Aggregate."*）。 |
| 7 | Doris 覆盖写 | 官方逐字：**`"Across all data models, the default behavior of INSERT INTO with a subset of columns is a full-row write. To prevent misuse, in the Merge-on-Write implementation INSERT INTO still defaults to full-row UPSERT semantics."`** ⚠️ **`partial_columns` 不是建表属性**，它是导入 header / `sink.properties.*`；`INSERT INTO` 走会话变量 `enable_unique_key_partial_update`。★ **「同一批内不同行更新不同列」普通 partial_columns 走不通**，官方逐字：**`"Batch constraint: all rows in the same batch write task … can update only the same set of columns."`** 官方路径是**灵活列更新（3.1.0+）**：需 `enable_unique_key_skip_bitmap_column=true` + `unique_key_update_mode:UPDATE_FLEXIBLE_COLUMNS` + **必须 JSON** + **禁用 `function_column.sequence_col`**。 |
| 8 | 2PC 与 label 分工 | 仓库引用句**逐字命中**（4.x 事务文档 "No Loss, No Duplication" 节）：*"The Label mechanism guarantees no duplication for a single transaction, and two-phase commit provides the ability to coordinate no duplication across multiple transactions."* 2PC 端点 `PUT /api/{db}/{table}/_stream_load_2pc` + `txn_operation:commit\|abort`（`txn_id:` 或 `label:`）。★ 仓库引用的 FAQ 句 **仍在**，但**旧 URL 已失效**，新位置为 `.../flink-doris-connector/faq/`。`sink.enable-2pc` 默认 TRUE。 |
| 9 | 副作用面 | 见下 §D 表（**14 条，其中 8 条会改变已有设计**）。改变设计的三条主干：**B5 成立**（强化 D11「混合路线」的硬约束地位）、**B6 的 deprecated 事实**（**削弱**「窗口内提前发射」的可用性——只剩一条正在被官方弃用的语法）、**C7 的 Batch constraint**（**收窄**「漏斗表窄行写入」的可行路径）。另有 **A3 证伪 catalog/database**、**A1 证伪 `MERGE INTO`**、**A2 部分证伪系统表列名**、**A8 补强 Paimon 默认 MOR**、**D-11 补强 SQL 侧无 allow-lateness**、**D-14 排除 CUMULATE 替代路径**。 |

### §D · 副作用面：哪些结论改变已有设计

> 处置词表沿用 `canonical-truth.md` §0 G2（封闭词表）。「影响哪张票」一列是**归属**，不是裁决——本票只做一手事实登记，裁决留给 [05]/[09]。

| # | 结论 | 是否改变已有设计 | 处置 | 影响哪张票 |
| --- | --- | --- | --- | --- |
| **D-1** | **B5 成立**：Flink 2.2 仍无 SQL 级迟到侧输出 | **不改变**——**强化** gdd D11「混合路线」的硬约束地位（此前是文档级断言，现为源码+双 JIRA 现状级） | `覆盖下游` | [02] [03] [05]；`canonical-truth.md` 冲突 **C** 状态由「挂起待证」→**可落笔**（D11 成立） |
| **D-2** | **B6 更强**：唯一支持 early-fire 的 Group Window Aggregation 在 2.2 官方文档标 `deprecated`，源码写 "will be dropped"；TVF 聚合官方明写 "do not emit intermediate results" | **改变**——gdd D12 现措辞只说「标 `@Experimental` + FLINK-29692 跟踪」，**未记 deprecated 这条更强的官方事实**。⇒ 「窗口内提前发射（10 s 暂定值）」这条路在 2.2 上**只能走一条正在被官方弃用的语法** | `补充落点` | [03]（Q13「提前发射」的取舍）、[09]；`gdd D12` 需补记 deprecated 事实 |
| **D-3** | **A3 证伪**：`ecom_lake` 官方对应 **database**，不是 catalog；层级 catalog→database→table | **改变**——`REQ-F-LAKE-06`「五层全部落 Paimon **单一 catalog**（库名 `ecom_lake`）」**一个词混说两件事**；判据「五层表均存在于同一 catalog」在官方语义下**恒真**（同一 catalog 内不同 database 也满足）⇒ 判据不具区分力 | `改写上游` | [05]（五层形状）、[09]；`canonical-truth.md` §1 需新增一条冲突（建议编号 **M**） |
| **D-4** | **A1 证伪**：`MERGE INTO` 在 Paimon 2.1.0 **不存在**（无 Flink 标准 MERGE 语句），官方等价物是 `sys.merge_into` procedure | **改变**——若任何设计把「MERGE INTO」当作 Paimon 2.1.0 的能力，属事实性错误 | `覆盖下游` | [05]（DWD/DIM 幂等写入的实现形态） |
| **D-5** | **A2 证伪（部分）**：`$files`/`$snapshots` **官方未给出完整列名清单**；`$schemas` 也没有 `schema` JSON 列 | **改变**——本仓 `research-notes.md` 的列名清单**不是官方逐字**；`REQ-F-LAKE-08` 判据若要写死具体列名，须以「官方只保证示例列」为准 | `改写上游` | [05]（快照判据的具体 SQL）、[03]（`$schemas` 供 F5 演练用） |
| **D-6** | **A2 补强**：`sys.expire_snapshots(retain_max => N)` 是官方提供的**显式钉住快照数**的手段 | **不改变**（补足判据可执行性）——`REQ-F-LAKE-08`「下限 5 + 实际数量受控」可由 `retain_max` 落成可执行 SQL | `补充落点` | [05]（`REQ-F-LAKE-08` 判据）、[09] |
| **D-7** | **A8 补强**：Paimon **主键表默认是 MOR 而非 MOW**（MOW 需显式 `deletion-vectors.enabled=true`）；官方 bucket 规模指引 "200 MB–1 GB per bucket" 明写是 **MOR 口径** | **改变**——若设计假定主键表默认 MoW，则「主键表在 compaction 前同一 key 有多份物理版本」的推理前提需重述（Doris 侧才是 MoW 默认） | `改写上游` | [05]（bucket 取值）、[07]（compaction 成本）、HLD §8-R5 |
| **D-8** | **A8 补强**：Paimon 侧**无**「主键必须含全部分区列」的禁令，官方给的是 **cross-partition upsert** 模式及代价（索引内存、启动成本、`index-ttl`）；**规避路径＝把分区字段放进主键** | **改变**——`[05]` Q5「Paimon 对分区键与主键先后关系有无官方约束」**答：有代价路径，无禁令**；但同时**收窄**了「主键不含分区列」的自由度 | `补充落点` | [05]（Q5/Q6 落子）、`canonical-truth.md` §6 索引第 5 行（该行原写「Paimon 对『分区键与主键先后关系』的官方约束」→ [08] → [05]） |
| **D-9** | **C7 收窄**：「同一批内不同行更新不同列」**普通 `partial_columns` 走不通**（Batch constraint），须用 3.1.0+ 灵活列更新（JSON + `skip_bitmap_column`，且**禁用 sequence_col**） | **改变**——gdd §1.5 现写「Doris 对 UNIQUE MoW 支持 `partial_columns=true`，但默认 INSERT INTO 是整行 UPSERT」——`partial_columns=true` **不是表属性**，且该路径**不支持一批内多列组** | `改写上游` | [03]（漏斗双列并陈 / `revision` 语义）、[05]、[09] |
| **D-10** | **C8 证伪（链接）**：仓库 FAQ 引用句仍在，但**旧 URL 已失效**；新位置 `.../flink-doris-connector/faq/` | **不改变**（仅链接更新） | `覆盖下游` | [09]（落笔时用新 URL） |
| **D-11** | **B5 机制映射补齐**：官方把迟到分成两条通道——`allowedLateness` 内的迟到仍进窗口并触发 **late firing**（官方：*"multiple results for the same computation … deduplicate them"*）；`sideOutputLateData` 侧输出的是 *"the data that was discarded as late"*。★ **但 SQL 侧连 allow-lateness 都没有**（FLINK-24000 Closed/Later，逐字 *"Currently, Window aggregate does not support allow-lateness like Group Window Aggregate."*）——`allowedLateness` 是 **DataStream-only** | **改变**——gdd D10 的落点「允许延迟内→窗口修正」在 **SQL 段没有对应机制**（只有 Group Window Agg 的 `@Experimental` `table.exec.emit.allow-lateness`），⇒ **「窗口修正」这一段必须落在 DataStream 段**，不能靠 SQL 段配置达成 | `补充落点` | [02] [03]（迟到通道判据）、`canonical-truth.md` §3.3 |
| **D-12** | **A4 补强**：`file://` compaction 的官方入口是 action jar + `flink run compact`；**成本数字官方未给出** | **不改变**（补足 [07] Q6 的「官方给了数字就引，没给就明说」）⇒ **官方未给出** | `补充落点` | [07]（Q6）、[05] |
| **D-13** | **A2/C8 官方文档内部不一致**（如实记录，未替官方裁决）：Paimon `snapshot.num-retained.max` 与 `snapshot.expire.limit` 的默认值在两处官方文档不一致；Doris 2PC **abort** 端点路径官方自身混用 `stream-load2pc` 与 `_stream_load_2pc`；Doris `sink.properties.column` vs `sink.properties.columns` 两页不一致 | **不改变**（登记为施工时的坑） | `补充落点` | [05] [07] [09] |
| **D-14** | **B6 补强**：CUMULATE 官方**只声明累计语义**，**未给出**其支持 `table.exec.emit.early-fire.*`；源码上 CUMULATE 走 Window TVF 路径 ⇒ 被 `checkEmitConfiguration` **直接抛异常** | **改变**——若把 CUMULATE 当作「提前发射的替代路径」，属事实性错误 | `改写上游` | [03]（Q13 提前发射的替代方案评估） |

**本票不改变任何版本锚点**（Paimon 2.1.0 / Flink 2.2.x / Doris 4.1.5 全部冻结），不重启选型论证。

### 未闭合 / 留给下游的

1. **冲突 C 可从「挂起待证」转正**——D11/D12 已由一手证据证实（D-1、D-2），`canonical-truth.md` §1-C 与 §6 索引第 1 行可由 [09] 落笔。
2. **建议新增冲突 M**（`REQ-F-LAKE-06` 的 catalog/database 混说，D-3），分档＝事实，处置＝`改写上游`，落点 SRS `REQ-F-LAKE-06` 判据（须能区分「同一 catalog 内不同 database」）。
3. **`research-notes.md` 的两处非官方清单需订正**：`$files`/`$snapshots` 列名清单（D-5）、`partial_columns=true` 作为表属性的说法（D-9）。二者都是**事实类**，按 G1 可直接订正——但落笔归 [09]（`canonical-truth.md` §5 统一落笔原则）。
4. **建议新增冲突 N / O**（D-7、D-9）：N＝Paimon 主键表默认 **MOR 而非 MOW**（影响 HLD §8-R5 的空间放大推理前提）；O＝Doris `partial_columns` **非表属性** + **Batch constraint**（影响 gdd §1.5 措辞）。
5. **D-11 的连带落笔**：「允许延迟内→窗口修正」必须落在 **DataStream 段**（SQL 段无 allow-lateness）——这条会影响 [02]/[03] 对「Flink SQL 段 vs DataStream 段」职责划分的写法。
6. **本票只读**：除三份 `research/*.md` 新文件与本票 `## Answer` 外，**未改动仓库任何其它文件**。（已按地图协议同步 `map.md` / `issues/map-dag.md` / `canonical-truth.md` 的**状态与索引**，未改任何上游需求文字。）

### 官方未给出的项（摘要，全表见三份长文）

- Paimon：`$files`/`$snapshots` 完整列名清单、snake_case↔camelCase 映射、`file://` 上的 compaction 成本数字、**针对 `file://` 的专门限制条款**、MoW 口径的 bucket 规模建议值、主键基数与 bucket 数的公式、**Paimon 专属 `CREATE DATABASE` 示例**（改引 Flink 官方 `CREATE DATABASE` 语法）、「读取表在快照过期后仍成功」的直接断言、孤儿文件占磁盘的因果陈述。
- Flink：2.2 官方 Configuration 文档页**不含** `table.exec.emit.*` 五个选项的条目（只在源码）、early-fire 与 upsert/retract sink 正确性的专门章节、**「Window TVF 迟到记录被丢弃」的文档级表述**（只有源码 javadoc）、「Group Window Aggregation 被标 `@Experimental`」的官方表述（官方只说 deprecated）、移除时间表、**CUMULATE 支持 `early-fire.*` 的官方声明**（源码上 CUMULATE 走 TVF 路径会被直接抛异常）、**2.2 文档树内任何 early-fire 说明页**（该内容只存在于 master/2.3 文档树）、SQL 侧「迟到触发 retraction」的官方说明（FLINK-6970 Open）。
- Doris：**「表属性 `partial_columns`」**（官方文档与源码均无此建表属性）、普通 `partial_columns` 是否支持 JSON 的独立表述、4.1.5 Release Notes 中 `enable_unique_key_skip_bitmap_column`/`UPDATE_FLEXIBLE_COLUMNS` 的逐条声明、2PC abort 端点路径的统一写法、connector README 的 sink 选项表（官方明确该文档维护在 doris-website）。

---

**处置**：本票 resolved；[09] 落笔时把 D-1/D-2/D-3/D-4/D-5/D-7/D-9 纳入 `canonical-truth.md` §1 的待执行清单（新增冲突 M）。
