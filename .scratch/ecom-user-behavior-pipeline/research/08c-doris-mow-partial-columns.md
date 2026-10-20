# 08c · Apache Doris 4.1.x 表模型与导入语义（一手取证）

- 调研时间：2026-10-20
- 文档版本：`doris.apache.org/docs/4.x/...`（各页自带 "Last updated on" 标注，见下表），核对目标版本 **Apache Doris 4.1.5**（Release 4.1.5 页面：https://doris.apache.org/releases/v4.1/release-4.1.5/ ）
- 取证方式：官方站点逐页 `read_page` + `apache/doris-website` 仓库 `versioned_docs/version-4.x/*.md` + `apache/doris` 仓库源码（FE/BE）。**未使用本仓 research-notes.md 作为证据**。
- 说明：4.x 文档站未给出版本号级语义，"4.1.5" 的适用性由 4.1.x 文档站 + 4.1.4/4.1.5 Release Notes 共同支撑。

---

## 7 · UNIQUE MoW 与 partial column update

### 7.1 UNIQUE 模型与 Merge-on-Write 的官方语义

**唯一键模型定义**（https://doris.apache.org/docs/4.x/key-features/unique-key/ ，"Last updated on 2026-10-07"）：

> "The Apache Doris Unique Key model is a table model that keeps exactly one row per primary key, enforced at write time by the Merge-on-Write engine. The `UNIQUE KEY(...)` clause in `CREATE TABLE` names the key columns; everything else is a value column. Two rows with the same key cannot coexist. A new write with an existing key replaces the row."

> "**Merge-on-Write (MoW)**: the default backend since 2.1. New writes look up the existing row, flip its row ID in a per-rowset delete bitmap, and append the new row to a fresh rowset."

同页对 MoW 五阶段的官方描述：

> "1. **Buffer the batch.** ... 2. **Look up each key.** ... 3. **Mark the old rows.** Each affected rowset gets the old row IDs flipped on in its delete bitmap. ... 4. **Resolve order with the sequence column.** If `function_column.sequence_col` is set, MoW compares the incoming row's sequence value against the current row's `__DORIS_SEQUENCE_COL__`. The larger value wins. Equal values fall back to load order. The sequence type must be an integer or `DATE`/`DATETIME`. 5. **Publish the new rowset.**"

同页关于部分列更新的定义：

> "**Partial column update**: an upsert that writes only some columns. Apache Doris reads the rest from the existing row and writes a complete row back."

**Merge-on-Write 专页**（https://doris.apache.org/docs/4.x/table-design/data-model/merge-on-write/ ，"Last updated on 2026-06-18"）：

> "Merge-on-write and merge-on-read are the two storage implementations of the Unique Key Model. Both keep Key columns unique and return the same query results, but they resolve duplicate Keys at different times ... Merge-on-write is the default and works best for most workloads."

> "With merge-on-write, Doris resolves duplicate Keys as data is written. For each incoming row, it checks whether the Key already exists and marks any previous version as deleted in a delete bitmap."

> "The `enable_unique_key_merge_on_write` table property controls the implementation."

> "The implementation is fixed at table creation and **cannot be changed later through schema change**, so decide before you create the table."

> "**Use merge-on-write (default)** for most workloads, including real-time updates, dimension synchronization, and any case where query performance matters. It also enables features such as partial column updates."

**Unique Key Model 页**（https://doris.apache.org/docs/4.x/table-design/data-model/unique.html ，"Last updated on 2026-06-18"）：

> "The Unique Key Model keeps Key columns unique: when you insert or update a row, the new data overwrites any existing row with the same Key, so the table always holds the latest version."

> "Declare the primary key with the `UNIQUE KEY` keyword. Merge-on-write is on by default and is right for almost all workloads, so you don't need any extra property"

> "Each row is identified by `(user_id, user_name)`. Writing a row whose Key already exists overwrites it; a row with a new Key is inserted."

### 7.2 `partial_columns` —— 官方文档中它是**导入参数/请求头**，不是建表属性

**关键纠正**：`partial_columns` 在 4.x 官方文档中**不存在于建表属性列表**。它是 Stream Load / Broker Load / Routine Load 的 **HTTP header（导入参数）**，以及 Flink Connector 的 `sink.properties.*`；`INSERT INTO` 走**会话变量**。BE 源码 `be/src/service/http/http_common.h` 中它是 HTTP header 常量：

```cpp
static const std::string HTTP_PARTIAL_COLUMNS = "partial_columns";
static const std::string HTTP_UNIQUE_KEY_UPDATE_MODE = "unique_key_update_mode";
static const std::string HTTP_PARTIAL_UPDATE_NEW_ROW_POLICY = "partial_update_new_key_behavior";
```

而 `apache/doris` FE 的 `fe/fe-core/src/main/java/org/apache/doris/catalog/TableProperty.java` 中，MoW 相关表属性只有：

```java
public boolean getEnableUniqueKeySkipBitmap() {
    return Boolean.parseBoolean(properties.getOrDefault(
        PropertyAnalyzer.ENABLE_UNIQUE_KEY_SKIP_BITMAP_COLUMN, "false"));
}
...
public boolean getEnableUniqueKeyMergeOnWrite() {
    return Boolean.parseBoolean(properties.getOrDefault(
            PropertyAnalyzer.ENABLE_UNIQUE_KEY_MERGE_ON_WRITE, "false"));
}
```

→ **官方未给出**「表属性 `partial_columns`」这一说法的依据；4.x 文档中与部分列更新相关的建表属性是 `enable_unique_key_merge_on_write` 与 `enable_unique_key_skip_bitmap_column`。

**官方文档标题为 "Column Update"**（https://doris.apache.org/docs/4.x/data-operate/update/partial-column-update.htm ，"Last updated on 2026-05-28"；`dev` 版同源 https://doris.apache.org/docs/dev/data-operate/update/partial-column-update/ ，"Last updated on 2026-05-17"），三种开启方式：

- Stream Load / Broker Load / Routine Load（请求头）：

> "Add the following header during the load: `partial_columns:true`"

> "In the `columns` parameter, specify the columns to load ( **all Key columns must be included**, otherwise the update cannot be performed)."

- INSERT INTO（会话变量）：

> "Across all data models, the default behavior of `INSERT INTO` with a subset of columns is a full-row write. To prevent misuse, in the Merge-on-Write implementation `INSERT INTO` still defaults to full-row UPSERT semantics. To enable partial column update semantics, first set the following session variable: `SET enable_unique_key_partial_update=true;`"

- Flink Connector（sink 属性）：

> "When using the Flink Connector, add the following configuration: `'sink.properties.partial_columns' = 'true'`"

> "Specify the columns to load in `sink.properties.column` ( **all Key columns must be included**, otherwise the update cannot be performed)."

（4.x 文档此处写 `sink.properties.column`；4.x 连接器页写的是 `sink.properties.columns`——见 §8.3。）

**能力边界（官方表格）**：

> "| Unique Key (Merge-on-Write) | Fill in the full row at write time | Medium (affected by IO) | High | ... |"

> "The Merge-on-Write implementation has to fill in the full row at write time to guarantee optimal query performance. As a result, partial column updates with Merge-on-Write reduce load performance to some extent."

**因此：可以只更新部分列、其余列保留原值——但必须是「同一批内所有行更新同一组列」**：

> "**Batch constraint:** all rows in the same batch write task (whether a load task or `INSERT INTO`) can update only the same set of columns. To update different columns, split the data into multiple batches. This constraint can be removed with the flexible column update feature described below."

**前置条件（官方 Note 逐字）**：

> "1. In version 2.0, partial column updates are supported only in the Merge-on-Write implementation of Unique Key.
> 2. Starting from version 2.0.2, you can use `INSERT INTO` for partial column updates.
> 3. Partial column updates are not supported on tables that have synchronous materialized views.
> 4. Partial column updates are not supported on tables that are undergoing a Schema Change."

### 7.3 灵活列更新（Flexible Column Update，3.1.0 起）—— 「同一批不同行更新不同列」的官方路径

> "Previously, the partial column update feature in Doris required all rows in a single load to update the same set of columns. **Starting from version 3.1.0**, Doris supports a more flexible update method: **each row in a single load can update a different set of columns**."

> "Note
> 1. Flexible column update supports Stream Load, Routine Load, and any tool that uses Stream Load as the underlying load method (such as the Doris Flink Connector).
> 2. The load file must be in JSON format when using flexible column update."

**建表侧要求**：

> "**New tables:** specify the following properties at table creation to enable Merge-on-Write and add the `bitmap` hidden column required by flexible column update:
> ```
> "enable_unique_key_merge_on_write" = "true"
> "enable_unique_key_skip_bitmap_column" = "true"
> ```"

> "**Existing tables:** for an existing Merge-on-Write table created in an older Doris version, you can enable flexible column update after upgrading Doris with the following statement:
> ```sql
> ALTER TABLE db1.tbl1 ENABLE FEATURE "UPDATE_FLEXIBLE_COLUMNS";
> ```"

**导入侧配置（官方表格逐字）**：

> "| Load method | Configuration item | Value |
> | --- | --- | --- |
> | Stream Load | header | `unique_key_update_mode:UPDATE_FLEXIBLE_COLUMNS` |
> | Flink Doris Connector | sink property | `'sink.properties.unique_key_update_mode' = 'UPDATE_FLEXIBLE_COLUMNS'` |
> | Routine Load | `PROPERTIES` | `"unique_key_update_mode" = "UPDATE_FLEXIBLE_COLUMNS"` |"

**限制（官方逐字，第 5 条尤其关键）**：

> "5. When using flexible column update, the following load parameters **cannot** be specified or enabled:
>    - `merge_type`
>    - `delete`
>    - `fuzzy_parse`
>    - `columns`
>    - `jsonpaths`
>    - `hidden_columns`
>    - `function_column.sequence_col`
>    - `sql`
>    - `memtable_on_sink_node` move-forward
>    - `group_commit`
>    - `where`"

其他限制：

> "1. As with normal partial column updates, flexible column update requires every row to contain **all Key columns**. Rows that do not satisfy this requirement are filtered and counted in `filter rows`. When `filtered rows` exceeds the upper bound allowed by the load's `max_filter_ratio`, the entire load fails."

> "3. Flexible column update is not supported on tables containing Variant columns.
> 4. Flexible column update is not supported on tables that have synchronous materialized views."

### 7.4 新行行为：`partial_update_new_key_behavior`

> "The session variable or load property `partial_update_new_key_behavior` controls the behavior of **newly inserted rows** in partial column updates and flexible column updates.
>
> | Value | Behavior |
> | --- | --- |
> | `ERROR` | The Key of every row must already exist in the table, otherwise the load fails. |
> | `APPEND` | Both updating existing rows and inserting new rows whose Key does not exist are allowed. |"

APPEND 模式下未指定列的取值规则（官方逐字）：

> "1. If the column **has a default value**, the default value is used.
> 2. Otherwise, if the column **allows NULL**, NULL is used.
> 3. Otherwise, the insert fails."

### 7.5 与 `function_column.sequence_col` 的交互

**Sequence 列的官方语义**（https://doris.apache.org/docs/4.x/data-operate/update/unique-update-concurrent-control.html ，"Last updated on 2026-05-17"）：

> "For rows with the same Key columns, replacement is performed based on the Sequence column value: **a larger value replaces a smaller value**, and the reverse does not trigger replacement. This puts the decision about ordering in your hands."

> "In implementation, Doris adds a hidden column `__DORIS_SEQUENCE_COL__`. ... Doris then uses this value to decide which row wins among rows that share the same Key columns."

> "Note: The Sequence column is currently only supported by the Unique model."

属性与类型：

> "| `function_column.sequence_col` | Maps the Sequence column to an existing column in the table | Yes |
> | `function_column.sequence_type` | Specifies only the type of the Sequence column, stored in a hidden column | No |"

> "Supported column types: integer types, `DATE`, and `DATETIME`. **The column type cannot be changed once the table is created.**"

> "**Conclusion**: During imports, Doris compares the Sequence column values across all batches and **writes the record with the largest value into the table**."

**部分列更新 + sequence 列的交互（官方逐字）**：

> "3. **Compatibility with partial column updates**: starting from version 2.0, the Merge-on-Write implementation of Doris Unique Key tables supports partial column updates. In a partial-column-update import, only some columns can be updated each time, so **the Sequence column is not required**:
>    - If the import job includes the Sequence column, the behavior is unchanged.
>    - If the import job does not include the Sequence column, Doris uses the Sequence column value from the matched historical data as the Sequence column value of the updated row.
>    - If no record with the same Key exists in the historical data, Doris automatically fills in `null` or the default value."

> "4. **Correctness guarantee under concurrent imports**: when concurrent imports occur, Doris uses MVCC to guarantee data correctness. If two batches of imports both update different columns of the same Key, after the import job with the lower version number succeeds, the import job with the higher version number uses the rows written by the lower-version job for the same Key to fill in the missing values."

**结论（官方口径）**：普通 partial column update **不要求** sequence 列；sequence 列只是决定「同 key 谁赢」的排序键。**但灵活列更新（flexible column update）明确禁止 `function_column.sequence_col`**（见 §7.3 限制 5）。

### 7.6 Stream Load (JSON) 与 INSERT INTO 的差别

- 官方明确普通部分列更新在 Stream Load / Broker Load / Routine Load 上通过**请求头 `partial_columns:true` + `columns:`** 实现（CSV 示例见官方页）。
- 官方明确 `INSERT INTO` 默认是**整行 UPSERT**，需 `SET enable_unique_key_partial_update=true;` 才变部分列语义。
- **灵活列更新**：官方 Note 2 明确 "The load file must be in JSON format when using flexible column update."，且该模式下 `columns` 参数被禁用——即**只能走 Stream Load(Routine Load) + JSON，不能靠 `columns:` 白名单控制更新列**。
- 官方 4.x 文档对「普通 partial_columns 是否支持 JSON 格式」**没有单独说明**；`partial_columns` 与 JSON 的组合仅在灵活列更新语境下被明确要求为 JSON。→ 该细分点**官方未给出**直接表述。

### 7.7 4.1.5 上 `enable_unique_key_skip_bitmap_column` 与 `UPDATE_FLEXIBLE_COLUMNS` 是否可用

- `enable_unique_key_skip_bitmap_column` 在 apache/doris 源码中确实存在（`PropertyAnalyzer.ENABLE_UNIQUE_KEY_SKIP_BITMAP_COLUMN`，见 §7.2 引用的 `TableProperty.java`），且 4.x 版本化文档（`versioned_docs/version-4.x/data-operate/update/partial-column-update.md`）要求它作为灵活列更新的建表属性。
- 4.1.5 Release Notes（https://doris.apache.org/releases/v4.1/release-4.1.5/ ）中**没有**关于 `enable_unique_key_skip_bitmap_column` / `UPDATE_FLEXIBLE_COLUMNS` 的条目；4.1.x 文档站该页仍在（"Last updated on 2026-05-28"），即 4.1.x 线支持该特性。
- 官方对 4.1.5 明确的支持性证据：4.1.5 Release Notes "Ingestion and Storage" 段含 "Improve write performance through better backpressure control. (#67728)"、"Skip redundant `createLocation` operations in the Group Commit Stream Load sink. (#63685)"，与部分列更新无直接绑定。
- 结论：**在 4.1.x 文档口径下可用**；官方未在 4.1.5 Release Notes 中单列该项（**官方未给出**针对 4.1.5 的逐条声明）。

### 7.8 分区键与主键的官方约束（Doris 侧，附 Paimon 侧事实性对照）

> ⚠️ **与本项目直接相关**：DWD「主键表 + `dt` 分区」的设计属 [05] 票取值的硬约束之一。

#### Doris 侧（官方硬约束）

来源：https://doris.apache.org/docs/4.x/key-features/unique-key/ （"Last updated on Oct 7, 2026"），"When should you use the Apache Doris Unique Key model?" 一节的 **Not a good fit** 列表，逐字：

> "- Using the partition key as a superset of the primary key. **Partition columns must be a subset of the unique key, never the other way around. Otherwise the same key can land in two partitions and dedup breaks.**"

同页 "Not a good fit" 段的其余条目（同一列表，用于对照理解该约束的语境）：

> "- Random or very wide primary keys (UUIDs, long composite keys). ... Floats, doubles, and complex types like `ARRAY`, `MAP`, `STRUCT`, `JSON`, and `VARIANT` are rejected as key columns outright."

同页 "Good fit" 段另有与「列组各自写入」相关的条目（与 §7 的部分列更新呼应）：

> "- Wide-table assembly where each upstream owns a column group and writes only its columns via `partial_columns: true` Stream Load. See [Partial Column Update]."

**Doris 侧约束的方向**：分区列**必须是** unique key 的子集（`PARTITION` 列 ⊆ `UNIQUE KEY` 列）；反之（分区列是主键的超集）不被支持，因为同一 key 可能落进两个分区导致去重失效。

同一约束在 4.x Unique Key Model 页也有对应表述（https://doris.apache.org/docs/4.x/table-design/data-model/unique.html ，"Last updated on 2026-06-18"，Notes 第 4 条）：

> "4. **Partition keys must be a subset of Key columns**: Doris requires this to guarantee data uniqueness."

#### Paimon 侧（方向相反，官方给的是代价路径而非禁止）

来源：https://paimon.apache.org/docs/2.1/primary-key-table/data-distribution （**Paimon 2.1 版本固定文档**，对应 tag release-2.1.0；master 2.2-SNAPSHOT 同页内容一致，仅用于交叉核对），"Pick Partition Fields" 一节，逐字：

> "| Mutable event time | Moving an entity requires a before image for the old partition and an after image for the new one, or a cross-partition index |"

同页 "Cross Partitions Upsert" 一节，逐字：

> "An update crosses partitions when a key's partition value changes and the primary key does not include all partition fields."

> "Fixed and postpone buckets do not maintain this cross-partition mapping. Their inputs must supply a complete changelog, including the retraction of the old partition's row, to preserve uniqueness across partition moves."

> "For complete CDC input, a primary key containing the partition field can retract the old partition's row and insert the new one. Use changelog-producer = input when streaming readers must receive those input before/after records; this producer does not create a missing before image."

（Paimon 2.0 stable 同名页 https://paimon.apache.org/docs/2.0/primary-key-table/data-distribution 有等价表述：cross partition upsert 推荐 `'bucket' = '-1'`，"Performance: For tables with a large amount of data, there will be a significant loss in performance. Moreover, initialization takes a long time."）

**事实性对照（各引各自原文，不做建议）**：

| | Doris | Paimon |
|---|---|---|
| 方向 | 分区列**必须是** unique key 的子集（"Partition columns must be a subset of the unique key, never the other way around"） | 主键**不含**全部分区字段时属 "cross partition upsert"，官方给的是 before/after image 或 cross-partition index 两条**代价路径**（"Moving an entity requires a before image for the old partition and an after image for the new one, or a cross-partition index"），**不是禁止** |
| 违规后果 | 同一 key 落进两个分区，"dedup breaks" | 性能与初始化成本显著上升（dynamic bucket 需建 key→partition 索引）；fixed/postpone bucket 需输入提供完整 changelog |

→ 指针：Paimon 侧的完整取证见 **08a**（本节只做一句指针式引用与对照，不重复取证）。

---

## 8 · Stream Load 2PC 与 label 的分工

### 8.1 Label 与 2PC 的分工——仓库引用的那句逐字核对

**仓库引用句**："The Label mechanism guarantees no duplication for a single transaction, and two-phase commit provides the ability to coordinate no duplication across multiple transactions."

**核对结果：命中，逐字一致。**

来源：https://doris.apache.org/docs/4.x/data-operate/transaction/ （"Last updated on 2026-08-18"），章节 **No Loss, No Duplication**：

> "Doris provides two mechanisms to support no-loss, no-duplication writes. The Label mechanism guarantees no duplication for a single transaction, and two-phase commit provides the ability to coordinate no duplication across multiple transactions."

**Label 机制细节（同页）**：

> "A Doris transaction or load can have a Label set on it. This Label is usually a user-defined string with some business-logic meaning; if it is not set, an internal UUID string is generated. The main purpose of the Label is to uniquely identify a transaction or load task and to ensure that transactions or loads with the same Label are executed successfully only once. The Label mechanism guarantees that loaded data is neither lost nor duplicated. If the upstream data source can guarantee At-Least-Once semantics, then together with the Doris Label mechanism, Exactly-Once semantics can be achieved. A Label is unique within a database."

> "Doris cleans up Labels based on time and count. By default, eviction is triggered once the number of Labels exceeds 2000, and Labels older than 3 days are also evicted by default. After a Label is evicted, a Label with the same name can be executed successfully again, that is, deduplication semantics no longer apply."

**Stream Load Manual 中 label 的 header 描述**（https://doris.apache.org/docs/4.x/data-operate/import/import-way/stream-load-manual/ ，"Last updated on 2026-09-09"）：

> "| label | Specifies the label for this import. Data with the same label cannot be imported more than once. If no label is specified, Doris generates one automatically. ... By default, Doris keeps import job labels for three days; you can adjust the retention period with `label_keep_max_second`. ... It is strongly recommended that the same batch of data uses the same label so that duplicate requests for the same batch are accepted only once, ensuring At-Most-Once semantics. When the import job for a label is in CANCELLED state, the label can be used again. |"

**事务状态机（4.x "Load Transactions" 页）**（https://doris.apache.org/docs/4.x/key-features/load-transaction/ ，源文件 `versioned_docs/version-4.x/key-features/load-transaction.mdx`，"last_update: 2026-05-10"）：

> "**`Label`**: a per-database string that identifies a transaction. Resubmitting the same Label deduplicates the load."

> "**`Stream Load 2PC`**: a Stream Load that stops at PRECOMMITTED until a follow-up HTTP call commits or aborts it."

> "**`Lifecycle states`**: `PREPARE` → `PRECOMMITTED` → `COMMITTED` → `VISIBLE` (or `ABORTED`). Rows are queryable only after `VISIBLE`."

> "1. **Open.** A load opens a transaction in `PREPARE`. The FE assigns a `TxnId` and ties it to the user-supplied or generated Label. A second submission with the same Label short-circuits to the original transaction's result.
> 2. **Write.** BEs receive the data, build segments, and report success. The transaction moves to `PRECOMMITTED` once all expected loads acknowledge.
> 3. **Commit.** A `COMMIT` (or the second-phase `txn_operation: commit` HTTP call) moves the transaction to `COMMITTED`. The data is on disk but not yet readable.
> 4. **Publish.** A background daemon ships a `PublishVersion` task to every replica. When all replicas of every affected tablet acknowledge, the transaction reaches `VISIBLE`."

> "Long-horizon deduplication. Labels are evicted after roughly 3 days or 2,000 entries per database, whichever hits first; reusing one after that does not dedupe."

### 8.2 2PC 端点与 `txn_operation`

**官方 4.x 事务文档 StreamLoad 2PC 章节**（https://doris.apache.org/docs/4.x/data-operate/transaction/#streamload-2pc ）：

> "**1\. Set `two_phase_commit:true` in the HTTP Header to enable two-phase commit.**
> ```shell
> curl  --location-trusted -u user:passwd -H "two_phase_commit:true" -T test.txt http://fe_host:http_port/api/{db}/{table}/_stream_load
> ```"

> "**2\. Trigger a commit operation on the transaction (the request can be sent to either FE or BE).**
> - The transaction can be specified by transaction id:
> ```shell
> curl -X PUT --location-trusted -u user:passwd -H "txn_id:18036" -H "txn_operation:commit" http://fe_host:http_port/api/{db}/{table}/_stream_load_2pc
> {
>       "status": "Success",
>       "msg": "transaction [18036] commit successfully."
> }
> ```
> - The transaction can also be specified by label:
> ```shell
> curl -X PUT --location-trusted -u user:passwd  -H "label:55c8ffc9-1c40-4d51-b75e-f2265b3602ef" -H "txn_operation:commit"  http://fe_host:http_port/api/{db}/{table}/_stream_load_2pc
> {
>       "status": "Success",
>       "msg": "label [55c8ffc9-1c40-4d51-b75e-f2265b3602ef] commit successfully."
> }
> ```"

> "**3\. Trigger an abort operation on the transaction (the request can be sent to either FE or BE).**"（后续同样给出 `txn_id:` 与 `label:` 两种指定方式，`txn_operation:abort`）

**端点名**：官方 4.x 文档给出的路径是 `PUT /api/{db}/{table}/_stream_load_2pc`（`txn_id` / `label` 两种定位方式）。

> ⚠️ **注意路径差异**：更早版本文档（以及部分外部引用）用的是 `.../stream-load2pc`。4.x 事务文档与 4.x 官方「Loading Transaction and Atomicity」页在 abort 示例中出现了 `.../stream-load2pc` 与 `.../_stream_load_2pc` **两种写法并存**（例如 abort 段用 `stream-load2pc`，commit 段用 `_stream_load_2pc`）。→ 官方 4.x 文档对 **commit** 端点明确写作 `_stream_load_2pc`；**abort** 段示例中同时出现 `stream-load2pc` 与 `_stream_load_2pc`。这是官方文档自身的不一致，**官方未给出**统一说明。

BE 源码中的相关 header 常量（`be/src/service/http/http_common.h`）：

```cpp
static const std::string HTTP_TWO_PHASE_COMMIT = "two_phase_commit";
static const std::string HTTP_TXN_ID_KEY = "txn_id";
static const std::string HTTP_TXN_OPERATION_KEY = "txn_operation";
```

### 8.3 Flink Doris Connector 的 2PC 与 sink 选项

**4.x 连接器页 Sink Configuration 表**（https://doris.apache.org/docs/4.x/connection-integration/data-integration/flink-doris-connector/ ，源文件 `versioned_docs/version-4.x/connection-integration/data-integration/flink-doris-connector.md`，"Last updated on 2026-05-22"）：

> "| sink.label-prefix | -- | Y | The label prefix used for Stream load import. In the 2pc scenario, it is required to be globally unique to ensure the EOS semantics of Flink. |"

> "| sink.properties.\* | -- | N | Import parameters for Stream Load. For example, 'sink.properties.column_separator' = ', ' defines the column separator, and 'sink.properties.escape_delimiters' = 'true' means that special characters as delimiters, like \x01, will be converted to binary 0x01. For JSON format import, 'sink.properties.format' = 'json', 'sink.properties.read_json_by_line' = 'true'. For detailed parameters, refer to [here](https://doris.apache.org/docs/2.1/data-operate/import/import-way/stream-load-manual/#load-configuration-parameters). For Group Commit mode, for example, 'sink.properties.group_commit' = 'sync_mode' sets the group commit to synchronous mode. ... |"

> "| sink.enable-2pc | TRUE | N | Whether to enable two-phase commit (2pc). The default is true, ensuring Exactly-Once semantics. For details about two-phase commit, refer to [here](https://doris.apache.org/docs/2.1/data-operate/transaction/#streamload-2pc). |"

> "| sink.enable.batch-mode | FALSE | N | Whether to use the batch mode to write to Doris. After enabling, the writing timing does not rely on Checkpoint, and it is controlled by parameters such as sink.buffer-flush.max-rows, sink.buffer-flush.max-bytes, and sink.buffer-flush.interval. Meanwhile, after enabling, Exactly-once semantics will not be guaranteed, but idempotency can be achieved with the help of the Uniq model. |"

**dev（最新）连接器页 Options 表**（https://doris.apache.org/docs/dev/connection-integration/data-integration/flink-doris-connector/write/ ，"Last updated on 2026-09-14"）：

> "| sink.enable-2pc | TRUE | N | Whether to enable two-phase commit (2pc). The default is true, which guarantees Exactly-Once semantics. For information on two-phase commit, see [Stream Load 2PC](...). |"

> "| sink.enable.batch-mode | FALSE | N | ... Once enabled, Exactly-Once semantics is no longer guaranteed. The Unique model can be used to achieve idempotency. |"

> "**Batch write**: Supported since Connector 1.5.0. It does not depend on Checkpoint: data is cached in memory and flushed according to `sink.buffer-flush.max-rows`, `sink.buffer-flush.max-bytes`, and `sink.buffer-flush.interval`. Enable it with `'sink.enable.batch-mode' = 'true'`. Exactly-Once is not guaranteed; idempotent writes can be achieved with the Unique model."

**4.x 连接器页的部分列更新示例**：

> "'sink.properties.columns' = 'id,name,bank,age', -- Columns that need to be updated
> 'sink.properties.partial_columns' = 'true' -- Enable partial column updates"

（注意此处是 `sink.properties.columns`，而 4.x 的 partial-column-update 页写 `sink.properties.column`——官方两页命名不一致。）

**dev 页的部分列更新示例**（同页 Common Write Scenarios → Partial Column Updates）：

> "Set the Stream Load parameter `partial_columns` to enable partial column updates and list the columns to update in `columns`."

### 8.4 仓库引用句「Under the Unique model, you can also disable 2pc to achieve idempotent writes」核对

**核对结果：仍然存在，逐字一致。**

- **当前所在 URL（dev 文档，已重组）**：https://doris.apache.org/docs/dev/connection-integration/data-integration/flink-doris-connector/faq/ （"Last updated on 2026-09-14"）
- **旧 URL**：`https://doris.apache.org/docs/dev/connection-integration/flink-doris-connector/faq` —— **已失效/被重组**（访问该路径会跳转到 Overview 页，非 FAQ）。
- **4.x 版本化页面**：FAQ 内容已并入 4.x 连接器单页 https://doris.apache.org/docs/4.x/connection-integration/data-integration/flink-doris-connector/ 的 `## FAQ` 段落，句子原样保留（源文件 `versioned_docs/version-4.x/connection-integration/data-integration/flink-doris-connector.md`）。

逐字原文（FAQ 第 3 条）：

> "In 2pc scenarios (Duplicate/Aggregate models), each job's label must be unique, and only when restarting from Checkpoint will the Flink job actively abort the previous txns that have been precommitted successfully but not yet committed. Frequently changing the label and restarting will cause a large number of successfully precommitted txns to be unable to abort, occupying transactions. **Under the Unique model, you can also disable 2pc to achieve idempotent writes.**"

同 FAQ 第 1 条（关闭 2PC 的具体选项）：

> "When Exactly-Once is not required, you can also resolve this by disabling 2PC commit (`sink.enable-2pc=false`) or by using a different `sink.label-prefix`."

**4.x 连接器 README**：https://github.com/apache/doris-flink-connector/blob/master/README.md —— 该仓库 README **不含** sink 选项表（只含版本矩阵、TLS 配置、构建说明），并明确说明：

> "More information about compilation and usage is available in the [Flink Doris Connector documentation](https://doris.apache.org/docs/dev/ecosystem/flink-doris-connector). The website documentation is maintained outside this repository and requires a separate change to keep its compatibility guidance in sync."

→ 即 **sink 选项的权威文档在 doris-website，不在 connector 仓库 README**。

### 8.5 「4.1.4 起 BE 侧 Stream Load 端点不再做全局 LOAD 权限预检」核对

**核对结果：官方有对应表述（表述在 4.x Stream Load Manual 页面，不是 Release Notes）。**

来源：https://doris.apache.org/docs/4.x/data-operate/import/import-way/stream-load-manual/ （"Last updated on 2026-09-09"），"Version note (4.1.4)" 段：

> "Starting from version 4.1.4, the Stream Load endpoints on BE (`PUT /api/{db}/{table}/_stream_load`, `PUT /api/_http_stream`, and `PUT /api/{db}/_stream_load_2pc`) no longer perform a global `LOAD` privilege pre-check. Authorization is handled uniformly on FE, against the database and table actually being written.
>
> As a result, an account that only holds database- or table-level `LOAD_PRIV` (without the global `LOAD` privilege) can now run Stream Load as well as 2PC commit / abort."

**Release Notes 侧佐证**（https://doris.apache.org/releases/v4.1/release-4.1.4/ ，Bugfix → Load & Streaming）：

> "- Avoid applying a global BE authentication precheck to Stream Load (#66629)"

**结论**：仓库表述「4.1.4 起 BE 侧 Stream Load 端点不再做全局 LOAD 权限预检」**与官方一致**；但注意官方精确措辞是「global `LOAD` privilege pre-check」（全局 LOAD 权限预检），**BE 侧的认证（authentication）预检并未整体取消**，只是把授权统一收敛到 FE。同一页另有 4.0.8/4.1.4 的 compute group 路由说明（与权限无关，勿混）。

---

## 官方未给出的项

1. **「表属性 `partial_columns`」** —— 官方 4.x 文档与 `apache/doris` 源码中均无此建表属性；`partial_columns` 是导入 header / `sink.properties.*`；`INSERT INTO` 用会话变量 `enable_unique_key_partial_update`。4.x 建表属性只有 `enable_unique_key_merge_on_write` 与 `enable_unique_key_skip_bitmap_column`。
2. **普通 `partial_columns` 是否支持 JSON 格式** —— 官方仅在**灵活列更新**语境下明确 "The load file must be in JSON format"；普通 partial_columns 的 JSON 用法无独立官方表述。
3. **4.1.5 Release Notes 中 `enable_unique_key_skip_bitmap_column` / `UPDATE_FLEXIBLE_COLUMNS` 的逐条声明** —— 官方未给出（4.1.5 Release Notes 无相关条目；可用性依据是 4.x 版本化文档）。
4. **4.x 官方文档中 2PC abort 端点路径的统一写法** —— 官方文档自身在 `stream-load2pc` 与 `_stream_load_2pc` 之间不一致；commit 段明确为 `_stream_load_2pc`。
5. **「Flink Connector README 中的 2PC 选项表」** —— apache/doris-flink-connector 仓库 README 无该表；官方明确 sink 文档维护在 doris-website 站点。
6. **`sink.properties.column` vs `sink.properties.columns` 的统一** —— 4.x 的 partial-column-update 页写 `sink.properties.column`，4.x 连接器页写 `sink.properties.columns`，官方未澄清。

---

## 引用来源清单

| # | 来源 | URL | 页面版本 / Last updated | 用于 |
|---|---|---|---|---|
| S1 | Apache Doris — Column Update (4.x) | https://doris.apache.org/docs/4.x/data-operate/update/partial-column-update.htm | 2026-05-28 | §7.2/7.3/7.4/7.6 |
| S2 | Apache Doris — Column Update (dev) | https://doris.apache.org/docs/dev/data-operate/update/partial-column-update/ | 2026-05-17 | §7.2/7.3（4.x 同源核对） |
| S3 | Apache Doris — Unique Key Model (4.x) | https://doris.apache.org/docs/4.x/table-design/data-model/unique.html | 2026-06-18 | §7.1 |
| S4 | Apache Doris — Merge-on-Write (4.x) | https://doris.apache.org/docs/4.x/table-design/data-model/merge-on-write/ | 2026-06-18 | §7.1 |
| S5 | Apache Doris — Unique Key (key-features, 4.x) | https://doris.apache.org/docs/4.x/key-features/unique-key/ | 2026-10-07 | §7.1/7.5 |
| S6 | doris-website — `versioned_docs/version-4.x/key-features/unique-key.mdx` | https://github.com/apache/doris-website/blob/master/versioned_docs/version-4.x/key-features/unique-key.mdx | last_update 2026-10-07 | §7.1 |
| S7 | Apache Doris — Concurrent Update Control for the Unique Key Model (4.x) | https://doris.apache.org/docs/4.x/data-operate/update/unique-update-concurrent-control.html | 2026-05-17 | §7.5 |
| S8 | Apache Doris — Transactions (4.x) | https://doris.apache.org/docs/4.x/data-operate/transaction/ | 2026-08-18 | §8.1/8.2 |
| S9 | doris-website — `versioned_docs/version-4.x/key-features/load-transaction.mdx` | https://github.com/apache/doris-website/blob/master/versioned_docs/version-4.x/key-features/load-transaction.mdx | last_update 2026-05-10 | §8.1 |
| S10 | Apache Doris — Stream Load Manual (4.x) | https://doris.apache.org/docs/4.x/data-operate/import/import-way/stream-load-manual/ | 2026-09-09 | §8.1/8.2/8.5 |
| S11 | Apache Doris — Flink Doris Connector (4.x) | https://doris.apache.org/docs/4.x/connection-integration/data-integration/flink-doris-connector/ | 2026-05-22 | §8.3/8.4 |
| S12 | doris-website — `versioned_docs/version-4.x/connection-integration/data-integration/flink-doris-connector.md` | https://github.com/apache/doris-website/blob/master/versioned_docs/version-4.x/connection-integration/data-integration/flink-doris-connector.md | sha 1e3bcff | §8.3/8.4 |
| S13 | Apache Doris — Flink Doris Connector: Writing Data to Doris (dev) | https://doris.apache.org/docs/dev/connection-integration/data-integration/flink-doris-connector/write/ | 2026-09-14 | §8.3 |
| S14 | Apache Doris — Flink Doris Connector FAQ (dev) | https://doris.apache.org/docs/dev/connection-integration/data-integration/flink-doris-connector/faq/ | 2026-09-14 | §8.4 |
| S15 | Apache Doris — Release 4.1.5 | https://doris.apache.org/releases/v4.1/release-4.1.5/ | — | §7.7 |
| S16 | Apache Doris — Release 4.1.4 | https://doris.apache.org/releases/v4.1/release-4.1.4/ | — | §8.5 |
| S17 | apache/doris — `be/src/service/http/http_common.h` | https://github.com/apache/doris/blob/master/be/src/service/http/http_common.h | master @ 1d03523 | §7.2/§8.2 |
| S18 | apache/doris — `fe/fe-core/src/main/java/org/apache/doris/catalog/TableProperty.java` | https://github.com/apache/doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/catalog/TableProperty.java | master @ ea770b2 | §7.2/§7.7 |
| S19 | apache/doris-flink-connector — README.md | https://github.com/apache/doris-flink-connector/blob/master/README.md | sha 3444f3e | §8.4 |
| S20 | Apache Doris — CREATE TABLE (4.x) | https://doris.apache.org/docs/4.x/sql-manual/sql-statements/table-and-view/table/CREATE-TABLE/ | 2026-09-16 | §7.5（sequence_col 属性表） |
| S21 | Apache Doris — Unique Key (key-features, 4.x) · **分区键约束 Not a good fit 段** | https://doris.apache.org/docs/4.x/key-features/unique-key/ | 2026-10-07 | §7.8 |
| S22 | Apache Doris — Unique Key Model (4.x) · Notes 第 4 条 | https://doris.apache.org/docs/4.x/table-design/data-model/unique.html | 2026-06-18 | §7.8 |
| S23 | Apache Paimon — Data Distribution (2.1) · Cross Partitions Upsert / Pick Partition Fields | https://paimon.apache.org/docs/2.1/primary-key-table/data-distribution | 2.1（tag release-2.1.0） | §7.8（对照，指针详见 08a） |
| S24 | Apache Paimon — Data Distribution (2.0 stable) | https://paimon.apache.org/docs/2.0/primary-key-table/data-distribution | 2.0 | §7.8（对照，指针详见 08a） |
