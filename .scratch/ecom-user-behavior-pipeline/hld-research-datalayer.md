# HLD 前置调研 — 数据层候选的复核与替代面

> 调研日期：2026-10-19 ｜ 性质：HLD 前置调研（数据层复核）｜ 上游：FSR/SRS/GDD 冻结项 ｜ 状态：一手核查，未实测的已标注

**边界声明**：本文件**不推翻**任何冻结项。仅复核一手官方证据现状，并为替代面补「决定性弃选理由 + 若真要用要多付的代价」。**未实测**项一律显式标注。

**本机只读事实**（2026-10-19，未实测任何容器）：

| 项 | 实测值 / 命令 |
| --- | --- |
| Docker CLI | `29.8.0`（`docker --version`）。但 **daemon 未运行**：`npipe:////./pipe/dockerDesktopLinuxEngine` 不存在 → **`docker images` 无法执行**（禁止启停，故不启）→ **镜像预拉状态无法核查**，归入 §未决 |
| 数据盘自由空间 | `C:` 9.6 GB 可用（92% 已用）；`F:` 78 GB 可用（88% 已用，`df -h`） |
| Docker Desktop 数据目录 | `CustomWslDistroDir = F:\Docker Desktop\DockerDesktopWSL`（据 `settings-store.json`）→ **WSL 数据盘在 F: 侧，不占 C:** |
| JDK 现状 | `java -version` = **18.0.2**（Oracle，2022-07-19），`JAVA_HOME=E:\space\09-tools\Jdk1.8`；在 `C:\Program Files\Java`、`Eclipse Adoptium`、`Microsoft` 三处**均未见 JDK17 安装**，PATH 上只有 Jdk1.8 与 Java 18 javapath |

> ⚠️ **JDK 双风险（新增，官方一手）**：Flink-Doris-Connector 26.3.0 README 版本表要求 Flink 2.2.x → **JDK 17**（JDK 21 experimental；「JDK 17 remains the recommended runtime」）；**Doris 3.0+ 起要求 Java 17**（官方 "Java environment check" 表：`3.0 (inclusive) and later → Java 17，jdk-17.0.10+`）。本机**既无 JDK17、默认 JDK 又分别是 18 与 8**——这是 P0 环境阶段的真实阻塞项，SRS/FSR/GDD 均未记载。

---

## 1. 复核结论摘要（对 R1 与既有 GDD 事实的处置）

| # | 复核对象 | 结论 | 处置 |
| --- | --- | --- | --- |
| **R1** | Doris Paimon Catalog 官方依赖版本 | **仍然成立**。Doris 4.x 页（页面 **Last updated on Oct 16, 2026**）原话：「The currently dependent Paimon version is **1.4.2** (1.3.1 before 4.1.4)」 | **维持** Catalog 直读 = 可选实验。无新证据推翻，**也无新证据支持升为正式路径** |
| **R7/D2** | `snapshot.num-retained.min` 官方默认 | FSR/GDD 记「官方默认 10」——**确认无误**（2.1 官方页 `Default: 10`） | **维持**显式设 5 |
| **D1/D8** | `dynamic_partition.start` 默认语义 | FSR/GDD 记「默认 -2147483648 ＝ 历史分区不删除」——**确认无误**（4.x 官方页原话逐字一致） | **维持**显式设 `-30`。**但见 §3 新发现 (a)** |
| **D9** | 近似 UV 降级 | 维持 | 无新证据 |
| — | **新发现** | **4 条**，见 §3 与 §6。其中 (a)(b) 影响 P0/P2 施工，(c)(d) 影响替代面与烟测 | 建议进 `spec.md` 收口 |

---

## 2. Paimon 2.1.0 一手确认

### 2.1 artifact 命名规则与 Flink 版本对应（官方 `docs/2.1/project/download`）

| 引擎版本 | artifact（2.1.0） | Central 实际存在 |
| --- | --- | --- |
| Flink **2.2** | `paimon-flink-2.2-2.1.0.jar` | ✅ `paimon-flink-2.2/2.1.0`（2026-10-08 18:22） |
| Flink 2.1 / 2.0 / 1.16–1.20 | `paimon-flink-<v>-2.1.0.jar` | ✅ |
| Spark 3.2–4.1 | `paimon-spark-<v>_<scala>-2.1.0.jar` | ✅ |
| Hive 2.1/2.2/2.3/3.1 | `paimon-hive-connector-<v>-2.1.0.jar` | ✅ |
| 文件系统 / API | `paimon-s3`、`paimon-oss`、`paimon-jindo`、`paimon-hadoop-uber`、`paimon-bundle` | ✅ |
| Trino | 官方只写「Download from master」 | ⚠️ **官方未给** 2.1.0 的 Trino 版本号 |

**命名规则**：`paimon-<engine>-<engineVersion>-<paimonVersion>.jar`；仅 Spark 带 Scala 后缀（`_2.12`/`_2.13`），**Flink artifact 不带 Scala 后缀**。

> **结论**：锚定的「Paimon 2.1.0 + Flink 2.2.x」在 artifact 层面**成立且可直接下载**，非 beta——这是主路径能立住的关键一手证据。

### 2.2 系统表原话（`docs/2.1/concepts/system-tables`）

> ⚠️ **订正（2026-10-21，[08] D-5 一手复核·事实类）**：下表给出的列名**不是官方逐字清单**——官方**没有**发布 `$files`/`$snapshots` 的完整列名表，只给出「示例 SQL 中出现的列 ＋ "Additional columns include …" 一句 ＋ snapshot spec 的 camelCase JSON 字段规范」；snake_case ↔ camelCase 的完整映射**官方未给出**（`$files` 官方小表写 `deleteRowCount`，不是 `delete_count`；`file_id` 不在官方小表里；`$schemas` **没有** `schema` JSON 列）。**施工判据若要写死具体列名，只允许用官方示例 SQL 中出现过的列**（`snapshot_id, schema_id, commit_kind, commit_time, total_record_count, delta_record_count` / `partition, bucket, file_path, level, record_count, file_size_in_bytes`），其余字段语义指向官方 Snapshot specification / Data File Metadata 页。

| 系统表 | 官方原话 / 要点 |
| --- | --- |
| `$snapshots` | 「lists retained snapshots and their commit metadata」；列 `snapshot_id, schema_id, commit_kind, commit_time, total_record_count, delta_record_count`，另有 manifest-list 名、`changelog_record_count`、`watermark`、`next_row_id`、`operation`、`writer_version` |
| `$schemas` | 「query the historical schemas」；列 `schema_id, fields, partition_keys, primary_keys, options, comment, update_time`；可与 `$snapshots` 按 `schema_id` join |
| `$options` / `$files` | 前者查 DDL 选项（未显示的即默认）；后者列文件大小/记录数/level/列统计，含 `deleteRowCount`、`first_row_id`、`write_cols` |
| `$ro` | 只扫不需 merge 的文件；**主键表只扫最顶层 level＝最新一次 full compaction 的结果**；官方明确不同 bucket 的 full compaction 发生在不同时刻 → **不同 key 可能来自不同快照** |
| `$audit_log` / `$binlog` | 前者 `rowkind` ∈ `+I/-U/+U/-D`；后者每列是数组，流读打包 `[before, after]`。两者均可开 `table-read.sequence-number.enabled` 附带 `_SEQUENCE_NUMBER`（**不能用 SQL hint 设**） |
| `$tags` / `$branches` / `$consumers` | 保留版本与流式进度 |

**`total_record_count` 官方定义（逐字）**：
> 「`total_record_count` and `delta_record_count` are **unmerged counts calculated from data files, not logical row counts**. All dedicated data files contribute to these values. …… Use `COUNT(*)` when you need the logical row count.」

→ **GDD 1.4 的判据（「有数据」须用 `COUNT(*)`，不得用 `total_record_count`）得到官方原话印证，正确。**

**支持系统表的引擎（官方原话）**：「Currently, **Flink, Spark, Trino and StarRocks** support querying system tables.」
→ ⚠️ **列表中没有 Doris**。GDD 1.4 那条「快照数由 Paimon 官方系统表直接读出」的判据，**只能在 Flink/Spark/StarRocks 侧执行**——本仓若走 Doris，该判据必须走 Flink SQL 客户端，不能写进 Doris 查询。这是判据可执行性的边界，建议写进 spec。

### 2.3 配置默认值（一手）

| 键 | 官方默认 | 与 GDD §A 比对 |
| --- | --- | --- |
| `snapshot.time-retained` | **1 h** | ✅ 一致 |
| `snapshot.num-retained.min` | **10** | ✅ GDD 记「官方默认 10」正确；显式设 5 |
| `snapshot.num-retained.max` | `Integer.MAX_VALUE` | — |
| `snapshot.expire.limit` / `.execution-mode` / `.clean-empty-directories` | 10 / `sync` / `false` | — |
| `compaction.min.file-num` | **5** | ✅ GDD「保持默认」成立 |
| `num-sorted-run.compaction-trigger` | **5** | 新增，未在 GDD |
| `num-sorted-run.stop-trigger` | 未设时 = `compaction-trigger + 3` = **8** | 新增 |
| `full-compaction.delta-commits` | **无静态官方默认**；原话「In Flink streaming writes with this producer, the interval defaults to **one checkpoint** when it is not explicitly configured」 | ⚠️ GDD §A 写「保持官方默认」——严格说**该键没有静态默认**，是「未设＝每 checkpoint」的**动态行为**，且只对 `changelog-producer = full-compaction` 有意义 |
| `changelog-producer` | **none** | 新增 |
| `lookup.cache-max-memory-size` / `bucket` / `precommit-compact` | 256 mb / -1 / `false` | 新增 |

> ⚠️ **与 GDD F11 的措辞冲突**：GDD F11 写「快照数受控为 **5**」。官方语义是 **min = 下限保护**——快照数**不会低于 5**，但在 5 以上由 `time-retained`(1 h) 与 `num-retained.max`(MAX) 控制，**实际数量可远超 5**。正确表述是「**下限 5**、实际由 1 h 窗口决定」，直接影响 §1.4 验收判据。

### 2.4 主键表 upsert / changelog 能力（官方）

| 能力 | 官方表述 |
| --- | --- |
| 主键表本质 | 「Merges records with the same key according to the configured merge engine」；「A primary-key table can keep **multiple physical versions** of a record. Reads and compaction reconcile those versions to produce the logical result」→ 主键表即 upsert 语义 |
| changelog 生产者 | `none`（默认，无完整 before image，下游 upsert sink 按 key 覆盖仍可正确）/ `input`（上游已给完整 changelog）/ `lookup`（查已有行，需 lookup compaction 提交后可见）/ `full-compaction`（对比两次 full compaction 结果） |
| 变更可见性 | `none` 下「Incremental reads expose changes **without complete before images**; they are **not an audit log** of every input record」 |
| ⚠️ 约束 | `lookup` **与 `full-compaction.delta-commits` 不兼容**；deletion-vector 表支持 `none`/`input`/`lookup`，**不支持 `full-compaction`** |
| 读优化代价 | `$ro` 可能读到**旧值**，且不同 key 可能来自不同快照 |

→ **FSR「Iceberg 流式 upsert/changelog 弱于 Paimon 主键表」这一判断，在 Paimon 侧得到官方能力表印证**：Paimon 有四种 changelog producer 且与 merge engine、table mode 有明确组合矩阵。

### 2.5 filesystem catalog 目录布局与 `file://` 磁盘行为

| 项 | 官方事实 |
| --- | --- |
| 默认 metastore | 「`filesystem` (default) — Uses the warehouse filesystem. Set up a catalog without an external metastore service」 |
| 默认目录 | `snapshot/`、`schema/`、`manifest/`、`index/`（基本概念页「Default directories」）；快照为 `snapshot/snapshot-<id>` JSON |
| **关键语义** | 「Readers use metadata references to find the correct files; **a directory listing alone does not tell you which files belong to the current table state**」 |
| `file://` / 迁移 | Filesystems 页：Local File System `file://` → **Pluggable = N，Built-in Support**（无需额外 jar）；快照/清单只是**引用**，`file://` ↔ `s3://` 切换**不自动搬数据**（与 GDD 1.11 一致，详见 §6.2） |

**对 ≤20 GB 磁盘预算的影响（评估，非实测）**：

- 物理删除**只在快照过期时**发生（官方：「Paimon files are deleted physically only when expiring snapshots」）。默认 `snapshot.time-retained=1h` 内所有历史版本文件都留着 → **磁盘峰值 = 1 小时内的全量写入 + 被替换但未过期的旧版本**。
- 主键表在 compaction 前同一 key 有**多份物理版本**（官方明示 A、B 并存到 snapshot 3 才合并）→ **空间放大与写入放大都是真实的**。
- 风险：`snapshot.expire.execution-mode=sync`（默认）下过期删除同步执行；官方警告文件多时「may not be deleted quickly and **back-pressured to the upstream operator**」。保护项：`remove_orphan_files` 默认只删 **older than 1 day** 的孤儿文件。
- **结论**：`file://` 零额外组件、零额外天花板（与 FSR §3 一致），但 **20 GB 预算能否达标取决于 `snapshot.time-retained` 与 compaction 配置**，**本调研未实测**，必须 P2 用真实写入量验证。建议 GDD F11 判据从「快照数 = 5」改为「快照数与磁盘占用受控，且 `snapshot.time-retained` 生效」。

---

## 3. 新发现（4 条，均为官方一手）

### (a) ★ Doris 动态分区有 FE 级总开关，默认 `false` ★

官方 4.x「Dynamic Partitioning」FE Configuration Parameters 表**逐字**：

| Parameter | Default | Description |
| --- | --- | --- |
| `dynamic_partition_enable` | **false** | 「**This parameter affects only partition operations on dynamic partitioned tables**, not on regular tables.」 |
| `dynamic_partition_check_interval_seconds` | 600 | 执行频率（秒） |
| `max_dynamic_partition_num` | 500 | 单次建表可创建的最大分区数 |

**后果**：即使表上写了 `"dynamic_partition.enable" = "true"`，**FE 侧总开关默认关闭时，分区创建/删除调度根本不跑**。GDD 1.5 的验收判据「建表后 `SHOW PARTITIONS` 显示历史分区被**实际删除**」在默认配置下**必然不通过**。D8 只记了表级 `start` 默认值，**漏了 FE 级总开关**——这是 D1/D8 之外的**第三个独立前置条件**。
**建议**：`spec.md` 必须在初始化脚本里显式 `ADMIN SET FRONTEND CONFIG ("dynamic_partition_enable"="true")`，并把该动作写进 `make verify` 分区保留断言的前置。

### (b) ★ 两侧官方文档均回避「Doris 是否支持 Paimon 2.1」★

| 出处 | 原话 |
| --- | --- |
| Paimon 2.1 `/docs/2.1/ecosystem/doris/`（Version 节） | 「**Select a Doris release that supports your catalog backend and Paimon table features.**」——把兼容性判断**推回给 Doris 侧文档**，不给版本矩阵 |
| 同上「Query Optimization」 | 「**check Doris support** before enabling it」（针对 deletion vectors）；「Primary-key tables can require merging multiple row versions」 |
| Doris 4.x paimon-catalog 页 | 「The currently dependent Paimon version is **1.4.2** (1.3.1 before 4.1.4)」；只提「Supports reading Paimon Deletion Vector」 |

→ **两页均无「Doris X 支持 Paimon 2.1」的声明**。R1 的落差**被两侧共同回避**：Paimon 不承诺，Doris 只报依赖版本。这**加强**（而非推翻）R1 的处置；但**任何「Doris 直读 Paimon 2.1 主键表」的乐观假设都没有一手依据**。

### (c) ★ 替代面纠正：ClickHouse **有**官方 Paimon 引擎，但读不了主键表 ★

FSR §3 记「ClickHouse 26.7：**无 Paimon 支持**、官方未给出 Flink connector」。**前半句在一手上不成立**——官方 `reference/engines/table-engines/integrations/paimon` 明示：

- 「This engine provides a **read-only** integration with existing Apache Paimon tables in Amazon S3, Azure, HDFS and **locally stored tables**」；支持 snapshot reads、incremental reads、partition pruning。
- **门槛**：建 `Paimon*` 表被 `allow_experimental_paimon_storage_engine` 控制，**默认 `0`（disabled）**。
- **★决定性证据★**：「Merge-on-read is not implemented, so **primary-key tables cannot be read**: the reader returns the raw union of the snapshot's data files, which still contains the row versions superseded by later upserts. Reading a table whose schema declares `primary-key` **therefore throws**.」
- support-matrix 标 Paimon 为 **Experimental**；Schema evolution ❌、Time travel ❌；增量读另有 **at-most-once** 与 Keeper 依赖。

→ **弃选理由应改写为**：「ClickHouse 官方 Paimon 引擎为 Experimental、只读、且**明确拒绝读主键表**——本仓 DWD/DIM/DWS/ADS 四层都是主键表，故不相容」。比原表述**更强且可引**。
→ 附带：**官方未给出 ClickHouse 的 Flink connector**（FSR 记对了）；「不应低于 8 GB」在官方 sizing 有对应表述（「total memory shouldn't be below 8GB」）。

### (d) ★ 替代面纠正：Hudi 有 Flink 2.0/2.1 bundle，但**没有 flink2.2** ★

Maven Central `org/apache/hudi` 实际目录（一手）：`hudi-flink2.0-bundle` 至 **1.2.1**（2026-09-24）、`hudi-flink2.1-bundle` 至 **1.2.1**、`hudi-flink1.20-bundle` 等 1.x 至 1.2.1；**`hudi-flink2.2-bundle` 不存在**。

→ **Hudi 对 Flink 2.2 的 artifact 缺失，就是「与本仓冻结栈不相容」的决定性官方证据**，与 FSR 记 StarRocks 的理由完全同构（「无 Flink 2.x artifact」）。这是替代面里**最干净的一条**。

**Iceberg 对照（反面证据：Iceberg 并非不可用，只是不合算）**：Central 有 **`iceberg-flink-runtime-2.2/1.12.0`**（2026-09-25）以及 `iceberg-flink-2.2`、`-2.3`。
→ **Iceberg 1.12.0 对 Flink 2.2 的 artifact 是齐的**。FSR「Iceberg 只作对照」的理由因此**不能是版本缺失**，而应回到它自己写的「流式 upsert/changelog 弱于 Paimon 主键表 + 多拼一层组件、多占一份天花板」——**这条表述是对的，且在 artifact 证据下更站得住**。

---

## 4. Doris 4.1.5 关键语义一手确认

| 语义 | 官方原话 / 事实 | 与既有文档比对 |
| --- | --- | --- |
| **UNIQUE MoW** | 「**Merge-on-write is on by default** and is right for almost all workloads, so you don't need any extra property」；「The implementation is fixed at table creation and **cannot be changed later through schema change**」 | ✅ GDD 1.5「保留 UNIQUE KEY 模型（MoW）」成立，且**无需显式写 property** |
| **整行 UPSERT** | 「Doris upserts by Key **no matter which load method you use**」；「**Whole-row UPSERT fills in default values**: even if `INSERT INTO` specifies only some columns, Doris fills the unspecified columns with NULL or default values」 | ✅ 与 GDD 1.5「默认 INSERT INTO 是整行 UPSERT，必须显式开启」**一致** |
| **`partial_columns`** | Stream Load / Broker Load / Routine Load 用 header `partial_columns:true`，且 `columns` 里**必须含全部 Key 列**。**INSERT INTO 路径的开关是 session 变量**：官方原话「**Across all data models, the default behavior of `INSERT INTO` with a subset of columns is a full-row write.** To prevent misuse, in the Merge-on-Write implementation `INSERT INTO` still defaults to full-row UPSERT semantics. **To enable partial column update semantics, first set the following session variable: `SET enable_unique_key_partial_update=true;`**」 | ⚠️ **GDD §A / 1.5 只记了 `partial_columns=true`（Stream Load 侧），漏了 `INSERT INTO` 路径的 session 变量 `enable_unique_key_partial_update`**。两条路是两个不同开关，spec 必须分清。★ **订正（2026-10-21，[08] D-9·事实类）**：`partial_columns` **不是建表属性**（4.x 官方文档与 `apache/doris` 源码均无此表属性；4.x 相关建表属性只有 `enable_unique_key_merge_on_write` 与 `enable_unique_key_skip_bitmap_column`）；且普通 `partial_columns` 受 **Batch constraint** 限制（官方逐字 "all rows in the same batch write task … can update only the same set of columns"）⇒ 同批多列组须用 3.1.0+ 灵活列更新（JSON＋`skip_bitmap_column`，**禁用 `sequence_col`**）。已落进 gdd §1.5（spec §⑦ #18） |
| **Flink Connector 侧** | `'sink.properties.partial_columns' = 'true'`（+ `sink.properties.column` 指定列，含全部 Key 列） | 新增，与 `REQ-F-RT-06` 的 2PC 写入路径直接相关 |
| **灵活列更新 / 分区键 / 同批列集** | 3.1.0+ 灵活列更新需 `"enable_unique_key_skip_bitmap_column" = "true"`，Flink 侧 `'sink.properties.unique_key_update_mode' = 'UPDATE_FLEXIBLE_COLUMNS'`，Routine Load 侧格式必须 JSON；「**Partition keys must be a subset of Key columns**」；「all rows in the same batch write task … can update only the **same set of columns**」（灵活列更新可解） | ⚠️ 影响 ADS/DWS 建表：`(window_start, 维度)` 作 unique key 时，分区列必须是它的子集 |
| **`dynamic_partition.start` 默认** | 「The start offset …, expressed as a negative number. **The default value is `-2147483648`, which means historical partitions are not dropped.**」 | ✅ D8 **逐字确认正确** |
| **30 天保留的正确写法** | `PROPERTIES("dynamic_partition.enable"="true", "dynamic_partition.time_unit"="DAY", "dynamic_partition.start"="-30", "dynamic_partition.end"="<正数>", "dynamic_partition.prefix"="p", "dynamic_partition.create_history_partition"="true")` ← 官方 Example 1 就是这个形状（`-7`/`3`）。**注意 `end` 是必填**，且 `prefix` 必填 | GDD 只写 `start=-30`，**spec 需补 `time_unit`/`end`/`prefix`/`create_history_partition` 四个必填/关键项** |
| **历史分区** | 「historical partitions are **not created by default** … Only partitions after the current time are created. To create all partitions at once, enable `create_history_partition`」 | ⚠️ 不设 `create_history_partition=true` 时，30 天历史分区**不会建** |
| **`max_dynamic_partition_num`** | FE 默认 **500**；超限则**拒绝建表** | 新增风险点 |
| **FE 总开关** | `dynamic_partition_enable` 默认 **false** | ★ 见 §3(a) ★ |
| **标签 vs 2PC** | 「The **Label mechanism guarantees no duplication for a single transaction**, and **two-phase commit provides the ability to coordinate no duplication across multiple transactions**」 | ✅ D1 引文**逐字确认** |
| **Label 淘汰** | 「By default, eviction is triggered once the number of Labels exceeds **2000**, and Labels older than **3 days** are also evicted by default. After a Label is evicted, a Label with the same name can be executed successfully again, that is, **deduplication semantics no longer apply**」 | ⚠️ **新增**：label 去重**不是永久的**。`REQ-F-VERIFY-02/03` 若依赖 label 做长期幂等，**3 天/2000 条后会失效**——兜底实际落在 MoW 整行 upsert 上，与 D1 的「兜底层」定位自洽 |
| **Stream Load 2PC / 隔离级别 / 事务混用** | 端点 `PUT /api/{db}/{table}/_stream_load_2pc`，带 `txn_id` **或** `label` + `txn_operation=commit\|abort`，Header `two_phase_commit:true`。唯一隔离级别 **READ COMMITTED**，且「It **cannot currently see changes made by previous statements in the same transaction**」。两种事务写模式（`INSERT INTO SELECT`/`UPDATE`/`DELETE` 与 `INSERT INTO VALUES`）**cannot be mixed**；跨库写入报 `Transaction insert must be in the same database`；JDBC 需 `useLocalSessionState=true` | ✅ D1 端点记录正确；隔离级别与混用禁令为新增（对离线批写 ADS 有影响） |

### 4.1 FE/BE 单机内存下限 —— **官方给出了**（修正 R4）

FSR §2 / SRS A2 / GDD §A 均记「官方**未给出**多家组件的单机最低内存（Kafka / Flink / **Doris** / Superset）」。**对 Doris 这一项，一手证据显示官方明确给出了下限**（`versioned_docs/version-4.x/install/preparation/env-checking.md`）：

| 场景 | 模块 | 官方最小内存 | 官方推荐内存 |
| --- | --- | --- | --- |
| 通用硬件检查 | 整机 | **CPU cores × 4 GB** | CPU cores × 8 GB |
| 内存检查 | **FE** | **16 GB** | 64 GB+ |
| 内存检查 | **BE** | **CPU cores × 4 GB** | CPU cores × 8 GB |
| **开发/测试环境**（FE+BE 同机） | Frontend | CPU 8 核 / **内存 8 GB** / 磁盘 SSD·SATA 10 GB+ | — |
| **开发/测试环境** | Backend | CPU 8 核 / **内存 16 GB** / 磁盘 SSD·SATA 50 GB+ | — |
| 生产环境 | FE / BE | — | 16 核+ / 64 GB+ 各 1 / 3 实例 |
| 存储 | BE | 总量 × 3 × 1.4 倍 | — |
| 官方原话 | 「**Doris does not enforce a memory limit.**」 | | |

→ ⚠️ **这是本仓最尖锐的资源冲突**：官方「开发与测试环境」口径是 **FE 8 GB + BE 16 GB**，而 FSR §2 给 Doris 的预算是 **~3.0 GB**（全链路 ≈8 GB）。FSR 的 3 GB 是**设计目标，远低于官方 dev/test 下限**。
→ **建议**：FSR/SRS A2 的「官方未给出 Doris 内存」表述**应更正**为「官方给出，且远高于本仓预算」；这份冲突必须写进 §8 风险登记（R3/R4 的修订），在 P0 用 `docker stats` 实测裁决，而不是继续以「官方未给出」为由规避。
→ 同时注意：Doris 要求 **AVX2**（官方：「Doris uses AVX2 vectorization to accelerate queries」，并提供 no-AVX2 包）；磁盘按官方口径需 **10 GB+**。

---

## 5. 替代面：决定性官方证据 + 若真要用要多付的代价

> 只作对照与弃选理由，**不改锚点**。

### 5.1 湖存储

| 候选 | **决定性官方证据** | 若真要用要多付的组件与内存代价 | 三轴 |
| --- | --- | --- | --- |
| **Iceberg 1.12.0** | **artifact 不缺**（`iceberg-flink-runtime-2.2/1.12.0` 存在）→ 决定性问题回到**能力**：流式 upsert/changelog 弱于 Paimon 主键表 | 多一份 metadata catalog（HMS/Nessie/REST 任选）+ 改写全部五层 DDL 与作业；无 Paimon「主键表四选一 changelog producer」的等价矩阵 | A 可比；B 官方未给内存，但多一个 catalog 服务；C **破**——表名/库名与「单一 catalog」全部重写 |
| **Hudi 1.2.1** | **★artifact 缺失★**：Central 有 `hudi-flink2.0-bundle`、`hudi-flink2.1-bundle`，**无 `hudi-flink2.2-bundle`** → 与锚定 Flink 2.2.x 不相容 | —（版本层即不相容，无需评估内存） | C **直接破**，与 `FSR §4` 链条冲突 |

### 5.2 OLAP

| 候选 | **决定性官方证据** | 若真要用要多付的组件与内存代价 | 三轴 |
| --- | --- | --- | --- |
| **StarRocks 4.1.3** | 页首 **「Beta feature」** +「**You can only use Paimon catalogs to query data. You cannot use Paimon catalogs to drop, delete, or insert data into your Paimon cluster.**」；**叠加** Flink connector v1.2.16 只有 flink-1.16~1.20 jar，**无 Flink 2.x artifact** | +BE/CN 常驻；**Paimon 只读**意味着写入路径仍必须由 Flink+Paimon 承担 → 组件不减、预算再压一份 | A 高（有类型映射/参数表）；B **官方未给出**单机下限；C **破**——无 Flink 2.x connector |
| **ClickHouse 26.7** | **★决定性★**「Merge-on-read is not implemented, so **primary-key tables cannot be read** … Reading a table whose schema declares `primary-key` **therefore throws**」；建表被 `allow_experimental_paimon_storage_engine`（**默认 0**）gated，support matrix 标 **Experimental**，Schema evolution / Time travel **❌** | 组件不减（仍需 Flink+Paimon 做 upsert 与 changelog）；要额外写 MergeTree 落地表 + 处理增量读的 **at-most-once** 与 Keeper 依赖 | A 可引（逐条 settings/limits）；B 官方 sizing「total memory shouldn't be below 8GB」，**单这一项吃满预算**；C **破**——四层主键表读不了 |
| **DuckDB 1.5.x** | **无官方 Paimon 支持**：访问来自**社区扩展** `community_extensions/extensions/paimon`（维护者 `mrdrivingduck`，基于 `paimon-cpp`），**非官方引擎**；且官方定位是**进程内单写入者**（「One process writes. Other processes can attach only as `READ_ONLY`」） | 与 Superset **不相容**：Superset 走 SQLAlchemy，需要可被独立进程并发连接的服务器；DuckDB 的「单写入者 + 其它进程只读且看不到后续写入」直接冲掉 `REQ-F-RT-06` 持续写入 + `REQ-F-BI-02` ≤10 s 刷新 | A 扩展文档质量尚可但**非官方**；B 极轻、MIT；C **破**——并发模型与看板供数路径冲突 |

> **许可证更正（本轮新增）**：DuckDB 引擎为 **MIT**（`LICENSE`，Copyright 2018–2026 Stichting DuckDB Foundation），**无 Commons Clause / 无 BSL**——FSR 未把它列为候选，故此条只作对照。
> **StarRocks 许可证**：FSR §3 的更正（Apache-2.0，排除原因是版本相容性而非许可证）**本轮未被推翻**；本轮进一步把「版本相容性」细化为两条可引事实：**Paimon Catalog = Beta 只读** + **无 Flink 2.x connector artifact**。

---

## 6. catalog 与存储载体

### 6.1 filesystem vs HMS vs JDBC vs REST（官方 `docs/2.1/concepts/catalog`）

| Metastore | 元数据落点 | 官方「Typical reason to choose it」 | 额外组件 | 与本仓冻结栈 |
| --- | --- | --- | --- | --- |
| **`filesystem`（默认）** | warehouse 文件系统内 | 「Set up a catalog without an external metastore service」 | **无** | ✅ **默认选择**（零额外组件、零额外天花板） |
| `hive` | Hive Metastore 注册库表 | 与 Hive 兼容工具共享元数据 | **+HMS 常驻** | ⚠️ 额外天花板；官方还提示「By default, Paimon does **not** synchronize newly created partitions into Hive metastore」→ 要分区裁剪需 `metastore.partitioned-table = true` |
| `jdbc` | 关系库（MySQL/PG） | 库支撑的 catalog | **+MySQL/PG 常驻** | ⚠️ 额外天花板；额外持久化 `paimon_views` 表 |
| `rest` | 远端 REST 服务 | 服务端托管后端逻辑与鉴权 | **+REST catalog 服务** | ⚠️ 额外天花板。Paimon 侧 REST 与 Doris 侧 REST 是两件事：Doris 的 `paimon.catalog.type='rest'` 官方示例仅给 DLF（需 3.1+ 与 token provider） |

**并发/原子发布的额外约束（官方）**：`filesystem` catalog 下「Catalog capabilities differ. See … **Concurrency Control** for snapshot publication and **locking requirements, especially with multiple writers on object storage**」→ 本仓单机单写入者是安全的；若将来多作业并发写同一张表，需按 concurrency-control 页配置。**本项目当前单写入者假设成立**。

### 6.2 `file://` ↔ `s3://(MinIO)` 的确切配置点

| 项 | `file://`（默认） | `s3://`（MinIO 可选 profile） |
| --- | --- | --- |
| warehouse | `'warehouse' = 'file:///path/to/warehouse'` | `'warehouse' = 's3://<bucket>/<path>'` |
| 额外 jar | **无**（Local File System `file://` → Pluggable = **N**，Built-in） | **必须** `paimon-s3-2.1.0.jar`（放 Flink `lib/`） |
| 额外配置 | 无 | `'s3.endpoint'`、`'s3.access-key'`、`'s3.secret-key'`（临时凭据另加 `'s3.session-token'`） |
| **MinIO 专用** | 无 | ★官方原话★「Some S3 compliant object stores might not have virtual host style addressing enabled by default, for example when using **Standalone MinIO for testing purpose**. In such cases, you will have to provide the property to enable **path style access**: `s3.path.style.access: true`」 |
| 性能坑 | 无 | 官方给出 `ConnectionPoolTimeoutException` → `'fs.s3a.connection.maximum'=1000` 的处置 |
| Doris 侧读同一 warehouse | Doris 支持 `FileSystem` metastore | Doris 支持 `MINIO`；需 `paimon.s3.*` 前缀透传（`paimon.s3.xxx` → `fs.s3a.xxx` 自动转换） |

**迁移代价（GDD 1.11 已记事实的确认）**：切换 profile 只改 **warehouse 指向**与 **jar/凭据**，**同一份数据不自动迁移**（Paimon 快照只存引用，官方明确 directory listing 不足以判定表状态）→ 必须重新写入或用 `dump` 重放。**本轮无新增推翻**，但补上一条官方坑：**MinIO 必须开 `s3.path.style.access: true`**，否则按默认 virtual-host 寻址会失败——这是 profile 落地时的必填项，建议写进 spec。

---

## 7. 逐候选三条轴（A 友好度 / B 资源 / C 兼容）

| 候选 | A. agent 友好度 | B. 本机资源占用 | C. 与既有接缝 | 合计 |
| --- | --- | --- | --- | --- |
| **Paimon 2.1.0**（已冻结） | **高**：键名/默认值可逐字引（`snapshot.num-retained.min=10`、`compaction.min.file-num=5`、`num-sorted-run.compaction-trigger=5`）；系统表原话齐全；官方 DDL/下载/文件系统页齐全；**但错误体的机读性官方未给**（无错误码表）；**无官方 compose** | **官方未给出 Paimon 自身内存/磁盘下限**；`file://` 走 Built-in，无额外 jar。磁盘行为见 §2.5（**未实测**） | **高**：`paimon-flink-2.2-2.1.0.jar` **实际存在**；与 Flink 2.2.x 链条闭合；单机单写入者并发假设成立；表名库名不动 | ✅ 维持 |
| **Doris 4.1.5**（已冻结） | **中高**：DDL 模板、参数表、事务/2PC 端点可逐字引；`SHOW DYNAMIC PARTITION TABLES` 提供可机读状态列（`State`/`LastDropPartitionMsg`）。**但**官方无 docker-compose（G2/D15 已记），且**错误是否可机读官方未给系统性说明** | ⚠️ **官方给出且冲突**：dev/test FE **8 GB** + BE **16 GB**（§4.1），预算仅 ~3 GB；磁盘官方口径 FE 10 GB+ / BE 50 GB+。**要求 AVX2** | **高**：Connector 26.3.0 README 覆盖 Flink 2.2.x；`ecom_ads` 库名可冻结；MoW 默认开启 | ✅ 维持，**但 R4 需更正** |
| Kafka / Flink / Superset | 本轮未复核 | 本轮未复核 | **链条复核**：Flink 2.2 官方 Kafka connector artifact 为 `flink-connector-kafka` version **`5.0.0-2.2`**（2.2 文档页逐字）→ FSR 锚定「Kafka connector 5.0.0」**确认无误** | ✅ 维持 |

**三条轴总结**：A 轴**唯一倒退**是「错误可机读性」——所有候选都缺官方错误码/错误体规范，本项目只能靠**自建 `make verify` 断言**补足（GDD 1.8 已在做）。B 轴**唯一确定的官方冲突**是 **Doris 内存**（§4.1）。C 轴**冻结栈内部是闭合的**（Paimon 2.1.0 ↔ Flink 2.2 ↔ connector 26.3.0 ↔ Doris 4.1.5），**唯一未闭合的是 Doris 直读 Paimon 的版本落差**（R1，两侧文档均回避）。

---

## 8. 对 `make verify` 五层判据的直接影响（供 spec 收口）

| 烟测项 | GDD 现判据 | 本轮复核结果 |
| --- | --- | --- |
| Paimon 五层有行 | 逐层 `SELECT COUNT(*)`，**不用** `total_record_count` | ✅ **官方原话支持**（unmerged, not logical row counts） |
| 快照数受控 | 「快照数受控为 5」 + `SELECT count(*) FROM <table>$snapshots` | ⚠️ **两处要改**：(i) `num-retained.min` 是**下限**，数是 1 h 窗口的函数，**不是 5**；(ii) 系统表**官方支持列表不含 Doris** → 该查询须走 **Flink SQL** |
| Doris ADS 有行 | `SELECT COUNT(*) FROM ecom_ads.<ads_table>`；FE 健康 `online_backend_num == total_backend_num` | ✅ 可执行，无新阻碍 |
| Doris 分区保留 30 天**行为** | 「建表后 `SHOW PARTITIONS` 显示历史分区被**实际删除**」 | ⚠️ **必须补前置**：FE `dynamic_partition_enable=true`（**默认 false**）+ 表级 `create_history_partition=true` + `time_unit`/`end`/`prefix` 必填（§3a、§4） |
| Flink job RUNNING / Superset 健康 | `GET /jobs/overview` → `jobs[].state == "RUNNING"`；`/health` 返回 200 且纯文本 `OK` | 本轮未复核（不变） |

---

## 未决 / 官方未给出 / 不确定

### A. 官方未给出

1. **Paimon 自身的单机内存与磁盘下限**——官方未给出（无任何 quantity 表）。
2. **Paimon 的错误体/错误码规范**——官方未给出系统性文档；系统表页与配置页均无错误码表。**错误可机读性无从引证**。
3. **Paimon 的 Trino 2.1.0 独立 artifact 版本号**——官方下载页只写「Download from master」。
4. **`full-compaction.delta-commits` 的静态默认值**——官方配置表未列静态默认；只有「未显式配置时，Flink 流式写 + 该 producer 下默认 = 一个 checkpoint」的动态表述。
5. **Flink / Superset 单机最低内存、StarRocks 单机容量下限、Iceberg 运行内存**——官方均未给出（Flink/Superset 本轮未复核，维持 FSR 记录）。
6. **Doris 官方 docker-compose**——仍不存在（G2/D15 维持；本轮未见新增）。
7. **Doris 4.1.5 发行说明中 Paimon 依赖是否会再升**——官方页只报当前依赖 1.4.2，**未给路线承诺**。

### B. 不确定 / 未实测（不得当事实使用）

8. **★Doris 内存可行性未实测★**——官方 dev/test 下限（FE 8 GB + BE 16 GB）与本仓 ~3 GB 预算的冲突，**必须 P0 实测裁决**；本轮只做证据对照，未启容器。
9. **Paimon `file://` 在 ≤20 GB 预算下的磁盘峰值未实测**——§2.5 的空间/写放大分析是**基于官方语义的评估**，非实测；P2 必须用真实写入量验证。
10. **本机 Docker 镜像预拉状态不明**——daemon 未运行（且禁止启停），`docker images` 无法执行。
11. **本机 JDK 17 是否存在**——仅核查了三个常见安装目录（未见）与 PATH（只有 Jdk1.8 / Java 18）；**不排除**其它路径或 `sdkman`/`jabba` 管理的 JDK 17。**Flink 与 Doris 都要求 JDK 17**，这是 P0 的**确定性前置工作**（需求确定，只是本机是否已满足未确认）。
12. **Paimon 源码默认值未直接读**——`compaction.min.file-num` 与 `full-compaction.delta-commits` 已用官方配置页与 changelog-producer 页交叉引证（前者 5 明确、后者为动态默认）；**未直接读 `CoreOptions.java`**（三条路由对该文件均失败：api NOT_FOUND / raw TRANSPORT / git not available）。
13. **Doris Paimon Catalog 对 Flink 版 Paimon 表的官方支持范围**——两侧官方文档**均未声明**；Paimon 侧把判断推回 Doris，Doris 侧只报依赖 1.4.2。**R1 的落差在文档层面被回避，不是被解决**。
14. **`hudi-flink2.2-bundle` 是否会在未来补齐**——当前不存在；**官方未给承诺**。
15. **ClickHouse 增量读的 Keeper 依赖对本仓的影响**——仅在考虑替代方案时才相关；**未深入**。
16. **本机 `.wslconfig` 当前实际生效值**——FSR/SRS 记 U1 已执行至 11.68 GB；本轮**未重新执行 `wsl` 复核**（只读了 Docker settings 与磁盘）。若 spec 需要新证据，应单独跑一次只读核验。
