# 08a · Apache Paimon 2.1.0 × Flink 2.2.x —— 一手官方事实调研（Group A）

- **调研时间（访问日期）**：2026-10-20
- **读到的文档版本**：**Paimon 2.1 版本固定文档站** `https://paimon.apache.org/docs/2.1/...`（对应 GitHub tag `release-2.1.0`，commit `3a5ffe7870697e2c2605e63413fb5ac5800aae7e`）。凡引用 Paimon 官方内容，URL 一律用 `/docs/2.1/` 前缀；仅在需要取「生成物 / 源码」时补 `apache/paimon` 仓库对应 tag 的文件链接。
- **对照的 Flink 官方文档版本**：`https://nightlies.apache.org/flink/flink-docs-release-2.2/...`（Flink 2.2 文档站，页面自述 `v2.2.1`）。
- **取数方式**：官方站点页用 `read_page` 逐页打开；仓库内文档 markdown 用 `github_file` 读 `apache/paimon` @ `release-2.1.0` 的 `docs/docs/**`；配置默认值读 `docs/generated/core_configuration.html`（该文件由 `CoreOptions` 生成，2.1.0 tag）。**未使用**本仓 `research-notes.md` 作为证据。
- **本文只做事实与逐字原文摘录，不含设计建议。**

---

## 1 · SQL 能力面（Paimon 2.1.0 + Flink 2.2.x）

### 1a. `INSERT INTO` 写入主键表（upsert / 流式写）

**结论**：官方明确 `INSERT INTO` 同时支持 batch 与 streaming 两种模式；写入主键表时由 merge engine 决定同 key 行的合并结果（默认 `deduplicate`）。

逐字原文（SQL Write 页，INSERT INTO 节）：

> `INSERT INTO` supports both batch and streaming mode. Before submitting a streaming writer, enable checkpointing:
>
> ```sql
> SET 'execution.runtime-mode' = 'streaming';
> SET 'execution.checkpointing.interval' = '10 s';
> ```
>
> Data becomes visible through committed snapshots. See [Runtime Configuration](./configuration#checkpointing).
> The streaming sink also performs configured maintenance, including compaction, snapshot
> expiration, and partition expiration.

逐字原文（同页，示例说明主键表的覆盖语义）：

> ```sql
> CREATE TABLE order_totals (
>     order_id BIGINT PRIMARY KEY NOT ENFORCED,
>     amount BIGINT
> );
>
> SET 'execution.runtime-mode' = 'batch';
> INSERT INTO order_totals VALUES (1, 100), (2, 200);
>
> -- With the default deduplicate merge engine, replace order 1's amount.
> INSERT INTO order_totals VALUES (1, 150);
> ```

逐字原文（同页顶部任务表）：

> | Append records or apply changes | [`INSERT INTO`](#insert-into) | Batch or streaming; enable checkpoints for streaming commits. |

URL：<https://paimon.apache.org/docs/2.1/flink/sql-write/>
（仓库内同源 markdown：`https://github.com/apache/paimon/blob/release-2.1.0/docs/docs/flink/sql-write.mdx`）

### 1b. `MERGE INTO`

**结论**：**Paimon 官方没有提供 Flink 标准 SQL `MERGE INTO` 语句**；官方给出的等价物是 **procedure `sys.merge_into`**（Flink 1.18/1.19+ 两套参数形式）与 **action jar 的 `merge_into` 作业**。

逐字原文（Action Jars 页，"Merging into table" 节）：

> Paimon supports "MERGE INTO" via submitting the 'merge_into' job through `flink run`.
>
> :::info
> Important table properties setting:
> 1. Only [primary key table](../primary-key-table/) supports this feature.
> 2. The action won't produce UPDATE_BEFORE, so it's not recommended to set 'changelog-producer' = 'input'.
> :::

逐字原文（Procedures → Table Operations 页 `merge_into` 节）：

> To perform "MERGE INTO" syntax. See [merge_into action](../action-jars#merging-into-table) for details of arguments.
>
> **Syntax**
>
> ```sql
> -- for Flink 1.18
> CALL [catalog.]sys.merge_into('identifier','targetAlias',
>     'sourceSqls','sourceTable','mergeCondition',
>     'matchedUpsertCondition','matchedUpsertSetting',
>     'notMatchedInsertCondition','notMatchedInsertValues',
>     'matchedDeleteCondition');
>
> -- for Flink 1.19 and later
> CALL [catalog.]sys.merge_into(
>     target_table => 'identifier',
>     target_alias => 'targetAlias',
>     source_sqls => 'sourceSqls',
>     source_table => 'sourceTable',
>     merge_condition => 'mergeCondition',
>     matched_upsert_condition => 'matchedUpsertCondition',
>     matched_upsert_setting => 'matchedUpsertSetting',
>     not_matched_insert_condition => 'notMatchedInsertCondition',
>     not_matched_insert_values => 'notMatchedInsertValues',
>     matched_delete_condition => 'matchedDeleteCondition',
>     not_matched_by_source_upsert_condition => 'notMatchedBySourceUpsertCondition',
>     not_matched_by_source_upsert_setting => 'notMatchedBySourceUpsertSetting',
>     not_matched_by_source_delete_condition => 'notMatchedBySourceDeleteCondition');
> ```

逐字原文（同节示例，Flink 1.19+ 命名参数形式，本项目锚定 Flink 2.2 ⇒ 可用）：

> ```sql
> -- for Flink 1.19 and later
> CALL sys.merge_into(
>     target_table => 'default.T',
>     source_table => 'default.S',
>     merge_condition => 'T.id=S.order_id',
>     matched_upsert_setting => 'price=T.price+20',
>     not_matched_insert_values => '*');
> ```

**Flink 侧佐证（官方 Flink 2.2 文档的「已支持语句清单」不含 MERGE）**：

> This page lists all the supported statements supported in Flink SQL for now:
>
> -   [SELECT (Queries)]
> -   [CREATE TABLE, CATALOG, DATABASE, VIEW, FUNCTION]
> -   [DROP TABLE, DATABASE, VIEW, FUNCTION]
> -   [ALTER TABLE, DATABASE, FUNCTION]
> -   [ANALYZE TABLE]
> -   [INSERT]
> -   [UPDATE]
> -   [DELETE]
> -   [DESCRIBE]
> -   [EXPLAIN]
> -   [USE]
> -   [SHOW]
> -   [LOAD]
> -   [UNLOAD]

URL：<https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/table/sql/overview/>

**关于「版本引入时间」**：官方未给出 `MERGE INTO` 的引入版本号 —— **官方未给出**。

URL：<https://paimon.apache.org/docs/2.1/flink/procedures/table-operations/>、<https://paimon.apache.org/docs/2.1/flink/action-jars/>

### 1c. `snapshot.num-retained.min` 及其相关选项

**结论**：`snapshot.num-retained.min` **是**官方文档化的表选项；官方默认值为 **10**；官方 CoreOptions 生成表中 `snapshot.num-retained.max` 默认值为 **infinite**，`snapshot.time-retained` 默认 **1 h**，`snapshot.expire.execution-mode` 默认 **sync**，`snapshot.expire.limit` 在 CoreOptions 表中为 **50**（但 Manage Snapshots 页正文表格写 **10**，两处官方文档不一致，见下方"官方文档内部不一致"）。语义是**下限（minimum）**而非"精确值"。

逐字原文（Manage Snapshots 页，选项表；`@@VERSION@@` 在 2.1 固定站渲染为 2.1.0）：

> | Option | Required | Default | Type | Description |
> | --- | --- | --- | --- | --- |
> | `snapshot.time-retained` | No | 1 h | Duration | The maximum time of completed snapshots to retain. |
> | `snapshot.num-retained.min` | No | 10 | Integer | The minimum number of completed snapshots to retain. Should be greater than or equal to 1. |
> | `snapshot.num-retained.max` | No | Integer.MAX_VALUE | Integer | The maximum number of completed snapshots to retain. Should be greater than or equal to the minimum number. |
> | `snapshot.expire.execution-mode` | No | sync | Enum | Specifies the execution mode of expire. |
> | `snapshot.expire.limit` | No | 10 | Integer | The maximum number of snapshots allowed to expire at a time. |
> | `snapshot.clean-empty-directories` | No | false | Boolean | Whether to try to delete empty directories (e.g. partition and bucket directories) left behind after the data files are deleted during snapshot expiration. Defaults to `false`: empty directories are kept. Enabling it has caveats: HDFS may print exceptions in NameNode, and object stores (OSS/S3) may suffer performance issues due to the extra prefix operations required to list and delete directory markers. |

逐字原文（同页，"minimum vs exact" 的官方解释段）：

> When the number of snapshots is less than `snapshot.num-retained.min`, no snapshots will be expired(even the condition `snapshot.time-retained` meet), after which `snapshot.num-retained.max` and `snapshot.time-retained` will be used to control the snapshot expiration until the remaining snapshot meets the condition.

逐字原文（CoreOptions 生成表，`release-2.1.0` tag，`docs/generated/core_configuration.html`；与上表 `max` / `expire.limit` 默认值表述不同）：

> | `snapshot.expire.execution-mode` | sync | Enum | Specifies the execution mode of expire.<br />Possible values:<br />- "sync": Execute expire synchronously. If there are too many files, it may take a long time and block stream processing.<br />- "async": Execute expire asynchronously. If the generation of snapshots is greater than the deletion, there will be a backlog of files. |
> | `snapshot.expire.limit` | 50 | Integer | The maximum number of snapshots allowed to expire at a time. |
> | `snapshot.num-retained.max` | infinite | Integer | The maximum number of completed snapshots to retain. Should be greater than or equal to the minimum number. |
> | `snapshot.num-retained.min` | 10 | Integer | The minimum number of completed snapshots to retain. Should be greater than or equal to 1. |
> | `snapshot.time-retained` | 1 h | Duration | The maximum time of completed snapshots to retain. |

**官方文档内部不一致（如实记录，不做调和）**：
- `snapshot.num-retained.max` 默认：Manage Snapshots 页写 `Integer.MAX_VALUE`，CoreOptions 生成表写 `infinite`。
- `snapshot.expire.limit` 默认：Manage Snapshots 页写 `10`，CoreOptions 生成表写 `50`。

URL：<https://paimon.apache.org/docs/2.1/maintenance/manage-snapshots/>、<https://paimon.apache.org/docs/2.1/maintenance/configurations/#coreoptions>、<https://github.com/apache/paimon/blob/release-2.1.0/docs/generated/core_configuration.html>

**显式钉住快照数量的官方手段**（`sys.expire_snapshots` procedure）逐字：

> To expire snapshots. Argument:
>
> - `table`: the target table identifier. Cannot be empty.
> - `retain_max`: the maximum number of completed snapshots to retain.
> - `retain_min`: the minimum number of completed snapshots to retain.
> - `older_than`: timestamp before which snapshots will be removed.
> - `max_deletes`: the maximum number of snapshots that can be deleted at once.
> - `options`: the additional dynamic options of the table. It prioritizes higher than original `tableProp` and lower than `procedureArg`.
>
> **Example**
>
> ```sql
> -- for Flink 1.19 and later
> CALL sys.expire_snapshots(`table` => 'default.T', retain_max => 2);
> CALL sys.expire_snapshots(`table` => 'default.T', older_than => '2024-01-01 12:00:00');
> CALL sys.expire_snapshots(`table` => 'default.T', older_than => '2024-01-01 12:00:00', retain_min => 10);
> CALL sys.expire_snapshots(
>     `table` => 'default.T',
>     older_than => '2024-01-01 12:00:00',
>     max_deletes => 10,
>     options => 'snapshot.expire.limit=1'
> );
> ```

**`retain_max` 与表属性 `snapshot.num-retained.max` 是否同一机制的两条入口**：官方未明说二者等价 —— **官方未给出**（官方只分别给出 procedure 参数说明与表属性说明）。

URL：<https://paimon.apache.org/docs/2.1/flink/procedures/snapshots/#expire_snapshots>

### 1d. `SELECT * FROM <table>$snapshots`

**结论**：官方支持，且给出可直接运行的示例。

逐字原文（System Tables 页）：

> The `snapshots` table lists retained snapshots and their commit metadata. For example, inspect the
> most recent commits:
>
> ```sql
> SELECT snapshot_id, schema_id, commit_kind, commit_time,
>        total_record_count, delta_record_count
> FROM my_table$snapshots
> ORDER BY snapshot_id DESC;
> ```

逐字原文（同页，跨 catalog 限定时的写法）：

> In some cases, the table name needs to be enclosed with back quotes to avoid syntax parsing conflicts, for example triple access mode:
> ```sql
> SELECT * FROM my_catalog.my_db.`my_table$snapshots`;
> ```

URL：<https://paimon.apache.org/docs/2.1/concepts/system-tables/#snapshots-table>

### 1e. `file://` warehouse 是否有官方限制

**结论（三点）**：

1. **`file://` 无额外 jar、无额外依赖** —— 官方 Supported FileSystems 表把 Local File System 标为 `Pluggable = N`、`Built-in Support`；Paimon 为 `oss://` / `s3://` 等分别提供 `paimon-oss-2.1.0.jar` / `paimon-s3-2.1.0.jar`，但本地文件系统没有任何对应 jar。
2. **官方对 `file://` 没有给出任何额外限制条款** —— Installation 页那句 "A `file:/tmp/...` warehouse is suitable for the single-machine tutorial." 属**适用性说明**（单机教程适用），不是对 `file://` 的限制；唯一真正的限制条款针对的是**分布式作业**（需共享存储 + 所有 task 可访问的路径），不是针对 `file://` 本身。
3. **官方未给出 `file://` 上的 compaction 成本数字** —— compaction 相关文档页（compaction / dedicated-compaction / action-jars）全篇无耗时、IO、内存、合并文件数的成本数字；官方只给配置类数值，详见 §4.4。

逐字原文（Filesystems 页，页首 —— Paimon 复用 Flink 的插件式文件系统）：

> Apache Paimon utilizes the same pluggable file systems as Apache Flink. Users can follow the
> [standard plugin mechanism](https://nightlies.apache.org/flink/flink-docs-stable/docs/deployment/filesystems/plugins/)
> to configure the plugin structure if using Flink as compute engine. However, for other engines like Spark
> or Hive, the provided opt jars (by Flink) may get conflicts and cannot be used directly. It is not convenient
> for users to fix class conflicts, thus Paimon provides the self-contained and engine-unified
> FileSystem pluggable jars for user to query tables from Spark/Hive side.

逐字原文（Filesystems 页，Supported FileSystems 表 —— `file://` 与 `hdfs://` 为 Pluggable=N/Built-in，对象存储为 Y）：

> | FileSystem | URI Scheme | Pluggable | Description |
> | --- | --- | --- | --- |
> | Local File System | file:// | N | Built-in Support |
> | HDFS | hdfs:// | N | Built-in Support, ensure that the cluster is in the hadoop environment |
> | Aliyun OSS | oss:// | Y | |
> | S3 | s3://, s3a:// | Y | Both schemes are served by `S3AFileSystem` |
> | Tencent Cloud Object Storage | cosn:// | Y | |
> | Microsoft Azure Storage | abfs:// | Y | |
> | Huawei OBS | obs:// | Y | |
> | Google Cloud Storage | gs:// | Y | |
> | Aliyun OSS-HDFS (DLS) | dls:// | Y | Requires `paimon-jindodls` and `paimon-jindo` jars, and the Jindo SDK in the environment (see [OSS](https://paimon.apache.org/docs/2.1/maintenance/filesystems/#oss)) |

**对比（官方逐字，对象存储才需要额外 jar）**：

> Download [paimon-oss-2.1.0.jar](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-oss/2.1.0/paimon-oss-2.1.0.jar).

> Download [paimon-s3-2.1.0.jar](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-s3/2.1.0/paimon-s3-2.1.0.jar).

（Filesystems 页的 `## OSS` 与 `## S3` 节首行；Local File System 一节在官方文档中**不存在**对应 jar 下载。）

逐字原文（Installation 页 Prerequisites + Configure Dependencies）：

> The local quick start uses Paimon's built-in local filesystem; distributed jobs need shared
> storage and the dependencies listed below.

> Set this before starting Flink processes and the SQL client. Use a warehouse on shared storage
> for a distributed cluster. A `file:/tmp/...` warehouse is suitable for the single-machine tutorial.

逐字原文（Quick Start 页，"The tutorial uses a single-machine warehouse at `file:/tmp/paimon`" + 建 catalog 示例）：

> The tutorial uses a single-machine warehouse at `file:/tmp/paimon`.
>
> ```sql
> CREATE CATALOG my_catalog WITH (
>     'type' = 'paimon',
>     'warehouse' = 'file:/tmp/paimon'
> );
>
> USE CATALOG my_catalog;
> ```

逐字原文（SQL DDL 页，"Work with Flink Temporary Tables" 节的分布式限制说明 —— 限制对象是分布式作业，不是 `file://`）：

> For a distributed job, use a path accessible to all tasks and install the required filesystem
> dependencies; see [Installation](./installation).

**注意（官方唯一一条与"local path"相关的措辞，出现在 Generic Catalog 节）**：

> Paimon will use `hive.metastore.warehouse.dir` in your `hive-site.xml`, please use path with scheme.
> For example, `hdfs://...`. Otherwise, Paimon will use the local path.

**依赖表逐字（Installation 页 "Configure Dependencies"，证明 `file://` 不需要 Hadoop 依赖）**：

> The warehouse filesystem and catalog determine the additional dependencies:
>
> | Dependency | When it is needed | Setup |
> | --- | --- | --- |
> | Hadoop libraries | Hadoop-backed filesystem access | Expose installed Hadoop libraries through `HADOOP_CLASSPATH`, or use the [Paimon Hadoop uber jar](https://paimon.apache.org/docs/2.1/maintenance/filesystems#paimon-hadoop-uber-package). |
> | Filesystem connector | HDFS or object storage such as S3 and OSS | Follow [Filesystems](https://paimon.apache.org/docs/2.1/maintenance/filesystems) for the chosen storage. |
> | Hive connector | Hive-backed catalogs | Follow [Creating Hive Catalog](./sql-ddl#creating-hive-catalog). |
> | JDBC driver | JDBC-backed catalogs | Follow [Creating JDBC Catalog](./sql-ddl#creating-jdbc-catalog). |

即：Hadoop 依赖只在 "Hadoop-backed filesystem access" 时才需要；`file://` 不在其列。

URL：<https://paimon.apache.org/docs/2.1/maintenance/filesystems/#supported-filesystems>、<https://paimon.apache.org/docs/2.1/flink/installation/>、<https://paimon.apache.org/docs/2.1/flink/quick-start/>、<https://paimon.apache.org/docs/2.1/flink/sql-ddl/>

### 1f. Paimon 2.1.0 官方支持的 Flink 版本 + `paimon-flink-2.2-2.1.0.jar` 存在性

**结论**：Paimon 2.1.0 分支的 Flink 连接器覆盖 **Flink 1.16–1.20 与 2.0–2.2**；`paimon-flink-2.2-2.1.0.jar` 确实存在于 Maven Central。

逐字原文（Installation 页，Choose an Artifact）：

> Choose the Paimon connector that matches your Flink **minor version**. This branch contains
> connectors for Flink 1.16–1.20 and 2.0–2.2. Use the documentation for your Paimon release when
> installing a released connector.

> | Artifact | Use it for | How to use it |
> | --- | --- | --- |
> | `paimon-flink-<flink-version>-2.1.0.jar` | Flink SQL, Table API, and DataStream reads and writes | Put the matching bundled connector in Flink's `lib` directory. |
> | `paimon-flink-action-2.1.0.jar` | Compaction, ingestion, and other command-line actions | Submit it with `flink run`; see [Action Jars](https://paimon.apache.org/docs/2.1/flink/action-jars). |

> | Version | Type | Jar |
> | --- | --- | --- |
> | Flink 2.2 | Bundled Jar | [paimon-flink-2.2-2.1.0.jar](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-flink-2.2/2.1.0/paimon-flink-2.2-2.1.0.jar) |
> | Flink 2.1 | Bundled Jar | [paimon-flink-2.1-2.1.0.jar](...) |
> | Flink 2.0 | Bundled Jar | [paimon-flink-2.0-2.1.0.jar](...) |
> | Flink 1.20 | Bundled Jar | [paimon-flink-1.20-2.1.0.jar](...) |
> | Flink 1.19 | Bundled Jar | [paimon-flink-1.19-2.1.0.jar](...) |
> | Flink 1.18 | Bundled Jar | [paimon-flink-1.18-2.1.0.jar](...) |
> | Flink 1.17 | Bundled Jar | [paimon-flink-1.17-2.1.0.jar](...) |
> | Flink 1.16 | Bundled Jar | [paimon-flink-1.16-2.1.0.jar](...) |
> | Flink Action | Action Jar | [paimon-flink-action-2.1.0.jar](...) |

逐字原文（同页 Build from Source）：

> ```bash
> # Flink 1.20 example
> mvn -pl paimon-flink/paimon-flink-1.20 -am -Pflink1 -DskipTests install
>
> # Flink 2.2 example
> mvn -pl paimon-flink/paimon-flink-2.2 -am -Pflink2 -DskipTests install
> ```

逐字原文（Ecosystem 页 Compatibility Matrix）：

> | [Flink](../flink/installation) | 1.16–1.20 and 2.0–2.2 | Batch and streaming reads/writes; [DDL](../flink/sql-ddl) and [row changes](../flink/sql-write) have version-specific requirements. |

**Maven Central 版本列表逐字**（`https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-flink-2.2/`）：

> ```
> ../
> 1.4.1/                                            2026-04-19 08:48         -
> 1.4.2/                                            2026-06-04 06:59         -
> 2.0.0/                                            2026-08-04 03:51         -
> 2.1.0/                                            2026-10-08 18:22         -
> maven-metadata.xml                                2026-10-12 09:29       433
> ```

**`2.1.0/` 目录逐字**（`https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-flink-2.2/2.1.0/`）：

> ```
> paimon-flink-2.2-2.1.0-sources.jar                2026-10-08 18:22      8162
> paimon-flink-2.2-2.1.0.jar                        2026-10-08 18:22  57697871
> paimon-flink-2.2-2.1.0.pom                        2026-10-08 18:22      7903
> ```

URL：<https://paimon.apache.org/docs/2.1/flink/installation/>、<https://paimon.apache.org/docs/2.1/ecosystem/>、<https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-flink-2.2/>、<https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-flink-2.2/2.1.0/>

---

## 2 · 系统表（`$snapshots` / `$files` / `$schemas` / `$partitions` / `$tags` / `$options`）

### 2.1 官方给出的"可查询引擎"列表

逐字原文（System Tables 页，"Data System Table" 节）：

> Currently, Flink, Spark, Trino and StarRocks support querying system tables.

**核验结论**：该句**不含 Doris**。本仓 research-notes 若称"Doris 不在列表内"，与官方原文一致。
**但** Paimon 生态页确有 Doris 集成章节，并提示 Doris 侧有各自的 system-table 语法支持：
逐字原文（Ecosystem 页 Compatibility Matrix）：

> | [Doris](./doris#version) | Select a release with the required catalog and reader features | Query existing tables through an external catalog; REST catalog access requires Doris 3.1+. |

逐字原文（Doris 页结尾）：

> Use the Doris guide for time-travel and system-table syntax supported by your release. For
> connection or visibility problems, follow [Connecting Engines](./connecting-engines#troubleshooting).

即：Paimon 自己的 system-tables 页只承认 Flink / Spark / Trino / StarRocks 四种引擎；Doris 的 system-table 支持由 Doris 官方文档决定。

URL：<https://paimon.apache.org/docs/2.1/concepts/system-tables/>、<https://paimon.apache.org/docs/2.1/ecosystem/>、<https://paimon.apache.org/docs/2.1/ecosystem/doris/>

### 2.2 `$snapshots` 的列名与含义

**重要事实**：官方 **没有** 给出一张"$snapshots 完整列名清单"表；官方给的是 **① 示例查询里出现的列** + **② 「Additional columns include …」一句** + **③ Snapshot specification（JSON 字段规范）**。因此本节按官方三种口径分别列出，不臆造。

逐字原文（System Tables 页，Snapshots Table 节）：

> The `snapshots` table lists retained snapshots and their commit metadata. For example, inspect the
> most recent commits:
>
> ```sql
> SELECT snapshot_id, schema_id, commit_kind, commit_time,
>        total_record_count, delta_record_count
> FROM my_table$snapshots
> ORDER BY snapshot_id DESC;
> ```
>
> Additional columns include manifest-list names, `changelog_record_count`, `watermark`,
> `next_row_id`, `operation`, and `writer_version`. Optional metadata can be `NULL`, depending on
> the writer and enabled features. See the [Snapshot specification](./spec/snapshot) for the meaning
> of these fields.

逐字原文（Snapshot specification 页，"Identity and Commit" 表）：

> | Field | Type | Meaning |
> | --- | --- | --- |
> | `version` | Integer | Snapshot JSON format version, currently `3`. |
> | `id` | Long | Snapshot ID. |
> | `uuid` | String, optional | UUID identifying the immutable snapshot; absent in older snapshots. |
> | `schemaId` | Long | Schema ID associated with this snapshot. Data files also carry their own schema IDs. |
> | `commitUser` | String | Writer identity used for commit recovery and deduplication. |
> | `commitIdentifier` | Long | Writer-supplied commit identifier; a write transaction can produce different commit kinds. |
> | `commitKind` | String | `APPEND`, `COMPACT`, `OVERWRITE`, or `ANALYZE`. |
> | `timeMillis` | Long | Commit time in milliseconds since the Unix epoch. |
> | `writerVersion` | String, optional | Paimon version of the writer that created the snapshot. |
> | `operation` | String, optional | Logical operation, such as `WRITE`, `DELETE`, `UPDATE`, or `MERGE`. |
>
> `commitKind` describes the physical commit category. `operation`, when present, records the
> logical operation that produced it; the two fields are not interchangeable.

逐字原文（同页，"Manifest References" 表）：

> | `baseManifestList` | String | Manifest list carrying the data-file state inherited from earlier commits. |
> | `deltaManifestList` | String | Manifest list carrying the data-file changes of this snapshot. |
> | `baseManifestListSize` | Long, optional | Base manifest-list size in bytes. |
> | `deltaManifestListSize` | Long, optional | Delta manifest-list size in bytes. |
> | `changelogManifestList` | String, optional | Manifest list for changelog files produced by this snapshot. |
> | `changelogManifestListSize` | Long, optional | Changelog manifest-list size in bytes. |
> | `indexManifest` | String, optional | Manifest describing the snapshot's table index files. |

逐字原文（同页，"Counts and Additional Metadata" 表）：

> | `totalRecordCount` | Long | Unmerged record count across all live data files. |
> | `deltaRecordCount` | Long | Net change in unmerged records from added and deleted data files. |
> | `changelogRecordCount` | Long, optional | Number of records in this snapshot's changelog files. |
> | `watermark` | Long, optional | Input watermark. It can be absent when neither the commit nor earlier state supplies one. |
> | `statistics` | String, optional | Table statistics file name. |
> | `properties` | Map of strings to strings, optional | Additional snapshot properties. |
> | `nextRowId` | Long, optional | Next row ID used by row tracking. |

**核验结论**：`base_manifest_list` / `delta_manifest_list` / `commit_identifier` / `commit_user` / `commit_time` / `watermark` / `statistics` 这些名字**在官方 system-tables 页与 snapshot spec 页中以 camelCase JSON 字段名出现**（`baseManifestList`、`deltaManifestList`、`commitIdentifier`、`commitUser`、`timeMillis`、`watermark`、`statistics`）；system-tables 页的示例 SQL 中使用的是 snake_case 列名（`snapshot_id`、`schema_id`、`commit_kind`、`commit_time`、`total_record_count`、`delta_record_count`）。官方**没有**给出 snake_case ↔ camelCase 的完整映射表 —— 该映射 **官方未给出**（官方只说 "See the [Snapshot specification](./spec/snapshot) for the meaning of these fields."）。

URL：<https://paimon.apache.org/docs/2.1/concepts/system-tables/#snapshots-table>、<https://paimon.apache.org/docs/2.1/concepts/spec/snapshot/>

### 2.3 `total_record_count` 的官方措辞（重点核验）

逐字原文（System Tables 页，Snapshots Table 节）：

> `total_record_count` and `delta_record_count` are unmerged counts calculated from data files, not
> logical row counts. All dedicated data files contribute to these values. For example, appending `N`
> logical rows to a table with one [dedicated BLOB column](../multimodal-table/blob#storage-layout)
> adds `N` records to the regular data files and `N` records to the BLOB files, so
> `delta_record_count` increases by `2 * N`. Use `COUNT(*)` when you need the logical row count.

逐字原文（Snapshot specification 页，"Interpreting Record Counts" 节）：

> The record counts are calculated per data file and are not logical row counts. For example, a
> Dedicated Format table stores the regular columns and each dedicated BLOB column in separate data
> files. Appending `N` logical rows to a table with one dedicated BLOB column therefore increases the
> `deltaRecordCount` by `2 * N`. Use `COUNT(*)` when you need the logical row count.

**核验结论**：本仓 research-notes 所称「unmerged counts calculated from data files, not logical row counts」**与官方原文逐字一致**（官方原句就是这句话）。官方还额外强调 "All dedicated data files contribute to these values."（专用文件也计入）。

URL：<https://paimon.apache.org/docs/2.1/concepts/system-tables/#snapshots-table>、<https://paimon.apache.org/docs/2.1/concepts/spec/snapshot/>

### 2.4 `$files` 的列名与含义

**重要事实**：官方 **没有** 给出 `$files` 的完整列名清单，只给出「说明 + 一个按列名与含义对照的小表 + 示例查询」。因此本仓 research-notes 里那份 `file_id, file_path, file_format, schema_id, min_key, max_key, level, record_count, file_size_in_bytes, creation_time, delete_count` 清单**并非官方逐字列表**（官方小表里出现的是 `deleteRowCount`，不是 `delete_count`；`file_id` 也不在官方小表里）。

逐字原文（System Tables 页，Files Table 节）：

> The `files` table lists the data files in a snapshot, including their sizes, record counts, levels,
> and column statistics. Select the columns relevant to your investigation:
>
> ```sql
> -- Files in the latest snapshot.
> SELECT partition, bucket, file_path, level, record_count, file_size_in_bytes
> FROM my_table$files;
>
> -- Flink: inspect a specific retained snapshot.
> SELECT partition, bucket, file_path, level, record_count, file_size_in_bytes
> FROM my_table$files /*+ OPTIONS('scan.snapshot-id'='1') */;
> ```
>
> | Columns | Meaning |
> | --- | --- |
> | `file_format`, `schema_id` | File encoding and the schema used to write it. |
> | `min_key`, `max_key` | Key bounds recorded for the file. |
> | `null_value_counts`, `min_value_stats`, `max_value_stats` | Column statistics stored in file metadata. |
> | `min_sequence_number`, `max_sequence_number` | Sequence-number bounds. |
> | `creation_time`, `file_source` | File creation time and origin. |
> | `deleteRowCount` | Delete-row count recorded in the data file metadata. |
> | `first_row_id` | Starting row ID for a file with row-tracking metadata. |
> | `write_cols` | Columns written to the file when a column subset is recorded. |
>
> Optional fields can be `NULL` for older files or when the corresponding feature is not enabled.
> See [Data File Metadata](./spec/manifest#data-file-metadata) for the underlying metadata structure.

**官方未给出**：`$files` 的完整列名清单（含 `file_id`、`file_path`、`file_format`、`level`、`record_count`、`file_size_in_bytes` 的逐字定义表）。官方只给出上表所列的分组含义 + 示例 SQL 里用到的 `partition, bucket, file_path, level, record_count, file_size_in_bytes`，其余字段语义指向 [Data File Metadata](./spec/manifest#data-file-metadata) 规范页。

URL：<https://paimon.apache.org/docs/2.1/concepts/system-tables/#files-table>

### 2.5 `$schemas` 的列名（重点核验）

**核验结论**：官方 `$schemas` 的列名是 **`schema_id | fields | partition_keys | primary_keys | options | comment | update_time`**，**不是** "schema (JSON)" 单一 JSON 列。官方没有 `schema` 这一列。

逐字原文（System Tables 页，Schemas Table 节）：

> You can query the historical schemas of the table through schemas table.
>
> ```sql
> SELECT * FROM my_table$schemas;
>
> /*
> +-----------+--------------------------------+----------------+--------------+---------+---------+-------------------------+
> | schema_id |                         fields | partition_keys | primary_keys | options | comment |       update_time       |
> +-----------+--------------------------------+----------------+--------------+---------+---------+-------------------------+
> |         0 | [{"id":0,"name":"word","typ... |             [] |     ["word"] |      {} |         | 2022-10-28 11:44:20.600 |
> |         1 | [{"id":0,"name":"word","typ... |             [] |     ["word"] |      {} |         | 2022-10-27 11:44:15.600 |
> |         2 | [{"id":0,"name":"word","typ... |             [] |     ["word"] |      {} |         | 2022-10-26 11:44:10.600 |
> +-----------+--------------------------------+----------------+--------------+---------+---------+-------------------------+
> 3 rows in set
> */
> ```
>
> You can join the snapshots table and schemas table to get the fields of given snapshots.
>
> ```sql
> SELECT s.snapshot_id, t.schema_id, t.fields
>     FROM my_table$snapshots s JOIN my_table$schemas t
>     ON s.schema_id=t.schema_id where s.snapshot_id=100;
> ```

URL：<https://paimon.apache.org/docs/2.1/concepts/system-tables/#schemas-table>

### 2.6 `$partitions` / `$tags` / `$options`

逐字原文（System Tables 页，Partitions Table 节）：

> The `partitions` table summarizes each partition's files and record counts. Its `partition` column
> uses a directory-style name, such as `pt=1` or `pt=1/dt=2026-09-10`, rather than the brace-delimited
> partition values shown by some file-level system tables.
>
> ```sql
> SELECT partition, record_count, file_size_in_bytes, file_count,
>        last_update_time, total_buckets, done, deleted_record_count
> FROM my_table$partitions
> WHERE partition = 'pt=1';
> ```
>
> Additional columns include `created_at`, `created_by`, `updated_by`, and `options`. These are
> populated from REST catalog audit information and are `NULL` for non-REST catalogs.
>
> `deleted_record_count` counts records marked deleted by deletion vectors. It is `0` when a
> partition has no deletion vectors and `NULL` when legacy deletion-vector metadata does not
> contain cardinality.

逐字原文（System Tables 页，Tags Table 节）：

> The `tags` table lists tag names and the snapshots they retain. A tag's creation time is separate
> from the commit time of its snapshot:
>
> ```sql
> SELECT tag_name, snapshot_id, schema_id, commit_time, create_time, time_retained
> FROM my_table$tags;
> ```
>
> `record_count` reports the snapshot's record count. `create_time` and `time_retained` can be `NULL`
> when that metadata is absent. See [Manage Tags](../maintenance/manage-tags) for creating tags and
> reading a table at a tag.

逐字原文（System Tables 页，Options Table 节）：

> You can query the table's option information which is specified from the DDL through options table. The options not shown will be the default value. You can take reference to [Configuration](../maintenance/configurations#coreoptions).
>
> ```sql
> SELECT * FROM my_table$options;
>
> /*
> +------------------------+--------------------+
> |         key            |        value       |
> +------------------------+--------------------+
> | snapshot.time-retained |         5 h        |
> +------------------------+--------------------+
> 1 rows in set
> */
> ```

### 2.7 快照过期与物理删除的关系；过期后最新快照是否仍可读

**结论**：数据文件被**物理删除**的唯一时机是快照过期（snapshot expiration）；标记删除（compaction 的 "deleted" 标记）不等于物理删除；最新快照始终是活的（过期只移除较早的快照）。

逐字原文（Manage Snapshots 页，Expire Snapshots 节）：

> Paimon writers generate one or two [snapshot](../concepts/basic-concepts#snapshot) per commit. Each snapshot may add some new data files or mark some old data files as deleted. However, the marked data files are not truly deleted because Paimon also supports time traveling to an earlier snapshot. They are only deleted when the snapshot expires.
>
> Currently, expiration is automatically performed by Paimon writers when committing new changes. By expiring old snapshots, old data files and metadata files that are no longer used can be deleted to release disk space.

逐字原文（同页，"Note that snapshot expiration is also what physically deletes data files dropped by partition expiration"）：

> Note that snapshot expiration is also what physically deletes data files dropped by [partition expiration](./manage-partitions#expiring-partitions). However, the empty partition and bucket directories left behind after the data files are deleted are **not** removed by default. To clean them up, enable `snapshot.clean-empty-directories` (see the option above). This is off by default because on object stores (OSS/S3) the prefix operations needed to delete directory markers can be expensive.

逐字原文（同页，Remove Orphan Files 节 —— 物理删除只发生在过期时）：

> Paimon files are deleted physically only when expiring snapshots. However, it is possible that some unexpected errors occurred
> when deleting files, so that there may exist files that are not used by Paimon snapshots (so-called "orphan files"). You can
> submit a `remove_orphan_files` job to clean them:

逐字原文（Basic Concepts 页，"Follow a Record Through Snapshots" 节）：

> Snapshot 3 replaces the file set without changing the query result. A reader using snapshot 1
> still needs file A and sees `(42, 10)`. Physical deletion must therefore wait until retention no
> longer requires that file. See [snapshot expiration](../maintenance/manage-snapshots#expire-snapshots).

逐字原文（Basic Concepts 页，"Snapshot" 节）：

> Publishing a snapshot makes its changes visible together. Earlier snapshots can still
> reference files that a later snapshot has replaced. Time travel requires those snapshots and
> their files to remain available; see [Manage Snapshots](../maintenance/manage-snapshots).

**关于「过期后最新快照仍可读」**：官方未用一句直接的话写"最新快照永远可读"，但由上面两条官方原文可确证：过期只移除被判定为可过期的旧快照（`snapshot.num-retained.min` 是下限，`snapshot.time-retained` / `num-retained.max` 是上限条件），文件只在对应快照过期时才被物理删除；最新快照的 `LATEST` 提示与它的文件集不参与删除。官方**未给出**"读取一张表在快照过期后仍然成功"这样一句断言 —— 逐字断言 **官方未给出**。

**分区过期与物理删除的官方关系**（Manage Partitions 页）：

> __Note:__ After the partition expires, it is logically deleted and the latest snapshot cannot query its data. But the
> files in the file system are not immediately physically deleted, it depends on when the corresponding snapshot expires.
> See [Expire Snapshots](./manage-snapshots#expire-snapshots).

**过期策略过短的两类官方风险**（Manage Snapshots 页）：

> Please note that too short retain time or too small retain number may result in:
>
> - Batch queries cannot find the file. For example, the table is relatively large and
>   the batch query takes 10 minutes to read, but the snapshot from 10 minutes ago
>   expires, at which point the batch query will read a deleted snapshot.
> - Streaming reading jobs on table files fail to restart.
>   When the job restarts, the snapshot it recorded may have expired. (You can use
>   [Consumer Id](../flink/consumer-id) to protect streaming reading
>   in a small retain time of snapshot expiration).

**异步过期模式的官方说明**：

> By default, paimon will delete expired snapshots synchronously. When there are too 
> many files that need to be deleted, they may not be deleted quickly and back-pressured 
> to the upstream operator. To avoid this situation, users can use asynchronous expiration 
> mode by setting `snapshot.expire.execution-mode` to `async`. However, if your job runs in 
> batch mode, it is not recommended to use asynchronous expiration mode, as the expire task 
> may fail to complete successfully.

**分区过期 procedure**（Procedures → Snapshots 页 `expire_partitions`）逐字：

> To expire partitions. Argument:
>
> - `table`: the target table identifier. Cannot be empty.
> - `expiration_time`: the expiration interval of a partition. A partition will be expired if it's lifetime is over this value. Partition time is extracted from the partition value.
> - `timestamp_formatter`: the formatter to format timestamp from string.
> - `timestamp_pattern`: the pattern to get a timestamp from partitions.
> - `expire_strategy`: specifies the expiration strategy for partition expiration, possible values: 'values-time' or 'update-time' , 'values-time' as default.
> - `max_expires`: The maximum of limited expired partitions, it is optional.
> - `options`: the additional dynamic options of the table. It prioritizes higher than original `tableProp` and lower than `procedureArg`.
>
> **Example**
>
> ```sql
> -- for Flink 1.19 and later
> CALL sys.expire_partitions(
>     `table` => 'default.T',
>     expiration_time => '1 d',
>     timestamp_formatter => 'yyyy-MM-dd',
>     expire_strategy => 'values-time'
> );
> ```

URL：<https://paimon.apache.org/docs/2.1/maintenance/manage-snapshots/>、<https://paimon.apache.org/docs/2.1/maintenance/manage-partitions/>、<https://paimon.apache.org/docs/2.1/concepts/basic-concepts/>、<https://paimon.apache.org/docs/2.1/flink/procedures/snapshots/>

### 2.8 孤儿文件（`file://` 场景常见坑）官方原文

**结论**：官方有 `sys.remove_orphan_files` procedure；但官方**没有**写"孤儿文件会占磁盘"这样的因果句，官方只说它们是"未被任何 snapshot 使用的数据文件/元数据文件"，并给出默认 1 天保护期。

逐字原文（Procedures → Cleanup and Repair 页 `remove_orphan_files`）：

> To remove the orphan data files and metadata files. Arguments:
>
> - `table`: the target table identifier. Cannot be empty, you can use database\_name.\* to clean whole database.
>
> - `olderThan`: to avoid deleting newly written files, this procedure only deletes orphan files older than 1 day by default. This argument can modify the interval.
>
> - `dryRun`: when true, view only orphan files, don't actually remove files. Default is false.
>
> - `parallelism`: The maximum number of concurrent deleting files. By default is the number of processors available to the Java virtual machine.
>
> - `mode`: The mode of remove orphan clean procedure (local or distributed) . By default is distributed.

逐字原文（Manage Snapshots 页，Remove Orphan Files 节）：

> Paimon files are deleted physically only when expiring snapshots. However, it is possible that some unexpected errors occurred
> when deleting files, so that there may exist files that are not used by Paimon snapshots (so-called "orphan files").

逐字原文（Procedures → Snapshots 页 `purge_files`）：

> To clear table with purge files. Argument:
>
> - `table`: the target table identifier. Cannot be empty.
>
> **Syntax**
>
> ```sql
> -- clear table with purge files.
> CALL [catalog.]sys.purge_files('identifier');
> ```

**「孤儿文件会占磁盘」这一因果陈述**：**官方未给出**（官方只说它们不被 snapshot 使用、需被清理）。

URL：<https://paimon.apache.org/docs/2.1/flink/procedures/repair/#remove_orphan_files>、<https://paimon.apache.org/docs/2.1/maintenance/manage-snapshots/#remove-orphan-files>、<https://paimon.apache.org/docs/2.1/flink/procedures/snapshots/#purge_files>

---

## 3 · Catalog 与 Database 语义（Paimon 官方定义）

### 3.1 "catalog" 是什么

逐字原文（Catalog 页）：

> A catalog organizes databases, tables, and their metadata. It lets engines access tables by name
> and centralizes operations such as creating a table or changing its schema. Use a Paimon catalog
> to share tables across jobs and engines.

逐字原文（同页，"Catalogs" 表）：

> | Metastore | Metadata integration | Typical reason to choose it |
> | --- | --- | --- |
> | `filesystem` (default) | Uses the warehouse filesystem. | Set up a catalog without an external metastore service. |
> | `hive` | Registers databases and tables in Hive metastore. | Share table metadata with Hive-compatible tools. |
> | `jdbc` | Stores catalog metadata in a relational database. | Use a database-backed catalog, such as MySQL or PostgreSQL. |
> | `rest` | Sends catalog operations to a REST service. | Access a remote catalog with server-managed backend logic and authentication. |
>
> Catalog capabilities differ. See [Views](./views#catalog-support) for view support and
> [Concurrency Control](./concurrency-control#atomic-publication) for snapshot publication and
> locking requirements, especially with multiple writers on object storage.

逐字原文（Filesystem Catalog 节）：

> Store catalog metadata and table files under the warehouse directory:
>
> ```sql
> CREATE CATALOG my_catalog WITH (
>     'type' = 'paimon',
>     'metastore' = 'filesystem',
>     'warehouse' = 'hdfs:///path/to/warehouse'
> );
> ```

### 3.2 "database" 在 catalog 内是什么

逐字原文（Basic Concepts 页，"Catalog and Schema" 节）：

> A [catalog](./catalog) organizes databases and tables so engines can use names such as
> `my_catalog.my_database.my_table`. The table schema defines field names and types, partition
> keys, primary keys, and options. Schema versions let readers interpret data written before a
> schema change.

### 3.3 Flink SQL 中 `warehouse` 是 catalog 级属性；表默认落在 catalog 的 `default` database

逐字原文（SQL DDL 页，Create Filesystem Catalog 节）：

> The following Flink SQL registers and uses a Paimon catalog named `my_catalog`. Metadata and table files are stored under `hdfs:///path/to/warehouse`.
>
> ```sql
> CREATE CATALOG my_catalog WITH (
>     'type' = 'paimon',
>     'warehouse' = 'hdfs:///path/to/warehouse'
> );
>
> USE CATALOG my_catalog;
> ```
>
> You can define any default table options with the prefix `table-default.` for tables created in the catalog.

逐字原文（同页，Create Table 节 —— **层级 catalog → database → table 的决定性原文**）：

> The following SQL assumes that you have registered and are using a Paimon catalog. It creates a managed table named
> `my_table` with five columns in the catalog's `default` database, where `dt`, `hh` and `user_id` are the primary keys.

逐字原文（同页，Create Table 节开头 —— 托管表删表即删数据）：

> The following examples create managed Paimon tables in the selected catalog. Dropping these
> tables also deletes their table files.

**结论（官方层级）**：`catalog` → `database` → `table`。`warehouse` 属于 **catalog 级**选项；**database 是 catalog 内的命名空间**；未显式 `USE <db>` 时，表落在 catalog 的 `default` database。因此「单一 catalog（库名 `ecom_lake`）」在官方语义下把两个层级混成了一个词：名为 `ecom_lake` 的那一层官方对应 **database**，catalog 本身只能由 `CREATE CATALOG <name>` 命名。

**关于 `CREATE DATABASE` / `USE <db>` 的官方示例**：Paimon 2.1 的 Flink 文档页（sql-ddl / quick-start）**没有** `CREATE DATABASE` 的 Paimon 专属示例；`CREATE DATABASE` 的语法与语义来自 **Flink 官方文档**（Flink 2.2）：

> ## CREATE DATABASE
>
> ```sql
> CREATE DATABASE [IF NOT EXISTS] [catalog_name.]db_name
>   [COMMENT database_comment]
>   [WITH (key1=val1, key2=val2, ...)]
> ```
>
> Create a database with the given database properties. If a database with the same name already exists in the catalog, an exception is thrown.
>
> **IF NOT EXISTS**
>
> If the database already exists, nothing happens.
>
> **WITH OPTIONS**
>
> Database properties used to store extra information related to this database.
> The key and value of expression `key1=val1` should both be string literal.

Flink 官方 CREATE DATABASE 语法来源：<https://github.com/apache/flink/blob/release-2.2/docs/content/docs/dev/table/sql/create.md>（渲染页 <https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/table/sql/create/#create-database>）

### 3.4 两种官方可表达的命名空间形态（均逐字引自官方 DDL）

**(a) 一个 catalog + 一个 database（`ecom_lake`）** —— 官方提供的构件逐字：

> ```sql
> CREATE CATALOG my_catalog WITH (
>     'type' = 'paimon',
>     'warehouse' = 'hdfs:///path/to/warehouse'
> );
>
> USE CATALOG my_catalog;
> ```

（来源：SQL DDL 页 Create Filesystem Catalog 节；`CREATE DATABASE ecom_lake;` 与 `USE ecom_lake;` 走 Flink 官方 CREATE DATABASE / USE 语句，官方 Paimon 页未给此组合示例。）

**(b) 一个 catalog + 每层一个 database（ods / dwd / …）** —— 官方构件逐字：

> ```sql
> USE CATALOG my_catalog;
> ```
>
> ```sql
> CREATE TABLE my_table (
>     user_id BIGINT,
>     item_id BIGINT,
>     behavior STRING,
>     dt STRING,
>     hh STRING,
>     PRIMARY KEY (dt, hh, user_id) NOT ENFORCED
> );
> ```
>
> ```sql
> CREATE TABLE partitioned_events (
>     user_id BIGINT,
>     item_id BIGINT,
>     behavior STRING,
>     dt STRING,
>     hh STRING,
>     PRIMARY KEY (dt, hh, user_id) NOT ENFORCED
> ) PARTITIONED BY (dt, hh);
> ```

（来源：SQL DDL 页 Create Table 节。官方例子中表在 catalog 的 `default` database；换成每层一个 database 需 `USE <db>`。）

**关于 `'primary-key'` / `'partition'` 作为表属性（CTAS 官方写法）** 逐字：

> -- Define a primary key for upsert semantics.
>
> ```sql
> CREATE TABLE keyed_events WITH ('primary-key' = 'dt,hh,user_id')
> AS SELECT * FROM source_events;
> ```
>
> -- Combine primary keys and partitions.
>
> ```sql
> CREATE TABLE partitioned_keyed_events WITH (
>     'primary-key' = 'dt,hh,user_id',
>     'partition' = 'dt'
> ) AS SELECT * FROM source_events;
> ```

**`CREATE TABLE ... WITH ('partition'='dt')` 与 `PARTITIONED BY (dt, hh)` 是否等价**：官方在两处分别给出这两种写法（`PARTITIONED BY` 在 Create Table 节；`'partition' = 'dt'` 在 CTAS 节），但**没有**说明二者等价 —— 逐字断言 **官方未给出**。可以确证的只有：官方在 CTAS 示例中确实使用 `'partition' = 'dt'` 这一表属性键名。

URL：<https://paimon.apache.org/docs/2.1/flink/sql-ddl/>、<https://paimon.apache.org/docs/2.1/concepts/catalog/>、<https://paimon.apache.org/docs/2.1/concepts/basic-concepts/>

---

## 4 · `file://` 上的 compaction

### 4.1 官方 compact 途径（Flink SQL procedure）

逐字原文（Procedures 页，页首）：

> Paimon procedures expose maintenance and metadata operations through Flink SQL `CALL`.
> They are available in **Flink 1.18 and later**. Some procedures submit Flink jobs; configure the
> runtime mode and parallelism before calling them. For command-line submission, see [Action Jars](.../flink/action-jars).

逐字原文（同页，"Calling a Procedure" 节）：

> Select your Paimon catalog with `USE CATALOG`, or qualify the procedure as `catalog_name.sys.procedure_name`.
> Table identifiers in the arguments are normally `database_name.table_name`.
>
> ```sql
> USE CATALOG my_catalog;
> SET 'execution.runtime-mode' = 'batch';
>
> -- Flink 1.19+: use named arguments and omit optional arguments.
> CALL sys.compact(`table` => 'default.t', options => 'sink.parallelism=4');
>
> -- Flink 1.18: select a positional signature and keep its argument order.
> -- Empty strings stand in for unused string arguments in this signature.
> CALL sys.compact('default.t', '', '', '', 'sink.parallelism=4');
> ```
>
> Named arguments require Flink 1.19+. On Flink 1.18, use a supported positional signature from
> the reference; do not replace numeric or Boolean arguments with empty strings.

逐字原文（Procedures → Compaction and Layout 页 `compact` 参数）：

> To compact a table. Arguments:
>
> - `table` (required): the target table identifier.
> - `partitions` (optional): partition filter.
> - `order_strategy` (optional): 'order' or 'zorder' or 'hilbert' or 'none'.
> - `order_by` (optional): the columns need to be sort. Left empty if 'order_strategy' is 'none'.
> - `options` (optional): additional dynamic options of the table. It prioritizes higher than original `tableProp` and lower than `procedureArg`.
> - `where` (optional): partition predicate(Can't be used together with "partitions"). Note: as where is a keyword,a pair of backticks need to add around like `where`.
> - `partition_idle_time` (optional): this is used to do a full compaction for partition which had not received any new data for 'partition_idle_time'. And only these partitions will be compacted. This argument can not be used with order compact.
> - `compact_strategy` (optional): this determines how to pick files to be merged, the default is determined by the runtime execution mode. 'full' strategy only supports batch mode. All files will be selected for merging. 'minor' strategy: Pick the set of files that need to be merged based on specified conditions.
> - `buckets` (optional): fixed-bucket IDs, comma-separated IDs, or inclusive ranges, for example `'0-3,7'`. Omit to include all buckets. Valid only for fixed-bucket tables without sort compaction.

**关于题面里的 `INSERT INTO <catalog>.<db>.$compaction`**：Paimon 2.1 官方文档**没有** `$compaction` 系统表；官方 compaction 入口是 **`CALL sys.compact(...)` / `CALL sys.compact_database(...)`** procedure（以及 action jar 的 `compact` / `compact_database` 作业）。`$compaction` 这一说法 —— **官方未给出**。

### 4.2 官方 compact 途径（Action jar / 命令行）

逐字原文（Dedicated Compaction 页，"Dedicated Compaction Job" 节）：

> By default, Paimon writers will perform compaction as needed during writing records. This is sufficient for most use cases.
>
> Compaction will mark some data files as "deleted" (not really deleted, see 
> [expiring snapshots](./manage-snapshots#expire-snapshots) for more info).
> If multiple writers mark the same file, a conflict will occur when committing the changes. Paimon will automatically
> resolve the conflict, but this may result in job restarts.
>
> To avoid these downsides, users can also choose to skip compactions in writers, and run a dedicated job only for compaction. As compactions are performed only by the dedicated job, writers can continuously write records without pausing and no conflicts will ever occur.

逐字原文（同页，Action Jar tab）：

> ```bash
> <FLINK_HOME>/bin/flink run \
>     /path/to/paimon-flink-action-2.1.0.jar \
>     compact \
>     --warehouse <warehouse-path> \
>     --database <database-name> \
>     --table <table-name> \
>     [--partition <partition-name>] \
>     [--buckets <bucket-ids-or-ranges>] \
>     [--compact_strategy <minor / full>] \
>     [--table_conf <table_conf>] \
>     [--catalog_conf <paimon-catalog-conf> [--catalog_conf <paimon-catalog-conf> ...]]
> ```
>
> * `--compact_strategy` Determines how to pick files to be merged, the default is determined by the runtime execution mode, streaming-mode use `minor` strategy and batch-mode use `full` strategy.
>   * `full` : Only supports batch mode. All files will be selected for merging.
>   * `minor` : Pick the set of files that need to be merged based on specified conditions.
>
> You can use `-D execution.runtime-mode=batch` or `-yD execution.runtime-mode=batch` (for the ON-YARN scenario) to control batch or streaming mode. If you submit a batch job, all
> current table files will be compacted. If you submit a streaming job, the job will continuously monitor new changes
> to the table and perform compactions as needed.

逐字原文（Action Jars 页，页首）：

> Submit the action jar to a running Flink cluster. See [Installation](./installation) for the
> artifact and dependencies. For SQL `CALL` equivalents on Flink 1.18+, see [Procedures](./procedures).
>
> ```bash
> <FLINK_HOME>/bin/flink run \
>  /path/to/paimon-flink-action-2.1.0.jar \
>  <action> \
>  <args>
> ```

### 4.3 `file://` 上是否有官方额外限制 / 本地文件系统注意事项

**结论**：**官方未给出**任何"compaction 在 `file://` 上不可用 / 需额外配置"的限制条款。官方给出的、与本地文件系统相关的事实只有以下几条：

逐字原文（Installation 页）：

> The local quick start uses Paimon's built-in local filesystem; distributed jobs need shared
> storage and the dependencies listed below.

> Use a warehouse on shared storage
> for a distributed cluster. A `file:/tmp/...` warehouse is suitable for the single-machine tutorial.

逐字原文（Filesystems 页，Supported FileSystems 表）：

> | Local File System | file:// | N | Built-in Support |

逐字原文（Compaction 页，"Asynchronous Compaction" 节 —— 官方唯一提到 checkpoint 会被 compaction 拖住的地方）：

> Similarly, the default is synchronous compaction, which may cause checkpoint timeouts.
> You can configure `table_conf` to use [Asynchronous Compaction](../primary-key-table/compaction#asynchronous-compaction).

（该句出现在 Dedicated Compaction 页的 info 块中；Compaction 页对应原文为：）

> Writers normally run compaction in background threads. They can still wait when too many sorted runs accumulate, or when lookup compaction is needed before a commit.

逐字原文（Compaction 页，"Asynchronous Compaction" 节的具体配置示例与免责说明）：

> The following table options illustrate a configuration that relaxes those waits:
>
> ```properties
> num-sorted-run.stop-trigger = 2147483647
> sort-spill-threshold = 10
> lookup-wait = false
> ```
>
> This effectively removes the sorted-run write-stall limit and allows pending work to accumulate.
> It is a throughput-oriented example, not a default recommendation. Size the spill storage and
> monitor file counts, compaction backlog, and read latency before using it.

**关于「`paimon` CLI 的 `compact` action」**：Paimon 2.1 官方提供的是 **Flink Action jar**（`paimon-flink-action-2.1.0.jar`）的 `compact` / `compact_database` 作业；官方另有一个 Python 侧 CLI（`paimon` 命令，PyPaimon 随 `pip install pypaimon` 安装），其官方命令表为 read/explain/get/snapshot/create/import/alter/db/tag/branch/sql，**不含 compact**。逐字原文（PyPaimon CLI 页，"Table Commands" 表）：

> | Read rows, inspect a plan, or search text | [Query and inspect](./cli-query) |
> | Show schemas, snapshots, and partitions | [Table metadata](./cli-query#table-get) |
> | Create a table or import local files | [Create and change tables](./cli-tables) |
> | Alter a schema or table options | [Table alter](./cli-tables#table-alter) |

**结论**：「用 `paimon` CLI 执行 `compact`」—— **官方未给出**（PyPaimon CLI 无 compact 命令；官方 compact 入口是 Flink action jar 与 `sys.compact` procedure）。

### 4.4 compaction 的官方成本数字

**结论（分两层，如实记录）**：

1. **官方未给出** compaction 的 **耗时 / IO / 内存 / 合并文件数** 这类成本数字（wall-clock、带宽、内存占用、一次 compaction 合并多少文件）。官方 compaction 文档全篇没有成本数字。**在 `file://` 上同样没有任何成本数字**（官方未对本地文件系统给出任何专门的 compaction 性能说明）。
2. 但官方 **确实给出** 若干与 compaction 行为直接相关的**配置数值**（属于"官方给出的数字"，但不是"成本数字"）：

逐字原文（CoreOptions 生成表，`release-2.1.0`）：

> | `compaction.min.file-num` | 5 | Integer | For file set [f_0,...,f_N], the minimum file number to trigger a compaction for append-only table. |
> | `compaction.size-ratio` | 1 | Integer | Percentage flexibility while comparing sorted run size for changelog mode table. If the candidate sorted run(s) size is 1% smaller than the next sorted run's size, then include next sorted run into this candidate set. |
> | `compaction.max-size-amplification-percent` | 200 | Integer | The size amplification is defined as the amount (in percentage) of additional storage needed to store a single byte of data in the merge tree for changelog mode table. |
> | `compaction.small-file-ratio` | 0.7 | Double | The ratio of target file size. Files whose size is smaller than target-file-size * compaction.small-file-ratio will be picked for compaction rewriting. |
> | `target-file-size` | (none) | MemorySize | Target size of a file.<br />- primary key table: the default value is 128 MB.<br />- append table: the default value is 256 MB. |
> | `write-buffer-size` | 256 mb | MemorySize | Amount of data to build up in memory before converting to a sorted on-disk file. |
> | `sort-spill-threshold` | (none) | Integer | If the maximum number of sort readers exceeds this value, a spill will be attempted. This prevents too many readers from consuming too much memory and causing OOM. |
> | `dynamic-bucket.target-row-num` | 2000000 | Long | If the bucket is -1, for primary key table, is dynamic bucket mode, this option controls the target row number for one bucket. |

逐字原文（Compaction 页，"Number of Sorted Runs to Trigger Compaction" / "to Pause Writing" 节）：

> `num-sorted-run.compaction-trigger` defaults to `5`. Each Level-0 file counts as one sorted run;
> each occupied higher level counts as one run.

> `num-sorted-run.stop-trigger` limits the backlog at which writers wait for compaction. If unset,
> it defaults to `num-sorted-run.compaction-trigger + 3`.

URL：<https://paimon.apache.org/docs/2.1/flink/procedures/compaction/>、<https://paimon.apache.org/docs/2.1/maintenance/dedicated-compaction/>、<https://paimon.apache.org/docs/2.1/flink/action-jars/>、<https://paimon.apache.org/docs/2.1/primary-key-table/compaction/>、<https://paimon.apache.org/docs/2.1/maintenance/configurations/#coreoptions>

---

## 8 · 主键 / 分区键 / bucket 的官方约束

### 8.1 分区键与主键的关系：官方**没有**禁止主键不含全部分区字段，而是提供「跨分区 upsert」模式

**结论**：官方文档中**没有**"primary key must contain the partition key"这类硬性约束条款。相反，官方在 SQL DDL 页给出提示，说明主键不含全部分区字段时走 **Cross partition upsert** 模式。

逐字原文（SQL DDL 页，Create Table 节，info 块）：

> If you need cross partition upsert (primary keys not contain all partition fields), see [Cross partition Upsert](https://paimon.apache.org/docs/2.1/primary-key-table/data-distribution#cross-partitions-upsert) mode.

逐字原文（Data Distribution 页，"Cross Partitions Upsert" 节）：

> An update crosses partitions when a key's partition value changes and the primary key does not
> include all partition fields. Dynamic buckets (`bucket = -1`) maintain a mapping from each key
> to its partition and bucket, using local disk. Starting a streaming writer initializes this index
> by reading existing keys.
>
> | Merge engine | Existing key arrives with a different partition value |
> | --- | --- |
> | `deduplicate` | Delete the row in the old partition and insert it in the new partition |
> | `partial-update` or `aggregation` | Apply the update in the old partition |
> | `first-row` | Keep the existing row and ignore the incoming row |
>
> The index can add substantial startup and storage costs for large tables. If updates only need
> to match recent data, `cross-partition-upsert.index-ttl` limits the history retained in the index
> and considered during initialization. Choose a TTL that covers the required update history;
> expiring an entry means it can no longer locate an older row through that index.
>
> Fixed and postpone buckets do not maintain this cross-partition mapping. Their inputs must
> supply a complete changelog, including the retraction of the old partition's row, to preserve
> uniqueness across partition moves.

逐字原文（CoreOptions 生成表，`release-2.1.0`）：

> | `cross-partition-upsert.index-ttl` | (none) | Duration | The TTL in local index for cross partition upsert (primary keys not contain all partition fields), this can avoid maintaining too many indexes and lead to worse and worse performance, but please note that this may also cause data duplication. |
> | `cross-partition-upsert.bootstrap-parallelism` | 10 | Integer | The parallelism for bootstrap in a single task for cross partition upsert. |

逐字原文（Data Distribution 页，"Pick Partition Fields" 节）：

> | Candidate | Guidance |
> | --- | --- |
> | Immutable creation time | Usually a good fit: it can be included in the primary key without moving an entity between partitions |
> | Mutable event time | Moving an entity requires a before image for the old partition and an after image for the new one, or a cross-partition index |
> | CDC operation timestamp (`op_ts`) | Usually a poor entity partition key: each change has a new timestamp, which may not identify the old row's partition |
>
> For complete CDC input, a primary key containing the partition field can retract the old
> partition's row and insert the new one.

**「primary key must contain the partition key」这一硬性约束的官方逐字原文**：**官方未给出**（官方未使用该措辞，也未给出任何"必须"式禁令）。

**官方给出的两条"代价路径"（事实归纳，逐字依据在前）**：

1. **把分区字段放进主键** —— 官方逐字（Data Distribution 页 "Pick Partition Fields" 节）：
   > For complete CDC input, a primary key containing the partition field can retract the old
   > partition's row and insert the new one. Use `changelog-producer = input` when streaming readers
   > must receive those input before/after records; this producer does not create a missing before
   > image. See [Changelog Producer](./changelog-producer#input).
2. **走 cross-partition upsert（承担索引/启动/存储代价）** —— 官方逐字（Data Distribution 页 "Cross Partitions Upsert" 节）：
   > The index can add substantial startup and storage costs for large tables. If updates only need
   > to match recent data, `cross-partition-upsert.index-ttl` limits the history retained in the index
   > and considered during initialization.
   >
   > Fixed and postpone buckets do not maintain this cross-partition mapping. Their inputs must
   > supply a complete changelog, including the retraction of the old partition's row, to preserve
   > uniqueness across partition moves.

   以及 CoreOptions 逐字：
   > | `cross-partition-upsert.index-ttl` | (none) | Duration | The TTL in local index for cross partition upsert (primary keys not contain all partition fields), this can avoid maintaining too many indexes and lead to worse and worse performance, but please note that this may also cause data duplication. |

**官方对"分区字段候选"的三档指引（逐字）**：

> | Candidate | Guidance |
> | --- | --- |
> | Immutable creation time | Usually a good fit: it can be included in the primary key without moving an entity between partitions |
> | Mutable event time | Moving an entity requires a before image for the old partition and an after image for the new one, or a cross-partition index |
> | CDC operation timestamp (`op_ts`) | Usually a poor entity partition key: each change has a new timestamp, which may not identify the old row's partition |

**Spark 侧官方对照**（Spark DDL 页，措辞更接近"必须"，但属 Spark 引擎文档）：

> To partition the data by date and hour, declare `PARTITIONED BY`. For a primary key table,
> the primary key must include every partition column:
>
> ```sql
> CREATE TABLE partitioned_events (
>     user_id BIGINT,
>     item_id BIGINT,
>     behavior STRING,
>     dt STRING,
>     hh STRING
> ) PARTITIONED BY (dt, hh)
> TBLPROPERTIES ('primary-key' = 'dt,hh,user_id');
> ```

URL：<https://paimon.apache.org/docs/2.1/flink/sql-ddl/>、<https://paimon.apache.org/docs/2.1/primary-key-table/data-distribution/>、<https://paimon.apache.org/docs/2.1/spark/sql-ddl/>

### 8.2 bucket 固定 vs `bucket = -1`（动态）；bucket-key 约束

逐字原文（Data Distribution 页，"Choose a Bucket Mode" 表）：

> | Mode | `bucket` | Assignment | Planning consideration |
> | --- | --- | --- | --- |
> | Fixed | Positive integer | Hash the bucket key into a fixed number of buckets | Predictable layout; resizing requires an explicit rescale operation |
> | Dynamic (default) | `-1` | Maintain a key-to-bucket index and grow buckets | Index memory and single-writer constraints |
> | Postpone | `-2` | Stage data, then assign real buckets | Batch/compaction workflow and per-partition sizing |

逐字原文（同页，"Fixed Bucket" 节）：

> Set `bucket` to a positive integer. Paimon calculates the bucket using
> `Math.abs(key_hashcode % numBuckets)`.
>
> For primary-key tables, `bucket-key` defaults to the primary key excluding partition columns.
> An explicit bucket key must be a subset of the primary key and must not contain partition columns.
> For example, with primary key `(dt, order_id)` and partition key `dt`, `order_id` is the default
> bucket key.

逐字原文（同页，"Dynamic Bucket" 节）：

> Dynamic buckets are the default for primary-key tables (`bucket = -1`). Paimon maintains an index
> that maps keys to buckets. Existing keys return to their assigned buckets; new keys can be
> assigned to new buckets as the table grows. The layout therefore depends on the arrival of keys.
> Do not configure `bucket-key` in this mode.
>
> | Option | Purpose |
> | --- | --- |
> | `dynamic-bucket.target-row-num` | Target row count per bucket |
> | `dynamic-bucket.initial-buckets` | Initial bucket count |
> | `dynamic-bucket.max-buckets` | Maximum bucket count; use `-1` or a value from `1` to `32768` |
>
> :::warning Concurrent writers
>
> Only one job may write to a given partition in dynamic-bucket mode. Concurrent writers can assign
> the same key independently and create duplicates. `write-only` with a dedicated compaction job
> does not remove this restriction.
> :::
>
> When updates stay within a partition, Paimon uses a hash index for bucket assignment. Budget
> additional memory for active partitions: 100 million entries in a partition require roughly 1 GB
> of index memory. Inactive partitions do not retain this in-memory index. Evaluate this cost
> against the benefit of growing buckets automatically, particularly when most arriving keys are new.

逐字原文（Primary-Key Table 页，"Bucket" 节）：

> A table, or each partition of a partitioned table, contains buckets. Each bucket has its own LSM
> tree of data files. Changelog files and indexes may accompany those data files, depending on the
> configuration.
>
> In fixed-bucket mode, Paimon hashes the `bucket-key` columns to select a bucket. If `bucket-key`
> is not set, it uses the primary-key columns excluding partition columns. Dynamic and postpone
> buckets use different assignment mechanisms; see [Data Distribution](./data-distribution).

逐字原文（Primary-Key Table 页，Quick Start 节 —— **动态 bucket 是省略 `bucket` 时的默认**）：

> This example uses four fixed buckets, the default `deduplicate` merge engine, and the default
> merge-on-read storage mode. Omitting `bucket` selects dynamic buckets.

逐字原文（CoreOptions 生成表，`release-2.1.0`）：

> | `bucket` | -1 | Integer | Bucket number for file store.<br />It should either be equal to -1 (dynamic bucket mode), -2 (postpone bucket mode), or it must be greater than 0 (fixed bucket mode). |
> | `bucket-key` | (none) | String | Specify the paimon distribution policy. Data is assigned to each bucket according to the hash value of bucket-key.<br />If you specify multiple fields, delimiter is ','.<br />If not specified, the primary key will be used; if there is no primary key, the full row will be used. |
> | `bucket-function.type` | default | Enum | The bucket function for paimon bucket.<br />Possible values:<br />- "default": The default bucket function which will use arithmetic: bucket_id = Math.abs(hash_bucket_binary_row % numBuckets) to get bucket.<br />- "mod": The modulus bucket function which will use modulus arithmetic: bucket_id = Math.floorMod(bucket_key_value, numBuckets) to get bucket. Note: the bucket key must be a single field of INT or BIGINT datatype.<br />- "hive": The hive bucket function which will use hive-compatible hash arithmetic to get bucket. |

### 8.3 bucket 数的建议值 / 主键基数与 bucket 数的关系

**结论**：官方 **没有** 给出"bucket 数 = f(主键基数)"的建议值公式；官方给的是**按每 bucket 数据量**给出的初始规模指引（区间而非定值），并明确要求自行测量。

逐字原文（Data Distribution 页，"Bucket Sizing" 节）：

> ### Bucket Sizing
>
> Too many buckets create small files; too few can concentrate writes and leave overlapping ranges
> that are expensive to read. For MOR workloads, 200 MB–1 GB per bucket is an initial sizing guide,
> not a limit. Measure skew, throughput, and read plans to select the actual count. A batch scan can
> split one bucket into independent key ranges, so bucket count is not a hard read-parallelism cap.
>
> To change an existing layout, use the offline [Rescale Bucket](../maintenance/rescale-bucket)
> workflow.

逐字原文（Primary-Key Table 页，"Bucket" 节末尾）：

> Buckets distribute writes and affect read parallelism, but are not a universal one-reader limit:
> [Table Mode](./table-mode) explains when files can be read independently. Use the
> [bucket sizing guidance](./data-distribution#bucket-sizing) to balance parallelism against
> small-file overhead.

**「主键基数与 bucket 数的关系」的具体建议值**：**官方未给出**（官方只给 "200 MB–1 GB per bucket is an initial sizing guide, not a limit"，并要求 "Measure skew, throughput, and read plans to select the actual count."）。

**适用边界（必须标注）**：上面 "200 MB–1 GB per bucket" 这条指引的官方措辞是 **"For MOR workloads"**，即它明确针对 **MOR（merge-on-read）** 工作负载；而**主键表的默认模式是 MOR，不是 MOW**。官方 Table Mode 页逐字：

> | Mode | Configuration | Read path | Main cost |
> | --- | --- | --- | --- |
> | Merge On Read (MOR) | Default | Merge overlapping sorted runs | Read CPU and memory grow with overlapping versions |
> | Copy On Write (COW) | `full-compaction.delta-commits = 1` | Read the fully compacted result | Frequent full compaction increases write amplification |
> | Merge On Write (MOW) | `deletion-vectors.enabled = true` | Read files and skip invalid row positions | Writers look up previous rows and maintain deletion vectors |
>
> For a typical `deduplicate` table with frequent analytical reads, consider MOW. MOR can suit
> workloads that favor write throughput. COW can suit workloads that can afford to fully compact
> each commit. Evaluate update rate, read latency, and compaction resources before choosing.

> MOR is the default. Writes create sorted files, and compaction reduces the number of runs over
> time.

即：**MOW 并非默认**；启用 MOW 需显式设置 `'deletion-vectors.enabled' = 'true'`。官方对 MOW 侧**没有**给出与 "200 MB–1 GB per bucket" 对应的 bucket 规模建议值 —— **官方未给出**。

URL（Table Mode）：<https://paimon.apache.org/docs/2.1/primary-key-table/table-mode/>

### 8.4 官方 DDL 语法示例（主键 + 分区键）

逐字原文（SQL DDL 页，Create Table 节）：

> ```sql
> CREATE TABLE my_table (
>     user_id BIGINT,
>     item_id BIGINT,
>     behavior STRING,
>     dt STRING,
>     hh STRING,
>     PRIMARY KEY (dt, hh, user_id) NOT ENFORCED
> );
> ```
>
> To partition the same row layout by day and hour, create a separate table:
>
> ```sql
> CREATE TABLE partitioned_events (
>     user_id BIGINT,
>     item_id BIGINT,
>     behavior STRING,
>     dt STRING,
>     hh STRING,
>     PRIMARY KEY (dt, hh, user_id) NOT ENFORCED
> ) PARTITIONED BY (dt, hh);
> ```

逐字原文（Primary-Key Table 页，Quick Start 节）：

> ```sql
> CREATE TABLE orders (
>     order_id BIGINT,
>     customer_id BIGINT,
>     amount DECIMAL(12, 2),
>     PRIMARY KEY (order_id) NOT ENFORCED
> ) WITH (
>     'bucket' = '4'
> );
>
> INSERT INTO orders VALUES (1, 101, 12.00);
> INSERT INTO orders VALUES (1, 101, 15.00);
>
> SELECT * FROM orders;
> -- 1, 101, 15.00
> ```

逐字原文（SQL DDL 页，CTAS 节 —— 表属性写法）：

> ```sql
> CREATE TABLE partitioned_keyed_events WITH (
>     'primary-key' = 'dt,hh,user_id',
>     'partition' = 'dt'
> ) AS SELECT * FROM source_events;
> ```

URL：<https://paimon.apache.org/docs/2.1/primary-key-table/>、<https://paimon.apache.org/docs/2.1/primary-key-table/data-distribution/>、<https://paimon.apache.org/docs/2.1/flink/sql-ddl/>

---

## 官方未给出的项（Official does not state）

1. **`MERGE INTO` 的引入版本号** —— 官方未给出。
2. **Flink 标准 SQL `MERGE INTO` 语句在 Paimon 2.1.0 中的支持声明** —— 官方未给出（Paimon 侧只给 `sys.merge_into` procedure 与 `merge_into` action；Flink 2.2 官方"已支持语句清单"中亦无 MERGE）。
3. **`$snapshots` 的完整 snake_case 列名清单**及其与 snapshot JSON camelCase 字段名的映射 —— 官方未给出（官方只给示例中出现的列 + "Additional columns include …" 一句 + snapshot spec 的 JSON 字段规范）。
4. **`$files` 的完整列名清单**（含 `file_id`、`delete_count` 等在本仓 research-notes 中出现的名字）—— 官方未给出；官方小表中的名字是 `deleteRowCount`。
5. **`$schemas` 存在名为 `schema` 的 JSON 列** —— 官方未给出；官方列名是 `fields` / `partition_keys` / `primary_keys` / `options` / `comment` / `update_time`。
6. **"快照过期后读取该表仍然成功"这句直接断言** —— 官方未给出（官方只分别说明过期移除旧快照、文件在快照过期时才被物理删除）。
7. **`sys.expire_snapshots` 的 `retain_max` 与表属性 `snapshot.num-retained.max` 是否为同一机制的两条入口** —— 官方未给出。
8. **`CREATE TABLE ... WITH ('partition' = 'dt')` 与 `PARTITIONED BY (dt, hh)` 是否等价** —— 官方未给出（官方分别给出两种写法）。
9. **Paimon 2.1 的 Flink 文档中 `CREATE DATABASE` / `USE <db>` 的 Paimon 专属示例** —— 官方未给出（Paimon 页只给 `USE CATALOG`；`CREATE DATABASE` 语法来自 Flink 官方文档）。
10. **"primary key must contain the partition key"式的硬性禁令（Flink 侧）** —— 官方未给出；官方反而提供 cross-partition upsert 模式（Spark 侧文档才写 "the primary key must include every partition column"）。
11. **bucket 数与主键基数关系的建议值/公式** —— 官方未给出（官方只给 "200 MB–1 GB per bucket is an initial sizing guide, not a limit"）。
12. **compaction 的耗时 / IO / 内存 / 合并文件数等成本数字** —— 官方未给出。
13. **compaction 在 `file://` 上的专门限制或注意事项条款** —— 官方未给出（官方只说明本地文件系统内建支持、`file:/tmp/...` 适用于单机教程）。
14. **`paimon` CLI 的 `compact` 命令** —— 官方未给出（PyPaimon CLI 无 compact；官方 compact 入口为 Flink action jar 与 `sys.compact`）。
15. **`$compaction` 系统表** —— 官方未给出。
16. **孤儿文件"会占磁盘"的因果陈述** —— 官方未给出（官方只说它们是不被 snapshot 使用的文件）。
17. **`snapshot.num-retained.max` 与 `snapshot.expire.limit` 的唯一权威默认值** —— 官方两处不一致（Manage Snapshots 页 vs CoreOptions 生成表），见 §1c。
18. **MoW 模式下与 "200 MB–1 GB per bucket" 对应的 bucket 规模建议值** —— 官方未给出（官方该指引明写 "For MOR workloads"，且 MOR 才是默认模式，MOW 需显式开 `deletion-vectors.enabled`）。
19. **Paimon 2.1 中独立于 Flink action jar 的 `paimon` 命令行 `compact` 动作** —— 官方未给出（PyPaimon CLI 无 compact；官方 compact 入口为 `paimon-flink-action-2.1.0.jar` + `flink run`，或 `CALL sys.compact(...)`）。
20. **针对 `file://` 的任何专门限制条款** —— 官方未给出（官方只给适用性说明 "A `file:/tmp/...` warehouse is suitable for the single-machine tutorial."，并把 `file://` 标为 Pluggable=N / Built-in Support；"限制"条款的对象是分布式作业而非 `file://`）。
21. **`file://` 上的 compaction 成本数字（耗时 / IO / 内存 / 合并文件数）** —— 官方未给出（compaction 系列文档全篇无成本数字，亦无本地文件系统专门性能说明）。

---

## 引用来源清单

| # | Claim（主张） | URL | 访问日期 |
| --- | --- | --- | --- |
| 1 | `INSERT INTO` 支持 batch/streaming；主键表由 merge engine 决定覆盖 | https://paimon.apache.org/docs/2.1/flink/sql-write/ | 2026-10-20 |
| 2 | `MERGE INTO` 官方路径 = `sys.merge_into` procedure + `merge_into` action | https://paimon.apache.org/docs/2.1/flink/procedures/table-operations/ | 2026-10-20 |
| 3 | `MERGE INTO` action 说明（"Paimon supports \"MERGE INTO\" via submitting the 'merge_into' job"） | https://paimon.apache.org/docs/2.1/flink/action-jars/ | 2026-10-20 |
| 4 | Flink 2.2 已支持 SQL 语句清单（不含 MERGE） | https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/table/sql/overview/ | 2026-10-20 |
| 5 | `snapshot.num-retained.min` 默认 10 / 语义为 minimum；`max`、`time-retained`、`expire.*` 选项表 | https://paimon.apache.org/docs/2.1/maintenance/manage-snapshots/ | 2026-10-20 |
| 6 | CoreOptions 默认值（`num-retained.max=infinite`、`expire.limit=50` 等） | https://paimon.apache.org/docs/2.1/maintenance/configurations/#coreoptions | 2026-10-20 |
| 7 | CoreOptions 生成表原始文件（`release-2.1.0`） | https://github.com/apache/paimon/blob/release-2.1.0/docs/generated/core_configuration.html | 2026-10-20 |
| 8 | `sys.expire_snapshots` 参数与示例 | https://paimon.apache.org/docs/2.1/flink/procedures/snapshots/#expire_snapshots | 2026-10-20 |
| 9 | `sys.expire_partitions` 参数与示例 | https://paimon.apache.org/docs/2.1/flink/procedures/snapshots/#expire_partitions | 2026-10-20 |
| 10 | `sys.purge_files` 原文 | https://paimon.apache.org/docs/2.1/flink/procedures/snapshots/#purge_files | 2026-10-20 |
| 11 | `$snapshots` 官方示例与 "Additional columns include …" | https://paimon.apache.org/docs/2.1/concepts/system-tables/#snapshots-table | 2026-10-20 |
| 12 | `total_record_count` "unmerged counts … not logical row counts" | https://paimon.apache.org/docs/2.1/concepts/system-tables/#snapshots-table | 2026-10-20 |
| 13 | Snapshot spec：JSON 字段含义表 / "Interpreting Record Counts" | https://paimon.apache.org/docs/2.1/concepts/spec/snapshot/ | 2026-10-20 |
| 14 | `$files` 官方列含义小表 + 示例 | https://paimon.apache.org/docs/2.1/concepts/system-tables/#files-table | 2026-10-20 |
| 15 | `$schemas` 官方列名（schema_id / fields / partition_keys / primary_keys / options / comment / update_time） | https://paimon.apache.org/docs/2.1/concepts/system-tables/#schemas-table | 2026-10-20 |
| 16 | `$partitions` 官方说明与示例 | https://paimon.apache.org/docs/2.1/concepts/system-tables/#partitions-table | 2026-10-20 |
| 17 | `$tags` 官方说明与示例 | https://paimon.apache.org/docs/2.1/concepts/system-tables/#tags-table | 2026-10-20 |
| 18 | `$options` 官方说明与示例 | https://paimon.apache.org/docs/2.1/concepts/system-tables/#options-table | 2026-10-20 |
| 19 | "Currently, Flink, Spark, Trino and StarRocks support querying system tables."（不含 Doris） | https://paimon.apache.org/docs/2.1/concepts/system-tables/ | 2026-10-20 |
| 20 | Doris 集成页（system-table 语法以 Doris 官方为准） | https://paimon.apache.org/docs/2.1/ecosystem/doris/ | 2026-10-20 |
| 21 | 生态兼容矩阵（Flink 1.16–1.20 与 2.0–2.2；Doris 条目） | https://paimon.apache.org/docs/2.1/ecosystem/ | 2026-10-20 |
| 22 | 快照过期 = 物理删除；标记删除 ≠ 物理删除；过期策略过短的风险 | https://paimon.apache.org/docs/2.1/maintenance/manage-snapshots/ | 2026-10-20 |
| 23 | 孤儿文件官方说明（"files that are not used by Paimon snapshots"） | https://paimon.apache.org/docs/2.1/maintenance/manage-snapshots/#remove-orphan-files | 2026-10-20 |
| 24 | `sys.remove_orphan_files` 参数与示例 | https://paimon.apache.org/docs/2.1/flink/procedures/repair/#remove_orphan_files | 2026-10-20 |
| 25 | 分区过期 = 逻辑删除；物理删除取决于快照过期 | https://paimon.apache.org/docs/2.1/maintenance/manage-partitions/ | 2026-10-20 |
| 26 | Catalog 定义与四类 metastore | https://paimon.apache.org/docs/2.1/concepts/catalog/ | 2026-10-20 |
| 27 | "catalog organizes databases and tables … `my_catalog.my_database.my_table`" | https://paimon.apache.org/docs/2.1/concepts/basic-concepts/ | 2026-10-20 |
| 28 | CREATE CATALOG（filesystem，`warehouse` 为 catalog 级） | https://paimon.apache.org/docs/2.1/flink/sql-ddl/#create-filesystem-catalog | 2026-10-20 |
| 29 | "in the catalog's `default` database"（catalog→database→table 层级） | https://paimon.apache.org/docs/2.1/flink/sql-ddl/#create-table | 2026-10-20 |
| 30 | 托管表 "Dropping these tables also deletes their table files." | https://paimon.apache.org/docs/2.1/flink/sql-ddl/#create-table | 2026-10-20 |
| 31 | CTAS 表属性写法（`'primary-key'` / `'partition'`） | https://paimon.apache.org/docs/2.1/flink/sql-ddl/#create-table-as-select | 2026-10-20 |
| 32 | cross partition upsert 提示 | https://paimon.apache.org/docs/2.1/flink/sql-ddl/#create-table | 2026-10-20 |
| 33 | Flink 官方 CREATE DATABASE 语法 | https://github.com/apache/flink/blob/release-2.2/docs/content/docs/dev/table/sql/create.md | 2026-10-20 |
| 34 | Flink 2.2 CREATE DATABASE（渲染页） | https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/table/sql/create/#create-database | 2026-10-20 |
| 35 | `file://` 为 Built-in Support（Pluggable=N）；HDFS 才要求 hadoop 环境 | https://paimon.apache.org/docs/2.1/maintenance/filesystems/#supported-filesystems | 2026-10-20 |
| 35b | Filesystems 页首（复用 Flink 插件式 FS；对象存储才需独立 jar：`paimon-oss-2.1.0.jar` / `paimon-s3-2.1.0.jar`） | https://paimon.apache.org/docs/2.1/maintenance/filesystems/ | 2026-10-20 |
| 36 | "A `file:/tmp/...` warehouse is suitable for the single-machine tutorial." | https://paimon.apache.org/docs/2.1/flink/installation/ | 2026-10-20 |
| 37 | Quick Start 用 `file:/tmp/paimon` 建 catalog | https://paimon.apache.org/docs/2.1/flink/quick-start/ | 2026-10-20 |
| 38 | 分布式作业需共享存储 / 所有 task 可访问的路径 | https://paimon.apache.org/docs/2.1/flink/sql-ddl/ | 2026-10-20 |
| 39 | 制品矩阵（Flink 2.2 → `paimon-flink-2.2-2.1.0.jar`） | https://paimon.apache.org/docs/2.1/flink/installation/#choose-an-artifact | 2026-10-20 |
| 40 | 构建命令（`-Pflink2`，`paimon-flink-2.2`） | https://paimon.apache.org/docs/2.1/flink/installation/#build-from-source | 2026-10-20 |
| 41 | Maven Central `paimon-flink-2.2` 版本列表 | https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-flink-2.2/ | 2026-10-20 |
| 42 | Maven Central `paimon-flink-2.2/2.1.0/` 目录（jar 存在，57697871 字节） | https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-flink-2.2/2.1.0/ | 2026-10-20 |
| 43 | Procedures 页首（"Flink 1.18 and later" / "Some procedures submit Flink jobs"）与调用方式 | https://paimon.apache.org/docs/2.1/flink/procedures/ | 2026-10-20 |
| 44 | `sys.compact` 参数与语法 | https://paimon.apache.org/docs/2.1/flink/procedures/compaction/ | 2026-10-20 |
| 45 | Dedicated Compaction Job（`write-only`、action jar `compact` 命令、batch/streaming 差异） | https://paimon.apache.org/docs/2.1/maintenance/dedicated-compaction/ | 2026-10-20 |
| 46 | Action Jars 页首（action jar 提交方式） | https://paimon.apache.org/docs/2.1/flink/action-jars/ | 2026-10-20 |
| 47 | Compaction 行为与配置（`num-sorted-run.*`、异步 compaction 示例与免责） | https://paimon.apache.org/docs/2.1/primary-key-table/compaction/ | 2026-10-20 |
| 48 | Bucket 模式表、Fixed/Dynamic Bucket、bucket-key 约束、Bucket Sizing | https://paimon.apache.org/docs/2.1/primary-key-table/data-distribution/ | 2026-10-20 |
| 49 | Cross Partitions Upsert 语义与代价 | https://paimon.apache.org/docs/2.1/primary-key-table/data-distribution/#cross-partitions-upsert | 2026-10-20 |
| 50 | Primary-Key Table 总览（bucket 默认动态、Nullable PK、DDL 示例） | https://paimon.apache.org/docs/2.1/primary-key-table/ | 2026-10-20 |
| 51 | Spark DDL "the primary key must include every partition column" | https://paimon.apache.org/docs/2.1/spark/sql-ddl/ | 2026-10-20 |
| 52 | PyPaimon CLI 命令表（无 compact） | https://paimon.apache.org/docs/2.1/pypaimon/cli/ | 2026-10-20 |
| 53 | Table Mode：MOR 为默认 / COW / MOW 三模式与成本；MOW 需显式开 `deletion-vectors.enabled` | https://paimon.apache.org/docs/2.1/primary-key-table/table-mode/ | 2026-10-20 |
