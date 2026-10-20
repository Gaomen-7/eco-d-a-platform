# 07 · Apache Doris 动态分区与保留策略 —— 一手取证（4.1.x）

- 调研时间：2026-10-20
- 目标版本：**Apache Doris 4.1.5**（本仓 `eco/doris:4.1.5`，构建自 `apache-doris-4.1.5-bin-x64.tar.gz`；见 `docker/doris/Dockerfile:11,44`，注意是 **gzip** 不是 xz）
- 文档版本：`https://doris.apache.org/docs/4.x/...`（各页自带 "Last updated on" 标注，见引用清单）+ `apache/doris-website` 仓库 `versioned_docs/version-4.x/*.md`（master 分支）
- 源码锚点：**`apache/doris` tag `4.1.5-release`**（commit `917b20be83878e98f94e3ebf7efccc80346e3e50`）。**全文以 tag 为准，不以 master、不以 branch-4.1 为准。**
- 取证方式：官方站点逐页 `read_page` / `web_fetch` + `apache/doris-website` 仓库文件 + `apache/doris` 仓库源码（FE/BE，tag `4.1.5-release`）+ Kafka 官方文档站 4.3 + Paimon 官方文档站 2.1。**未使用本仓 `research-notes.md` / `hld-research-*.md` 作为证据**（它们只作为「待核对主张」清单）。
- 说明：4.x 文档站未给出版本号级语义；"4.1.5 适用"由 4.1.x 文档站 + `4.1.5-release` tag 源码共同支撑。

> ### ⚠️ 本文的证据规则（方法论声明）
>
> **凡涉及「本仓实际运行的二进制」的任何默认值，一律以 `apache/doris` tag `4.1.5-release` 的源码为权威（authoritative）；4.x 文档页只作参考，不作判据。**
>
> 理由：本次取证在 **6 个**配置项上发现 4.x 文档页与 4.1.5 源码**不一致**（见 §1.3、§1.4、§3.1、§6.1）——**4.x 文档页并非与所发布二进制可靠同步**。所有冲突一律**并列给出**，并显式标注「以源码为准（推导）」。凡需给出「官方保证」的地方，仍需看该断言是否在**源码与文档之间一致**；若二者冲突，则该项**不构成官方保证**。

---

## 0 · 结论速览

| # | 问题 | 结论 | 证据口径 |
|---|---|---|---|
| Q1 | `dynamic_partition.start` 出厂默认 | **`-2147483648`（= `Integer.MIN_VALUE`），语义「历史分区不删除」。本仓主张正确。** | 官方给出（文档 + 源码一致） |
| Q1b | ⚠️ `dynamic_partition_enable` 默认 | **4.1.5 源码 = `true`，不是 `false`。本仓「官方默认是 false」在 4.1.5 上是错的。** | 源码给出；4.x FE 配置页给 `true`，4.x 动态分区页给 `false`（官方两页自相矛盾） |
| Q1c | ⚠️ `max_dynamic_partition_num` 默认 | **4.1.5 源码 = `20000`，不是 `500`。本仓注释「默认 500」是错的。** | 源码给出；4.x 两页均写 `500` |
| Q2 | 调度间隔 | **`dynamic_partition_check_interval_seconds` = `600` 秒（10 分钟）**，IsMutable=true，MasterOnly=true | 官方给出（文档 + 源码一致） |
| Q2b | `start=-30, DAY` 时哪天删 | 保留 **30 个历史日分区**，被删的是**第 31 个最老的**分区；不是「第 31 天触发」 | 推导（源码语义） |
| Q3 | DROP PARTITION 是否物理立即 | FE 侧是元数据操作；BE 侧走 GC 回收路径，**`trash_file_expire_time_sec` 4.1.5 源码默认 `0`**（无 trash 保留窗）；4.x BE 配置文档页却写 `86400`（24 h）——**官方两处冲突** | 源码给出；冲突并列 |
| Q3b | 「磁盘在 N 秒内释放」的官方保证 | **官方未给出** | — |
| Q4 | 能否加速时钟做行为演练 | **能。`time_unit = HOUR` + 小 `start`（如 `-2`）是官方支持配置**；前提是分区列为 DATETIME/DATETIMEV2/TIMESTAMPTZ（DATE/DATEV2 列**不能**用 HOUR）。分区名形如 `p2025010110` | 官方给出（文档 + 源码） |
| Q4b | 官方是否推荐用 HOUR 做测试 | **官方未给出**（无任何"加速时钟/测试用途"表述）；HOUR 本身是官方「Applicable Scenarios」列出的正规粒度 | — |
| Q5 | Kafka retention 语义 | `retention.ms`（默认 7 天）/`retention.bytes`（默认 `-1`）**任一先满足即删**（`cleanup.policy=delete`）；`retention.bytes` 是**每分区**上限 | 官方给出 |
| Q5b | Kafka 与 Doris 是否同类问题 | **同类，但可证性差一个量级**：Kafka 的 2 GB 是**体积**上限（按 200 ev/s × ~500 B ≈ 8.05 GB/天 ⇒ ≈6 h 可观测）；Doris 的 30 天是**时间**上限（31 天内无论多大量都不可观测） | 推导（依据官方语义） |
| Q6 | Doris BE compaction 成本数字 | **磁盘预留：官方给出**（env-checking 页 BE = 总量 ×3× **1.4**，×1.4 逐字注明 "40% reserved space for background compaction"）；**耗时 / 写放大 / IO 数字：官方未给出**（只给触发阈值类 knob） | 官方给出（预留因子）；其余未给出 |
| Q6b | Paimon compaction 成本数字 | **官方未给出**（2.1 compaction 页全篇无成本数字）；只给出 `compaction.max-size-amplification-percent=200` 这类**配置上限** | 官方给出（配置值） |

**对本仓的直接结论**：`REQ-F-OLAP-03` 的判据应写成 **「配置 + 小时级演练」**，**不是**「等到第 31 天实测」——但演练必须用**独立的 HOUR 粒度演练表**，不能改生产表的 `start=-30`（详见 §7）。

### 0.1 ⚠️ 4.x 文档页 vs 4.1.5 源码：6 处默认值不一致（本仓的取证规则由来）

| # | 配置项 | 4.x 文档页 | 4.1.5 源码（权威） | 本仓主张受影响 | 详见 |
|---|---|---|---|---|---|
| 1 | `dynamic_partition_enable` | 动态分区页 `false` / FE 配置页 `true` | **`true`** | ✅ **本仓「默认 false」错** | §1.3 |
| 2 | `max_dynamic_partition_num` | `500` | **`20000`** | ✅ **本仓注释「默认 500」错** | §1.4 |
| 3 | BE `trash_file_expire_time_sec` | `86400`（24 h） | **`0`**（无 trash 保留窗） | ⚠️ 直接影响 20 GB 磁盘分析 | §3.1 |
| 4 | BE `vertical_compaction_max_row_source_memory_mb` | `200` | **`1024`** | ⚠️ 影响 compaction 内存估算 | §6.1 |
| 5 | BE `vertical_compaction_max_segment_size` | `268435456`（256 MiB） | **`1073741824`**（1 GiB） | ⚠️ 影响磁盘占用估算 | §6.1 |
| 6 | BE `generate_compaction_tasks_interval_ms` | `10` | **`100`** | 影响 compaction 任务节奏 | §6.1 |

**⇒ 结论（推导）**：**4.x BE Configuration 页与 FE Configuration 页并非与所发布的 4.1.5 二进制可靠同步**。本仓一切与默认值相关的断言，**必须回到 `4.1.5-release` tag 源码**。
**⇒ 同时**：这三处 `dynamic_partition.*` 分歧**不是**「文档写错了」这么单纯——`dynamic_partition_enable` 在**官方自己的两个文档页之间就已经不一致**（动态分区页 `false` vs FE 配置页 `true`）⇒ 这属于**官方文档自身的不一致**，官方**未给出**调和说明。

---

## 1 · `dynamic_partition.start` 的默认值（Q1）

### 1.1 官方文档逐字

来源：https://doris.apache.org/docs/4.x/table-design/data-partitioning/dynamic-partitioning/ （"Last updated on Oct 16, 2026"；同源文件 `apache/doris-website` `versioned_docs/version-4.x/table-design/data-partitioning/dynamic-partitioning.md` @ `bcb7f009`），Dynamic Partition Properties 表逐字：

> | `dynamic_partition.start` | No | The start offset for dynamic partitioning, expressed as a negative number. The default value is `-2147483648`, which means historical partitions are not dropped. Based on the `time_unit` property, partitions whose range is before this offset relative to the current day (week / month) are dropped. Whether historical partitions between this offset and the current time are created if they do not exist is controlled by `dynamic_partition.create_history_partition`. |

### 1.2 4.1.5 源码逐字

`apache/doris` @ `4.1.5-release` —— `fe/fe-core/src/main/java/org/apache/doris/catalog/DynamicPartitionProperty.java`：

```java
public static final int MIN_START_OFFSET = Integer.MIN_VALUE;
...
// In order to compatible dynamic add partition version
this.start = Integer.parseInt(properties.getOrDefault(START, String.valueOf(MIN_START_OFFSET)));
```

同仓 `fe/fe-core/src/main/java/org/apache/doris/common/util/DynamicPartitionUtil.java`（`checkInputDynamicPartitionProperties`）：

```java
if (Strings.isNullOrEmpty(start)) {
    properties.put(DynamicPartitionProperty.START, String.valueOf(DynamicPartitionProperty.MIN_START_OFFSET));
}
```

**"未设置" 的判定是「与 `MIN_START_OFFSET` 精确相等」**，不是「小于某个阈值」。删除路径短路于此（`fe/fe-core/src/main/java/org/apache/doris/clone/DynamicPartitionScheduler.java` @ `4.1.5-release`，`getDropPartitionClauseForDynamic`）：

```java
if (dynamicPartitionProperty.getStart() == DynamicPartitionProperty.MIN_START_OFFSET) {
    // not set start offset, so not drop any partition
    return dropPartitionClauses;
}
```

→ **本仓 `gdd.md` D8 / `HLD.md` §7.2 G-D8 的主张「官方 `dynamic_partition.start` 出厂默认 `-2147483648` ＝ 历史分区不删除」= 正确。**（⚠️ 但注意它同时也**误判了第三个前置条件**，见 §1.3。）

### 1.3 ⚠️ 重大发现一：`dynamic_partition_enable` 的 4.1.5 默认是 `true`，不是 `false`

`apache/doris` @ `4.1.5-release` —— `fe/fe-common/src/main/java/org/apache/doris/common/Config.java`：

```java
/**
 * If set to true, dynamic partition feature will open
 */
@ConfField(mutable = true, masterOnly = true)
public static boolean dynamic_partition_enable = true;
```

消费点：`fe/fe-core/src/main/java/org/apache/doris/clone/DynamicPartitionScheduler.java` @ `4.1.5-release`：

```java
@Override
protected void runAfterCatalogReady() {
    if (!initialize) {
        initDynamicPartitionTable();
    }
    setInterval(Config.dynamic_partition_check_interval_seconds * 1000L);
    if (Config.dynamic_partition_enable) {
        try {
            executeDynamicPartition(dynamicPartitionTableInfo, false);
        } catch (Exception e) { ... }
    }
}
```

**官方文档自身不一致（如实并列，不调和）**：

| 出处 | 逐字 | 默认值 |
|---|---|---|
| 4.x 动态分区页（"Last updated on Oct 16, 2026"） | "\| `dynamic_partition_enable` \| false \| Whether to enable the dynamic partitioning feature in Doris. This parameter affects only partition operations on dynamic partitioned tables, not on regular tables. \|" | `false` |
| 4.x FE Configuration 页（`versioned_docs/version-4.x/admin-manual/config/fe-config.md`） | "#### `dynamic_partition_enable`\n\nDefault：true\n\nIsMutable：true\n\nMasterOnly：true\n\nWhether to enable dynamic partition scheduler, enabled by default" | `true` |
| 4.1.5 源码 `Config.java` | `public static boolean dynamic_partition_enable = true;` | `true` |

另：`apache/doris` @ `4.1.5-release` 的 `conf/fe.conf` **完全没有出现 `dynamic_partition_enable`**（该文件只显式设了 `http_port` / `rpc_port` / `query_port` / `edit_log_port` / `arrow_flight_sql_port` / `sys_log_level` / `sys_log_mode` 等），⇒ **在 stock 4.1.5 部署上生效值就是代码默认 `true`**。

**⇒ 对本仓的推翻**：
- `gdd.md` D8（原文：「★ 补充（本轮新发现，原 D8 漏了）：Doris 动态分区另有 **FE 级总开关 `dynamic_partition_enable`，默认 `false`**」）、`HLD.md` §7.2 G-D8、`canonical-truth.md` 条目 E、`docker/doris/init.sql:3-7` 的注释，**在 4.1.5 上是错的**。
- `docker/doris/init.sql:16` 的 `ADMIN SET FRONTEND CONFIG ("dynamic_partition_enable" = "true");` **无害但多余**——它只是把已经是 `true` 的值再设一次。⚠️ **更值得注意的风险**：`ADMIN SET FRONTEND CONFIG` 是**运行时**修改，官方 FE Configuration 页明确说：

  > "**Configuration items modified in this way will become invalid after the FE process restarts.**"

  ⇒ 仓库在容器启动时执行的这一行**在 FE 重启后失效**；若某天有人误以为「默认是 false 所以必须靠这行」，一旦 FE 重启到 `fe_custom.conf` 之外的路径，行为依据就断了。**正确做法**是把 `dynamic_partition_enable = true` 写进 `fe.conf`（或 `fe_custom.conf`），而不是靠一次性 DDL。
- 仓库「三个前置条件」中的**第三个（FE 总开关）不成立**。真正的前置条件是：① 表级 `dynamic_partition.enable="true"`；② `dynamic_partition.start` 必须**显式设**（默认 `MIN_START_OFFSET` ⇒ 永不删）；③ `time_unit` / `end` / `prefix` 必填（源码 `checkInputDynamicPartitionProperties` 对缺失 `time_unit` / `prefix` 抛 `"Must assign dynamic_partition.time_unit properties"` / `"Must assign dynamic_partition.prefix properties"`；`end` 缺失时 `checkEnd` 抛 `ERROR_DYNAMIC_PARTITION_END_EMPTY`）；④ `create_history_partition="true"` 才有历史分区。

### 1.4 ⚠️ 重大发现二：`max_dynamic_partition_num` 的 4.1.5 默认是 `20000`，不是 `500`

`apache/doris` @ `4.1.5-release` —— `fe/fe-common/src/main/java/org/apache/doris/common/Config.java`：

```java
/**
 * Used to limit the maximum number of partitions that can be created when creating a dynamic partition table,
 * to avoid creating too many partitions at one time.
 * The number is determined by "start" and "end" in the dynamic partition parameters.
 */
@ConfField(mutable = true, masterOnly = true)
public static int max_dynamic_partition_num = 20000;
```

官方文档两处均写 `500`：4.x 动态分区页（"\| `max_dynamic_partition_num` \| 500 \| Limits the maximum number of partitions that can be created when creating a dynamic partitioned table, to avoid creating too many partitions at once. \|"）与 4.x FE Configuration 页（"#### `max_dynamic_partition_num`\n\nDefault：500"）。

**⇒ 对本仓的推翻**：`docker/doris/init.sql:18-19` 的注释「顺便放宽单表可建分区数（默认 500；30 天保留 × 多表不需要更多，保持默认即可）」**是错的**（默认 20000），且那行被注释掉的 `ADMIN SET FRONTEND CONFIG ("max_dynamic_partition_num" = "500")` 若取消注释，会**把上限从 20000 降到 500**（即"收紧"而非"放宽"）。

**对本仓无实际影响**（推导，依据源码 `analyzeDynamicPartition`）：

```java
expectCreatePartitionNum = (long) end - start;
int dynamicPartitionLimit = Config.max_dynamic_partition_num;
if (!isReplay && hasEnd && ...) {
    if (expectCreatePartitionNum > dynamicPartitionLimit) {
        throw new DdlException("Too many dynamic partitions: " + expectCreatePartitionNum + ". Limit: " + dynamicPartitionLimit);
    } else if (expectCreatePartitionNum > dynamicPartitionLimit * 8L / 10) {
        LOG.warn("Dynamic partition count {} is approaching limit {} (>80%)." ...);
    }
}
```

本仓 `start=-30`、`end=3`、`create_history_partition=true` ⇒ `getRealStart(-30, -1) = -30` ⇒ `expectCreatePartitionNum = 3 - (-30) = 33`，远低于 20000 也低于 500 ⇒ **建表不会因上限被拒**。但 **33 这个数字本身是本仓必须知道的**（见 §2.3）。

### 1.5 该参数的历史变动（Q1(c)）

**官方未给出**该参数的"从什么改成什么、哪个版本"的变更记录：4.x 动态分区页与 4.x FE Configuration 页**均无**「Last updated: 4.1.5 起改为 X」之类的版本化说明（对比同页 `autobucket_min_buckets` 明确写了 "**Starting from version 4.0.8 in the Doris 4.0 series and version 4.1.4 in the 4.1 series, the default value changed from `1` to `3`.**"）。4.1.5 Release Notes（https://doris.apache.org/releases/v4.1/release-4.1.5/ ）中**无** `dynamic_partition.start` 相关条目。

**⇒ 结论**：该参数的"出厂默认 = 不删历史分区"在**当前 4.1.x 线上是稳定的**（文档 + 源码一致），但官方**未给出**其历史变更点（从什么值改到 `-2147483648`、何时改）——**官方未给出**。本仓只需断言「4.1.5 的默认是不删」，不需要断言「历史上曾改成过」。

---

## 2 · 调度与粒度（Q2）

### 2.1 官方调度间隔

**配置名**：`dynamic_partition_check_interval_seconds`
**默认值**：**600**（秒 = 10 分钟）
**可变性**：IsMutable=true，MasterOnly=true
**单位**：秒

官方文档（4.x FE Configuration 页，`versioned_docs/version-4.x/admin-manual/config/fe-config.md`）逐字：

> #### `dynamic_partition_check_interval_seconds`
>
> Default：600 （s）
>
> IsMutable：true
>
> MasterOnly：true
>
> Decide how often to check dynamic partition

4.x 动态分区页逐字（"Modifying Dynamic Partition Properties" 节）：

> :::info Tip
> Modifying dynamic partitions with the `ALTER TABLE` statement does not take effect immediately. Doris polls dynamic partitions at the interval specified by `dynamic_partition_check_interval_seconds` and performs the required partition creation and drop operations.
> :::

4.1.5 源码（`apache/doris` @ `4.1.5-release`，`fe/fe-common/src/main/java/org/apache/doris/common/Config.java`）：

```java
/**
 * Decide how often to check dynamic partition
 */
@ConfField(mutable = true, masterOnly = true)
public static long dynamic_partition_check_interval_seconds = 600;
```

消费点：`fe/fe-core/src/main/java/org/apache/doris/clone/DynamicPartitionScheduler.java` @ `4.1.5-release`：

```java
setInterval(Config.dynamic_partition_check_interval_seconds * 1000L);
```

同文件类注释逐字：

> ```
>  * This class is used to periodically add or drop partition on an olapTable which specify dynamic partition properties
>  * Config.dynamic_partition_enable determine whether this feature is enable,
>  * Config.dynamic_partition_check_interval_seconds determine how often the task is performed
> ```

**类名说明**：**4.1.5 上并不存在 `DynamicPartitionStats` / `DynamicPartitionUpdater` / `DynamicPartitionDaemon`**，实际类名是 **`org.apache.doris.clone.DynamicPartitionScheduler`**（继承 `MasterDaemon`）。→ 票面假设的类名在 4.1.x 上**不存在**（属 1.x/2.0 早期 Doris 的历史命名）。

**⇒ 「是否多一个调度周期」**：删除动作发生在**每次 scheduler 醒来时**（每 600 s 一次），不在"天"的边界上。因此：
- 一次 DROP 生效的**最坏延迟 = 一个调度周期 = 600 s**（不是"一天"）。
- 但**分区边界本身按 `time_unit` 的整单位对齐**（DAY ⇒ 00:00:00 边界，见 §2.3），所以"哪一批分区被判定为过期"是按天的，**判定的切换时刻**在跨日那一刻；而**实际执行**仍要等最多 600 s 的轮询。

### 2.2 `start` / `end` 的单位 = `time_unit`

4.x 动态分区页逐字：

> `dynamic_partition.start` — The start offset for dynamic partitioning, expressed as a negative number. ... **Based on the `time_unit` property**, partitions whose range is before this offset relative to the current day (week / month) are dropped.
>
> `dynamic_partition.end` — The end offset for dynamic partitioning, expressed as a positive number. **Based on the `time_unit` property**, partitions for the corresponding range are pre-created relative to the current day (week / month).

4.1.5 源码（`fe/fe-core/src/main/java/org/apache/doris/common/util/DynamicPartitionUtil.java`）逐字（HOUR 分支）：

```java
/**
 * return the partition range date string formatted as yyyy-MM-dd[ HH:mm::ss]
 ...
 * return partition range in HOUR granularity.
 * offset: The offset from the current hour. 0 means current hour, 1 means next hour, -1 last hour.
 * ...
 * Eg:
 * Today is 2020-05-24 00:12:34, offset = -1
 * It will return 2020-05-23 23:00:00
 */
public static String getPartitionRangeOfHour(ZonedDateTime current, int offset, String format) {
    return getFormattedTimeWithoutMinuteSecond(current.plusHours(offset), format);
}
```

DAY 分支逐字：

```java
/**
 * return partition range in DAY granularity.
 * offset: The offset from the current day. 0 means current day, 1 means tomorrow, -1 means yesterday.
 */
private static String getPartitionRangeOfDay(ZonedDateTime current, int offset, String format) {
    return getFormattedTimeWithoutHourMinuteSecond(current.plusDays(offset), format);
}
```

⇒ **`start` / `end` 的数值以 `time_unit` 为单位**。`time_unit=DAY` + `start=-30` ⇒ 30 天；`time_unit=HOUR` + `start=-2` ⇒ 2 小时。

### 2.3 `start=-30, DAY` 时到底保留几个分区、哪一天删（推导）

**删除判定**（`DynamicPartitionScheduler.getDropPartitionClauseForDynamic` @ `4.1.5-release`，逐字）：

```java
int realStart = dynamicPartitionProperty.getStart();
// For TIMESTAMPTZ, use UTC clock so the drop cutoff aligns with the
// UTC-midnight partition boundaries created by getAddPartitionOp().
...
String lowerBorder = DynamicPartitionUtil.getPartitionRangeString(dynamicPartitionProperty, now, realStart, partitionFormat);
String limitBorder = DynamicPartitionUtil.getPartitionRangeString(dynamicPartitionProperty, now, 0, partitionFormat);
...
reservePartitionKeyRange = Range.atLeast(lowerBound);
...
for (Long dropPartitionId : isContaineds.keySet()) {
    // Do not drop the partition "by force", or the partition will be dropped directly instread of being in
    // catalog recycle bin. This is for safe reason.
    if (!isContaineds.get(dropPartitionId)) {
        String dropPartitionName = olapTable.getPartition(dropPartitionId).getName();
        dropPartitionClauses.add(new DropPartitionClause(false, dropPartitionName, false, false));
    }
}
```

**推导（非官方逐字断言）**：
- `lowerBorder` = 今天 00:00:00 − 30 天；保留区间 = `[lowerBorder, +∞)`。
- 覆盖 `[今天−30d, 今天−29d)` 的分区落在保留区间内（闭端点）⇒ **保留**；覆盖 `[今天−31d, 今天−30d)` 的分区与保留区间**不相交**（上端点开）⇒ **被删**。
- ⇒ **恰好保留 30 个历史日分区**；**被删的是第 31 个最老的分区**。
- **创建**侧（`getAddPartitionClause` @ `4.1.5-release`）：`for (; idx <= dynamicPartitionProperty.getEnd(); idx++)`，`idx` 从 `getRealStart(start, historyPartitionNum) = -30` 到 `end = 3` ⇒ **共建 34 个分区**（30 历史 + 今天 + 3 个未来）。
- ⇒ 官方文档的 4 分区示例与本推导一致：4.x 动态分区页逐字——"Based on these rules, **four partitions are always retained** as time advances: the partition from one day ago, the partition for the current day, and the partitions for the next two days."（`start=-1, end=2` ⇒ 1+1+2 = 4）。

**⇒ 修正票面与仓库的口径**：不是「第 31 天**触发**删除」，而是「**第 31 个最老的日分区在它变成第 31 老的那一刻被删**」；且**要观察到这一行为，必须先有 31 天的数据**——这正是 §4 要解决的问题。

**⇒ 对本仓的直接后果**：`create_history_partition="true"` + `start=-30` 会**立刻建出 30 个历史日分区**（官方逐字佐证：4.x 动态分区页 "When `create_history_partition = true`, 5 partitions are created immediately for the range `[10-09, 10-13]`."（`start=-2,end=2` 的例子））。⇒ **历史分区现在就能造出来；缺的只是"让时钟走 30 天"或"把单位改成 HOUR"。**

---

## 3 · DROP PARTITION 是否物理且立即回收（Q3）

### 3.1 有 trash / 回收站机制，且 4.1.5 默认值是 `0`

`apache/doris` @ `4.1.5-release` —— `be/src/common/config.h`：

```cpp
// It is only a recommended value. When the disk space is insufficient,
// the file storage period under trash dose not have to comply with this parameter.
DECLARE_mInt32(trash_file_expire_time_sec);
```

`be/src/common/config.cpp`：

```cpp
// It is only a recommended value. When the disk space is insufficient,
// the file storage period under trash dose not have to comply with this parameter.
DEFINE_Int32(trash_file_expire_time_sec, "0");
```

⚠️ **官方文档与源码冲突（如实并列）**：4.x BE Configuration 页（https://doris.apache.org/docs/4.x/admin-manual/config/be-config/ ，"Last updated on Oct 16, 2026"）逐字：

> trash_file_expire_time_sec
>
> Description: The interval for cleaning the recycle bin is 24 hours. When the disk space is insufficient, the file retention period under trash may not comply with this parameter
>
> Default value: 86400

⇒ **4.x 文档页写 `86400`（24 h），4.1.5 源码写 `0`**。本仓运行的是 4.1.5 ⇒ **以源码为准（推导）：trash 保留窗为 0，即无额外保留期**。⚠️ 这两条是**互相矛盾**的官方表述，官方**未给出**调和说明。

**⚠️ 但即使是 `0`，源码本身也不构成「立即回收」的保证**（逐字，源码注释）：

> ```
> // It is only a recommended value. When the disk space is insufficient,
> // the file storage period under trash dose not have to comply with this parameter.
> ```

⇒ 该参数**只是「建议值」**，且源码明说**磁盘空间不足时可以不被遵守**。⇒ **即便取 `0`，官方也没有承诺「删除后 T 秒内磁盘释放」**。这与 §3.1 的「官方未给出」结论一致。

### 3.2 相关 BE 回收/GC 旋钮（4.1.5 源码 `be/src/common/config.cpp` 逐字默认值）

| 参数 | 默认值 | 逐字语义（源码注释 / 文档） |
|---|---|---|
| `trash_file_expire_time_sec` | **0** | "It is only a recommended value. When the disk space is insufficient, the file storage period under trash dose not have to comply with this parameter." |
| `max_garbage_sweep_interval` | 3600 | 垃圾回收最大扫描间隔（s） |
| `min_garbage_sweep_interval` | 180 | 垃圾回收最小扫描间隔（s） |
| `garbage_sweep_batch_size` | 100 | 每次回收批量 |
| `snapshot_expire_time_sec` | 172800 | 快照过期（2 天） |
| `tablet_rowset_stale_sweep_time_sec` | 600 | 过期 rowset 扫描间隔（s） |
| `tablet_rowset_stale_sweep_by_size` | false | 按大小清理过期 rowset |
| `tablet_rowset_stale_sweep_threshold_size` | 100 | 上一项的阈值 |
| `pending_data_expire_time_sec` | 1800 | pending 数据过期（30 min） |
| `unused_rowset_monitor_interval` | 30 | 过期 rowset 清理检查间隔（s） |

### 3.3 结论（严格区分口径）

1. **「DROP PARTITION 后磁盘立刻释放」—— 官方未给出这样的断言。** 官方没有"删除后 N 秒内磁盘回收"的保证。**官方未给出**。
2. **可确证的官方事实**（推导自源码 + 文档）：DROP PARTITION 是 **FE 侧的元数据操作**（源码走 `Env.getCurrentEnv().dropPartition(...)` → `DropPartitionClause(false, name, false, false)`，其中 `isForce=false`，源码注释逐字：*"Do not drop the partition "by force", or the partition will be dropped directly instread of being in **catalog recycle bin**. This is for safe reason."*）；**BE 上的 tablet 文件通过垃圾回收路径异步释放**，受 `trash_file_expire_time_sec`（4.1.5 = 0）与 GC sweep 周期（`max_garbage_sweep_interval` = 3600 s）影响。
3. **⇒ 对 ≤20 GB 磁盘预算的含义（推导）**：分区被删 ≠ 磁盘立刻回收；**最坏情况下 trash 内的文件要等 GC sweep（源码默认最大 3600 s）**。因此**不能**把「分区删掉了」当成「磁盘已释放」。官方**未给出**"分区删除到磁盘释放"的时间上界数字。
4. **官方磁盘口径**：Doris 官方 dev/test 磁盘下限为 **FE 10 GB+ / BE 50 GB+**（一手出处：**https://doris.apache.org/docs/4.x/install/preparation/env-checking/** ，"Last updated on May 17, 2026"；仓库同源 `apache/doris-website` `versioned_docs/version-4.x/install/preparation/env-checking.md`）。逐字：

   > | Module | Minimum CPU | Minimum memory | Minimum disk | Network | Instances |
   > | Frontend | 8 cores | 8 GB | **SSD/SATA, 10 GB+** | 1GbE/10GbE | 1 |
   > | Backend | 8 cores | 16 GB | **SSD/SATA, 50 GB+** | 1GbE/10GbE | 1 |

   ⇒ 官方下限 **≥60 GB** vs 本仓 **≤20 GB** 设计值 ⇒ **正面冲突，本仓 20 GB 是低于官方口径的设计值**（本仓 `HLD.md` §8-R5 的偏离登记成立，不因本文改变；本轮**已把该口径从「二手转述」升格为一手 URL**）。
5. **⚠️ 本轮在该页另取到一条与 Q6 直接相关的官方数字**（「Disk space calculation」节逐字）：

   > | Component | Recommended space | Description |
   > | FE | 100 GB+ | SSD, used for metadata storage |
   > | BE | **Total data volume x 3 x 1.4** | LZ4 compression ratio 0.3-0.5, **3 replicas + 40% reserved space for background compaction** |

   ⇒ 这是**官方给出的、唯一一个与 compaction 磁盘开销直接相关的数字**：**×1.4 ＝ 为 background compaction 预留 40%**。注意其前提口径：**3 副本**——本仓 `docker/doris/init.sql` 三张表均为 `"replication_allocation" = "tag.location.default: 1"`（单 BE ⇒ 1 副本）⇒ 官方公式的 ×3 因子对本仓不适用，但 **×1.4 的 compaction 预留是独立因子**，可单独套用（**推导**）。
   ⇒ 同页限定逐字："The BE storage calculation above is mainly for the **integrated storage and compute** deployment mode."（本仓正是 integrated 模式 ⇒ 该口径适用。）
6. ⚠️ **另一个官方未给出的项**：**BE 数据盘上「动态分区保留 30 天」这一策略本身能省多少磁盘** —— 官方**未给出**任何"30 天保留 × 写入量 ⇒ 磁盘占用"的换算。

---

## 4 · 核心问题：30 天窗口能否在开发机上行为可证（Q4）

### 4.1 (a) `time_unit` 接受哪些值

4.x 动态分区页逐字：

> `dynamic_partition.time_unit` — Yes — The scheduling unit for dynamic partitioning. Can be set to `HOUR`, `DAY`, `WEEK`, `MONTH`, or `YEAR`, indicating that partitions are created or dropped by hour, day, week, month, or year, respectively.

4.1.5 源码（`fe/fe-core/src/main/java/org/apache/doris/common/util/DynamicPartitionUtil.java`，`checkTimeUnit`）逐字：

```java
if (Strings.isNullOrEmpty(timeUnit)
        || !(timeUnit.equalsIgnoreCase(TimeUnit.DAY.toString())
        || timeUnit.equalsIgnoreCase(TimeUnit.HOUR.toString())
        || timeUnit.equalsIgnoreCase(TimeUnit.WEEK.toString())
        || timeUnit.equalsIgnoreCase(TimeUnit.MONTH.toString())
        || timeUnit.equalsIgnoreCase(TimeUnit.YEAR.toString()))) {
    ErrorReport.reportDdlException(ErrorCode.ERROR_DYNAMIC_PARTITION_TIME_UNIT, timeUnit);
}
```

⇒ **HOUR 是官方支持的一等粒度。**

### 4.2 (b) `start` / `end` 以 `time_unit` 为单位 —— 见 §2.2

`time_unit=HOUR` + `start=-2` ⇒ 保留最近 2 小时（外加今天当前小时 + `end` 个未来小时）。

### 4.3 (c) 分区名格式与单位绑定

4.1.5 源码（`DynamicPartitionUtil.getFormattedPartitionName`）逐字：

```java
public static String getFormattedPartitionName(TimeZone tz, String formattedDateStr, String timeUnit) {
    formattedDateStr = formattedDateStr.replace("-", "").replace(":", "").replace(" ", "");
    if (timeUnit.equalsIgnoreCase(TimeUnit.DAY.toString())) {
        return formattedDateStr.substring(0, 8);
    } else if (timeUnit.equalsIgnoreCase(TimeUnit.MONTH.toString())) {
        return formattedDateStr.substring(0, 6);
    } else if (timeUnit.equalsIgnoreCase(TimeUnit.YEAR.toString())) {
        return formattedDateStr.substring(0, 4);
    } else if (timeUnit.equalsIgnoreCase(TimeUnit.HOUR.toString())) {
        return formattedDateStr.substring(0, 10);
    } else {
        ... // WEEK: String.format("%s_%02d", year, weekOfYear)
    }
}
```

⇒ **HOUR 的分区名 = `prefix` + `yyyyMMddHH`**，即 `p2025010110`（`p` 前缀 + 10 位）。
⇒ **DAY 的分区名 = `prefix` + `yyyyMMdd`**，即 `p20250101`（本仓现有预期正确）。
⇒ **WEEK 的分区名 ≠ 纯数字**，是 `yyyy_ww`（如 `p2025_03`）——⚠️ 本仓若将来改 WEEK，名字形态会变。
⇒ 分区名**必须与 `time_unit` 匹配**，否则既有分区名与调度算出的名字对不上（源码 `getAddPartitionClause` 用 `nowPartitionName` 做「已存在」判定，`getDropPartitionClauseForDynamic` 按名字取 `olapTable.getPartition(dropPartitionId).getName()`）。

### 4.4 (d) 分区粒度与分区列类型的关系

**结论：粒度不必与列类型「一一对应」，但列类型限制了可选粒度。**

4.1.5 源码（`DynamicPartitionUtil.checkTimeUnit`）逐字：

```java
if ((partitionColumn.getDataType() == PrimitiveType.DATE
        || partitionColumn.getDataType() == PrimitiveType.DATEV2)
        && (timeUnit.equalsIgnoreCase(TimeUnit.HOUR.toString()))) {
    ErrorReport.reportDdlException(DynamicPartitionProperty.TIME_UNIT + " could not be "
            + TimeUnit.HOUR + " when type of partition column "
            + partitionColumn.getDisplayName() + " is " + PrimitiveType.DATE + " or " + PrimitiveType.DATEV2);
} else if (PrimitiveType.getIntegerTypes().contains(partitionColumn.getDataType())
        && timeUnit.equalsIgnoreCase(TimeUnit.HOUR.toString())) {
    // The partition column's type is INT, not support HOUR
    ErrorReport.reportDdlException(DynamicPartitionProperty.TIME_UNIT + " could not be "
            + TimeUnit.HOUR.toString() + " when type of partition column "
            + partitionColumn.getDisplayName() + " is Integer");
}
```

`DynamicPartitionUtil.getPartitionFormat(column)` 逐字：

```java
if (column.getDataType().equals(PrimitiveType.DATE) || column.getDataType().equals(PrimitiveType.DATEV2)) {
    return DATE_FORMAT;              // "yyyy-MM-dd"
} else if (column.getDataType().equals(PrimitiveType.DATETIME)
        || column.getDataType().equals(PrimitiveType.DATETIMEV2)
        || column.getDataType().equals(PrimitiveType.TIMESTAMPTZ)) {
    return DATETIME_FORMAT;          // "yyyy-MM-dd HH:mm:ss"
} else if (PrimitiveType.getIntegerTypes().contains(column.getDataType())) {
    return TIMESTAMP_FORMAT;         // "yyyyMMdd"
} else {
    throw new DdlException("Dynamic Partition Only Support DATE, DATETIME and INTEGER Type Now.");
}
```

**⇒ 对本仓三张表的判定（推导）**：

| 表 | 分区列 | `time_unit=DAY` | `time_unit=HOUR` |
|---|---|---|---|
| `ads_window_metric` | `window_start DATETIMEV2(0)` | ✅ 合法（边界按日 00:00:00 对齐） | ✅ **合法** |
| `ads_late_expired_count` | `window_start DATETIMEV2(0)` | ✅ 合法 | ✅ **合法** |
| `ads_funnel_step` | `session_date DATE` | ✅ 合法 | ❌ **官方 DdlException 拒绝**（DATE/DATEV2 不能用 HOUR） |

**⇒ 加速时钟的可行路径**（推导 + 官方语义）：
- 建一张**独立的演练表**（例如 `ads_window_metric_rehearsal`），分区列 `DATETIMEV2(0)`，`dynamic_partition.time_unit = "HOUR"`、`start = "-2"`、`end = "1"`、`prefix = "p"`、`create_history_partition = "true"`。
- 写入 3–4 小时前的数据 ⇒ 观察：**≤2 个调度周期（每个 600 s）内**，覆盖 3 小时前那一小时的分区（名如 `p2025010110`）被 `SHOW PARTITIONS` 移除、`SHOW DYNAMIC PARTITION TABLES` 的 `LastDropPartitionMsg` 有记录。
- **该表形态与生产表不同**（小时粒度、分区名多两位），⇒ **不能**用它替代生产表的 `start=-30` 断言，只能用它**证明「Doris 的动态分区删除机制在本机确实工作」**。

### 4.5 官方是否推荐用 HOUR 做测试？

**官方未给出。** 4.x 动态分区页全篇**没有**任何"加速时钟 / 缩短时间窗 / 测试用途"的表述。官方对 HOUR 的定位是**正规生产粒度**，逐字（4.x 动态分区页 Applicable Scenarios 表）：

> | Log data management | Retain the last N days of logs by day **or hour**, and automatically drop expired partitions |
> | Time-series data management | Automatically maintain partitions along the time dimension for monitoring, IoT, and similar scenarios |
> | Data lifecycle management | Use TTL to control the data retention range and reduce storage cost |
> | Pre-creating future partitions | Create partitions for the next N days in advance to avoid write failures caused by missing partitions |

官方对该页 Best Practices 给出的 3 个示例全是 `DAY` / `MONTH`（Example 1 = DAY `start=-7,end=3`；Example 2 = MONTH 无 start；Example 3 = DAY + `reserved_history_periods`）——**没有**一个「用 HOUR 加速测试」的示例。

**⇒ 判定（严格区分）**：
- **「`time_unit=HOUR` + 小 `start`」这个配置本身是官方支持的**（文档 + 源码双重支撑），**不是 hack**。
- **但「用它来加速验证 30 天保留策略」是本仓自选的工程做法，官方未推荐、未提及、也未给出任何测试指引** —— 官方**未给出**。
- 官方页本身给的限制逐字（"Limitations" 节）：

  > - Dynamic partitioning supports only Range partitioning on `DATE` / `DATETIME` columns.
  > - Dynamic partitioning supports only a single partition key.

### 4.6 `dynamic_partition.time_zone` 能否当"时钟旋钮"用？

4.x 动态分区页逐字：

> `dynamic_partition.time_zone` — No — The time zone for dynamic partitioning. Defaults to the system time zone of the current server, for example, `Asia/Shanghai`. For more time zone settings, see [Time Zone Management].

4.1.5 源码（`DynamicPartitionProperty.java`）逐字：

```java
private TimeZone tz = TimeUtils.getSystemTimeZone();
...
this.tz = TimeUtils.getOrSystemTimeZone(properties.get(TIME_ZONE));
```

**判定（推导）**：`time_zone` 只改变**分区边界相对哪个墙上时钟计算**，**不改变 `start`/`end` 的语义**（偏移量始终是「相对当前的 N 个单位」）。⇒ **它不能把 30 天缩短成 2 小时**，最多只能把分区的**边界相位**平移（如 `Asia/Shanghai` vs `UTC` 相差 8 h ⇒ 跨日边界平移 8 h）⇒ 最多让「删除判定」提前/推迟 **不足 1 个 `time_unit`**。**用它当"加速时钟"无效。** 官方**未给出**任何关于「用 time_zone 做测试」的表述。

⚠️ **但它对本仓有一个真实风险**（推导）：`ads_window_metric` 的分区键 `window_start` 是**分钟级窗口起点**（`DATETIMEV2(0)`），而分区按 **DAY** 切。若容器系统时区与写入时区的时区不一致（容器 TZ vs 业务 TZ），分区边界会与"业务日"错位。官方页只说"默认取服务器系统时区"，**官方未给出**「跨时区场景下分区边界与业务日对齐」的指引。**⇒ 本仓应显式设 `dynamic_partition.time_zone`**（与业务时区一致），否则分区边界随宿主 TZ 漂移。

### 4.7 `dynamic_partition.reserved_history_periods` —— 另一条官方杠杆

4.x 动态分区页逐字：

> `dynamic_partition.reserved_history_periods` — No — The historical partition time ranges to retain. When `dynamic_partition.time_unit` is `DAY` / `WEEK` / `MONTH` / `YEAR`, set the value in the format `[yyyy-MM-dd,yyyy-MM-dd],[...,...]`. When `dynamic_partition.time_unit` is `HOUR`, set the value in the format `[yyyy-MM-dd HH:mm:ss,yyyy-MM-dd HH:mm:ss],[...,...]`. If not set, the default is `"NULL"`.

源码（`DynamicPartitionScheduler.getDropPartitionClauseForDynamic`）把 `reserved_history_periods` 的闭区间**加入保留集合**（与 `[lowerBorder, +∞)` 并列）：

```java
String reservedHistoryPeriods = dynamicPartitionProperty.getReservedHistoryPeriods();
List<Range> ranges = DynamicPartitionUtil.convertStringToPeriodsList(reservedHistoryPeriods,
        dynamicPartitionProperty.getTimeUnit());
if (ranges.size() != 0) {
    for (Range range : ranges) { ... reservedHistoryPartitionKeyRangeList.add(reservedHistoryPartitionKeyRange); }
}
```

**⇒ 判定**：这个参数**只能"多留"，不能"少留"**，**不能**用来加速删除。官方**未给出**它用于测试的表述。

---

## 5 · 类比：Kafka 的 retention（Q5）

### 5.1 官方逐字

来源：Kafka 官方文档站 4.3 —— Topic Configs（https://kafka.apache.org/43/configuration/topic-configs/ ，页脚 "Last modified May 22, 2026"）：

> **retention.ms**
> This configuration controls the maximum time we will retain a log before we will discard old log segments to free up space if we are using the "delete" retention policy. This represents an SLA on how soon consumers must read their data. If set to -1, no time limit is applied. Additionally, retention.ms configuration operates independently of "segment.ms" and "segment.bytes" configurations. Moreover, it triggers the rolling of new segment if the retention.ms condition is satisfied.
> Type: long / Default: **604800000 (7 days)** / Server Default Property: `log.retention.ms`

> **retention.bytes**
> This configuration controls the maximum size **a partition** (which consists of log segments) can grow to before we will discard old log segments to free up space if we are using the "delete" retention policy. **By default there is no size limit only a time limit. Since this limit is enforced at the partition level, multiply it by the number of partitions to compute the topic retention in bytes.** Additionally, retention.bytes configuration operates independently of "segment.ms" and "segment.bytes" configurations. Moreover, it triggers the rolling of new segment if the retention.bytes is configured to zero.
> Type: long / Default: **-1** / Server Default Property: `log.retention.bytes`

> **cleanup.policy**
> This config designates the retention policy to use on log segments. The "delete" policy (which is the default) will discard old segments when their retention time **or** size limit has been reached. ...
> Type: list / Default: delete / Server Default Property: `log.cleanup.policy`

同页另两条与「删除不是立即的」相关：

> **file.delete.delay.ms** — The time to wait before deleting a file from the filesystem / Default: **60000 (1 minute)**
> **segment.bytes** — This configuration controls the segment file size for the log. **Retention and cleaning is always done a file at a time** so a larger segment size means fewer files but less granular control over retention. / Default: **1073741824 (1 gibibyte)**

来源：Kafka 官方文档站 4.3 —— Log（https://kafka.apache.org/43/implementation/log/ ，"Last modified May 22, 2026"），"Deletes" 节逐字：

> Data is deleted one log segment at a time. The log manager applies two metrics to identify segments which are eligible for deletion: time and size. For time-based policies, the record timestamps are considered, with the largest timestamp in a segment file (order of records is not relevant) defining the retention time for the entire segment. Size-based retention is disabled by default. When enabled the log manager keeps deleting the oldest segment file until the overall size of the partition is within the configured limit again. **If both policies are enabled at the same time, a segment that is eligible for deletion due to either policy will be deleted.**

> **这才是"任一先满足即删"的官方逐字出处**（Log 实现页）。本仓 `gdd.md:309` 引用的那句 "A segment will be deleted whenever either of these criteria are met" —— ⚠️ **在 Kafka 4.3 官方文档站未逐字命中**；等价语义由上面 Log 页 "a segment that is eligible for deletion due to **either** policy will be deleted" 与 Topic Configs 页 `cleanup.policy` 的 "when their retention time **or** size limit has been reached" 共同支撑。**官方未给出**本仓所引那一句的逐字出处。

来源：Kafka 官方文档站 4.3 —— Broker Configs（https://kafka.apache.org/43/configuration/broker-configs/ ）：

> **log.retention.ms** — The number of milliseconds to keep a log file before deleting it (in milliseconds), If not set, the value of log.retention.minutes is used. If set to -1, no time limit is applied. / Default: null
> **log.retention.bytes** — The maximum size of the log before deleting it / Default: **-1**
> **log.retention.hours** — The number of hours to keep a log file before deleting it (in hours), tertiary to log.retention.ms property / Default: **168**

### 5.2 判定：两者是不是同一类问题？

**是同一类，但可证性差一个量级**（推导，依据上面的官方语义）：

| | Kafka「7 天 or 2 GB 取小」 | Doris「30 天分区保留」 |
|---|---|---|
| 保留条件的**类型** | **体积上限**（`retention.bytes`）**或** 时间上限（`retention.ms`） | **只有时间上限**（`start=-30` 天） |
| 生效粒度 | **按 segment**（"Retention and cleaning is always done a file at a time"），且 `retention.bytes` 是**每分区**上限 | 按**分区**（整分区 drop） |
| 文件真正删掉的时间 | segment 满足条件后 + `file.delete.delay.ms`（默认 60 s） | 分区 drop 后由 BE GC 回收（`trash_file_expire_time_sec`=0 / GC sweep ≤3600 s） |
| **在开发机上何时可观测** | **≈6 小时**（本仓设计值：200 ev/s × ~500 B ≈ 8.05 GB/天 ⇒ 2 GB ÷ (8.05/24 GB/h) ≈ 6 h） | **31 天**（无论写多大量都必须在 31 天后才能看到第 31 个最老分区被删） |
| 能否在本机加速 | **能**（改 `retention.bytes` 到很小即可立刻观测） | **能，但需换粒度**（`time_unit=HOUR`，见 §4.4）；**不能**用 `time_zone` 或 `reserved_history_periods` 加速 |

**⇒ 关键结构差异（本仓框架成立）**：
- **Kafka 的 2 GB 是「体积」上限** ⇒ 在给定摄入速率下**数小时即可触达** ⇒ 其保留策略**在开发机上可行为可证**。
- **Doris 的 30 天是「时间」上限** ⇒ **无论写多少数据，31 天内都不可观测** ⇒ 其保留策略**在开发机上不能靠等**。

⚠️ 但两者的**可观测对象也不同**（推导，不可混为一谈）：
- Kafka 观测到的是「**segment 文件被删**」⇒ 磁盘字节数下降。
- Doris 观测到的是「**分区从元数据中消失**」⇒ **分区内的数据随之被回收**，但**磁盘释放是异步的**（§3.3）⇒ 观测「分区消失」≠ 观测「磁盘下降」。

**⇒ 判据措辞必须分开**（详见 §7.2）：
- `REQ-F-MQ-03`（Kafka）⇒ **可以**是行为判据（`retention.bytes=2GB` ⇒ 数小时内看到 segment 被删 / 磁盘降）。
- `REQ-F-OLAP-03`（Doris）⇒ **不能**是「等 31 天」的行为判据；必须是「**配置 + 小时级演练**」。

---

## 6 · compaction 成本 vs 20 GB 磁盘预算（Q6）

### 6.1 Doris BE compaction

**官方给出的旋钮与默认值**（4.1.5 源码 `be/src/common/config.cpp` 逐字；括号内为 4.x BE Configuration 页所载值，二者不一致处标注）：

| 参数 | 4.1.5 源码默认 | 官方语义 |
|---|---|---|
| `disable_auto_compaction` | false | "Whether disable automatic compaction task" |
| `enable_vertical_compaction` | true | "Whether to enable vertical compaction" |
| `vertical_compaction_num_columns_per_group` | 5 | 列组大小 |
| `vertical_compaction_max_row_source_memory_mb` | **1024**（文档页 200） | "In vertical compaction, max memory usage for row_source_buffer" |
| `vertical_compaction_max_segment_size` | **1073741824**（文档页 268435456） | "In vertical compaction, max dest segment file size" |
| `enable_ordered_data_compaction` | true | — |
| `ordered_data_compaction_min_segment_size` | 10485760 | 有序 compaction 最小输入 rowset |
| `max_base_compaction_threads` | 4 | "The maximum of thread number in base compaction thread pool, -1 means one thread per disk." |
| `max_cumu_compaction_threads` | -1 | — |
| `base_compaction_min_rowset_num` | 5 | base compaction 触发阈值之一 |
| `base_compaction_max_compaction_score` | 20 | base compaction score 上限 |
| `mow_base_compaction_max_compaction_score` | **200** | **MoW 表专用**（本仓三张表全是 UNIQUE KEY ⇒ MoW） |
| `base_compaction_min_data_ratio` | 0.3 | — |
| `base_compaction_dup_key_max_file_size_mbytes` | 1024 | — |
| `compaction_promotion_size_mbytes` | 1024 | — |
| `compaction_promotion_ratio` | 0.05 | — |
| `compaction_promotion_min_size_mbytes` | 128 | — |
| `compaction_promotion_version_count` | 1000 | "this config will work for unique key merge-on-write table only" |
| `compaction_min_size_mbytes` | 64 | cumulative compaction 起始尺寸门槛 |
| `cumulative_compaction_min_deltas` | 5 | — |
| `cumulative_compaction_max_deltas` | 1000 | — |
| `cumulative_compaction_max_deltas_factor` | 10 | — |
| `total_permits_for_compaction_score` | 1000000 | compaction 内存 permit 上限 |
| `compaction_memory_bytes_limit` | （配置项存在） | — |
| `generate_compaction_tasks_interval_ms` | **100**（文档页 10 ms） | 任务生成最小间隔 |
| `compaction_num_per_round` | -1 | — |
| `max_automatic_compaction_num_per_round` | 64 | "Only valid if compaction_num_per_round = 0" |
| `mem_limit` | "90%" | BE 内存上限 |
| `storage_page_cache_limit` | "20%" | — |
| `row_cache_mem_limit` | "20%" | — |

**⇒ 官方给出的磁盘放大 / compaction 成本数字：几乎未给出，除了一条预留因子。**
- 4.x BE Configuration 页的 "compaction" 段只给**配置项与默认值**，**无**任何 "磁盘放大倍数 / 写放大 / compaction 耗时 / 每次合并多少文件 / 每 GB 数据额外占多少盘" 的数字。
- 官方也**未给出** "MoW 表的 compaction 额外空间需求" 的估算公式。⇒ 这些**官方未给出**。
- **唯一的例外**（本轮新取，见 §3.3 第 5 条）：env-checking 页给出 **BE 磁盘 = 总量 × 3 × 1.4**，且逐字注明 ×1.4 的来源是 **"40% reserved space for background compaction"** —— 这是**官方给出的、compaction 磁盘开销的唯一系数**（口径：integrated 部署、3 副本；本仓 1 副本 ⇒ ×1.4 因子可单独套用，×3 不适用——**推导**）。URL：https://doris.apache.org/docs/4.x/install/preparation/env-checking/ （"Last updated on May 17, 2026"）。

**⇒ 对本仓的可用杠杆（推导）**：`mow_base_compaction_max_compaction_score=200`（远高于普通表的 20）说明 **MoW 表默认允许更深的 compaction 积压** ⇒ 在 ≤20 GB 预算下，MoW 表在小写入量下会**长期保持多个 rowset 版本**（叠加 `compaction.max-size-amplification-percent` 类的 Paimon 概念在 Doris 侧没有对应参数）⇒ **磁盘峰值是「未 compact 的多版本」而非「compact 后的单版本」**。这与本仓 `HLD.md` §8-R5 已登记的「Paimon 主键表在 compaction 前同一 key 有多份物理版本 ⇒ 空间放大与写入放大都是真实的」是**同一类**问题，但 Doris 侧**官方未给出**任何放大倍数。

### 6.2 Paimon compaction on `file://`

**结论：官方未给出任何成本数字。**

逐字（Paimon 2.1 官方文档站，Compaction 页，https://paimon.apache.org/docs/2.1/primary-key-table/compaction/ ）：

> Compaction combines sorted runs and applies the merge engine to records with the same key. It reduces file and merge overhead for reads, **while consuming CPU and storage I/O on the write side.** Paimon's default strategy selects runs using a universal compaction policy.

> **Compaction itself does not mean that old files are immediately deleted: snapshot, tag, and partition retention have separate lifecycles.** See Maintenance.

> Writers normally run compaction in background threads. They can still wait when too many sorted runs accumulate, or when lookup compaction is needed before a commit.

> `num-sorted-run.compaction-trigger` defaults to `5`. Each Level-0 file counts as one sorted run; each occupied higher level counts as one run.

> `num-sorted-run.stop-trigger` limits the backlog at which writers wait for compaction. If unset, it defaults to `num-sorted-run.compaction-trigger + 3`.

**该页全篇无**耗时 / IO / 内存 / 合并文件数 / 磁盘放大倍数的**成本数字**。⇒ **官方未给出**（与本仓 `08a-paimon-flink-sql.md` §4.4 的既有结论**一致**；本文复核确认 2.1 稳定站 Compaction 页仍无成本数字）。

**官方给出的、与「放大」最接近的一个数字**（属**配置上限**，不是实测成本；Paimon 2.1 `CoreOptions` 生成表 `docs/generated/core_configuration.html` @ tag `release-2.1.0`）：

> | `compaction.max-size-amplification-percent` | 200 | Integer | The size amplification is defined as the amount (in percentage) of additional storage needed to store a single byte of data in the merge tree for changelog mode table. |

⚠️ 注意其限定语：**"for changelog mode table"**。⇒ **它不是给 `file://` / 本地文件系统用的实测数字，也不是"本项目会放大 200%"的承诺**；它是一个**参数定义**。⇒ **官方未给出** Paimon 在 `file://` 上的 compaction 成本数字。

**Paimon 侧 `file://` 的官方定性**（逐字，与 `08a` 一致）：Installation 页 "Use a warehouse on shared storage for a distributed cluster. **A `file:/tmp/...` warehouse is suitable for the single-machine tutorial.**"；Filesystems 页 "Local File System | `file://` | N | Built-in Support"。⇒ **`file://` 无额外限制条款，但也无任何成本保证。**

**⇒ Q6 总结论（官方口径）**：
- Doris BE compaction 的**磁盘放大倍数 / 成本**：**官方未给出**。
- Paimon compaction 在 `file://` 上的**成本**：**官方未给出**。
- **两者的官方口径下都只有"阈值类配置值"，没有"成本数字"。**
- ⇒ 本仓 ≤20 GB 预算与 compaction 成本的关系，**只能靠 P1/P2 的真实写入量实测**，官方无法给数。

---

## 7 · 对本仓设计的直接影响

### 7.1 `docker/doris/init.sql` 需要改什么

| 位置 | 现状 | 建议 | 依据 |
|---|---|---|---|
| L3-7 注释 + L16 `ADMIN SET FRONTEND CONFIG ("dynamic_partition_enable" = "true");` | 声称「官方默认是 false」「必须显式开」 | **改注释**：4.1.5 源码默认已是 `true`；此行**无害但多余**，且**运行时修改在 FE 重启后失效**（官方 FE Configuration 页逐字）。⇒ 建议改为在 `fe.conf` 写 `dynamic_partition_enable = true`（显式、持久、且与"我们依赖它"这一事实相符），DML 那行可保留但注明是「幂等冗余」 | §1.3 |
| L18-19 注释掉的 `max_dynamic_partition_num = 500` | 「默认 500」 | **改注释**：4.1.5 源码默认 **20000**；该行若启用是**收紧**不是放宽。**建议直接删除该行**（33 个分区远低于 20000，用不着） | §1.4 |
| L47-48 注释「`dynamic_partition.start` 出厂默认 -2147483648」 | — | **保留，逐字正确** | §1.2 |
| 三张表的 `PROPERTIES` | `time_unit=DAY, start=-30, end=3, prefix=p, create_history_partition=true` | **保留**（这是**生产**保留窗口，不应为可证性而改）。补一条注释：`start=-30,end=3` ⇒ **建表即建 34 个分区**（30 历史 + 今天 + 3 未来） | §2.3 |
| 三张表 | 未设 `dynamic_partition.time_zone` | **建议显式设**为业务时区（与 `window_start` 的写入时区一致），避免分区边界随宿主容器 TZ 漂移 | §4.6 |
| **新增** | — | **新增一张小时级演练表**（`DATETIMEV2(0)` 分区列，`time_unit=HOUR`, `start=-2`, `end=1`）⇒ 专供 §7.2 的「小时级演练」判据使用。⚠️ **不得**用它替代生产表的 30 天断言 | §4.4、§4.5 |
| **新增** | — | `ads_funnel_step` 的分区列是 `DATE` ⇒ **不能用 HOUR 演练**（官方 DdlException）。若要为漏斗表补行为证据，只能靠同一张 HOUR 演练表覆盖"动态分区机制"这一层，**不能**覆盖"漏斗表自身的 30 天保留" | §4.4 |

### 7.2 `REQ-F-OLAP-03` 的判据该怎么写

**答案：写「配置 + 小时级演练」，不是「等到第 31 天实测」。**

**理由（推导，依据 §2/§4）**：
1. `start=-30, DAY` 下，被删的是**第 31 个最老的日分区**；要产生第 31 个最老的分区，**必须先有 31 天的数据** ⇒ 等待路线**在开发机上不可能通过**。
2. 但动态分区的**删除机制本身**可以用官方支持的 `time_unit=HOUR` + 小 `start` 在**≤2 个调度周期（≤1200 s）**内观测到 ⇒ 机制可证。
3. 官方**未给出**任何"用 HOUR 加速验证 30 天策略"的推荐 ⇒ 因此判据**必须诚实地写成两段**（"配置的语义正确" + "机制的行为可证"），**不得**声称已验证「30 天窗口本身」。

**建议的判据措辞（可直接落 SRS / gdd §1.5）**：

> **REQ-F-OLAP-03 判据（两层）**
> 1. **配置层（对生产表 `ads_window_metric` / `ads_funnel_step` / `ads_late_expired_count`）**：`SHOW DYNAMIC PARTITION TABLES` 显示 `Enable=true`、`TimeUnit=DAY`、`Start=-30`、`End=3`、`Prefix=p`；`SHOW PARTITIONS` 显示**恰好 34 个**分区（30 历史 + 今天 + 3 未来），且分区名形如 `p<yyyyMMdd>`。
> 2. **行为层（对独立的 HOUR 粒度演练表 `ads_window_metric_rehearsal`）**：写入 3–4 小时前的数据后，**≤1200 s（2 × `dynamic_partition_check_interval_seconds` = 600 s）**内，`SHOW PARTITIONS` 中对应小时的分区（名形如 `p<yyyyMMddHH>`）消失，且 `SHOW DYNAMIC PARTITION TABLES` 的 `LastDropPartitionMsg` 有记录。
> 3. **明确不声称**：本判据**不**证明「30 天窗口的边界时刻正确」——那需要真实 31 天数据，登记为**后续实机验证任务**。

**⇒ 需新增的后续任务（票面要求：不在本次实机验证）**：
- 「Doris 30 天分区的真实边界」实机观察：需连续运行 ≥31 天且有真实写入，**判定条件**为「第 31 个最老的日分区在它变为第 31 老后被删」。
- 「DROP PARTITION 后 BE 磁盘回收时延」实测：需在实机上测「分区消失 → `du` 下降」的时延（官方**未给出**上界，见 §3.3）。

### 7.3 `gdd.md` D8 / `HLD.md` §7.2 G-D8 / `canonical-truth.md` E 需要修订的**具体条目**

1. **删/改**：「Doris 动态分区另有 **FE 级总开关 `dynamic_partition_enable`，默认 `false`**」⇒ 改为「4.1.5 源码默认 `true`；4.x 动态分区页与 4.x FE 配置页口径不一致（前者 `false`、后者 `true`）；本仓以 4.1.5 源码为准 = `true`；`init.sql` 中那一行是幂等冗余，且运行时设置在 FE 重启后失效，应改写进 `fe.conf`」。
2. **改**：「`max_dynamic_partition_num` 默认 500」⇒ 「4.1.5 源码默认 **20000**；本仓实际只需 33 个分区」。
3. **保留并加强**：「`dynamic_partition.start` 默认 `-2147483648` = 不删历史分区」⇒ 补上源码锚点（`DynamicPartitionProperty.MIN_START_OFFSET = Integer.MIN_VALUE`）与"未设置 = 与哨兵精确相等"的判定细节。
4. **改**：「三处必须齐备」⇒ 实际是**四处**：表级 `enable` / 显式 `start` / `time_unit`+`end`+`prefix` / `create_history_partition`（FE 总开关不是第 4 个前置条件，4.1.5 上它默认就开着）。
5. **改**：「D8 的第三个前置条件」这一编号体系整体作废（见上一条）。

### 7.4 `gdd.md` D3（Kafka）需要修订的**具体条目**

1. **引用修正**：`gdd.md:309` 写的「官方原话『A segment will be deleted whenever **either** of these criteria are met』」—— ⚠️ **在 Kafka 4.3 官方文档站未逐字命中**。**官方等价表述**（可逐字引用）：
   - Log 实现页（https://kafka.apache.org/43/implementation/log/ ）：「**If both policies are enabled at the same time, a segment that is eligible for deletion due to either policy will be deleted.**」
   - Topic Configs 页 `cleanup.policy`：「The "delete" policy (which is the default) will discard old segments when their retention time **or** size limit has been reached.」
2. **补一个官方限定**：`retention.bytes` 是**每分区**上限 ⇒ 官方逐字「Since this limit is enforced at the partition level, multiply it by the number of partitions to compute the topic retention in bytes.」⇒ **本仓 6 分区 ⇒ 2 GB/分区 ⇒ 主题级上限实为 12 GB**。⚠️ **这直接影响 gdd D3 的 6 小时算术**：`2 GB` 若按**主题级**理解则 6 h；若按**每分区**理解则 12 GB ⇒ ≈36 h。**必须先确认本仓那 2 GB 是配在 broker 级 `log.retention.bytes`（每分区）还是每 topic 覆盖**——`.env.example:28` 的 `KAFKA_RETENTION_BYTES=2147483648` 与 `docker/scripts/init-kafka.sh:11` 的 `RETENTION_BYTES` 走的是**哪条路径**决定了实际窗口。**官方未给出**该仓库的具体口径（属本仓实机配置项，须本仓自查）。
3. **补「不是立即删」**：官方 `file.delete.delay.ms` 默认 60000（1 分钟）⇒ segment 满足条件后**还需 1 分钟**才从文件系统删除。⇒ 观测判据的等待窗应写「**≤ 保留条件触达时间 + 60 s**」。

### 7.5 `HLD.md` §8-R5（Paimon 物理删除只在快照过期）—— 本次复核**不改变**

本文 §6.2 引的 Paimon 逐字 "Compaction itself does not mean that old files are immediately deleted: snapshot, tag, and partition retention have separate lifecycles." 与 `08a` §2.7 的一致 ⇒ **Paimon 侧结论不变，官方未给出的项也不变**（compaction 成本数字、`file://` 上的成本数字）。

---

## 官方未给出的项

1. **`dynamic_partition.start` 的历史变更点**（从什么值改到 `-2147483648`、哪个版本改的）—— **官方未给出**（4.x 动态分区页与 FE 配置页均无版本化说明；4.1.5 Release Notes 无相关条目）。
2. **4.x 动态分区页与 4.x FE Configuration 页对 `dynamic_partition_enable` 默认值的矛盾**（`false` vs `true`）—— **官方未给出**调和说明；4.1.5 源码为 `true`。
3. **`max_dynamic_partition_num` 在 4.x 文档（500）与 4.1.5 源码（20000）之间的不一致** —— **官方未给出**调和说明。
4. **BE `trash_file_expire_time_sec` 在 4.x 文档（86400）与 4.1.5 源码（0）之间的不一致** —— **官方未给出**调和说明。
5. **4.x BE Configuration 页与 4.1.5 源码在 `vertical_compaction_max_row_source_memory_mb`（200 vs 1024）、`vertical_compaction_max_segment_size`（268435456 vs 1073741824）、`generate_compaction_tasks_interval_ms`（10 vs 100）上的不一致** —— **官方未给出**调和说明。
6. **「DROP PARTITION 后磁盘在多久内释放」的官方保证 / 上界** —— **官方未给出**。
7. **Doris BE compaction 的耗时 / 每次合并文件数 / 单版本写放大的官方数字** —— **官方未给出**（官方只给阈值类配置值 + env-checking 页的 ×1.4 磁盘预留因子，见 §3.3/§6.1；「compaction 花多少时间 / 多少 IO」没有任何官方数字）。
8. **「MoW 表的 compaction 额外空间需求」的官方估算** —— **官方未给出**。
9. **Doris 官方对「动态分区保留 30 天 ⇒ 需要多少磁盘」的换算** —— **官方未给出**。
10. **Paimon 2.1 compaction 在 `file://` 上的耗时 / IO / 内存 / 磁盘放大成本数字** —— **官方未给出**（Compaction 页全篇无成本数字）。
11. **官方对「用 `time_unit=HOUR` 加速验证保留策略」的任何推荐或测试指引** —— **官方未给出**。
12. **官方对 `dynamic_partition.time_zone` 与业务时区对齐 / 用作测试手段的任何说明** —— **官方未给出**（官方只说"默认取服务器系统时区"）。
13. **本仓 gdd D3 所引 Kafka 逐字句 "A segment will be deleted whenever either of these criteria are met" 的官方出处** —— **官方未给出**（Kafka 4.3 文档站未逐字命中；等价语义见 Log 页 / `cleanup.policy` 条目）。
14. **Kafka 官方对 "retention.bytes 是主题级还是分区级" 的额外澄清** —— 官方**已给出**（分区级），故不在此列；但**本仓那 2 GB 实际配在哪一层**属本仓实机配置项，**官方未给出**（本仓须自查）。

---

## 引用来源清单

| # | 来源 | URL | 页面版本 / Last updated | 用于 |
|---|---|---|---|---|
| S1 | Apache Doris — Dynamic Partitioning (4.x) | https://doris.apache.org/docs/4.x/table-design/data-partitioning/dynamic-partitioning/ | Last updated on Oct 16, 2026 | §1.1、§1.3、§1.5、§2.1、§2.2、§4.1、§4.3、§4.5、§4.6、§4.7 |
| S2 | doris-website — `versioned_docs/version-4.x/table-design/data-partitioning/dynamic-partitioning.md` | https://github.com/apache/doris-website/blob/master/versioned_docs/version-4.x/table-design/data-partitioning/dynamic-partitioning.md | sha `bcb7f009` | §1.1、§2.1、§4.1–§4.5（S1 的仓库同源） |
| S3 | doris-website — `versioned_docs/version-4.x/admin-manual/config/fe-config.md` | https://github.com/apache/doris-website/blob/master/versioned_docs/version-4.x/admin-manual/config/fe-config.md | sha `066a3e6f` | §1.3、§1.4、§2.1、§7.1（FE 配置逐字） |
| S4 | Apache Doris — FE Configuration (4.x) | https://doris.apache.org/docs/4.x/admin-manual/config/fe-config/ | Last updated on Oct 16, 2026 | §1.3、§2.1、§7.1 |
| S5 | Apache Doris — BE Configuration (4.x) | https://doris.apache.org/docs/4.x/admin-manual/config/be-config/ | Last updated on Oct 16, 2026 | §3.1、§6.1（BE 旋钮与官方冲突项） |
| S6 | doris-website — `versioned_docs/version-4.x/admin-manual/config/be-config.md` | https://github.com/apache/doris-website/blob/master/versioned_docs/version-4.x/admin-manual/config/be-config.md | sha `d7dcda95` | §3.1、§6.1 |
| S7 | apache/doris @ `4.1.5-release` — `fe/fe-core/.../catalog/DynamicPartitionProperty.java` | https://github.com/apache/doris/blob/4.1.5-release/fe/fe-core/src/main/java/org/apache/doris/catalog/DynamicPartitionProperty.java | tag `4.1.5-release` | §1.2、§4.6 |
| S8 | apache/doris @ `4.1.5-release` — `fe/fe-core/.../common/util/DynamicPartitionUtil.java` | https://github.com/apache/doris/blob/4.1.5-release/fe/fe-core/src/main/java/org/apache/doris/common/util/DynamicPartitionUtil.java | tag `4.1.5-release` | §1.2、§2.2、§4.1、§4.4 |
| S9 | apache/doris @ `4.1.5-release` — `fe/fe-core/.../clone/DynamicPartitionScheduler.java` | https://github.com/apache/doris/blob/4.1.5-release/fe/fe-core/src/main/java/org/apache/doris/clone/DynamicPartitionScheduler.java | tag `4.1.5-release` | §1.2、§1.3、§2.1、§2.3、§3.3、§4.7 |
| S10 | apache/doris @ `4.1.5-release` — `fe/fe-common/.../common/Config.java` | https://github.com/apache/doris/blob/4.1.5-release/fe/fe-common/src/main/java/org/apache/doris/common/Config.java | tag `4.1.5-release` | §1.3、§1.4、§2.1 |
| S11 | apache/doris @ `4.1.5-release` — `be/src/common/config.h` | https://github.com/apache/doris/blob/4.1.5-release/be/src/common/config.h | tag `4.1.5-release` | §3.1、§6.1 |
| S12 | apache/doris @ `4.1.5-release` — `be/src/common/config.cpp` | https://github.com/apache/doris/blob/4.1.5-release/be/src/common/config.cpp | tag `4.1.5-release` | §3.1、§3.2、§6.1 |
| S13 | apache/doris @ `4.1.5-release` — `conf/fe.conf` | https://github.com/apache/doris/blob/4.1.5-release/conf/fe.conf | tag `4.1.5-release` | §1.3（该文件未出现 `dynamic_partition_enable`） |
| S14 | apache/doris @ `4.1.5-release` — `conf/be.conf` | https://github.com/apache/doris/blob/4.1.5-release/conf/be.conf | tag `4.1.5-release` | §3.1（未出现 `trash_file_expire_time_sec`） |
| S15 | Apache Kafka — Topic Configs (4.3) | https://kafka.apache.org/43/configuration/topic-configs/ | Last modified May 22, 2026 | §5.1 |
| S16 | Apache Kafka — Log (4.3) | https://kafka.apache.org/43/implementation/log/ | Last modified May 22, 2026 | §5.1（"either policy" 逐字出处）、§5.2 |
| S17 | Apache Kafka — Broker Configs (4.3) | https://kafka.apache.org/43/configuration/broker-configs/ | AK 4.3.X | §5.1 |
| S18 | Apache Paimon — Compaction (2.1) | https://paimon.apache.org/docs/2.1/primary-key-table/compaction/ | Paimon 2.1（稳定站） | §6.2 |
| S19 | Apache Paimon — CoreOptions 生成表（`release-2.1.0`） | https://github.com/apache/paimon/blob/release-2.1.0/docs/generated/core_configuration.html | tag `release-2.1.0` | §6.2（`compaction.max-size-amplification-percent=200`） |
| S20 | Apache Paimon — Installation (2.1) | https://paimon.apache.org/docs/2.1/flink/installation/ | Paimon 2.1 | §6.2（`file://` 适用性说明） |
| S21 | Apache Paimon — Filesystems (2.1) | https://paimon.apache.org/docs/2.1/maintenance/filesystems/ | Paimon 2.1 | §6.2（`file://` = Built-in Support） |
| S22 | Apache Paimon — Compaction / Dedicated Compaction / Action Jars（2.1，指针见 08a） | https://paimon.apache.org/docs/2.1/maintenance/dedicated-compaction/ | Paimon 2.1 | §6.2（官方 compact 入口；细节见 `08a-paimon-flink-sql.md` §4） |
| S23 | Apache Doris — Release 4.1.5 | https://doris.apache.org/releases/v4.1/release-4.1.5/ | — | §1.5（无 `dynamic_partition.start` 相关条目） |
| S24 | ★ Apache Doris — Hardware and Software Environment Check (4.x) | https://doris.apache.org/docs/4.x/install/preparation/env-checking/ | Last updated on May 17, 2026 | §3.3（dev/test 磁盘下限 FE 10 GB+ / BE 50 GB+ 的**一手出处**）、§6.1（**BE 磁盘 = 总量 ×3× 1.4，×1.4 逐字为 "40% reserved space for background compaction"** —— compaction 磁盘开销的唯一官方系数） |
| S25 | doris-website — `versioned_docs/version-4.x/install/preparation/env-checking.md` | https://github.com/apache/doris-website/blob/master/versioned_docs/version-4.x/install/preparation/env-checking.md | sha `7386e88f` | §3.3、§6.1（S24 的仓库同源逐字） |
