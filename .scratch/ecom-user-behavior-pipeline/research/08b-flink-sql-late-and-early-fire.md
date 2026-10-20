# Flink 2.2 SQL：迟到侧输出 与 early-fire / late-fire 一手取证

- **调研时间**：2026-10-20
- **文档版本**：Apache Flink **2.2**（nightlies 站点 `flink-docs-release-2.2`，页面右上角版本标识 `v2.2.1`）；源码分支 `apache/flink` **`release-2.2`**；JIRA 数据取自 `issues.apache.org` REST API 与页面。
- **取证方式**：官方文档站（`nightlies.apache.org/flink/flink-docs-release-2.2/...`）+ `apache/flink` GitHub 仓库 `release-2.2` 分支源码/文档 markdown + ASF JIRA。每条承重结论都附 URL 与英文逐字引用。
- **说明**：本文不引用本仓库既有 `research-notes.md` 的任何内容作为证据。

---

## 5 · 迟到侧输出

### 5.1 FLINK-10031 现状：Open / Unresolved

**结论**：截至 2026-10-20，FLINK-10031 仍为 **Open**，Resolution **Unresolved**，最后更新 2025-02-10；即"Table API & SQL 支持迟到事件"这一需求**至今未实现**，官方也从未宣告关闭。

逐字原文（JIRA 页面与 REST API 一致）：

> **Status:** Open
> **Resolution:** Unresolved
> **Type:** Improvement
> **Component/s:** Table SQL / API
> **Created:** 02/Aug/18 13:06
> **Updated:** 10/Feb/25 13:10
>
> #### Description
> The Table API & SQL drop late events right now. We should offer something like a side channel that allows to capture late events for separate processing. For example, this could either be simply a table sink or a special table (in Table API) derived from the original table that allows further processing. The exact design needs to be discussed.

- URL: https://issues.apache.org/jira/browse/FLINK-10031
- REST: https://issues.apache.org/jira/rest/api/2/issue/FLINK-10031

### 5.2 FLINK-20527 现状：Open / Unresolved

**结论**：FLINK-20527 同样仍为 **Open**，Resolution **Unresolved**，标签含 `auto-deprioritized-major` / `auto-deprioritized-minor`，最后更新 2025-02-10。

逐字原文：

> **Status:** Open
> **Resolution:** Unresolved
> **Type:** New Feature
> **Priority:** Not a Priority
> **Affects Version/s:** 1.11.2
> **Component/s:** Table SQL / API
> **Created:** 08/Dec/20 08:03
> **Updated:** 10/Feb/25 13:10
>
> #### Description
> the datastream api has watermarker and can add a side output,how to add a side output for flink sql watermaker?

- URL: https://issues.apache.org/jira/browse/FLINK-20527

### 5.3 Flink 2.2 官方文档中不存在任何 SQL 级迟到侧输出机制

**结论**：逐页核对 Flink 2.2 官方 Table API & SQL 文档树（SQL 全部查询页、Table API 页、DataStream API Integration 页、Configuration 页），**没有任何** SQL 级的"迟到数据侧输出 / late side output / too-late sink / 迟到表"语法或配置项；`Side Outputs` 只出现在 **DataStream API** 的导航树下（`docs/dev/datastream/side_output/`），不在 Table SQL 之下。

官方文档对**DataStream** 侧的侧输出有明确表述（Flink 2.2 `docs/dev/datastream/operators/windows.md`）：

> ```
> stream
>        .keyBy(...)               <-  keyed versus non-keyed windows
>        .window(...)              <-  required: "assigner"
>       [.trigger(...)]            <-  optional: "trigger" (else default trigger)
>       [.evictor(...)]            <-  optional: "evictor" (else no evictor)
>       [.allowedLateness(...)]    <-  optional: "lateness" (else zero)
>       [.sideOutputLateData(...)] <-  optional: "output tag" (else no side output for late data)
>        .reduce/aggregate/apply()      <-  required: "function"
>       [.getSideOutput(...)]      <-  optional: "output tag"
> ```

以及专节标题与说明：

> ### Getting late data as a side output
> Using Flink's [side output](...) feature you can get a stream of the data **that was discarded as late**.
> You first need to specify that you want to get late data using `sideOutputLateData(OutputTag)` on the windowed stream. Then, you can get the side-output stream on the result of the windowed operation:

> ### Late elements considerations
> When specifying an allowed lateness greater than 0, the window along with its content is kept after the watermark passes the end of the window. In these cases, when a late but not dropped element arrives, it could trigger another firing for the window. These firings are called `late firings`, as they are triggered by late events and in contrast to the `main firing` which is the first firing of the window. In case of session windows, late firings can further lead to merging of windows, as they may "bridge" the gap between two pre-existing, unmerged windows.
>
> The elements emitted by a late firing should be treated as updated results of a previous computation, i.e., your data stream will contain multiple results for the same computation. Depending on your application, you need to take these duplicated results into account or deduplicate them.

**Python DataStream API 同名能力存在**（同一文档的 Python 段落逐字）：

> ```
>         .key_by(...)
>         .window(...)                 <-  required: "assigner"
>        [.trigger(...)]               <-  optional: "trigger" (else default trigger)
>        [.allowed_lateness(...)]      <-  optional: "lateness" (else zero)
>        [.side_output_late_data(...)] <-  optional: "output tag" (else no side output for late data)
>         .reduce/aggregate/apply()    <-  required: "function"
>        [.get_side_output(...)]       <-  optional: "output tag"
> ```

- URL: https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/datastream/operators/windows/#getting-late-data-as-a-side-output
- 源码（markdown）: `docs/content/docs/dev/datastream/operators/windows.md`，blob `6ef7c6492eca62d8e9e8779b9e4bec509076a44d` @ release-2.2 — https://github.com/apache/flink/blob/release-2.2/docs/content/docs/dev/datastream/operators/windows.md

### 5.3b DataStream 侧输出机制本身（`side_output.md`）逐字

**结论**：DataStream 侧输出是**官方通用机制**，可从多种函数发出；取用统一用 `getSideOutput(OutputTag)`。这是与 SQL 侧（FLINK-10031/20527 Open）的直接对照。

逐字原文（`docs/content/docs/dev/datastream/side_output.md`，blob `1111853cd143e41cd1ea7810ce411e1c876d1e67` @ release-2.2）：

> In addition to the main stream that results from `DataStream` operations, you can also produce any number of additional side output result streams. The type of data in the result streams does not have to match the type of data in the main stream and the types of the different side outputs can also differ.

> Emitting data to a side output is possible from the following functions:
>
> - [ProcessFunction](...)
> - [KeyedProcessFunction](...#the-keyedprocessfunction)
> - CoProcessFunction
> - KeyedCoProcessFunction
> - [ProcessWindowFunction](...#processwindowfunction)
> - ProcessAllWindowFunction

> For retrieving the side output stream you use `getSideOutput(OutputTag)` on the result of the `DataStream` operation. This will give you a `DataStream` that is typed to the result of the side output stream:

> **Note** If it produces side output, `get_side_output(OutputTag)` must be called in Python API. Otherwise, the result of side output stream will be output into the main stream which is unexpected and may fail the job when the data types are different.

- URL: https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/datastream/side_output/
- 源码 URL: https://github.com/apache/flink/blob/release-2.2/docs/content/docs/dev/datastream/side_output.md

### 5.3c SQL 侧 vs DataStream 侧对照（gdd「混合路线」的官方依据）

| 能力 | SQL / Table API（Flink 2.2） | DataStream API（Flink 2.2） |
| --- | --- | --- |
| 迟到数据单独流出 | **无**（FLINK-10031 / FLINK-20527 均 Open/Unresolved；`WindowAggOperator` 逐字 "late elements ... will be simply dropped."） | **有**：`.sideOutputLateData(OutputTag)` / `.side_output_late_data(...)` + `.getSideOutput(tag)` / `.get_side_output(tag)`；侧输出内容为逐字 "that was discarded as late" |
| 迟到但未丢弃的记录 | SQL 窗口无 allow-lateness（FLINK-24000 Closed/Later）；Group Window Agg 侧仅 `table.exec.emit.allow-lateness`（@Experimental） | **有**：`.allowedLateness(...)`，默认 0；>0 时保留窗口状态，迟到记录触发 `late firings`，官方要求消费端 "take these duplicated results into account or deduplicate them" |
| 窗口内提前发射 | 仅 legacy Group Window Aggregation（`table.exec.emit.early-fire.*`，@Experimental）；Window TVF 直接抛异常 | 可用自定义 `Trigger` / `EventTimeTriggers.withEarlyFirings` |

**注意**：Table API 与 DataStream API 之间**没有** SQL 侧的侧输出通道 —— 官方逐字：

> Since the DataStream API does not support changelog processing natively, the code assumes append-only/insert-only semantics during the stream-to-table and table-to-stream conversion.


**Table API 与 DataStream API 的转换只处理 changelog，不提供侧输出通道**（Flink 2.2 `docs/dev/table/data_stream_api/`）：

> Since the DataStream API does not support changelog processing natively, the code assumes append-only/insert-only semantics during the stream-to-table and table-to-stream conversion.

- URL: https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/table/data_stream_api/#converting-between-datastream-and-table

### 5.4 2.2 官方文档对"SQL 窗口中迟到记录"的处理：直接丢弃

**结论**：在 Flink 2.2 中，**SQL 窗口（TVF 窗口聚合与 Group Window Aggregation）遇到迟到记录一律丢弃**，没有任何 SQL 级补救通道。官方逐字表述（DataStream 窗口文档中的通用窗口语义；SQL 层沿用同一语义，且 5.5/5.6 的 SQL 源码证实 SQL 侧同样是"drop"）：

> By default, late elements are dropped when the watermark is past the end of the window. However, Flink allows to specify a maximum *allowed lateness* for window operators. Allowed lateness specifies by how much time elements can be late before they are dropped, and its default value is 0. Elements that arrive after the watermark has passed the end of the window but before it passes the end of the window plus the allowed lateness, are still added to the window.

> By default, the allowed lateness is set to `0`. That is, elements that arrive behind the watermark will be dropped.

- URL: https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/datastream/operators/windows/#allowed-lateness
- 源码（markdown）: `docs/content/docs/dev/datastream/operators/windows.md`，blob `6ef7c6492eca62d8e9e8779b9e4bec509076a44d` @ release-2.2

另有一条官方对"迟到元素被忽略"的表述（Flink 2.2 `docs/dev/datastream/event-time/built_in.md`）：

> ...the `maxOutOfOrderness`, i.e. the maximum amount of time an element is allowed to be late before being ignored when computing the final result for the given window. ... If `lateness > 0` then the element is considered late and is, by default, ignored when computing the result of the job for its corresponding window.

- URL: https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/datastream/event-time/built_in/#fixed-amount-of-lateness
- 源码（markdown）: `docs/content/docs/dev/datastream/event-time/built_in.md`，blob `af5bc0c5ca4d0eeef5e237e70894c4681ecf4429` @ release-2.2

### 5.4b 官方对「late firing」与「重复结果」的逐字表述（★本项目迟到落点的机制依据）

**结论**：官方把「允许延迟内的迟到元素」与「超期被丢弃的元素」分成**两条通道**，并明确要求消费端处理「同一计算的多个结果」。这三段逐字原文是本项目「允许延迟内 → 窗口修正；超期 → 只计数」落点的官方机制映射。

逐字原文（DataStream 窗口文档 "Late elements considerations" 节，release-2.2）：

> When specifying an allowed lateness greater than 0, the window along with its content is kept after the watermark passes the end of the window. In these cases, when a late but not dropped element arrives, it could trigger another firing for the window. These firings are called `late firings`, as they are triggered by late events and in contrast to the `main firing` which is the first firing of the window. In case of session windows, late firings can further lead to merging of windows, as they may "bridge" the gap between two pre-existing, unmerged windows.

> **The elements emitted by a late firing should be treated as updated results of a previous computation, i.e., your data stream will contain multiple results for the same computation. Depending on your application, you need to take these duplicated results into account or deduplicate them.**

逐字原文（DataStream 窗口文档 "Getting late data as a side output" 节，release-2.2）：

> Using Flink's [side output]({{< ref "docs/dev/datastream/side_output" >}}) feature you can get a stream of the data **that was discarded as late**.

> You first need to specify that you want to get late data using `sideOutputLateData(OutputTag)` on the windowed stream. Then, you can get the side-output stream on the result of the windowed operation:

窗口算子链上的官方注释（逐字，`windows.md` Keyed Windows 代码块）：

> ```
>    [.allowedLateness(...)]    <-  optional: "lateness" (else zero)
>    [.sideOutputLateData(...)] <-  optional: "output tag" (else no side output for late data)
> ```

**⇒ 三条官方机制与本项目落点的对应**（仅为机制对应，官方未提及本项目）：

| 官方机制 | 官方语义 | 本项目落点（gdd §1.3 / A.1） |
| --- | --- | --- |
| `allowedLateness`（默认 **0**） | 元素迟到在「watermark 越过窗口末尾 + allowed lateness」之前**仍进窗口**，并可触发一次 **late firing** | 允许延迟 **120 s** → 落「窗口修正」通道 |
| late firing 的产出 | *"multiple results for the same computation"*，官方要求消费端 *"take these duplicated results into account or deduplicate them"* | Doris UNIQUE MoW same-key upsert + `revision` 递增承接该语义 |
| `sideOutputLateData` | 侧输出的是 *"the data that was discarded as late"*（**超期被丢弃**的那部分） | 落「迟到计数表」通道（超期 → 只计数） |

- URL: https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/datastream/operators/windows/#late-elements-considerations
- 源码（markdown）: `docs/content/docs/dev/datastream/operators/windows.md`，blob `6ef7c6492eca62d8e9e8779b9e4bec509076a44d` @ release-2.2
- side output 机制页: `docs/content/docs/dev/datastream/side_output.md`，blob `1111853cd143e41cd1ea7810ce411e1c876d1e67` @ release-2.2 — 官方逐字：侧输出可从 ProcessFunction / KeyedProcessFunction / CoProcessFunction / KeyedCoProcessFunction / ProcessWindowFunction / ProcessAllWindowFunction 发出，取用为 `getSideOutput(OutputTag)`（Python 为 `get_side_output`）。

### 5.5 Window TVF（SQL 新式窗口）迟到记录：**源码级明确"直接丢弃"**

**结论**：Flink 2.2 的 Window TVF 聚合算子（`WindowAggOperator`）在源码 javadoc 中逐字写明：**不支持 early-fire 与 late-arrival，迟到元素被直接丢弃**。这是 SQL 侧"迟到数据没有出口"最硬的官方证据。

逐字原文（`flink-table/flink-table-runtime/src/main/java/org/apache/flink/table/runtime/operators/window/tvf/common/WindowAggOperator.java`）：

> ```
> * <p>Note: currently, {@link WindowAggOperator} doesn't support early-fire and late-arrival. Thus
> * late elements (elements belong to emitted windows) will be simply dropped.
> ```

同文件中还有迟到计数的 metric 常量（官方把"被丢弃的迟到记录"作为可观测指标暴露）：

> ```
>     private static final String LATE_ELEMENTS_DROPPED_METRIC_NAME = "numLateRecordsDropped";
>     private static final String LATE_ELEMENTS_DROPPED_RATE_METRIC_NAME = "lateRecordsDroppedRate";
> ```

以及丢弃逻辑：

> ```
>         boolean isElementDropped = windowProcessor.processElement(currentKey, inputRow);
>         if (isElementDropped) {
>             // markEvent will increase numLateRecordsDropped
>             lateRecordsDroppedRate.markEvent();
>         }
> ```

- 文件路径: `flink-table/flink-table-runtime/src/main/java/org/apache/flink/table/runtime/operators/window/tvf/common/WindowAggOperator.java`
- blob sha: `093a58828070b1c778f84874e8a43c2b81be10f8`（ref = release-2.2）
- URL: https://github.com/apache/flink/blob/release-2.2/flink-table/flink-table-runtime/src/main/java/org/apache/flink/table/runtime/operators/window/tvf/common/WindowAggOperator.java

### 5.6 Group Window Aggregation（SQL 旧式窗口）迟到记录：**同样丢弃**（源码级）

**结论**：SQL 的 **Group Window Aggregation** 走 `WindowOperator` → `GeneralWindowProcessFunction`，迟到窗口在 `assignStateNamespace` 阶段被过滤掉（不进入累加器、不触发 trigger），并计入 `numLateRecordsDropped` metric。

逐字原文（`.../groupwindow/internal/GeneralWindowProcessFunction.java`）：

> ```
>     @Override
>     public Collection<W> assignStateNamespace(RowData inputRow, long timestamp) throws Exception {
>         Collection<W> elementWindows = windowAssigner.assignWindows(inputRow, timestamp);
>         reuseAffectedWindows = new ArrayList<>(elementWindows.size());
>         for (W window : elementWindows) {
>             if (!isWindowLate(window)) {
>                 reuseAffectedWindows.add(window);
>             }
>         }
>         return reuseAffectedWindows;
>     }
> ```

逐字原文（`.../groupwindow/internal/InternalWindowProcessFunction.java`）：

> ```
>     /**
>      * Returns {@code true} if the watermark is after the end timestamp plus the allowed lateness of
>      * the given window.
>      */
>     protected boolean isWindowLate(W window) {
>         return (windowAssigner.isEventTime()
>                 && (toEpochMillsForTimer(cleanupTime(window), ctx.getShiftTimeZone())
>                         <= ctx.currentWatermark()));
>     }
> ```

逐字原文（`.../groupwindow/operator/WindowOperator.java`，含 metric 名与丢弃计数）：

> ```
>     private static final String LATE_ELEMENTS_DROPPED_METRIC_NAME = "numLateRecordsDropped";
>     private static final String LATE_ELEMENTS_DROPPED_RATE_METRIC_NAME = "lateRecordsDroppedRate";
> ```
> ```
>         if (isElementDropped) {
>             // markEvent will increase numLateRecordsDropped
>             lateRecordsDroppedRate.markEvent();
>         }
> ```

- 文件路径:
  - `flink-table/flink-table-runtime/src/main/java/org/apache/flink/table/runtime/operators/window/groupwindow/internal/GeneralWindowProcessFunction.java`，blob `57180b6c2bcba906a5fcece31b3e34437970fafb`
  - `flink-table/flink-table-runtime/src/main/java/org/apache/flink/table/runtime/operators/window/groupwindow/internal/InternalWindowProcessFunction.java`，blob `3a3a4574429dff23b160955f5c5e263faf6a3e70`
  - `flink-table/flink-table-runtime/src/main/java/org/apache/flink/table/runtime/operators/window/groupwindow/operator/WindowOperator.java`，blob `ccc0265c10064012d8aaaee46836e27be564d236`
- URL（以 WindowOperator 为例）: https://github.com/apache/flink/blob/release-2.2/flink-table/flink-table-runtime/src/main/java/org/apache/flink/table/runtime/operators/window/groupwindow/operator/WindowOperator.java

### 5.7 "Group Window Aggregation 支持迟到数据更新"仍是**未实现**的开放票

**结论**：JIRA FLINK-6970（为 Group Window Aggregation 增加迟到数据更新，即"保留窗口状态一段时间、迟到记录到达时重发并撤回旧结果"）**至今 Open**，说明"SQL 窗口结果因迟到记录被更新/撤回"在 2.2 也**没有**。

逐字原文：

> **Status:** Open
> **Resolution:** Unresolved
> **Updated:** 2021-12-16
>
> #### Description
> Late arriving data is a common issue for group window aggregates. At the moment, the Table API simply drops late arriving records. Another approach are deferred computation (FLINK-6969) and late data updates.
> This issue proposes to add late data updates for group window aggregates. Instead of discarding the state of a window when the result has been computed, the state is kept for a certain time interval. If a late record for a window is received within this interval, an updated result is emitted (and the previous result is retracted).

- URL: https://issues.apache.org/jira/browse/FLINK-6970

相关旁证：FLINK-24000 "window aggregate support allow lateness"（Window TVF 聚合的 allow-lateness 支持）已 **Closed / Resolution: Later**，说明 **Window TVF 至今没有 allow-lateness**：

> **Status:** Closed
> **Resolution:** Later
>
> Currently, Window aggregate does not support allow-lateness like Group Window Aggregate. We aims to support allow-lateness in this ticket.

- URL: https://issues.apache.org/jira/browse/FLINK-24000

### 5.8 结论（问题 5）

- **"Flink SQL 没有迟到侧输出"在 Flink 2.2 依然成立。** 两张票 FLINK-10031 / FLINK-20527 均为 Open/Unresolved，官方没有新增 SQL 迟到侧输出特性；Flink 2.2 release notes 的 "Table SQL / API" 章节也**未**列出任何 late-data / side-output 相关条目。
- **最接近的官方替代是 DataStream-only**：`.sideOutputLateData(OutputTag)` + `.getSideOutput(...)`，仅存在于 DataStream API（`docs/dev/datastream/operators/windows.md`）。
- **SQL 层的迟到处理是"丢弃"**：`WindowAggOperator` javadoc 逐字 "late elements (elements belong to emitted windows) will be simply dropped."；Group Window Agg 同样通过 `isWindowLate` 过滤并计入 `numLateRecordsDropped`。

---

## 6 · early-fire / late-fire 与 Window TVF / Experimental 状态

### 6.1 官方配置项（5 个）逐字：key / 默认值 / 描述 / `@Experimental`

**结论**：`table.exec.emit.early-fire.*`、`table.exec.emit.late-fire.*`、`table.exec.emit.allow-lateness` 这 5 个配置项定义在 **planner 内部**（非 `ExecutionConfigOptions`），全部标注 `@Experimental`，并注明其移除由 **FLINK-29692** 跟踪。**它们没有出现在 Flink 2.2 官方文档的 Configuration 选项表里**（因为带 `@Experimental` 且不在 `@Documentation.TableOption` 列表中）。

逐字原文（`flink-table/flink-table-planner/src/main/scala/org/apache/flink/table/planner/plan/utils/WindowEmitStrategy.scala`，blob `1e841c0722ebc90eaf89ef5b2d8d82a7e05ce857` @ release-2.2）：

> ```
>   /**
>    * It is an experimental config, and its removal will be consistently monitored and addressed
>    * through FLINK-29692.
>    */
>   @Experimental
>   val TABLE_EXEC_EMIT_EARLY_FIRE_ENABLED: ConfigOption[JBoolean] =
>     key("table.exec.emit.early-fire.enabled")
>       .booleanType()
>       .defaultValue(Boolean.box(false))
>       .withDescription("Specifies whether to enable early-fire emit." +
>         "Early-fire is an emit strategy before watermark advanced to end of window.")
> ```

> ```
>   /**
>    * It is an experimental config, and its removal will be consistently monitored and addressed
>    * through FLINK-29692.
>    */
>   @Experimental
>   val TABLE_EXEC_EMIT_EARLY_FIRE_DELAY: ConfigOption[Duration] =
>     key("table.exec.emit.early-fire.delay")
>       .durationType()
>       .noDefaultValue()
>       .withDescription(
>         "The early firing delay in milli second, early fire is " +
>           "the emit strategy before watermark advanced to end of window. " +
>           "< 0 is illegal configuration. " +
>           "0 means no delay (fire on every element). " +
>           "> 0 means the fire interval. ")
> ```

> ```
>   /**
>    * It is an experimental config, and its removal will be consistently monitored and addressed
>    * through FLINK-29692.
>    */
>   @Experimental
>   val TABLE_EXEC_EMIT_LATE_FIRE_ENABLED: ConfigOption[JBoolean] =
>     key("table.exec.emit.late-fire.enabled")
>       .booleanType()
>       .defaultValue(Boolean.box(false))
>       .withDescription("Specifies whether to enable late-fire emit. " +
>         "Late-fire is an emit strategy after watermark advanced to end of window.")
> ```

> ```
>   /**
>    * It is an experimental config, and its removal will be consistently monitored and addressed
>    * through FLINK-29692.
>    */
>   @Experimental
>   val TABLE_EXEC_EMIT_LATE_FIRE_DELAY: ConfigOption[Duration] =
>     key("table.exec.emit.late-fire.delay")
>       .durationType()
>       .noDefaultValue()
>       .withDescription(
>         "The late firing delay in milli second, late fire is " +
>           "the emit strategy after watermark advanced to end of window. " +
>           "< 0 is illegal configuration. " +
>           "0 means no delay (fire on every element). " +
>           "> 0 means the fire interval.")
> ```

> ```
>   /**
>    * It is an experimental config, and its removal will be consistently monitored and addressed
>    * through FLINK-29692.
>    */
>   @Experimental
>   val TABLE_EXEC_EMIT_ALLOW_LATENESS: ConfigOption[Duration] =
>     key("table.exec.emit.allow-lateness")
>       .durationType()
>       .noDefaultValue()
>       .withDescription("Sets the time by which elements are allowed to be late. " +
>         "Elements that arrive behind the watermark by more than the specified time " +
>         "will be dropped. " +
>         "Note: use the value if it is set, else use 'minIdleStateRetentionTime' in table config." +
>         "< 0 is illegal configuration. " +
>         "0 means disable allow lateness. " +
>         "> 0 means allow-lateness.")
> ```

- 文件路径: `flink-table/flink-table-planner/src/main/scala/org/apache/flink/table/planner/plan/utils/WindowEmitStrategy.scala`
- URL: https://github.com/apache/flink/blob/release-2.2/flink-table/flink-table-planner/src/main/scala/org/apache/flink/table/planner/plan/utils/WindowEmitStrategy.scala

**默认值汇总（源码 `defaultValue` / `noDefaultValue`）**：

| 配置项 | 类型 | 默认值 |
| --- | --- | --- |
| `table.exec.emit.early-fire.enabled` | Boolean | `false` |
| `table.exec.emit.early-fire.delay` | Duration | **无默认值**（`noDefaultValue()`） |
| `table.exec.emit.late-fire.enabled` | Boolean | `false` |
| `table.exec.emit.late-fire.delay` | Duration | **无默认值**（`noDefaultValue()`） |
| `table.exec.emit.allow-lateness` | Duration | **无默认值**（`noDefaultValue()`） |

**这些选项不在 Flink 2.2 官方 Configuration 文档表中**：2.2 文档页 `docs/dev/table/config/` 的 Execution Options 由 `docs/layouts/shortcodes/generated/execution_config_configuration.html` 生成（blob `e236499de28790b8ca6d66a01e0436fa92ab313b` @ release-2.2），逐项核对**不含任何 `table.exec.emit.*` 条目**。
- URL: https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/table/config/#execution-options
- 源码 URL: https://github.com/apache/flink/blob/release-2.2/docs/layouts/shortcodes/generated/execution_config_configuration.html

### 6.2 "window table function based aggregate doesn't support early-fire and late-fire configuration" —— **确切出处已定位**

**结论**：这句话是**源码中的运行时异常消息**，位于 `WindowUtil.scala` 的 `checkEmitConfiguration(tableConfig: TableConfig)` 方法里，由 `StreamPhysicalWindowAggregate.translateToExecNode()` 调用（Window TVF 聚合的物理节点）。因此该限制**只作用于 Window TVF 形式的窗口聚合**（`TUMBLE(...)`/`HOP(...)`/`CUMULATE(...)`/`SESSION(...)` TVF + `GROUP BY window_start, window_end`），**不作用于** legacy 的 `GROUP BY TUMBLE(time_attr, interval)` 形式的 Group Window Aggregation。

逐字原文（`flink-table/flink-table-planner/src/main/scala/org/apache/flink/table/planner/plan/utils/WindowUtil.scala`，blob `9102f68528dcce96bba40fe56df6c626b23f9d51` @ release-2.2；父 agent 另一次读取得 blob `fd965e3ef5f0d7ae34f1b976dfaba158589192f5`，**逐字内容一致**，差异应属缓存/ref 读取时点）：

> ```
>   /**
>    * Window TVF based aggregations don't support early-fire and late-fire, throws exception when the
>    * configurations are set.
>    */
>   def checkEmitConfiguration(tableConfig: TableConfig): Unit = {
>     if (
>       tableConfig.get(TABLE_EXEC_EMIT_EARLY_FIRE_ENABLED) ||
>       tableConfig.get(TABLE_EXEC_EMIT_LATE_FIRE_ENABLED)
>     ) {
>       throw new TableException(
>         "Currently, window table function based aggregate doesn't " +
>           s"support early-fire and late-fire configuration " +
>           s"'${TABLE_EXEC_EMIT_EARLY_FIRE_ENABLED.key()}' and " +
>           s"'${TABLE_EXEC_EMIT_LATE_FIRE_ENABLED.key()}'.")
>     }
>   }
> ```

即运行时异常消息逐字为：

> `Currently, window table function based aggregate doesn't support early-fire and late-fire configuration 'table.exec.emit.early-fire.enabled' and 'table.exec.emit.late-fire.enabled'.`

调用点逐字原文（`flink-table/flink-table-planner/src/main/scala/org/apache/flink/table/planner/plan/nodes/physical/stream/StreamPhysicalWindowAggregate.scala`，blob `1b98f62329214b3509d84520246ebde7d2703e99` @ release-2.2）：

> ```
>   override def translateToExecNode(): ExecNode[_] = {
>     checkEmitConfiguration(unwrapTableConfig(this))
> ```

该文件同时逐字说明了两类窗口聚合的分野与 Group Window Agg 的最终归宿：

> ```
> /**
>  * Streaming window aggregate physical node which will be translate to window aggregate operator.
>  *
>  * Note: The differences between [[StreamPhysicalWindowAggregate]] and
>  * [[StreamPhysicalGroupWindowAggregate]] is that, [[StreamPhysicalWindowAggregate]] is translated
>  * from window TVF syntax, but the other is from the legacy GROUP WINDOW FUNCTION syntax. In the
>  * long future, [[StreamPhysicalGroupWindowAggregate]] will be dropped.
>  */
> ```

- 限制所在文件 URL: https://github.com/apache/flink/blob/release-2.2/flink-table/flink-table-planner/src/main/scala/org/apache/flink/table/planner/plan/utils/WindowUtil.scala
- 限制所在文件 blob sha: `9102f68528dcce96bba40fe56df6c626b23f9d51`（复核时通过 `github_file` 读到的 sha；GitHub code-search 索引中另见同一文件的 `fd965e3ef5f0d7ae34f1b976dfaba158589192f5`，两者内容逐字一致，差异应为索引缓存/ref 解析差异）
- 调用点 URL: https://github.com/apache/flink/blob/release-2.2/flink-table/flink-table-planner/src/main/scala/org/apache/flink/table/planner/plan/nodes/physical/stream/StreamPhysicalWindowAggregate.scala

### 6.3 early-fire / late-fire 的**使用前提与硬性限制**（源码逐字）

**结论**：early-fire / late-fire 在 2.2 **仅对 legacy Group Window Aggregation 生效**（`WindowEmitStrategy` 由 `StreamPhysicalGroupWindowAggregateRule` 构造，其 `getWindow` 是 `LogicalWindow`/`SessionGroupWindow`；TVF 路径在 `translateToExecNode` 直接抛错）。使用时有以下硬性约束：

**(a) Session window 不支持 EMIT 策略**（`WindowEmitStrategy.checkValidation`）：

> ```
>     if (isSessionWindow && (earlyFireDelayEnabled || lateFireDelayEnabled)) {
>       throw new TableException("Session window doesn't support EMIT strategy currently.")
>     }
> ```

**(b) 事件时间 + 开启 late-fire 但 allow-lateness ≤ 0 ⇒ 作业直接抛错**：

> ```
>     if (isEventTime && lateFireDelayEnabled && allowLateness <= 0L) {
>       throw new TableException(
>         "The 'AFTER WATERMARK' emit strategy requires set " +
>           "'allow-lateness' or 'minIdleStateRetentionTime' in table config.")
>     }
> ```

**(c) delay 不能为 null 或负值**：

> ```
>     if (earlyFireDelayEnabled && (earlyFireDelay == null || earlyFireDelay.toMillis < 0)) {
>       throw new TableException(
>         "Early-fire delay should not be null or negative value when" +
>           "enable early-fire emit strategy.")
>     }
> ```
> ```
>     if (lateFireDelayEnabled && (lateFireDelay == null || lateFireDelay.toMillis < 0)) {
>       throw new TableException(
>         "Late-fire delay should not be null or negative value when" +
>           "enable late-fire emit strategy.")
>     }
> ```

**(d) late-fire delay 不得大于 allow-lateness**：

> ```
>     if (lateFireDelayEnabled && (lateFireDelay.toMillis > allowLateness)) {
>       throw new TableException(
>         s"Allow-lateness [${allowLateness}ms] should not be smaller than " +
>           s"Late-fire delay [${lateFireDelay.toMillis}ms] when enable late-fire emit strategy.")
>     }
> ```

**(e) allow-lateness 的推导顺序（`parseAllowLateness`）**：session window 一律为 0；未开启 late-fire 时为 0；否则优先取 `table.exec.emit.allow-lateness`，其次取 `minIdleStateRetentionTime`：

> ```
>   private def parseAllowLateness(isSessionWindow: Boolean, config: ReadableConfig): Long = {
>     val enableLateFireDelay = config.get(TABLE_EXEC_EMIT_LATE_FIRE_ENABLED)
>     val emitAllowLateness: Duration = config
>       .getOptional(TABLE_EXEC_EMIT_ALLOW_LATENESS)
>       .orElse(null)
>     if (isSessionWindow) {
>       // ignore allow lateness in session window because retraction is not supported
>       0L
>     } else if (!enableLateFireDelay) {
>       // ignore allow lateness if disable late-fire delay
>       0L
>     } else if (emitAllowLateness != null) {
>       // return emit allow-lateness if it is set
>       emitAllowLateness.toMillis
>     } else if (config.get(IDLE_STATE_RETENTION).toMillis < 0) {
>       // min idle state retention time is not set, use 0L as default which means not allow lateness
>       0L
>     } else {
>       // use min idle state retention time as allow lateness
>       config.get(IDLE_STATE_RETENTION).toMillis
>     }
>   }
> ```

**(f) processing time 下 late trigger 被忽略**：

> ```
>       // late trigger is ignored, as no late element in processing time
> ```

- 全部逐字出处: `flink-table/flink-table-planner/src/main/scala/org/apache/flink/table/planner/plan/utils/WindowEmitStrategy.scala`，blob `1e841c0722ebc90eaf89ef5b2d8d82a7e05ce857` @ release-2.2
- URL: https://github.com/apache/flink/blob/release-2.2/flink-table/flink-table-planner/src/main/scala/org/apache/flink/table/planner/plan/utils/WindowEmitStrategy.scala
- early-fire / late-fire 生效路径（Group Window Agg）: `flink-table/flink-table-planner/src/main/scala/org/apache/flink/table/planner/plan/rules/physical/stream/StreamPhysicalGroupWindowAggregateRule.scala`，blob `13998116eaca651281a03d1e983ff60432c6497f` — 其中逐字 `val emitStrategy = WindowEmitStrategy(tableConfig, agg.getWindow)`。
  URL: https://github.com/apache/flink/blob/release-2.2/flink-table/flink-table-planner/src/main/scala/org/apache/flink/table/planner/plan/rules/physical/stream/StreamPhysicalGroupWindowAggregateRule.scala

### 6.4 Group Window Aggregation 的状态：**官方文档标 "deprecated"**（非 "@Experimental"）

**结论**：Flink 2.2 官方文档把 **Group Window Aggregation 标为 deprecated**（并说明未来将用 Window TVF Aggregation 取代）；**但官方文档中没有把 Group Window Aggregation 本身标注为 `@Experimental`**。被官方以 `@Experimental` 标注的是 **early-fire / late-fire 这 5 个配置项**。Group Window Aggregation 在 Java API 层是 `@PublicEvolving`（`GroupWindow`、`Tumble`、`GroupWindowedTable` 均如此）。

官方文档逐字警告（Flink 2.2 `docs/dev/table/sql/queries/window-agg/`）：

> ```
> Warning: Group Window Aggregation is deprecated. It's encouraged to use Window TVF Aggregation which is more powerful and effective.
>
> Compared to Group Window Aggregation, Window TVF Aggregation have many advantages, including:
> - Have all performance optimizations mentioned in [Performance Tuning].
> - Support standard `GROUPING SETS` syntax.
> - Can apply [Window TopN] after window aggregation result.
> - and so on.
> ```

- URL: https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/table/sql/queries/window-agg/#group-window-aggregation
- 源码（markdown）: `docs/content/docs/dev/table/sql/queries/window-agg.md`，blob `39f5146226a095c53f6168406fc3bb1c742bbb03` @ release-2.2 — https://github.com/apache/flink/blob/release-2.2/docs/content/docs/dev/table/sql/queries/window-agg.md

Windowing TVF 与 Group Window Agg 的定位关系（同一文档，逐字）：

> Windowing TVFs is a replacement of legacy [Grouped Window Functions](...#group-window-aggregation-deprecated). Windowing TVFs is more SQL standard compliant and more powerful to support complex window-based computations, e.g. Window TopN, Window Join. However, [Grouped Window Functions](...#group-window-aggregation) can only support Window Aggregation.

源码侧"Group Window Aggregate 终将被移除"的逐字说明见 6.2（`StreamPhysicalWindowAggregate` 类注释）：

> In the long future, [[StreamPhysicalGroupWindowAggregate]] will be dropped.

Java API 层 `GroupWindow` / `GroupWindowedTable` / `Tumble` 均为 `@PublicEvolving`（非 `@Experimental`、非 `@Deprecated`）：
- `flink-table/flink-table-api-java/src/main/java/org/apache/flink/table/api/GroupWindow.java`，blob `4a0489d405a29e00b93b59c989ac64dd10dcaaa6`
- `flink-table/flink-table-api-java/src/main/java/org/apache/flink/table/api/GroupWindowedTable.java`，blob `d5d8933e971dca6d577e435aeed2d6e1830e11de`
- `flink-table/flink-table-api-java/src/main/java/org/apache/flink/table/api/Tumble.java`，blob `7a4be0a980b0cf3b04217df4d967e814dab66af8`

### 6.5 FLINK-29692 现状：**Open / Unresolved**

**结论**：FLINK-29692 "Support early/late fires for Windowing TVFs" 至今 **Open**、Resolution **Unresolved**，最后更新 2024-05-21。因此"给 Windowing TVF 加 early/late fire"在 2.2 **尚未实现**，而 6.1 中 early-fire 配置项被官方标 `@Experimental` 且注明移除由该票跟踪 —— 两者一致。

逐字原文（Description）：

> I have cases where I need to 1) output data as soon as possible and 2) handle late arriving data to achieve eventual correctness. In the logic, I need to do window deduplication which is based on Windowing TVFs and according to source code, early/late fires are not supported yet in Windowing TVFs.
>
> Actually 1) contradicts with 2). Without early/late fires, we had to compromise, either live with fresh incorrect data or tolerate excess latency for correctness.

逐字原文（状态）：

> **Status:** Open
> **Resolution:** Unresolved
> **Type:** New Feature
> **Priority:** Major
> **Affects Version/s:** 1.15.3
> **Component/s:** Table SQL / Planner
> **Created:** 19/Oct/22 14:17
> **Updated:** 21/May/24 07:49

同一票中社区维护者 Jark Wu 给出的官方替代方案逐字（说明 legacy group window agg 才支持 early/late fire）：

> I'm fine with supporting this feature. But we have supported early/late fires for the legacy group window aggregation. You can use `last_value(col)` agg function to get window deduplication result. For example:
>
> ```sql
> -- output window result for every 5 seconds before window fire
> SET table.exec.emit.early-fire.enabled = true;
> SET table.exec.emit.early-fire.delay = 5s;
> -- emit window result for every late arriving record
> SET table.exec.emit.late-fire.enabled = true;
> SET table.exec.emit.late-fire.delay = 0;
>
> SELECT TUMBLE_START(rowtime, INTERVAL '5' MINUTES), last_value(col_a), last_value(col_b)
> FROM my_source_table
> GROUP BY mykey, TUMBLE(rowtime, INTERVAL '5' MINUTES);
> ```

同票维护者 Jing Zhang 对 early-fire 语义与局限的逐字说明：

> when users enable early fire/ late fires of window aggregate, he/she would allow:
>  # the emitted result would has no restrict mapping with time
>  # the intermediate result could be allowed to emitted
>  # the window result might be retracted later

以及：

> 'Early fire' would periodically sent intermediate result to downstream, the frequency to send intermediate result is based on processing time, even thought the window itself based on event time. It mixed up processing time semantics with the time attribute of Window.

- URL: https://issues.apache.org/jira/browse/FLINK-29692
- REST（含全部评论）: https://issues.apache.org/jira/rest/api/2/issue/FLINK-29692

### 6.6 Flink 2.2 的 Windowing TVF 文档本身对迟到/early-fire **没有任何表述**

**结论**：逐字核对 Flink 2.2 官方 `docs/dev/table/sql/queries/window-tvf/` 全文，**没有**任何关于"late record 被丢弃"的描述，也没有 early-fire / late-fire 的说明。该文档对 CUMULATE 的"early firing"只是一句比喻性描述（指累进窗口，而非 emit 策略）：

> Cumulating windows are very useful in some scenarios, such as tumbling windows with early firing in a fixed window interval. For example, a daily dashboard draws cumulative UVs from 00:00 to every minute, the UV at 10:00 represents the total number of UV from 00:00 to 10:00. This can be easily and efficiently implemented by CUMULATE windowing.

同一文档也确认 TVF 是 legacy group window 的替代者（见 6.4 引文）。

- URL: https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/table/sql/queries/window-tvf/
- 源码（markdown）: `docs/content/docs/dev/table/sql/queries/window-tvf.md`，blob `e7b34a4ce5c78d93e7b3459f4a224a3208359e5c` @ release-2.2

### 6.6b ★ 决定性结论：「窗口内提前发射」在 Flink 2.2 上只能走一条正在被官方弃用的语法

**结论（官方文档陈述，非推断）**：
1. **唯一支持 early-fire 的 SQL 语法是 legacy Group Window Aggregation**，而它在 Flink 2.2 官方文档中**已被标 deprecated**；
2. **官方推荐的替代（Window TVF Aggregation）明确不发射中间结果**；
3. 因此「窗口内提前发射」在 Flink 2.2 上**只有一条被官方弃用的路可走**。

逐字原文 1 —— Group Window Aggregation 的 deprecated 警告（Flink 2.2 `docs/dev/table/sql/queries/window-agg.md`）：

> ```
> Warning: Group Window Aggregation is deprecated. It's encouraged to use Window TVF Aggregation which is more powerful and effective.
>
> Compared to Group Window Aggregation, Window TVF Aggregation have many advantages, including:
> - Have all performance optimizations mentioned in [Performance Tuning].
> - Support standard `GROUPING SETS` syntax.
> - Can apply [Window TopN] after window aggregation result.
> - and so on.
> ```

逐字原文 2 —— Window TVF Aggregation **不发射中间结果**（同一文档，"Window TVF Aggregation" 节）：

> Unlike other aggregations on continuous tables, window aggregation do not emit intermediate results but only a final result, the total aggregation at the end of the window. Moreover, window aggregations purge all intermediate state when no longer needed.

逐字原文 3 —— Windowing TVF 的定位（Flink 2.2 `docs/dev/table/sql/queries/window-tvf.md`）：

> Windowing TVFs is a replacement of legacy [Grouped Window Functions](...#group-window-aggregation-deprecated). Windowing TVFs is more SQL standard compliant and more powerful to support complex window-based computations, e.g. Window TopN, Window Join. However, [Grouped Window Functions](...#group-window-aggregation) can only support Window Aggregation.

- URL 1: https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/table/sql/queries/window-agg/#group-window-aggregation
- URL 2: https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/table/sql/queries/window-agg/#window-tvf-aggregation
- URL 3: https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/table/sql/queries/window-tvf/
- 源码 1: `docs/content/docs/dev/table/sql/queries/window-agg.md`，blob `39f5146226a095c53f6168406fc3bb1c742bbb03` @ release-2.2
- 源码 3: `docs/content/docs/dev/table/sql/queries/window-tvf.md`，blob `e7b34a4ce5c78d93e7b3459f4a224a3208359e5c` @ release-2.2

### 6.6c CUMULATE：提供「累计语义」，**但官方未给出**其支持 `table.exec.emit.early-fire.*`

**结论（必须区分两件事）**：
- CUMULATE 提供的是**累进窗口语义**（窗口 start 固定、end 按 step 递进），官方用它来描述"早期发射"场景；
- **官方未给出**任何"CUMULATE 支持 `table.exec.emit.early-fire.enabled` / `early-fire.delay`"的表述。事实上，源码层面 CUMULATE 走的是 **Window TVF 路径**，而该路径在 `translateToExecNode()` 处被 `checkEmitConfiguration` **直接抛异常**拒绝（见 6.2）。

逐字原文（Flink 2.2 `docs/dev/table/sql/queries/window-tvf.md`，CUMULATE 节）：

> Cumulating windows are very useful in some scenarios, such as tumbling windows with early firing in a fixed window interval. For example, a daily dashboard draws cumulative UVs from 00:00 to every minute, the UV at 10:00 represents the total number of UV from 00:00 to 10:00. This can be easily and efficiently implemented by CUMULATE windowing.

以及 CUMULATE 的语义定义逐字：

> The `CUMULATE` function assigns elements to windows that cover rows within an initial interval of step size and expand to one more step size (keep window start fixed) every step until the max window size. You can think `CUMULATE` function as applying `TUMBLE` windowing with max window size first, and split each tumbling windows into several windows with same window start and window ends of step-size difference. So cumulating windows do overlap and don't have a fixed size.

- URL: https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/table/sql/queries/window-tvf/#cumulate
- 源码 URL: https://github.com/apache/flink/blob/release-2.2/docs/content/docs/dev/table/sql/queries/window-tvf.md

### 6.6d 官方在 2.2 文档中**另有** early-fire 说明页？—— 没有

**结论**：对 `apache/flink` `release-2.2` 分支的 `docs/content/` 全树做代码检索，`early-fire` 字符串**只出现在 `docs/content/docs/sql/reference/queries/joins.md`（master/2.3 文档树，2.2 渲染站点对应路径为 `docs/dev/table/sql/queries/joins/`，其 2.2 版内容中**不含**该小节）** 一处；`docs/content/docs/dev/table/` 全树（2.2 的 Table 文档树）**不含任何 `early-fire`** 字符串。

也就是说：**2.2 的 Table/SQL 文档树里没有任何 early-fire 说明页**；`table.exec.emit.early-fire.*` 只在源码中定义（且带 `@Experimental`），既不在 2.2 Configuration 选项表，也不在 2.2 tuning 页。

（说明：`docs/content/docs/sql/reference/queries/joins.md` 中的 "Early Fire for Outer Joins" 是 **master/2.3 文档树**的产物，涉及 outer join 的 early fire 提示；**Flink 2.2 的对应文档路径 `docs/dev/table/sql/queries/joins/` 页面目录结构为 Regular Joins / Interval Joins / Temporal Joins / Lookup Join / Array, Multiset and Map Expansion / Table Function，**不含** "Early Fire for Outer Joins" 小节。）

- 2.2 Joins 页面目录（逐字小节列表）：`Joins / Regular Joins / INNER Equi-JOIN / OUTER Equi-JOIN / Multiple Regular Joins / Interval Joins / Temporal Joins / Event Time Temporal Join / Processing Time Temporal Join / Temporal Table Function Join / Lookup Join / Array, Multiset and Map Expansion / Table Function / INNER JOIN / LEFT OUTER JOIN`
- URL: https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/table/sql/queries/joins/
- master 版（2.3）含 "Early Fire for Outer Joins" 的文件: `docs/content/docs/sql/reference/queries/joins.md`，blob `761fa0cb16edec1640c343c2fceec4f8017608d2`
  URL: https://github.com/apache/flink/blob/master/docs/content/docs/sql/reference/queries/joins.md



### 6.7 early-fire 与 retract / upsert sink 的正确性：官方怎么说

**结论**：官方文档**没有**专门关于 early-fire 与 retract/upsert sink 交互的正确性章节。但有两条可引用的官方逐字证据说明 early-fire 会产生 changelog（因此下游必须能处理 retract）：

**(a)** `StreamPhysicalWindowAggregate.scala` 源码注释逐字：

> Note: The differences between [[StreamPhysicalWindowAggregate]] and [[StreamPhysicalGroupWindowAggregate]] is that, [[StreamPhysicalWindowAggregate]] is translated from window TVF syntax, but the other is from the legacy GROUP WINDOW FUNCTION syntax.

（同文件；此处强调两条路径的产出语义不同）

**(b)** legacy Group Window Agg 在 `produceUpdates` 为真时**确实发出 UPDATE_BEFORE / UPDATE_AFTER / DELETE**（源码 `AggregateWindowOperator.emitWindowResult`）：

> ```
>             if (previousAggResult != null) {
>                 // current agg is not equal to the previous emitted, should emit retract
>                 if (!equaliser.equals(aggResult, previousAggResult)) {
>                     // send UPDATE_BEFORE
>                     collect(
>                             RowKind.UPDATE_BEFORE,
>                             (RowData) getCurrentKey(),
>                             previousAggResult);
>                     // send UPDATE_AFTER
>                     collect(RowKind.UPDATE_AFTER, (RowData) getCurrentKey(), aggResult);
>                     // update previousState
>                     previousState.update(aggResult);
>                 }
> ```

- 文件路径: `flink-table/flink-table-runtime/src/main/java/org/apache/flink/table/runtime/operators/window/groupwindow/operator/AggregateWindowOperator.java`，blob `05b117e51b2d75697bad8d0b7d24bfa70bdd2b6b` @ release-2.2
- URL: https://github.com/apache/flink/blob/release-2.2/flink-table/flink-table-runtime/src/main/java/org/apache/flink/table/runtime/operators/window/groupwindow/operator/AggregateWindowOperator.java

**(c)** 官方对 changelog 下游的通用要求（`docs/dev/table/data_stream_api/`）：

> During table-to-stream conversion, this could lead to an exception similar to
> `Table sink 'Unregistered_DataStream_Sink_1' doesn't support consuming update changes [...].`
> in which case one needs to revise the query again or switch to `toChangelogStream`.

- URL: https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/table/data_stream_api/#converting-between-datastream-and-table

### 6.8 Flink 2.2 release notes 未涉及 early-fire / late-fire / 迟到侧输出

**结论**：Flink 2.2 官方 release notes 的 "Table SQL / API" 章节列出的是 VECTOR_SEARCH、Realtime AI Function、Materialized Table、SinkUpsertMaterializer V2、Delta Join、SQL Types、UniqueKeys state management —— **没有** early-fire / late-fire / 迟到侧输出 / allow-lateness 相关条目。

- URL: https://nightlies.apache.org/flink/flink-docs-stable/release-notes/flink-2.2/#table-sql--api

---

## 7（bonus）· GROUP WINDOW AGG 中窗口关闭后迟到记录的结果处理

**结论**：在 SQL 的 **Group Window Aggregation**（legacy）中，窗口关闭（watermark 越过 `window.maxTimestamp + allowedLateness`）后到达的迟到记录**不会**触发对已发结果的 retraction，也不会产生新结果 —— 该记录被**直接丢弃**，窗口状态被清理。官方依据有两层：

**(a) 文档层（通用窗口语义，Flink 2.2 `docs/dev/datastream/operators/windows/`）**：

> By default, the allowed lateness is set to `0`. That is, elements that arrive behind the watermark will be dropped.

> Once this happens, Flink removes the window and deletes its state, as also described in the [Window Lifecycle] section.

> In order to make this work, Flink keeps the state of windows until their allowed lateness expires. Once this happens, Flink removes the window and deletes its state

**(b) SQL 源码层（`WindowOperator` + `GeneralWindowProcessFunction` + `InternalWindowProcessFunction`，见 5.6 逐字）**：`isWindowLate(window)` 为真时该窗口不进入 `reuseAffectedWindows`，既不累加、也不触发 trigger；同时 `WindowOperator` 把 `isElementDropped` 置真并累加 `numLateRecordsDropped` metric。窗口状态在 `cleanWindowIfNeeded` → `ctx.clearWindowState(window)` / `ctx.clearPreviousState(window)` / `ctx.clearTrigger(window)` 处被清空。

**(c) 反例（尚未实现的"迟到触发 retraction"）**：JIRA FLINK-6970 逐字描述了"如果实现会怎样"——即当前**没有**这个行为：

> If a late record for a window is received within this interval, an updated result is emitted (and the previous result is retracted).

（该票 **Open**，故 2.2 无此行为。）

**(d) 与 `table.exec.emit.allow-lateness` 的区别（务必不要混淆）**：

- `table.exec.emit.allow-lateness` 的描述逐字为 *"Elements that arrive behind the watermark by more than the specified time will be dropped."* —— 它是 **late-fire emit 策略的允许迟到阈值**（配合 `table.exec.emit.late-fire.enabled`），作用对象是 legacy Group Window Agg 的 emit 行为；它**不是**窗口关闭后记录处理的开关。
- 窗口关闭后记录的处理规则由 `isWindowLate` + `allowedLateness`（Group Window Agg 侧的 `WindowOperator.allowedLateness`）决定，见 (b)。

- 文档 URL: https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/datastream/operators/windows/#allowed-lateness
- JIRA URL: https://issues.apache.org/jira/browse/FLINK-6970
- 源码 URL（Group Window Agg 运行时）: https://github.com/apache/flink/blob/release-2.2/flink-table/flink-table-runtime/src/main/java/org/apache/flink/table/runtime/operators/window/groupwindow/operator/WindowOperator.java

---

## 官方未给出的项

以下条目经检索 Flink 2.2 官方文档站、`apache/flink` `release-2.2` 分支源码/文档与 ASF JIRA，**官方未给出**明确表述：

1. **「Group Window Aggregation 被官方标记为 `@Experimental`」** —— 官方文档只用 **"deprecated"**（"Warning: Group Window Aggregation is deprecated."）；Java API 层是 `@PublicEvolving`。`@Experimental` 只出现在 **early-fire / late-fire 的 5 个配置项**上。**官方未给出**把 Group Window Aggregation 本身标为 `@Experimental` 的表述。
2. **FLINK-29692 之外的"移除 / 废弃 Group Window Aggregation"的官方时间表** —— 仅有源码注释 "In the long future, [[StreamPhysicalGroupWindowAggregate]] will be dropped."，**官方未给出**具体版本号或移除 JIRA 编号（在 JIRA 中按 `summary ~ "Remove group window"` / `summary ~ "deprecate"` 检索均无对应票）。
3. **Flink 2.2 官方 Configuration 文档页对 `table.exec.emit.early-fire.*` / `table.exec.emit.late-fire.*` / `table.exec.emit.allow-lateness` 的选项表条目** —— **官方未给出**（2.2 文档的 Execution Options 表中不存在这些键；其定义只在源码中，且带 `@Experimental`）。
4. **官方关于 early-fire 与 upsert/retract sink 正确性的专门说明** —— **官方未给出**独立章节；只有 JIRA FLINK-29692 中维护者的评论（"the window result might be retracted later"）与 `AggregateWindowOperator` 的 UPDATE_BEFORE/UPDATE_AFTER 源码。
5. **官方对"Window TVF 迟到记录被丢弃"的文档级表述** —— **官方未给出**；只有源码 javadoc "late elements (elements belong to emitted windows) will be simply dropped."（`WindowAggOperator`）。
6. **SQL 级的"too-late sink" / 迟到专用表 / 迟到侧输出** —— 官方未给出；仅有两张 Open 票（FLINK-10031、FLINK-20527）描述该需求。
7. **FLINK-27849 与 Group Window Aggregation 的 Experimental 状态的关系** —— FLINK-27849 的标题逐字为 "Harden correctness for non-deterministic updates present in the changelog pipeline"（Status: Open, Fix Version: 2.4.0），**与 Group Window Aggregation 的 Experimental/移除问题无关**；任务描述中"FLINK-29692"才是 early-fire 移除跟踪票。
8. **「CUMULATE 支持 `table.exec.emit.early-fire.*` 配置」** —— 官方**未给出**该声明。2.2 文档只说 CUMULATE 提供"固定窗口间隔内的早期发射"**语义**；源码层面 CUMULATE 属 Window TVF 路径，该路径对 early-fire/late-fire 配置直接抛异常。
9. **Flink 2.2 文档树中任何 early-fire 说明页** —— 官方**未给出**（`docs/content/docs/dev/table/` 全树不含 `early-fire` 字符串；相关小节只存在于 master/2.3 文档树的 `sql/reference/queries/joins.md`）。
10. **「early/late fire 将被移除或迁移到 Windowing TVF」的官方时间表** —— FLINK-29692 仅为 Open 票，官方**未给出**移除/迁移的具体版本或承诺；仅源码注释说明 5 个配置项是 experimental 且"removal will be consistently monitored and addressed through FLINK-29692"。

---

## 引用来源清单

| # | 论断（claim） | URL | 访问日期 | 版本/分支 |
| --- | --- | --- | --- | --- |
| 1 | FLINK-10031 仍 Open/Unresolved；"The Table API & SQL drop late events right now." | https://issues.apache.org/jira/browse/FLINK-10031 | 2026-10-20 | JIRA 现状 |
| 1b | FLINK-10031 REST 字段（status/resolution/updated） | https://issues.apache.org/jira/rest/api/2/issue/FLINK-10031 | 2026-10-20 | JIRA REST |
| 2 | FLINK-20527 仍 Open/Unresolved；"how to add a side output for flink sql watermaker?" | https://issues.apache.org/jira/browse/FLINK-20527 | 2026-10-20 | JIRA 现状 |
| 3 | DataStream 侧 `sideOutputLateData` / `getSideOutput`（唯一的迟到侧输出机制） | https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/datastream/operators/windows/#getting-late-data-as-a-side-output | 2026-10-20 | Flink 2.2 文档 (v2.2.1) |
| 3b | 同上（markdown 源） | https://github.com/apache/flink/blob/release-2.2/docs/content/docs/dev/datastream/operators/windows.md | 2026-10-20 | blob `6ef7c6492eca62d8e9e8779b9e4bec509076a44d` |
| 3c | "late firings" 定义与 "multiple results for the same computation … deduplicate them"；`sideOutputLateData` 侧输出的是 "the data that was discarded as late"；`side_output.md` 的可用函数与 `getSideOutput` | https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/datastream/operators/windows/#late-elements-considerations ／ https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/datastream/side_output/ | 2026-10-20 | Flink 2.2 文档（v2.2.1）；markdown 源 blob `6ef7c6492eca62d8e9e8779b9e4bec509076a44d` / `1111853cd143e41cd1ea7810ce411e1c876d1e67` |
| 4 | "By default, late elements are dropped when the watermark is past the end of the window." / allowed lateness 默认 0 → 迟到被丢弃 | https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/datastream/operators/windows/#allowed-lateness | 2026-10-20 | Flink 2.2 文档 |
| 5 | 迟到元素 "ignored when computing the result" | https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/datastream/event-time/built_in/#fixed-amount-of-lateness | 2026-10-20 | Flink 2.2 文档 |
| 6 | Window TVF 迟到记录 "will be simply dropped"（javadoc） | https://github.com/apache/flink/blob/release-2.2/flink-table/flink-table-runtime/src/main/java/org/apache/flink/table/runtime/operators/window/tvf/common/WindowAggOperator.java | 2026-10-20 | blob `093a58828070b1c778f84874e8a43c2b81be10f8` |
| 7 | Group Window Agg：`isWindowLate` 过滤迟到窗口 | https://github.com/apache/flink/blob/release-2.2/flink-table/flink-table-runtime/src/main/java/org/apache/flink/table/runtime/operators/window/groupwindow/internal/GeneralWindowProcessFunction.java | 2026-10-20 | blob `57180b6c2bcba906a5fcece31b3e34437970fafb` |
| 8 | `isWindowLate` 定义（watermark 越过 end + allowed lateness 即迟到） | https://github.com/apache/flink/blob/release-2.2/flink-table/flink-table-runtime/src/main/java/org/apache/flink/table/runtime/operators/window/groupwindow/internal/InternalWindowProcessFunction.java | 2026-10-20 | blob `3a3a4574429dff23b160955f5c5e263faf6a3e70` |
| 9 | `numLateRecordsDropped` / `lateRecordsDroppedRate` metric 与丢弃计数 | https://github.com/apache/flink/blob/release-2.2/flink-table/flink-table-runtime/src/main/java/org/apache/flink/table/runtime/operators/window/groupwindow/operator/WindowOperator.java | 2026-10-20 | blob `ccc0265c10064012d8aaaee46836e27be564d236` |
| 10 | FLINK-6970 Open：迟到数据更新（撤回+重发）未实现 | https://issues.apache.org/jira/browse/FLINK-6970 | 2026-10-20 | JIRA 现状 |
| 11 | FLINK-24000 Closed/Later：Window TVF 无 allow-lateness | https://issues.apache.org/jira/browse/FLINK-24000 | 2026-10-20 | JIRA 现状 |
| 12 | 5 个 emit 配置项的 key/默认值/描述 + `@Experimental` + FLINK-29692 注释 | https://github.com/apache/flink/blob/release-2.2/flink-table/flink-table-planner/src/main/scala/org/apache/flink/table/planner/plan/utils/WindowEmitStrategy.scala | 2026-10-20 | blob `1e841c0722ebc90eaf89ef5b2d8d82a7e05ce857` |
| 13 | "window table function based aggregate doesn't support early-fire and late-fire configuration" 异常原文与出处 | https://github.com/apache/flink/blob/release-2.2/flink-table/flink-table-planner/src/main/scala/org/apache/flink/table/planner/plan/utils/WindowUtil.scala | 2026-10-20 | blob `9102f68528dcce96bba40fe56df6c626b23f9d51`（另一次读取得 `fd965e3ef5f0d7ae34f1b976dfaba158589192f5`，内容一致） |
| 14 | `checkEmitConfiguration` 调用点（Window TVF 物理节点）；"Group Window Aggregate will be dropped" | https://github.com/apache/flink/blob/release-2.2/flink-table/flink-table-planner/src/main/scala/org/apache/flink/table/planner/plan/nodes/physical/stream/StreamPhysicalWindowAggregate.scala | 2026-10-20 | blob `1b98f62329214b3509d84520246ebde7d2703e99` |
| 15 | early/late-fire 生效路径（Group Window Agg 规则） | https://github.com/apache/flink/blob/release-2.2/flink-table/flink-table-planner/src/main/scala/org/apache/flink/table/planner/plan/rules/physical/stream/StreamPhysicalGroupWindowAggregateRule.scala | 2026-10-20 | blob `13998116eaca651281a03d1e983ff60432c6497f` |
| 16 | Group Window Aggregation 官方 "deprecated" 警告；TVF 为其替代 | https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/table/sql/queries/window-agg/#group-window-aggregation | 2026-10-20 | Flink 2.2 文档 |
| 16b | 同上（markdown 源） | https://github.com/apache/flink/blob/release-2.2/docs/content/docs/dev/table/sql/queries/window-agg.md | 2026-10-20 | blob `39f5146226a095c53f6168406fc3bb1c742bbb03` |
| 17 | FLINK-29692 Open/Unresolved；维护者给出的 legacy group window 用法与 early-fire 语义说明 | https://issues.apache.org/jira/browse/FLINK-29692 | 2026-10-20 | JIRA 现状 + 全部评论 |
| 18 | Windowing TVF 文档全文（无 late/early-fire 表述；CUMULATE "early firing" 比喻） | https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/table/sql/queries/window-tvf/ | 2026-10-20 | Flink 2.2 文档 |
| 18b | 同上（markdown 源） | https://github.com/apache/flink/blob/release-2.2/docs/content/docs/dev/table/sql/queries/window-tvf.md | 2026-10-20 | blob `e7b34a4ce5c78d93e7b3459f4a224a3208359e5c` |
| 19 | 2.2 Configuration 文档 Execution Options 表（不含任何 `table.exec.emit.*`） | https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/table/config/#execution-options | 2026-10-20 | Flink 2.2 文档 |
| 19b | 该表的生成源（核对无 emit 条目） | https://github.com/apache/flink/blob/release-2.2/docs/layouts/shortcodes/generated/execution_config_configuration.html | 2026-10-20 | blob `e236499de28790b8ca6d66a01e0436fa92ab313b` |
| 20 | Group Window Agg 发出 UPDATE_BEFORE / UPDATE_AFTER（retract 语义） | https://github.com/apache/flink/blob/release-2.2/flink-table/flink-table-runtime/src/main/java/org/apache/flink/table/runtime/operators/window/groupwindow/operator/AggregateWindowOperator.java | 2026-10-20 | blob `05b117e51b2d75697bad8d0b7d24bfa70bdd2b6b` |
| 21 | Table↔DataStream 转换不支持 changelog / update changes 报错信息 | https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/table/data_stream_api/#converting-between-datastream-and-table | 2026-10-20 | Flink 2.2 文档 |
| 22 | Flink 2.2 release notes "Table SQL / API"（无 early-fire/late-fire/迟到侧输出条目） | https://nightlies.apache.org/flink/flink-docs-stable/release-notes/flink-2.2/#table-sql--api | 2026-10-20 | Flink 2.2 release notes |
| 23 | `GroupWindow` / `GroupWindowedTable` / `Tumble` 为 `@PublicEvolving`（非 `@Experimental`） | https://github.com/apache/flink/blob/release-2.2/flink-table/flink-table-api-java/src/main/java/org/apache/flink/table/api/GroupWindow.java | 2026-10-20 | blob `4a0489d405a29e00b93b59c989ac64dd10dcaaa6` |
| 24 | FLINK-27849 与本题无关（Non-deterministic updates，Open，Fix 2.4.0） | https://issues.apache.org/jira/browse/FLINK-27849 | 2026-10-20 | JIRA 现状 |
| 25 | DataStream 侧输出通用机制（可从 ProcessFunction 等发出；`getSideOutput(OutputTag)`；Python 须调 `get_side_output`） | https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/datastream/side_output/ | 2026-10-20 | Flink 2.2 文档 |
| 25b | 同上（markdown 源） | https://github.com/apache/flink/blob/release-2.2/docs/content/docs/dev/datastream/side_output.md | 2026-10-20 | blob `1111853cd143e41cd1ea7810ce411e1c876d1e67` |
| 26 | late firings 语义与消费端去重要求（"take these duplicated results into account or deduplicate them"） | https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/datastream/operators/windows/#late-elements-considerations | 2026-10-20 | Flink 2.2 文档 |
| 27 | Window TVF Aggregation "do not emit intermediate results but only a final result" | https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/table/sql/queries/window-agg/#window-tvf-aggregation | 2026-10-20 | Flink 2.2 文档 |
| 28 | 2.2 Joins 页小节目录（不含 "Early Fire for Outer Joins"） | https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/table/sql/queries/joins/ | 2026-10-20 | Flink 2.2 文档 |
| 28b | master/2.3 文档树的 joins.md（含 "Early Fire for Outer Joins"，**不属于 2.2**） | https://github.com/apache/flink/blob/master/docs/content/docs/sql/reference/queries/joins.md | 2026-10-20 | blob `761fa0cb16edec1640c343c2fceec4f8017608d2`（master） |

---

## 一句话结论

- **问题 5**：**是**，"Flink SQL 没有迟到侧输出"在 Flink 2.2 依然成立（FLINK-10031 / FLINK-20527 均 Open/Unresolved；2.2 文档无任何 SQL 级迟到侧输出机制；`WindowAggOperator` 逐字 "late elements ... will be simply dropped."）。官方最接近的替代是 **DataStream-only** 的 `.sideOutputLateData(...)`（Python: `.side_output_late_data(...)`）+ `.getSideOutput(...)`，侧输出内容为逐字 "that was discarded as late"。DataStream 侧同时提供 `.allowedLateness(...)`：>0 时保留窗口状态，迟到记录触发 `late firings`，官方要求消费端 "take these duplicated results into account or deduplicate them"。SQL 侧则既无侧输出也无 allow-lateness（FLINK-24000 Closed/Later）。
- **问题 6**：early-fire / late-fire **在 2.2 只对 legacy Group Window Aggregation 生效**，对 **Window TVF 聚合直接抛异常**（运行时消息逐字：`Currently, window table function based aggregate doesn't support early-fire and late-fire configuration 'table.exec.emit.early-fire.enabled' and 'table.exec.emit.late-fire.enabled'.`，出处 `WindowUtil.checkEmitConfiguration`）。这 5 个配置项**全部带 `@Experimental`**，注明移除由 **FLINK-29692（Open/Unresolved）** 跟踪，且**不在** 2.2 官方 Configuration 文档表中（2.2 文档树内无任何 early-fire 说明页）。
- **★ 官方文档层面的决定性事实**：唯一支持 early-fire 的那套语法（Group Window Aggregation）在 Flink 2.2 官方文档中**已被标 deprecated**（"Warning: Group Window Aggregation is deprecated."），而官方推荐的替代（Window TVF Aggregation）**明确不发射中间结果**（"window aggregation do not emit intermediate results but only a final result, the total aggregation at the end of the window."）。⇒ **「窗口内提前发射」在 Flink 2.2 上只能走一条正在被官方弃用的语法。**
- **运维前置条件**：early-fire 开启必须显式给 `early-fire.delay`（该选项无默认值，不给会抛 "Early-fire delay should not be null or negative value whenenable early-fire emit strategy."）；事件时间下开启 late-fire 必须同时给 `table.exec.emit.allow-lateness` 或 `table.exec.state.ttl`，否则抛 "The 'AFTER WATERMARK' emit strategy requires set 'allow-lateness' or 'minIdleStateRetentionTime' in table config."；session window 不支持 EMIT 策略（"Session window doesn't support EMIT strategy currently."）。
- **CUMULATE**：官方只声明它提供"固定窗口间隔内的早期发射"**累计语义**；**官方未给出** CUMULATE 支持 `table.exec.emit.early-fire.*` 配置的声明（源码上 CUMULATE 属 Window TVF 路径，该路径对 early-fire 直接抛异常）。
