# 拷问工作底稿（GDD 前置）

> **性质**：起草 `gdd.md` 前的**临时工作底稿**，不是 GDD 本身。
> **日期**：2026-10-19（Asia/Shanghai），第 3 轮拷问中
> **上游**：`SRS.md`（需求）、`FSR.md`（路线冻结）、`research-notes.md`（调研证据）
> **用途**：把逐轮拷问中**已确凿**的结论钉死在这里，防止多轮对话上下文丢失；GDD 成稿时 consolidation，本文件即弃。
> **权威**：本文件不声明任何需求。冲突时 `SRS.md` / `FSR.md` 优先；本文只记「拷问结论」与「已证伪的假设」。

---

## A. 已冻结的拷问结论（用户已裁决）

| # | 议题 | 结论 |
| --- | --- | --- |
| Q1 | 文档权威关系 | **SRS / FSR / GDD 都不是 spec**；实施规格由用户另开会话产出 `spec.md`。GDD 定位为设计决策与结论文档，不承担需求权威 |
| Q2 | GDD 结构 | (b) 四要素主结构 + 三个横切附录（状态机图集 / 数值参数总表 / 失败路径登记表），**每个附录一屏硬上限**；借 `ai-observability-lakehouse` 的 500 B ADR 模板（Context / Decision / Consequences）登记被否决路线 |
| Q3 | 拷问范围 | (b) P0/P1 深挖到可实现；P2/P3 只定边界（表形态 + 触发条件 + 验收判据） |
| Q4 | 状态机建模对象 | 建 ①生成器运行态 ②Flink 作业生命周期 ④单事件处置态 ⑤离线分层作业态；**只有 ④ 出逻辑原型** |
| Q5 | 迟到与脏的关系 | (a) 正交两通道，判定顺序：①结构与取值 → ②时间范围 → ③唯一性 → ④迟到；**脏优先**于迟到 |
| Q6 | `event_id` 去重状态 | (a) 实时只保 **1 小时**滚动去重状态（≈70 MB）+ (c) 下游 Doris unique key 兜底；24h 全量去重因预算比否决 |
| Q7 | 原型存放与命名 | `.scratch/ecom-user-behavior-pipeline/prototypes/`，文件名含 `prototype`；GDD 以相对路径链接引用，不抄内容 |

---

## B. 已证伪 / 已修正的假设（拷问的实质产出）

### B1. `REQ-NF-REL-03` 与 `REQ-F-OLAP-02` 把一件事说成两道独立防线
SRS 一边要「end-to-end exactly-once：source offset + checkpoint + sink 2PC 三处对齐」，一边要 unique key 幂等。
**官方原文**：*Under the Unique model, you can also disable 2pc to achieve idempotent writes* —— [Flink-Doris-Connector FAQ](https://doris.apache.org/docs/dev/connection-integration/data-integration/flink-doris-connector/faq)；[Writing Data to Doris](https://doris.apache.org/docs/dev/connection-integration/data-integration/flink-doris-connector/write) 批量写对比表亦写「Exactly-Once is not guaranteed; idempotent writes can be achieved with the Unique model」。
**机制分层**：label 去重管**事务级重放**；unique key upsert 管**行级重放**。Unique Key MoW 表上后者已把重放变成幂等 no-op，故 2PC 剩的主要是**可见性/原子性**价值。
**→ GDD 写法**：必须写成「一层主防 + 一层兜底」，不得并列成两个独立保证。

### B2. `REQ-F-RT-04` 的迟到通道在 SRS 中**没有落点**，字面不可验收
SRS 要求迟到走侧输出，判据是「侧输出确有迟到事件」，但**全篇未定义落点**：无 topic、无 Doris 表、无离线标记列。结果：只能开 Flink UI 肉眼看；看板永远看不到迟到；对账口径永远分不出「少算」与「本来就没这条」。
**→ GDD 必须补全落点**，并写入对 SRS 的偏离记录（见 Q8 裁决）。

### B3. Flink SQL **没有**迟到侧输出机制（官方承认缺口）
- [FLINK-10031](https://issues.apache.org/jira/browse/FLINK-10031)（**Open**）原话：「The Table API & SQL **drop late events right now**. We should offer something like a side channel that allows to capture late events for separate processing.」
- [FLINK-20527](https://issues.apache.org/jira/browse/FLINK-20527)（「Support side output for late events in Flink SQL」）亦 **Open**。
- 侧输出**只能挂在 DataStream 窗口算子**（`WindowedStream.sideOutputLateData`），且侧输出的是**原始元素**（`OutputTag<T>`，T=输入元素类型），**不是聚合结果** —— [windows.md](https://github.com/apache/flink/blob/release-2.2/docs/content/docs/dev/datastream/operators/windows.md)。
**→ 工程后果**：「迟到走侧输出」这条 MUST **强制**作业至少有一部分必须是 DataStream；纯 SQL 路线无法满足。

### B4. `table.exec.emit.early-fire.*` 存在但有硬约束
- 2.2 **仍在**，未废弃改名；`delay` 是 duration 类型，**10 s 合法** —— [WindowEmitStrategy.scala (release-2.2)](https://github.com/apache/flink/blob/release-2.2/flink-table/flink-table-planner/src/main/scala/org/apache/flink/table/planner/plan/utils/WindowEmitStrategy.scala)。
- 两个 option 均标 `@Experimental`，原话「its removal will be consistently monitored and addressed through FLINK-29692」。
- **只对 Group Window Aggregation（TUMBLE/HOP）生效**；SESSION 抛异常；**Window TVF 不支持** —— [WindowUtil.scala](https://github.com/apache/flink/blob/release-2.2/flink-table/flink-table-planner/src/main/scala/org/apache/flink/table/planner/plan/utils/WindowUtil.scala) 原话「window table function based aggregate doesn't support early-fire and late-fire configuration」。
**→ 若采纳 Q13(c)（窗口内提前发射）**：必须用旧的 Group Window Agg 语法，且接受 Experimental 风险。

### B5. 空闲超时的**正确配置项名**（我之前说错了）
不是 DataStream 的 `withIdleness` 了事。SQL 层：
- 全局 `table.exec.source.idle-timeout`；
- 逐表 `scan.watermark.idle-timeout`（**preferred**，前提：连接器实现 `SupportsWatermarkPushDown`，Kafka 属于）；
—— [Time Attributes (release-2.2)](https://github.com/apache/flink/blob/release-2.2/docs/content/docs/dev/table/concepts/time_attributes.md) 原话。
**注意**：Flink 2.2 的 Kafka SQL connector 文档**未**把 `scan.watermark.idle-timeout` 列为 connector 选项，只推荐全局那个。

### B6. 全链路 **JDK 基线 = 17**（SRS 与 FSR 都没写）
[doris-flink-connector README](https://github.com/apache/doris-flink-connector) 版本表（本人直读核对）：Flink `2.0.x/2.1.x/2.2.x/2.3.x → JDK 17`（2.1+ 的 JDK 21 标 experimental）；Flink 2.x artifact 以 `--release 17` 编译。
**修正**：connector **支持** Flink 2.3.x —— 卡住 2.3 的不是 Doris 侧，只是 Kafka connector 5.0.0 那条线。R2 应更精确地表述为「单点断链」而非「全栈不支持」。

### B7. Superset **无**「刷新不得小于 10 秒」的硬下限
- `SUPERSET_DASHBOARD_PERIODICAL_REFRESH_LIMIT = 0`（默认）仅是**警告阈值**，非硬下限 —— [6.0.0 config.py](https://github.com/apache/superset/blob/6.0.0/superset/config.py)；6.0.0 前端自定义下限 **1 秒**（[RefreshIntervalModal.tsx](https://github.com/apache/superset/blob/6.0.0/superset-frontend/src/dashboard/components/RefreshIntervalModal.tsx)）；master 有 `MINIMUM_REFRESH_INTERVAL = 1`（[RefreshFrequencySelect.tsx](https://github.com/apache/superset/blob/master/superset-frontend/src/dashboard/components/RefreshFrequency/RefreshFrequencySelect.tsx)）。
**→ `REQ-F-BI-02` 的 ≤10 s 可实现**（10 s 只是预设项，非限制）。

### B8. Doris 部分列更新官方支持于 UNIQUE KEY MoW
[Column Update (4.x)](https://doris.apache.org/docs/4.x/data-operate/update/partial-column-update) 原话：「In version 2.0, partial column updates are supported only in the Merge-on-Write implementation of Unique Key.」；`partial_columns:true`。
**但**：「Across all data models, the default behavior of `INSERT INTO` with a subset of columns is a **full-row write**」——要**显式开启**才只更新指定列。
**→ 对「窗口覆盖写修正」有利**，但必须显式配置。

### B9. 近似算法的误差数字（纠正「精确」一词）
- `approx_count_distinct`：HLL，固定内存，**相对标准误 0.8125%** —— [Doris 文档](https://doris.apache.org/docs/4.x/sql-manual/sql-functions/aggregate-functions/approx-count-distinct/)。
- `bitmap_hash`：32 位 MurMur3，**无全局字典时误差约千分之一** —— [bitmap-hash](https://doris.apache.org/docs/4.x/sql-manual/sql-functions/scalar-functions/bitmap-functions/bitmap-hash/)、[BITMAP 类型页](https://doris.apache.org/docs/4.x/sql-manual/basic-element/sql-data-types/aggregate/BITMAP/)。
**→ 「Doris 侧精确去重」这个说法对 bitmap_hash 不成立**，须写「约 0.1% 误差」或改用 `bitmap_hash64`。

### B10. `REQ-F-RT-03` 的立论前提**未量化**，且把两个不同基数混为一谈 ★（本轮最重要的发现）
SRS 原文：`REQ-F-RT-03` 「UV 用近似算法（HyperLogLog / Theta sketch）…**全量去重状态会顶满天花板**」。
**算术核对**（200 ev/s，即 SRS 默认速率）：

| 去重对象 | 1 小时基数 | 1 小时状态量（按 100 B/条） | 24 小时状态量 |
| --- | --- | --- | --- |
| **`event_id`**（事件级去重） | 720,000 | **≈68.7 MB** | ≈1.6 GB |
| **`user_id`**（UV 去重） | **未定义**（取决于用户池大小） | 若 10 万用户 ≈4 MB | ≈数十 MB |

**结论**：
1. 「顶满天花板」这句话，只有在 **`event_id` 全量去重且保留 7 天**（≈11.3 GB，与 Kafka retention 同长）时成立——但那不是 UV 要解决的问题。
2. **UV 去重按 `user_id`，基数 = 用户池大小，而 SRS 通篇从未定义用户池规模**。`REQ-F-GEN-08` 只要求「产出并维护维度数据（商品/品类/地域/渠道）」，**没有给用户数**。
3. 若用户池取 10 万（本项目自助生成，完全可控），**精确去重的状态是 MB 级**——近似算法要省的资源在此规模上**不存在**。
**→ 这直接动摇 `REQ-F-RT-03` 作为 MUST 的依据**：它把「事件级去重的基数」错当成「UV 去重的基数」。
**→ 连带**：Q12 参数表必须**新增一项「用户池规模」**（建议 10 万，可配置），否则 UV 的口径与验证判据都无法量化；`REQ-F-GEN-05` 的漏斗递减、Q9 的步进漏斗状态量也都依赖它。
（注：Q6 关于 `event_id` 去重保 1 小时的结论**仍然成立**——那里算的正是 68.7 MB 这一列。）

### B11. 「迟到的迟到程度」在文档中不可判定（验收判据的隐性依赖）
`REQ-F-GEN-07` 要求 `--late-ratio`（默认 5%）「把该比例事件的事件时间显式置于**水位线之前**」——但**置于水位线之前多少、水位线取哪个时刻**未定义。而「允许延迟内（可修正）」与「超出允许延迟（只计数）」是**两种不同处置**（见 Q10），落点不同、验收判据也不同。
**→ GDD 必须定义注入偏移公式**（已在 Q12/后续轮待问），否则 `REQ-F-GEN-07` 的判据无法区分「制造了可修正的迟到」还是「制造了超期的迟到」。

---

## C. 可比开源项目的**文档形态**（本人 `github_file` 直读核对）

| 仓库 | ★ | 文档形态 | 栈 |
| --- | --- | --- | --- |
| [MrSuiChuan/data-warehouse-learning](https://github.com/MrSuiChuan/data-warehouse-learning) | 1261 | **仅 `README.md` 分节 + 版本表 + 架构图**；无 `docs/` 无 ADR | Flink 1.18 / Paimon 0.8 / Doris 2.0.4 |
| [CHEUKFUNGWU/ai-observability-lakehouse](https://github.com/CHEUKFUNGWU/ai-observability-lakehouse) | 0 | `docs/README.md`（索引四类）+ `architecture.md` + `data_model.md` + `metric_definitions.md` + `runtime_runbook.md` + `adr/NNN-*.md`（ADR 模板仅 500 B） | Kafka→Flink→Paimon→Doris→Superset |
| [flowerfine/scaleph](https://github.com/flowerfine/scaleph) | 393 | `docs/` 下**只有 `image/`** | Doris/Flink/K8s |
| [datavane/datavines](https://github.com/datavane/datavines) | 765 | `docs/` 下**只有 `img/`** | 数据质量平台 |

**关键事实**：`Mrkuhuo/data-warehouse-learning` 与 `MrSuiChuan/...` 根 tree sha **完全相同**（`30de2340d76edbf6f0d59405573e9bfac89d4d3c`），确证为镜像，**两者都无 `docs/`、无 ADR**。
**同类项目一致的缺口**（调研报告「未找到」汇总）：① UV 近似算法选型证据；② **1 分钟窗口跨步漏斗**（它们的漏斗是**日批**，`ads_user_action.sql` 用 `dt` + recent_days 1/7/30）；③ 迟到 + 脏数据**双 DLQ 侧输出**；④ 时态维表**变更流生成器**（`dim_user_zip_full` 是拉链表但生成器只做一次性静态 INSERT）；⑤ Kafka 分区数与分区键。
**参照值**（不可直接移植，量级不符）：`MrSuiChuan` 是 5 s 乱序 + 2 s 允许延迟配 **5 s 窗口**、checkpoint 10 s、Doris `UNIQUE KEY + BUCKETS 32 + 动态分区 -60/+3`。

**Doris Catalog 直读 Paimon（`REQ-F-OLAP-04`）**：`MrSuiChuan` 仓内唯一 catalog 文件是 **Flink 侧**的 `paimon_hive_catalog.sql`（`paimon.catalog.type=hms`），**不是** Doris 的 `CREATE CATALOG`；README 该节**仅有截图**，无 DDL 无验证 SQL → **不可验证，支持降级为实验路径**。可抄的 Doris 原生写法见 [ai-observability-lakehouse 的 sql/doris_create_paimon_catalog.sql](https://github.com/CHEUKFUNGWU/ai-observability-lakehouse/blob/main/sql/doris_create_paimon_catalog.sql)（4 行）。

---

## D. 已交付原型（三个，均可双击；Q17 已批入库，尚未 `git add`）

| 文件 | 类型 | 它回答什么 | 验证状态 |
| --- | --- | --- | --- |
| [`event-routing.prototype.html`](prototypes/event-routing.prototype.html) | 逻辑（单文件） | 事件处置判定顺序、脏/迟到/重复三通道落点、**Q11**「重复计脏会不会污染脏率」 | 语法 ✅ / 逻辑断言 18 条全绿 ✅ |
| [`dashboard.prototype.html`](prototypes/dashboard.prototype.html) | UI（多变体 `?variant=A\|B\|C`） | 看板三结构范式（大屏镶边 / 左筛选网格 / 旅程时间轴）+ **Q9(b) 按用户步进漏斗** | 语法 ✅ / 逻辑断言 16 条全绿 ✅ |
| [`funnel.prototype.html`](prototypes/funnel.prototype.html) | 逻辑（单文件） | **Q9 正下游**：乱序步/跳步/重复步的记账规则（补记/拒绝/只记自身 × 只记首次/每次计数） | 语法 ✅ / 逻辑断言 33 条全绿 ✅ |

**原型行为验证结论**（均已实跑，脚手架用完即删）：

- **event-routing**：脏优先 ✅；允许延迟内→窗口修正（revisions 递增）✅；超出→只计数不修正 ✅；**「重复计脏」时合法重放把脏率从 0% 推到 50%，其中真脏 0 条** ✅。
- **dashboard**：漏斗按用户步进逐级递减 ✅；常规 **201.6 ev/s**（`REQ-F-GEN-03` 目标 ≈200）✅；尖峰 **1001.6 ev/s**（`REQ-F-GEN-04` 目标 ≥1000）✅；`REQ-F-BI-02` 的 ≤10 s 刷新经 B7 证实可实现。
- **funnel**：抓到并修复一个**真 bug**——步进水位原先**无条件抬升**，导致 `reject` 策略下被拒步骤也抬高水位 → 该用户此后任何更低步都被判「跳步」继续拒绝 → **永久锁死**。修复后加断言钉住（H1–H4）。三条策略实测差异见 E2-3。

---

## E. 裁决状态（2026-10-19 更新：用户「全按推荐」）

### E1. 已冻结

| # | 议题 | 冻结结论 |
| --- | --- | --- |
| Q8 | SRS 缺陷回写 | (a) GDD 内设「对 SRS 的偏离与缺陷」一节，**不改** `SRS.md`；不为「文档说错了」开票 |
| Q9 | 4 步漏斗口径 | (b) **按用户步进**推进；与 1 分钟窗口正交 |
| Q10 | 迟到落点 | (a) 允许延迟内**修正**已落定窗口 + 超期**只计数**；计数落 Doris，看板加「迟到率」 |
| Q11 | 重复是否算脏 | (b) **摘出为独立「重复通道」**：计数、不进 DLQ、不计脏率；脏通道只留 ①②④ |
| Q12 | 实数表 | watermark 5 s / 空闲 30 s / checkpoint 30 s/10 s/10 min / 分区 6 / 键 `user_id` / **JDK 17**；allowed lateness 2 分钟（由 Q10 定） |
| Q13 | 刷新 vs 窗口 | (c) 窗口内**提前发射** + as-of 指示；**受 B4 约束**（须用 Group Window Agg、`@Experimental`） |
| Q15 | `GLOSSARY.md` | (a) **已建**（仓库根，111 行，按 `GLOSSARY-FORMAT.md` 只收冻结术语） |
| Q16 | 作业表达方式 | **混合**：SQL 做窗口聚合与 Doris sink，DataStream 做迟到侧输出与修正段（受 B3 强制） |
| Q17 | 原型入库 | 入库为证据（**尚未执行 `git add`**，见 E3） |
| Q18 | 用户池规模 | **10 万**，可配置（B10 逼出；决定 UV 口径与漏斗状态量） |
| Q19 | 漏斗记账 | **双列并陈**：`funnel_cnt_observed`（真实到达）+ `funnel_cnt_inferred`（补记值）；主口径 `autofill` 保单调，补记值不冒充观测值 |
| Q20 | 生成器会话连贯性 | (d) 默认按**会话轨迹**产出（`purchase` 必有前置步）+ 另设 `--chaos-ratio` 开关故意跳步，用于测试补记逻辑 |
| Q21 | `--late-ratio` 的注入公式 | 拆为 `--late-correctable 0.03`（水位线 − 随机 5–115 s → 允许延迟内，触发修正）+ `--late-expired 0.02`（水位线 − 随机 130–300 s → 超期只计数）；**相对水位线**计算，`--seed` 可复现 |
| Q22 | 漏斗状态 TTL | 会话 **30 分钟**；`StateTtlConfig` 清理 **32 分钟**（30 + 2 分钟允许延迟）；跨会话同用户重开漏斗 |
| **Q23** | **UV 与幂等** | **(ii) Flink 侧精确滚动 UV + 保留 UNIQUE 模型**：UV 由 Flink 1 小时滚动去重状态精确算出（用户池 10 万 → 状态几 MB 级），写 Doris 的只是**已算好的整数**；`REQ-F-OLAP-02` 的 unique key 模型**不改**；`REQ-F-RT-03` 的近似算法**降级为可选实验**（理由见 B10） |

### E2. ⚠️ 尚未落定

**E2-1 · Q23 已裁决（2026-10-19，选项 ii）—— 决策与依据**

*决策*：UV 走 **Flink 侧精确滚动去重**，Doris 只存整数结果；保留 UNIQUE 模型与 `REQ-F-OLAP-02`。

*为什么否掉一开始推荐的 (c)「Doris 侧 BITMAP 精确滚动」：*
- [CREATE TABLE](https://doris.apache.org/docs/4.x/sql-manual/sql-statements/table-and-view/table/CREATE-TABLE) 原话：「`<col_aggregate_type>` ... **Can only be used when the table is an aggregation model**」→ 聚合类型**只能用于 AGGREGATE 模型**。
- 源码级（[ColumnDefinition.java](https://github.com/apache/doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/nereids/trees/plans/commands/info/ColumnDefinition.java)）：Unique 表非键列 `aggType = REPLACE`（MoW 下 `NONE`），复杂类型仅放行 NONE/REPLACE/REPLACE_IF_NOT_NULL。
- **→ UNIQUE 表里的 BITMAP 列是「整值替换」不是 union**，「Doris 侧自动滚动去重 + 保幂等」不成立。

*为什么也否掉 AGGREGATE 替代路线（六条风险）：*
| 事实 | 出处 | 影响 |
| --- | --- | --- |
| `REPLACE` = last-write-wins | [Aggregate Model](https://doris.apache.org/docs/4.x/table-design/data-model/aggregate) | 标量列幂等 ✅ |
| `BITMAP_UNION` 重复导入幂等性**无官方表述** | BITMAP 类型页/函数页/表模型页皆无 | ⚠️ 仅数学推论 |
| 「同键重复导入」被列**使用限制**，仅对 REPLACE 松绑 | [tips](https://doris.apache.org/docs/4.x/table-design/data-model/tips) | ⚠️ 整体幂等不能无条件推出 |
| 查询期额外聚合算子，**比 MoW 慢 5–10 倍**（`count(*)` 慢 10 倍以上） | [tips](https://doris.apache.org/docs/4.x/table-design/data-model/tips)、[partial-column-update](https://doris.apache.org/docs/4.x/data-operate/update/partial-column-update) | ⚠️ 看板 10 s 刷新吃这个开销 |
| AGGREGATE **不支持 Sequence 列** | update-overview | ⚠️ 无版本序控制 |
| connector **类型映射表无 BITMAP**；写 BITMAP 靠 `to_bitmap()`，**只接整数** | [write](https://doris.apache.org/docs/dev/connection-integration/data-integration/flink-doris-connector/write)、[data-type-mapping](https://doris.apache.org/docs/dev/connection-integration/data-integration/flink-doris-connector/data-type-mapping) | 🔴 `user_id` 是字符串，路径别扭 |

*连带效应（须写入 GDD）*：
1. `REQ-F-RT-03` 从 MUST **降级为可选实验**，理由**不是**「近似不够好」，而是 **B10 证明其立论前提（全量去重会顶满天花板）在本规模不成立**——UV 去重基数是用户池而非事件数。按 Q8(a) 记入偏离节。
2. 对账性质**升级**：实时与离线**都是精确值**，故 `REQ-F-RT-03` 的「离线算精确 UV 并对账」从「误差量化」变成**一致性校验**（更强，且面试可讲「我用精确滚动去重，因此不需要近似算法」）。
3. `REQ-F-VERIFY-02` 的比对面含义更明确：同一 `--seed` 跑两遍，实时 UV 与离线精确 UV 应**逐窗口相等**。

*数字更正*：`bitmap_hash` 无全局字典时官方承认**约千分之一误差**；`approx_count_distinct` 为 HLL、相对标准误 **0.8125%**（均作为可选实验的参考，不再是主路径）。

*旁证（Paimon 侧，供 §3.1.4 用）*：`snapshot.num-retained.min` 默认 **10**（非 5）、`max` 默认 Integer.MAX_VALUE、`time-retained` 默认 **1h**、`compaction.min.file-num` 默认 **5**、`full-compaction.delta-commits` 默认 **1**（[Manage Snapshots](https://paimon.apache.org/docs/2.1/maintenance/manage-snapshots)、[Configurations](https://paimon.apache.org/docs/2.1/maintenance/configurations)）。

**E2-2 · Q12 的空闲超时落点更正**
`scan.watermark.idle-timeout` 在 **connector 5.0.0 中未实现为 Kafka 表选项**（`KafkaConnectorOptions` / `KafkaDynamicTableFactory.optionalOptions()` / `KafkaSourceOptions` 逐项皆无）；它是 **core planner 支持、connector 未暴露**。**故 30 s 必须落在 `table.exec.source.idle-timeout`（全局）**——亦是 connector 官方文档推荐的那个。

**E2-3 · 漏斗记账策略**（Q19 已冻结为双列并陈；下面是被冻结的理由）
[`funnel.prototype.html`](prototypes/funnel.prototype.html) 实测三策略：
- `autofill`（补记）：**永不倒挂**，但计数是**推断值**（直接支付的新用户被补记成 4 条）。
- `reject`（拒绝）：**系统性低估**（实测 H5：`highest` 停在 0 时任何 >1 的步都被拒）。
- `asis`（只记自身）：**产生倒挂**（实测 D3：第 3 步 1 > 第 2 步 0）→ 看板出现「>100% 转化率」。
- **冻结方案**：主口径 `autofill` + **双列并陈**（`funnel_cnt_observed` 真实到达 / `funnel_cnt_inferred` 含补记），补记值不冒充观测值。

### E4. 保留策略与容量验算（三处缺陷，D1–D3）

**D1 · `REQ-F-OLAP-03` 的「保留 30 天」默认不生效**
官方 `dynamic_partition.start` 默认 **-2147483648**，语义是「**历史分区不删除**」。要让 30 天保留真正生效，**必须显式设 `start=-30`**。照默认建表的结果是**永不删除**——与需求**恰好相反**。出处：[Dynamic Partitioning](https://doris.apache.org/docs/4.x/table-design/data-partitioning/dynamic-partitioning)。

**D2 · `REQ-F-LAKE-08` 的「保留最近 5 个」与官方默认冲突**
Paimon `snapshot.num-retained.min` 官方默认是 **10**，非 5。要么表属性显式设 5，要么改需求。出处：[Manage Snapshots 2.1](https://paimon.apache.org/docs/2.1/maintenance/manage-snapshots)。

**D3 · `REQ-F-MQ-03` 的「7 天或 2 GB 取小」实际生效窗是约 6 小时** ★新发现
算术（按 200 ev/s、约 500 B/条 JSON）：**≈8.05 GB/天**；故 2 GB 上限约在 **6 小时**处触发。
→ 「7 天」在本速率下是**装饰性上限**，真实保留窗 ≈6 小时。这不是缺陷，但**必须写明**，因为它有下游后果：
- `REQ-F-GEN-07` 的「离线回放」若从 **Kafka 重读**，超过约 6 小时的历史**读不到**——离线链路的输入应走 `dump` 文件（`REQ-F-GEN-01` 的 dump 模式）而非 Kafka 重读，否则长回放会静默丢数据。
- `verify-fault` 的恢复窗口若超过 6 小时，offset 可能已过期（`auto.offset.reset` 行为必须先定）。

**容量验算（`REQ-NF-CAP-02` 磁盘 ≤20 GB，此前从未被验算）**
| 项 | 估算 | 依据 |
| --- | --- | --- |
| Kafka（2 GB 上限） | 2.0 GB | `REQ-F-MQ-03` |
| Paimon 五层（100 万事件 × 500 B × 放大 2–3×） | ~1.5 GB | `REQ-NF-CAP-05` 的最小可跑量 |
| Doris（30 天窗口指标 + 明细，压缩后） | 3–6 GB | `REQ-F-OLAP-03` |
| Superset 元数据 + 镜像层 | 1–2 GB | 官方未给数字（A2） |
| **合计** | **7–11 GB** | ✅ 在 20 GB 内，留有余量 |

→ 「20 GB」这个门槛**成立**，但**不是自明的**：若采纳亿级回放（`REQ-NF-CAP-06`，官方口径约 1 亿事件 / ~3.4 GB 解压、约 37 GB 原始 JSON 级）则**必然击穿** 20 GB——故 `REQ-NF-CAP-06` 的 COULD 定级是对的，且 GDD 必须写明「亿级回放与 20 GB 门槛互斥，二选一」。

### E3. 已建但未推送的仓库改动

| 项 | 现状 | 待办 |
| --- | --- | --- |
| `GLOSSARY.md` | **已建并已提交**（`e7e6480`） | 无 |
| `prototypes/` 入库 | **已提交**（`e7e6480`，3 个 HTML + 底稿 + 术语表，2253 行） | **未推送**（`main` 领先 `origin/main` 1 个提交）——推送即公开，等用户确认 |
| `docs/adr/` | **尚未创建** | GDD 成稿时按 Q2 借 500 B 模板创建，登记被否决路线（R1 Catalog 直读、R2 Flink 2.3、Q14 近似 UV 的降级） |

### E5. GDD 与 spec 的落点（待用户裁决）

GDD 文本尚未开始写。落点有两处候选，且与 `spec.md` 的关系需要一句话确认：
- **(a)** `.scratch/ecom-user-behavior-pipeline/gdd.md`（与 SRS/FSR 同址，spec 也在此目录）
- **(b)** 仓库根 `gdd.md` 或 `docs/gdd.md`

用户已明确「SRS 和 GDD 都不是 spec，后面会专门开会话生成 spec.md」——故 GDD 与 spec 同址最为自然。

---

## F. 本底稿自身的生命周期

本文件是**脚手架**：`gdd.md` 成稿后，其内容应被 consolidation 吸收（结论进对应功能节、修正进「对 SRS 的偏离」节、项目形态事实进附录），然后**删除本文件**。
