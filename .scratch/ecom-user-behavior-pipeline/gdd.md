# GDD — 电商用户行为数据全链路（离线 + 实时）

> **性质**：游戏/产品设计文档（Game/General Design Document）体例的**功能设计文档**。本文回答「每个功能**怎么做**、触发条件与状态怎么变、怎么算做到了」。
> **日期**：2026-10-19（Asia/Shanghai）
> **上游**：`SRS.md`（需求权威）｜`FSR.md`（路线冻结）｜`research-notes.md`（一手调研证据）
> **下游**：本 spec 施工规格；**收口**：`canonical-truth.md`
> **术语**：一律以仓库根 `GLOSSARY.md` 为准；本文出现的领域概念均在该表内定义。
> **权威关系**：本文**不是** spec，也不声明需求。凡与 `SRS.md` 冲突处，本文在 §D 显式列出并说明。上游改写按 `canonical-truth.md` §0 G1 分档：**事实类**（官方/物理限制把上游陈述证伪）可直接改写上游，**意图类**保留原话并登记。上一版此处「**不修改** `SRS.md`」的自述**从未获 SRS/FSR 背书**（两文件无「不得修改」条款、无变更控制章节），已按本段订正。

## 0. 这份文档怎么用

| 你要做的事 | 读哪节 |
| --- | --- |
| 看每个功能的触发/流程/状态/判据 | §1 功能设计（11 节） |
| 查全文所有数值参数 | §A 参数总表 |
| 看状态机 | §B 状态机图集 |
| 排查失败与恢复 | §C 失败路径登记表 |
| 知道哪些地方与 SRS 不一致 | §D 对 SRS 的偏离与缺陷 |
| 看原型结论 | §E 原型索引 |

**已确认 vs 按推荐默认**：本文内容分两类来源——
- **【已确认】**：用户在拷问中逐条裁决过的（Q1–Q23、以及 D4/D5/D6 的处置）。
- **【按推荐】**：拷问中已给出推荐、但用户尚未逐条答复的（Q24–Q31）。这些条目在文内以 `【按推荐】` 标注；若你反对，直接指出条目号，本文可局部修订。

两条贯穿全文的锚定词（同 `FSR.md`/`SRS.md`）：**天花板**（容器群可用的内存预算顶格，≈8 GB）、**锚点**（组件精确 pin 版本）。

一个必须说清的区分：**事件结构分布**（同窗口内各事件类型的计数比）**不是**转化率——用户的浏览与支付通常落在不同分钟的窗口里。本文的漏斗一律指**转化漏斗（步进式）**，其单位是 `user_id` 的步进推进，与 1 分钟窗口正交。

---

## 1. 功能设计

### 1.1 模块① 数据生成器

**触发条件**
- 开发者显式调用：`live` 模式常驻启动；`dump` 模式一次性产出文件；`replay` 模式按原始时间戳回放。三者互斥。
- 【已确认 Q20】生成器内部以**会话轨迹**为单位推进：每个会话沿 `view_item → add_to_cart → begin_checkout → purchase` 按步进概率前行，`purchase` 必有前置步。

**流程**
1. 载入配置：速率（默认 200 events/s）、`--seed`、注入器参数、用户池规模。
2. `dump`/`replay` 模式：先产出维度数据（商品/品类/用户/地域/渠道），再产出事件；【已确认 Q26】维度**带变更流**（`--dim-change-ratio`），属性变更写入拉链区间。
3. 事件生成：按会话轨迹推进；每条事件经三个独立注入器改写：
   - `--late-correctable`（默认 **0.03**）：`event_time = 当前水印 − 随机(5 s, 115 s)` → 落 2 分钟**允许延迟内**，用于触发窗口修正。
   - `--late-expired`（默认 **0.02**）：`event_time = 当前水印 − 随机(130 s, 300 s)` → **超期**，用于触发「只计数」通道。
   - `--chaos-ratio`（默认 **0**）：按比例故意**跳过**漏斗中间步，用于测试补记逻辑。
   偏移一律**相对水印**而非墙上时钟计算，故任何速率下迟到程度都是设计值；`--seed` 下可复现。
4. 【已确认 Q21】三个注入器语义**互不混用**；旧的单参数 `--late-ratio` 拆为上列两项。
5. `live` 模式按速率写 Kafka；大促尖峰模式短时速率 ≥1000 events/s。

**状态与数据变化**
- 生成器运行态：`idle → warmup → live → (spike ⇄ live) → draining → stopped`（见 §B.1）。
- 产出物：事件 JSON（含 `schema_version`，**生产端产出 `1.0`**；分级与未知版本处置见 `canonical-truth.md` §3.1 与 SRS §3.3.1）、维度快照与维度变更流。
- 用户池：默认 **10 万** 个不同 `user_id`（【已确认 Q18】），可配置。这是滚动 UV 与漏斗状态量的基数来源。

**验收判据**
- 两模式均可独立运行且退出码 0（`REQ-F-GEN-01`）。
- 默认运行 60 s 产出 12,000 ±10%（`REQ-F-GEN-03`）——**原型已实测 201.6 ev/s**，见 §E。
- 尖峰模式实测 ≥1000 events/s（`REQ-F-GEN-04`）——**原型已实测 1001.6 ev/s**。
- 同 `--seed` 同参数跑两遍，输出文件哈希一致（`REQ-F-GEN-06`）。
  - ⚠️ **2026-10-19 补前提**：Faker **必须 pin 到补丁号**。官方 seeding 文档原话：「results are **not guaranteed to be consistent across patch versions**. If you hardcode results in your test, make sure you pinned the version of Faker **down to the patch number**」⇒ 不 pin 补丁号则该判据**不成立**。
  - ⚠️ 生成器运行时 pin **Python 3.12**（由 `uv python install` 托管），**不吃**宿主环境（Windows 3.13 / WSL 3.14）——否则违反 C3 锚点不漂移。
- **GDD 新增判据**：注入器生效可证——`--late-correctable` 出去的事件必须有对应数量的窗口修正记录；`--late-expired` 出去的事件必须有对应数量的迟到计数。仅断言「配置可见」不算通过。
- **GDD 新增判据**：会话连贯性可证——在 `--chaos-ratio 0` 下，产出的 `purchase` **100%** 有其前置步。

### 1.2 模块② 消息队列

**触发条件**：`docker compose up` 拉起 Kafka；初始化脚本建 topic 并施加配置。

**流程**
1. KRaft 单容器 combined mode（broker + controller 同进程）。
2. 建主 topic `ecom.behavior.event.v1` 与死信 topic `ecom.behavior.event.v1.dlq`。
3. 施加 retention、分区数、副本因子。
4. 【已确认 Q12】分区键取 **`user_id`**——保证**每用户有序**，这是步进漏斗与去重状态的前提。

**状态与数据变化**
- topic 状态：不存在 → 已创建 → 已配置（RF=1、固定分区数）。
- 【已确认 D3·2026-10-21 按配置层级重算】retention「7 天或 2 GB 取小」：`retention.bytes` **按分区生效** ⇒ 2 GB × 6 分区 ＝ 主题级 **≈12 GB**，本速率（200 ev/s × ~500 B/条 ≈ 8.05 GB/天）下**实际生效窗 ≈36 小时**（**设计值**，算术推算；旧「≈6 小时」把 `retention.bytes` 误当主题级总量，**已作废**）。这是设计值而非缺陷，但**下游必须知情**（见模块④与 §D D3）。

**验收判据**
- 两 topic 均存在（`REQ-F-MQ-01`）。
- topic 描述显示 RF=1 且分区数等于配置值（`REQ-F-MQ-02`）——默认 **6** 个分区（≥ 并行度上限 4 留余量）。
- topic 配置的 retention 与 §A 参数表一致（`REQ-F-MQ-03`）。
- 采样消息可解析、键名为 `snake_case`、含 `schema_version`（`REQ-F-MQ-04`）。
- **GDD 新增判据**：**分区键可证**——同一 `user_id` 的事件必须全部落在同一分区（否则 Q9 的步进序不成立）。

### 1.3 模块③ 实时作业

**触发条件**：常驻运行；`docker compose up` 拉起 Flink JM/TM 后由脚本提交作业。作业生命周期见 §B.2。

**流程（【已确认 Q16】混合路线）**
1. **Flink SQL 段**：Kafka source → 窗口聚合 → Doris sink（经 Flink-Doris-Connector，Stream Load 2PC）。
2. **DataStream 段**：**迟到侧输出**。这不是选型偏好而是**硬约束**——Flink SQL 官方**没有**迟到侧输出机制（[FLINK-10031](https://issues.apache.org/jira/browse/FLINK-10031) 与 [FLINK-20527](https://issues.apache.org/jira/browse/FLINK-20527) 均 **Open**，原话「The Table API & SQL **drop late events right now**」）；`sideOutputLateData` 只能挂在 DataStream 的窗口算子上，且侧输出的是**原始元素**。
3. 事件处置流水线（**四关顺序冻结**，见 §B.4 与原型）：
   - ① 结构与取值 → ② 时间范围 → ③ 唯一性 → ④ 迟到；**脏优先于迟到**。
   - **脏**（①②违规）→ DLQ topic。**重复**（③）→ 独立重复通道计数，**不进 DLQ、不计脏率**。【已确认 Q11】
   - **迟到**（④）：允许延迟内 → **覆盖写修正**已落定窗口；超期 → **只计数**。【已确认 Q10】
4. 窗口：1 分钟滚动；【已确认 Q13】窗口内**提前发射**（窗口语义仍是 1 分钟，对未关闭窗口每 10 s 发射中间结果，Doris 以 `(window_start, 维度)` 作 unique key 覆盖写，窗口关闭时落定）。
   - **⚠️ 机制约束（B4）**：`table.exec.emit.early-fire.*` 只对**旧式 Group Window Aggregation**（TUMBLE/HOP）生效；**Window TVF 不支持**（官方原话见 §D）。且该配置标 `@Experimental`、有移除跟踪 FLINK-29692。
5. 【已确认 Q9/Q19】四步漏斗按**用户步进**推进（key = `user_id`），与 1 分钟窗口**正交**。记账用**双列并陈**：`funnel_cnt_observed`（真实到达）与 `funnel_cnt_inferred`（含**补记**）。补记值不冒充观测值。
6. 【已确认 Q23】滚动 UV：由 **Flink 侧键控去重状态精确算出**（1 小时滚动，用户池 10 万 → 状态 MB 级），写入 Doris 的只是**已算好的整数**。近似算法**不作主路径**。
7. 作业并行度 ≤ 4；checkpoint 开启，RocksDB 增量。

**状态与数据变化**
- 作业态：`created → running → (restarting ⇄ running) → suspended/finished`。
- 单事件处置态：`received → validated → deduped → admitted / corrected / late_counted / duplicate / dirty_dlq`。
- 窗口态：`累加中 → 已提前发射（暂定值） → 已落定 → (已修订)`。
- 状态规模（【已确认 Q6/Q22】）：`event_id` 去重状态保 **1 小时**（约 68.7 MB）；漏斗键控状态会话 **30 分钟**、TTL 清理 **32 分钟**；UV 滚动去重状态约 MB 级。

**验收判据**
- 窗口触发周期实测 ≈60 s（`REQ-F-RT-01`）；Doris 指标表均有数据（`REQ-F-RT-02`）。
- Flink UI 可见 checkpoint 成功记录（`REQ-F-RT-05`）；Doris 表持续增长，重启后无重复无丢失（`REQ-F-RT-06`）。
- DLQ topic 有消息且内容可读（`REQ-F-RT-07`）；并行度 ≤4（`REQ-F-RT-08`）。
- **GDD 新增判据（替代原「配置可见」）**：
  - **迟到通道可证**：设 `--late-correctable 0.03` 后，Doris 侧出现 `revision > 0` 的窗口；设 `--late-expired 0.02` 后，迟到计数表出现对应条目。**不能**用「Flink UI 里能看到」当判据。
  - **重复与脏可分可证**：故障恢复重放期间，**脏率不得上升**，重复计数上升（D2 见 §D）。原型已实测：若把重复计为脏，重放 5 条合法事件会把脏率从 0% 推到 50%（真脏 0 条）。
  - **漏斗单调且诚实**：`funnel_cnt_inferred` 沿 `step_no` 1→4 **非增**，违反即 fail；`funnel_cnt_observed` **不设**单调断言（`--chaos-ratio` 下跳步合法）。
  - **漏斗行修正语义**：`purchase` 迟到 **≤120 s** 落入已落定窗口 ⇒ 该分钟漏斗客数被修正、`ads_funnel_step.revision` **+1**（`revision` 语义与 `ads_window_metric` 同构）；**超期（>120 s）只计数、绝不修正**。依据：TTL 32 min ＝ 30 min 会话 ＋ 2 min 允许延迟。**漏斗唯一键不含 revision**：维持 `(session_date, step_no, window_start)`，`revision` 是值列。
  - **UV 对账（精确性）**：实时滚动 UV 由 Flink 侧**精确**去重（近似降级为可选实验）；**对账断言条文**——P2 起 `lake_assert.sh` 逐 `metric_name` 比对 Paimon（离线口径基准）与 Doris（看板供数点）两套 `ads_window_metric`，**只比 `is_settled=1`**，逐值严格相等；**P1 阶段显式 SKIP**（场景前置标记，**不是永久豁免**）。切窗口径：`window_start = floor(event_time_ms/60000)×60000`（UTC epoch 对齐），UV 按 1 h epoch 对齐同理。

### 1.4 模块④ 湖仓分层

**触发条件**：离线分层由 **Makefile 触发**（无调度器）。`make replay` 跑完整五层。

**流程**
1. 【已确认 Q25】**ODS 只落原样字节**：`payload` 原文 + `kafka_partition` / `offset` / `timestamp` 元数据。**一切解析、类型归属、时间单位转换都在 DWD 完成**。
   理由：`REQ-F-LAKE-01` 要求「不做有损加工」；秒→毫秒转换一旦在 ODS 发生便不可回滚，重放会拿到被改过的数据。且同类项目的坑正是「schema 变更炸历史分区」——把解析推迟到 DWD 可让 ODS 完全免疫 schema 演进。
2. DWD：清洗 + 规范化 + 统一 UTC epoch 毫秒 + 超集 schema；**主键表**（`event_id`）。
   - **对 `schema_version` 的义务**（铁律全文见 `canonical-truth.md` §3.1）：同 major 更高 minor 的**未知可选字段丢弃并计数**；**更高 major 拒收** ⇒ 落 `dwd_behavior_event_reject`，原因码 `schema_major_unsupported`。
3. DIM：**时态维表**（商品/品类/用户/地域/渠道），主键表 + 拉链区间。【已确认 Q26】由生成器的维度变更流喂给。
4. DWS：主题宽表聚合；ADS：应用层指标。
5. 五层全部落 **单一 Paimon catalog**，该 catalog 下的 **`ecom_lake` database** 承载全部五层表（层级 catalog→database→table，**catalog 与 database 不可混说**）。
6. 表维护：快照保留与 compaction（参数见 §A）。

**状态与数据变化**
- 分层作业态（per-layer）：`pending → running → failed/published`。
- 【已确认 Q24】**离线三通道与实时严格同构**：

| 通道 | 实时落点 | 离线落点 |
| --- | --- | --- |
| 脏（①②违规） | DLQ topic | `dwd_behavior_event_reject`（含 `reject_reason`、`raw_payload`、计数） |
| 迟到（合法但错过窗口） | 允许延迟内→窗口修正；超期→计数 | **不单独分流**：落 DWD 主表并加 `is_late` + `late_lag_ms` 标记列，供对账区分「少算」与「本来没这条」 |
| 重复 | 独立重复通道计数 | 靠 DWD `event_id` 主键表**天然吸收**（与 Q6(c) 同一机制） |

**验收判据**
- ODS 行数 ≥ 摄入事件数；`payload` 列非空（`REQ-F-LAKE-01`）。
- 查到 DWD 行数 > 0；`event_time_ms` 为毫秒整数（`REQ-F-LAKE-02`）。
- 五层表均存在于同一 catalog（`REQ-F-LAKE-06`）；`make replay` 可跑完五层（`REQ-F-LAKE-07`）。
- **GDD 升级判据（替代原「表属性配置可见、快照数受控」的空泛说法，`REQ-F-LAKE-08`）**：
  - ⚠️ **2026-10-19 修订两处**：①`snapshot.num-retained.min` 是**下限**（快照数**不会低于** 5），实际数量由 `snapshot.time-retained`（1 h）与 `num-retained.max` 决定 ⇒ **「快照数 = 5」是错误预期**，判据须改为「下限 5 生效 + 实际数量受控」；②官方支持系统表的引擎列表（Flink/Spark/Trino/**StarRocks**）**不含 Doris** ⇒ 该查询**必须在 Flink SQL 侧执行**，不能写进 Doris。
  - **快照窗 [5, 20]**：`snapshot.num-retained.min = 5`（**显式**）＋ `snapshot.num-retained.max = 20`（把「受控」变成**有限数**）＋ `snapshot.time-retained` 保持 1 h 默认；判据 SQL＝`SELECT count(*) FROM <table>$snapshots` 落 **[5, 20]**（官方原话：「The `snapshots` table lists retained snapshots and their commit metadata」）；`replay` 末尾 `sys.expire_snapshots(retain_max => 20)` 只收**上界**（`retain_max` 是上界，不是「钉住」）。
  - 「有数据」也须用 `COUNT(*)` 判，**不得**用 `total_record_count` —— 官方明确后者是「unmerged counts calculated from data files, **not logical row counts**」（这是个容易算错数的坑）。
  - schema 演进可查：`SELECT * FROM <table>$schemas`（供 F5 演练用）。
- **GDD 升级判据（原判据空表也能通过）**：
  - DIM 判据从「可 join 出结果」升级为「**同一 `item_id` 在不同 `event_time` 下 join 出不同属性值，且拉链区间无重叠无空洞**」（`REQ-F-LAKE-03` 的强化）。
  - 拒绝表判据为「违规样本被正确分流**且有计数**」，且**必须能区分三类原因**（结构/取值、时间范围、重复）。
  - **不变量替换为 A1/A2/A3**（原「`ODS 行数 = DWD 行数 + 拒绝表行数 + 迟到标记差异`」**删除**——「迟到标记差异」不是可加项，写不出精确等式）：
    | # | 断言 | 判据 |
    | --- | --- | --- |
    | **A1 分流完备** | `count(DISTINCT event_id)` 在 ODS / DWD / 拒绝表三者闭合 | `ods_distinct = dwd_distinct + reject_distinct`，**全局比对**（ODS 无事件日分区，降为全局粒度，可判性不变） |
    | **A2 重复记账** | `count(*) − count(DISTINCT event_id)` 记为重复率 | **只入日志、不作断言**（承认恒真；它是「为什么不能可加」的实例） |
    | **A3 迟到对账** | `count(*) FROM dwd WHERE is_late=1` ＝ `sum(cnt) FROM dwd_late_event_count` | 「只存计数」的唯一用途 |
  - **主键表写入纪律**：DWD/DIM/DWS/ADS 全部设 `table.exec.sink.upsert-materialize = NONE`（写进 `docker/flink/conf/config.yaml`，P1/P2 共用）；**不设** `sequence.field`——靠重放确定性，同 `event_id` 重复行该值相同 ⇒ 回落内部序，白设。

### 1.5 模块⑤ OLAP

**触发条件**：看板查询触发读；实时作业触发写。

**流程**
- 看板直连库 `ecom_ads`。
- 【已确认 Q23】表模型**保留 UNIQUE KEY 模型**（Merge-on-Write），写入幂等；滚动 UV 由上游算好以整数写入。
- 【已确认 Q27·2026-10-21 订正】分区保留**必须显式配置**，且**真正的前置条件是表级四项**：`dynamic_partition.enable=true`、**显式** `start=-30`（官方默认 **-2147483648**＝「历史分区不删除」⇒ 照默认建表**永不删除**，与需求**恰好相反**）、`time_unit`＋`end`＋`prefix`、`create_history_partition=true`。⚠️ **FE 级总开关 `dynamic_partition_enable` 不是前置条件**：4.1.5 源码默认 **`true`**（**不是** `false`）⇒ 原「第三个前置条件」**作废**（详见 §D D8）。
- 覆盖写修正依赖 same-key upsert。**`partial_columns=true` 不是表属性**——它是**导入 header / `sink.properties.*`**（`INSERT INTO` 路径走会话变量 `enable_unique_key_partial_update`），建表时写它无效；`INSERT INTO` 默认仍是**整行 UPSERT**。普通 `partial_columns` 受 **Batch constraint** 限制（**一批内只能更新同一组列**）；同批多列组须用 **3.1.0+ 灵活列更新**（JSON ＋ `skip_bitmap_column`，**禁用 `sequence_col`**）。依据与逐字见 `spec.md` §7.2 #18（冲突 O，事实类）。

**状态与数据变化**：窗口行 `写入 → 暂定值 → 落定 → (被修正，revision+1)`。

**验收判据**
- Superset 中 `ecom_ads` 连接可用（`REQ-F-OLAP-01`）；同批数据写两遍结果一致（`REQ-F-OLAP-02`）。
- **GDD 新增判据**：分区保留**行为**可证——建表后 `SHOW PARTITIONS` 显示历史分区被**实际删除**，而不是仅「配置项存在」。
- 【已确认】`REQ-F-OLAP-04`（Paimon Catalog 直读）保持 COULD：同类项目那条路**不可验证**（其仓内唯一的 catalog 文件是 Flink 侧的 HMS catalog，不是 Doris 的 `CREATE CATALOG`；README 仅有截图），且官方依赖 Paimon 1.4.2 与锚定的 2.1.0 有落差。实验成败均不阻塞验收。

### 1.6 模块⑥ 看板

**触发条件**：评审者打开本机浏览器（**零账号**）；面板按 ≤10 s 自动刷新。

**流程**
- 面板：实时 PV / UV、4 步转化漏斗（**步进式**）、实时 GMV、品类分布、地域分布；【SHOULD】脏数据率。
- 地域与品类**不依赖 GIS/底图**：省份**横条排行**（ECharts Bar 横向，`orientation`）＋ 品类 treemap。⚠️ **方格地图条目已否决、改为横条排行**：`deck_geojson` 需 GeoJSON 列 ＋ 默认 OSM 底图，**违背「不依赖 GIS/底图」**；`REQ-F-BI-01` 只要求「地域分布」、不指定形式。
- **实时感**：数值跳动 + 相对基线 delta + sparkline + 阈值带，再加 **as-of / 已修订指示**。
- 【已确认 Q13】必须**视觉区分暂定值与落定值**（窗口关闭前是暂定），否则是「假实时」。
- **三态载体（已签）**：**B 单表行内状态为主** ＋ **A 式补充**——
  - **B（主载体）**：Table ＋ 自定义 SQL `CASE WHEN is_settled=0 THEN '暂定' WHEN revision>0 THEN '已修订' ELSE '落定' END` ＋ 条件格式。三态、`revision`、as-of 同行可见，零插件。
  - **A（补充）**：当前窗口「暂定」Big Number 与落定值并排（跳动的那个数是暂定值）。
  - **C 单图双色叠加不实现**：「修订描边」需 DOM 挂钩，CSP `script-src: 'self' 'strict-dynamic'` 禁自定义 JS；**留原型证据**。
- 【已确认】刷新下限：Superset **没有**「不得小于 10 秒」的硬下限（`SUPERSET_DASHBOARD_PERIODICAL_REFRESH_LIMIT = 0` 仅警告阈值，自定义下限 1 s），故 ≤10 s 可实现。

**状态与数据变化**：面板值 `暂定 → 落定 → 已修订`；脏率分子只含**真脏**，重复不进分子。

**验收判据**
- 5 个面板均可渲染出数据（`REQ-F-BI-01`）；刷新间隔 ≤10 s 且**观察到数值变化**（`REQ-F-BI-02`，SRS §4.3 的 GIF 证据即此）。
- 无任何外部平台账号即可打开并看到刷新（`REQ-F-BI-03`）。
- **GDD 新增判据**：脏率面板在**故障演练期间不得飙升**（这是 Q11 的直接后果，原型已实证）。
- **GDD 新增判据**：漏斗面板须同时可见/可切换**观测值与推断值**，且图例标明哪个是补记。

### 1.7 模块⑦ 编排

**触发条件**：`docker compose up`；Makefile 目标按需触发。

**流程**
- 【**2026-10-19 按一手调研修订** G2】**Doris 有官方 compose 与官方 all-in-one 镜像**（`docker/runtime/all-in-one/4.1/compose/{multi-node,cloud}.yml`；镜像 `apache/doris:all-in-one-<ver>` ＝ 1 FE + 1 BE，<20 s healthy，自带 HEALTHCHECK，`replicas` 强制 1 故建表免 `replication_num`，持久化挂 `/opt/apache-doris/fe/doris-meta` + `/be/storage`）→ **自写量降为「裁剪 + 加 `mem_limit`/端口绑定」**，不再需要自行承担 FE/BE 注册。
  ⚠️ **未变的一条**：官方明确 Docker 部署**仅用于本地开发与测试、容器销毁即丢数据** ⇒ **named volume 仍是必需而非优化**。
  ★ **新增硬约束（SRS/FSR/GDD 原文均无）**：Docker Desktop 下**容器地址宿主不可路由**，而 Doris 的 Stream Load 是「FE 收标 → **重定向到某个 BE**」的**两跳**，第二跳是容器地址 ⇒ 官方原话「a stream load **from the host cannot follow FE's redirect to a BE; run it from the `client` service instead**, which is inside the network」。**冲击 `REQ-F-RT-06` 的验证落点与 `make verify`**：须在 compose 网内声明一个按需拉起的 `client` 服务（持 `mysql`/`curl`）承担导入与 Doris 断言。**Flink-Doris-Connector 的写入不受影响**（TaskManager 本就在网内）。
  ★ **P0 实测补充（2026-10-19）——镜像可得性修正**：实测官方镜像站**没有可直接拉取的 Doris 4.1.5**（`apache/doris:4.1.5`、`all-in-one-4.1.5`、`base-4.1.5` 全部 `denied`），只有 **`apache/doris:base-latest`**（1.96 GB）可拉，而它是**构建基座**——实测 `/opt/apache-doris` 存在但 **`fe/bin` 为空、容器内无 `java`**，**不含可运行的 FE/BE**。⇒ 上面「all-in-one 镜像可直接用」只在**官方仓库存在**的意义上成立；**落到本机必须本地构建**（与官方 `docker/runtime` 用二进制包自建镜像的做法一致）。P0 骨架因此先以 Kafka+Flink+Superset 验证，Doris 延后到能下载二进制包时。
  ★ **另一条 P0 实测**：`registry-1.docker.io` **不可达**（`docker manifest inspect` 报 `Client.Timeout`）⇒ 必须走中国镜像站；compose 已参数化 `${REGISTRY_MIRROR}`（默认 `docker.m.daocloud.io`），实测 Kafka/Flink/Superset **拉取成功**。
- 【已确认 G3】Superset 复用官方 `docker/` 骨架（compose + entrypoint + 初始化脚本），但剥离其演示用示例数据。
  **初始化的确切顺序**（官方 `docker-init.sh` 实证）：① `superset db upgrade` → ② `superset fab create-admin` → ③ `superset init`（建默认角色与权限）→ ④ `superset load_examples`（**可选，本项目应关闭**）。密钥由环境变量 `SUPERSET_SECRET_KEY` 提供（官方变量名）。
  看板交付形式：`superset import-dashboards -p <.zip>`（产物由 `superset export-dashboards` 生成；zip 内含 `metadata.yaml`、`dashboards/*.yaml`、`charts/*.yaml`、`datasets/*.yaml` 等）。⚠️ 该 CLI 的官方原话**只在源码 docstring**（「Export dashboards to ZIP file」），官方文档页未见——故文档引用时注明「源码级证据」。
  **zip 定位**：zip 保留为 `make init` 的**复现手段**（手段归本节 §1.7）；**判据归 SRS**（浏览器能看能刷，`REQ-F-BI-01..03`）——手段与判据不混说。
  ⚠️ **新硬约束（源码级，`database_uuid`）**：dashboard importer 逐字 `config["database_id"] = database_ids[config["database_uuid"]]` ⇒ **dataset 经 `database_uuid` 匹配目标实例的 database**。`make init` 建 `ecom_ads` 连接时**必须固定/回填该 UUID**，否则导入后全部图表断链。
- 逐服务显式 `mem_limit`；`restart: unless-stopped`；端口仅绑 `127.0.0.1`；日志轮转 10 MB × 3。
  - ⚠️ **2026-10-19 补**：`mem_limit` **管不到 swap**（官方：只设 `memory` 不设 `memswap_limit` 时容器可借**等量** swap）⇒ **每个服务同时写 `memswap_limit` ＝ `mem_limit`**，否则 `REQ-F-ORCH-02` 有渗漏点。
  - ⚠️ **不与** `deploy.resources.limits.memory` **并存**（官方：`deploy` 是可选段、未实现即忽略；并存时要求二者一致）。
  - ⚠️ 「逐服务不允许缺省」须由 `make verify` **脚本断言**：`docker compose config` 能**列出** `mem_limit`（实测会把 `128m` 正常化为字节值）但**不报缺**。
  - ⚠️ 体积/冷启动记账用 **`docker image inspect`**，**不用 `docker images`**（本机 `UseContainerdSnapshotter=true`，官方警告此时读数是**压缩 blob 叠加**、显著虚高）。
- named volume 持久化 Kafka/Paimon/Doris/Superset 元数据——【已确认 G2】在 Doris 这一侧这是**必需而非优化**，否则 `down` 即丢数据、直接违反 `REQ-NF-REL-02`。
- Makefile 目标：`up` / `down` / `init` / `replay` / `verify` / `spike` / `verify-fault` / `logs`；**FC-1（2026-10-21 已批）新增 `lint` / `typecheck`**——静态检查与运行时烟测分开，`verify` 保持冻结、只做运行时烟测，`make check` **不新增**（原生多目标并列）。

**状态与数据变化**：服务态 `created → running → (restarting) → exited`；卷在 `down` 后保留、在 `down -v` 后销毁。

**验收判据**
- 干净机器一条命令起链路（`REQ-F-ORCH-01`）；`docker stats` 总值 ≤8 GB（`REQ-F-ORCH-02`）；各 Makefile 目标可执行成功（`REQ-F-ORCH-03`）；重启后自动恢复（`REQ-F-ORCH-04`）；`down` 再 `up` 数据仍在（`REQ-F-ORCH-05`）；局域网不可访问（`REQ-F-ORCH-06`）；日志驱动配置可见（`REQ-F-ORCH-07`）。
- **GDD 新增判据**：`docker compose config` 能列出**每个**服务的 `mem_limit`，且总和 ≤8 GB——「逐服务显式」意味着不允许有服务缺省。

### 1.8 模块⑧ 可复现校验

**触发条件**：`make verify`（烟测）、`make verify-fault`（故障注入）、lag 检查脚本。

**流程**：见 §C 失败路径登记表的「检测信号」列。

**状态与数据变化**：无自身状态；产出断言结果与退出码。

**验收判据**
- **`make verify` 判定分层（已签，进码语义）**：**P1 ＝ 4 真断言 ＋ 1 显式 SKIP**（第三项「Paimon 五层有行」SKIP，**不计通过、不计 fail**）；**P2 ＝ 5 真断言**（五项全真）。**五项的确切断言形式**（据官方接口，避免「配置可见」式空判据）：

| 烟测项 | 可执行断言 | 出处/接口 |
| --- | --- | --- |
| Kafka 有数据 | 消费组 lag 查询返回非空；或 `kafka-topics --describe` 的 `retention.ms`/`retention.bytes` 与 §A 一致 | 官方 CLI |
| Flink job RUNNING | `GET /jobs/overview` 的 `jobs[].state == "RUNNING"`（**字段名确切为 `state`**） | [REST API 2.2](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/ops/rest_api/) |
| Paimon 五层有行 | 逐层 `SELECT COUNT(*) FROM <table>`（**不用** `total_record_count`） | [system-tables](https://paimon.apache.org/docs/master/concepts/system-tables/) |
| Doris ADS 有行 | `SELECT COUNT(*) FROM ecom_ads.<ads_table>`；FE 健康 `GET :8030/api/health` 返回 `online_backend_num == total_backend_num` | [FE health-action](https://doris.apache.org/docs/3.x/admin-manual/open-api/fe-http/health-action/) |
| Superset 健康 | `GET /health` 返回 **200 且响应体为纯文本 `OK`**（⚠️ **不是 JSON**，别按 JSON 解析） | [health.py](https://github.com/apache/superset/blob/master/superset/views/health.py) |

- **场景依赖判据**带「场景前置」标记：`revision>0` 出现、UV 对账等**仅在对应注入器/离线场景激活时判定**，否则 SKIP——**不是永久豁免**。
- `REQ-F-VERIFY-02` 幂等**不进 `verify` 每次运行**（全量重放两遍是重操作）：P2 起由**离线幂等断言**（同批 dump 重放两遍，DWD `count(*)` 与逐列校验和 `SUM(crc32(concat_ws('|', event_id, is_late, late_lag_ms)))` 两次一致）＋ `verify-fault` **恢复重放**共同覆盖。
- `REQ-F-VERIFY-03` **`verify-fault` 全参数化判据**（两处超时均为**设计值**）：
  1. **输入冻结**（停生成器或固定 dump 回放）→ 等 lag==0；
  2. **快照基线**：`ads_window_metric` 与 `ads_funnel_step` **两表** `count`+`sum`、DLQ 计数、Kafka committed offset；
  3. **kill TM**；
  4. 轮询 `GET /jobs/overview` 到 `RUNNING`，超时 **120 s** → **超时即 fail**；
  5. 等 lag==0 **且此后完成一次 checkpoint**，超时 **300 s**；
  6. 比对两表 `count`/`sum` 与基线**全等**、**重复通道计数上升**（证明确有重放且被幂等吸收）。
  **前提**：`execution.checkpointing.externalized-checkpoint-retention` 必须**显式设为 `RETAIN_ON_CANCELLATION`/`RETAIN_ON_FAILURE`**（官方默认 `NO_EXTERNALIZED_CHECKPOINTS`，默认值下**无法**从指定点恢复）。
- **GDD 新增判据（把「配置可见」升级为「行为可证」）**：
  - `verify-fault` 期间**脏率不得上升**（重复与脏必须可分）。
  - UV 对账断言＝「实时 == 离线精确」，**只比 `is_settled=1`**、逐值严格相等（**P1 显式 SKIP**，见 §1.3）。
  - **反例保护**：若 kill 后条数一致但**脏率飙升**，判为**不通过**——那说明重复被误当脏（D2）。

### 1.9 加强项 A · 数据质量校验（SHOULD）

**触发条件**：每条事件进入处置流水线时；离线在 DWD 清洗阶段。
**流程**：按质量规则①②③④⑤校验；实时进 DLQ，离线进拒绝表并计数。
**状态与数据变化**：违规样本从主流分流；计数累加。
**验收判据**：违规样本被正确分流且有计数（`REQ-F-VERIFY-04`）；**GDD 新增**：三类原因可区分（结构/取值、时间范围、重复）。
> ⚠️ **规则①的处置见 §D D4**（绝对年份下界使公开数据集回放 100% 被判脏）；【按推荐】改为**相对下界**。

### 1.10 加强项 B · Kafka lag 检查（SHOULD）

**触发条件**：脚本显式调用；或作为 `make verify` 的附带断言自动运行。
**流程**
1. 枚举主 topic 与 DLQ 的消费组。
2. 对每组取各分区 `log-end-offset` 与 `current-offset`，差值即 lag。
3. 输出每组、每分区的 lag 与总量；超阈值时退出码非 0。
**状态与数据变化**：无自身状态；只读消费组元数据。lag 本身是一个随消费推进而变化的观测量——**它是「链路是否活着」的先行指标**（比看板更早暴露停滞）。
**验收判据**：脚本可输出各消费组 lag（`REQ-F-VERIFY-05`）；**GDD 新增**：lag 超阈值时退出码非 0（否则脚本只能看、不能判，无法用于 CI 或烟测）。

### 1.11 加强项 C · 可选 profile（SHOULD）

> ⚠️ **本节随 `REQ-F-ORCH-08` 撤需求而失效（2026-10-25 签·spec §7.1 #40）**：minio/spark 两个 profile 均不实现。以下原文保留作决策档案。

**触发条件**：`docker compose --profile minio up`（S3 协议演示）；`--profile spark up`（离线对照，用完即停）。
**流程**
1. 不启用 profile 时，默认链路**完全不受影响**（湖存储走本地 `file://`）。
2. 启用 `minio` 时额外拉起 MinIO（约 +0.4 GB），Paimon catalog 的 warehouse 由 `file://` 切到 `s3://`。
3. 启用 `spark` 时额外拉起 Spark 作离线对照，**用完即停**（不常驻）。
**状态与数据变化**：profile 开关改变**容器集合**与 **Paimon warehouse 指向**；同一份数据在两种 warehouse 下不自动迁移（切换 profile 后需重新写入或用 `dump` 重放）。
**验收判据**：不启用 profile 时不影响默认链路（`REQ-F-ORCH-08`）；**GDD 新增**：启用 `minio` 后 `docker stats` 总值仍 ≤8 GB（即 profile 不得击穿**天花板**）。
> 【已确认 G4】备选降级手段：Kafka 有 GraalVM 原生镜像 `apache/kafka-native:4.3.1`，8 GB 预算吃紧时可替换（R3 降级序列里 SRS 未列此项）。

---

## A. 参数总表

> 全部为**设计目标**（`SRS.md` A2：官方未给出各组件单机最低内存，故**天花板**预算是设计值而非官方保证）。实现后须**实测更新**，不得当官方保证。

### A.1 时间与窗口

| 参数 | 值 | 说明 |
| --- | --- | --- |
| 主窗口 | **60 s 滚动** | 窗口语义 |
| watermark 乱序容忍 | **5 s** | 窗口的 8% |
| 允许延迟 | **120 s**（2 分钟） | 由 Q10 修正策略决定 |
| 空闲分区超时 | **30 s** | ⚠️ 落 **`table.exec.source.idle-timeout`（全局）**——`scan.watermark.idle-timeout` 在 connector 5.0.0 **未暴露为 Kafka 表选项**（core planner 支持、connector 未实现） |
| 窗口内提前发射周期 | **10 s** | 满足 `REQ-F-BI-02`；受 B4 约束 |
| 会话时长 | **30 分钟** | 漏斗键控状态 |
| 漏斗状态 TTL 清理 | **32 分钟** | 30 + 2 分钟允许延迟 |
| `event_id` 去重状态保留 | **1 小时** | 约 68.7 MB @200 ev/s |
| 滚动 UV 窗口 | **1 小时** | Flink 侧精确去重 |
| 看板刷新 | **10 s** | Superset 无硬下限（B7） |
| 端到端延迟目标 | **≤60 s** | 设计目标 |
| 快照保留（Paimon） | `snapshot.num-retained.min = **5**` | ⚠️ 官方默认 **10**，须显式设 5（D2）。**且这是下限不是目标值**——实际数量由 `snapshot.time-retained`(1 h) 决定 |
| Flink JM/TM 内存 | `jobmanager.memory.process.size` / `taskmanager.memory.process.size` **必须显式配** | ⚠️ **官方这两个键无默认值**，原话「One of the three mentioned ways has to be used to configure Flink's memory …, or the **Flink startup will fail**」。分项默认（managed.fraction 0.4、network.fraction 0.1、metaspace 256m、jvm-overhead 0.1）为官方默认 |
| `snapshot.time-retained` | 1 h（官方默认） | 保持默认 |
| compaction | `compaction.min.file-num = 5`、`full-compaction.delta-commits = 1` | **保持官方默认**；不照抄其它为治小文件调成 10 的项目 |

### A.2 容量与分区

| 参数 | 值 | 说明 |
| --- | --- | --- |
| Kafka 分区数 | **6** | ≥ 并行度上限 4 留余量 |
| Kafka 分区键 | **`user_id`** | 保每用户有序 |
| DLQ 分区数 / 键 | 6 / `user_id` | 便于定位哪个用户的数据出问题 |
| Kafka RF | **1** | 单机 KRaft |
| Kafka retention | **`retention.ms=604800000`（7 天）或 `retention.bytes=2147483648`（2 GB）取小** | ⚠️ 官方原话「A segment will be deleted whenever **either** of these criteria are met」——即**任一先满足即删**。实测**约 6 小时**触发 2 GB（D3，≈8.05 GB/天）。两键默认值分别为 7 天与 `-1`（无上限），**2 GB 须显式设** |
| Flink 并行度 | **≤4** | |
| checkpoint 间隔 / 最小间隔 / 超时 | **30 s / 10 s / 10 min** | 官方默认：`interval`=(none)、`min-pause`=0 ms、`timeout`=10 min（超时用默认） |
| checkpoint 语义 | `execution.checkpointing.mode=EXACTLY_ONCE` | 官方默认即 EXACTLY_ONCE；另一合法值 `AT_LEAST_ONCE` |
| 状态后端 | `state.backend.type=**rocksdb**` | ⚠️ 合法取值**只有 `hashmap` / `rocksdb` / `forst`**，**没有 `embedded`**（官方原话：「Recognized shortcut names are 'hashmap', 'rocksdb' and 'forst'」）。默认是 `hashmap`，须显式改 |
| 增量 checkpoint | `execution.checkpointing.incremental=true` | ⚠️ **2.2 的确切键名是这个**；旧的 `state.backend.incremental` 已是**废弃别名**（源码 `withDeprecatedKeys("state.backend.incremental")`）。默认 `false`，须显式开 |
| externalized retention | `execution.checkpointing.externalized-checkpoint-retention=**RETAIN_ON_CANCELLATION`**（或 `RETAIN_ON_FAILURE`） | 官方默认 `NO_EXTERNALIZED_CHECKPOINTS` —— **默认值下 verify-fault 无法从指定点恢复**，须显式改。这是 `REQ-F-RT-05` + `REQ-F-VERIFY-03` 的前提 |
| 用户池规模 | **10 万**（可配置） | UV 与漏斗状态量基数 |
| 生成速率 | 默认 **200 ev/s**；尖峰 **≥1000 ev/s** | 原型已实测 201.6 / 1001.6 |
| 容器常驻内存 | **≤8 GB**（逐服务 `mem_limit`） | **天花板**。（【2026-10-19 偏离登记】按官方**出厂默认**参数此预算**不成立**：Doris FE `fe.conf` 默认 `-Xmx8192m` ＝ 8.0 GiB ＝ 整个预算；BE `mem_limit` 默认 **90%** ⇒ 天花板下 ≈10.5 GB；官方 dev/test 口径 **FE 8 GB + BE 16 GB**。缺口 ≈5 GB 全部落在 Doris。**处置**：`-Xmx` 与 `mem_limit` **必须显式下调**（如 1536m / 25%），预算须 P0 实测回填。详见 `HLD.md` §8-R1 与 §7.1 D-B） |
| 磁盘占用 | **≤20 GB** | §D 已验算：合计 7–11 GB 成立。（【2026-10-19 偏离登记】官方 dev/test 磁盘口径 **FE 10 GB+ / BE 50 GB+**，与本预算**正面冲突** ⇒ 20 GB 是**低于官方口径的设计值**；容器数据与镜像层**已在 F:**（实测 `CustomWslDistroDir=F:\Docker Desktop\DockerDesktopWSL`），但 `UV_CACHE_DIR` 未设 ⇒ **Python 依赖默认落 C 盘**（C: 仅剩 9.6 GB），须显式改向。详见 `HLD.md` §4.5 与 §8-R5） |

### A.3 运行时与版本

| 参数 | 值 | 说明 |
| --- | --- | --- |
| **JDK 基线** | **17** | connector README 版本表：Flink 2.0–2.3 → JDK 17（2.1+ 的 JDK 21 为 experimental）；Flink 2.x artifact 以 `--release 17` 编译。**SRS/FSR 均未记载**。⚠️ **2026-10-19 补**：`start_fe.sh` 源码逐字要求 **恰为 17**（非 17 打印 `ERROR: The jdk_version is N, must be 17.` 后 `exit 1`）；而**本机实测无 JDK 17**（三处常见目录全无；宿主 PATH 是 Oracle **18.0.2**、`JAVA_HOME=E:\space\09-tools\Jdk1.8`；WSL 是 **11.0.32.1**）⇒ **P0 确定性阻塞项**，须在 WSL2 内装 JDK 17 |
| Flink | 2.2.x | 不得用 2.3.0（Kafka connector 断链） |
| 空闲超时配置项 | `table.exec.source.idle-timeout` | 见 A.1 注 |
| 注入器 | `--late-correctable 0.03`、`--late-expired 0.02`、`--chaos-ratio 0`、`--seed` | 相对**水印**计算 |
| **Kafka connector 完整坐标** | `org.apache.flink:flink-connector-kafka:**5.0.0-2.2`** | ⚠️ **2026-10-19 补**：Maven metadata 实测 `<release>5.0.0-2.2</release>`。FSR/SRS 记的裸「5.0.0」**不完整**——`-2.2` 是 Flink 线后缀，施工必须写全 |
| 运行面锚点（工具链） | `uv`（已装 **0.12.19**，需加 PATH）｜Python **3.12**（uv 托管）｜`Ruff`（lint+format，pin）｜**`basedpyright` 1.40.2**（类型门禁，pin；自带 node）｜`confluent-kafka`｜`pydantic v2`｜**Faker pin 补丁号** | 2026-10-19 由 HLD §3 定案；`ty` 因诊断可任意版本间变更被否 |

---

## B. 状态机图集

### B.1 生成器运行态
```
idle ──启动──> warmup ──首条写入──> live ⇄ spike（尖峰模式开关）
                                      │
                              Ctrl-C/结束 ──> draining ──flush 完成──> stopped
```
非法转移：`stopped → live`（须重新启动）；`warmup → spike`（须先 live）。

### B.2 Flink 作业生命周期
```
created ──提交──> running ⇄ restarting（故障/TM 丢失）
                    │                │
                    │        超过重启上限
                    │                ▼
                    └──> finished   failing ──> suspended
```
**恢复判据**：从最近一次**成功的** checkpoint 恢复；externalized retention 保证恢复点可选。

### B.3 checkpoint 与 2PC 事务态
```
事务：pre-committed ──commit──> committed
              └──abort──> aborted
checkpoint 失败后：pre-committed 事务进入 aborted，作业重启
```
**本项目依赖它保证什么**：**可见性/原子性**。
**⚠️ 与 unique key 的分层**（见 §D D1 缺陷记录）：label 去重管**事务级重放**、unique key upsert 管**行级重放**。在 UNIQUE MoW 表上后者已把重放变成幂等 no-op，故 2PC 是**一层主防 + 一层兜底**里的兜底，**不是**与幂等并列的第二道独立防线。

### B.4 单事件处置态（核心状态机）
```
事件到达
   │
   ├─① 结构与取值 ──违规──> dirty ──> DLQ topic（离线：拒绝表 + 计数）
   │        │通过
   ├─② 时间范围  ──违规──> dirty ──> 同上
   │        │通过
   ├─③ 唯一性    ──重复──> duplicate（独立计数，不进 DLQ、不计脏率）
   │        │首次/超期
   └─④ 迟到
            ├─窗口未关闭 ──> admitted（进累加器）
            ├─已关闭且在允许延迟内 ──> corrected（覆盖写修正，revision+1）
            └─超出允许延迟 ──> late_counted（只计数，绝不修正）
```
**顺序冻结**：①②③④；**脏优先于迟到**（脏事件的值不可信，修正它只会污染指标）。
**原型**：此状态机已实现并实跑验证 → [event-routing.prototype.html](prototypes/event-routing.prototype.html)。实跑结论：脏优先 ✅；允许延迟内→修正且 `revisions` 递增 ✅；超期→只计数不修正 ✅；**「重复计脏」时合法重放把脏率从 0% 推到 50%（真脏 0 条）** ✅。

### B.5 步进漏斗状态机
```
用户首个事件 → step=1(浏览)
   │步进
   ├─正常顺序 ──> advance（记下一步）
   ├─跳步/乱序 ──> autofill（补记中间步）| reject（拒绝）| asis（只记自身）
   ├─重复步 ────> idempotent（只记首次）| countall（每次都记）
   └─会话超时(30min) ──> 开新会话，漏斗重开
```
**冻结为 `autofill` + 双列并陈**（`observed` / `inferred`）。
**原型**：三策略差异已实跑 → [funnel.prototype.html](prototypes/funnel.prototype.html)。实跑结论：`autofill` 永不倒挂但含**推断值**；`reject` **系统性低估**；`asis` **产生倒挂**（第 3 步 > 第 2 步 → 看板会出现 >100% 转化率）。
**原型抓到的真 bug（已修）**：步进水位原先**无条件抬升**，导致 `reject` 策略下被拒步骤也抬高水位 → 该用户此后任何更低步都判「跳步」继续拒绝 → **永久锁死**。修复为「只在确实推进时抬升」。

### B.6 离线分层作业态
```
pending ──> running ──> published
              │
          失败 ──> failed ──重跑──> running
```
每层独立；层间由 Makefile 依赖关系定序（**无调度器**）。

---

## C. 失败路径登记表

| # | 失败模式 | 检测信号 | 自动恢复动作 | 人工兜底 | 验收判据 |
| --- | --- | --- | --- | --- | --- |
| F1 | TaskManager 被 kill / 丢失 | Flink UI 作业态 `RESTARTING`；REST `/jobs/overview` 的 `state` | 从最近**成功** checkpoint 恢复；2PC pre-committed 事务 abort | `make verify-fault` 手动触发 | 恢复后 Doris 条数与 kill 前一致（`REQ-F-VERIFY-03`）；**且脏率未上升** |
| F2 | 重复事件涌入（故障重放） | 重复通道计数上升 | 去重状态 1 小时内识别；行级重复由 unique key 幂等吸收 | 无需 | 重复**不进 DLQ、不计脏率**；脏率面板不飙升（原型实证） |
| F3 | 迟到超期事件 | 迟到计数表增长 | 只计数，绝不修正 | 按需重跑离线补数（见 `is_late` 标记列，不算**窗口修正**） | 迟到计数与 `--late-expired` 注入量对应 |
| F4 | 脏数据持续涌入 | DLQ topic 增长；脏率面板上升 | 计数并留痕 | 查 DLQ 内容定位原因 | 三类原因可区分；**丢弃有痕迹** |
| F5 | Schema 变更导致恢复失败 | 从 checkpoint 恢复时 `IllegalStateException`（同类项目实证的坑） | 无自动恢复 | 预留 `ext_field` 列；或从 ODS 重放 | **GDD 新增**：schema 演进演练可跑通 |
| **F14** | **★ P0 实测新增** 配置格式与镜像内容不符预期 | ①flink-jm 变 `unhealthy`（日志 `The Flink config file '/opt/flink/conf/config.yaml' does not exist`）；②Superset worker 启动即 `ModuleNotFoundError: No module named 'psycopg2'`；③`kafka-init` 退出码 1（`--alter` 报「the kafka-configs tool can be used」） | 三条均已定位并修复（见下） | 无 | 骨架四服务全 healthy + topic 配置与 §A.2 一致 |
| **F15** | **★ P0 实测新增** 镜像仓库不可达 | `docker manifest inspect` 报 `registry-1.docker.io` `Client.Timeout`；官方标签 `apache/doris:4.1.5` 拉取 `denied` | 走中国镜像站（compose 已参数化 `${REGISTRY_MIRROR}`） | 自建 Doris 镜像 | 四个锚点镜像可拉（Doris 需本地构建） |
| **F16** | **★ P0 实测新增** named volume 失效致 `REQ-F-ORCH-05` 不成立 | `docker compose down` 后再 `up`，Kafka offsets 由 1 **归 0**（数据丢失）；卷内容为空 | **显式设 `KAFKA_LOG_DIRS`** | — | `down`→`up` 后 offsets 不变（**已证：3→3**） |

**★ F16 详情（P0 实测，2026-10-19）**：官方 `apache/kafka:4.3.1` 镜像的默认 `log.dirs` 是 **`/tmp/kraft-combined-logs`**（`/opt/kafka/config/server.properties:73` 逐字）——**`/tmp` 是容器内路径，`down/up` 后清空**。因此即使 compose 里挂了 named volume，**数据也从未落进去**（实测卷内容 `total 4.0K`）。修复＝显式 `KAFKA_LOG_DIRS=/var/lib/kafka/data`，复验两条：`restart` → offsets 3→3；**`down`+`up` → offsets 3→3**（后者是 `REQ-F-ORCH-05` 的原文判据），topic 与 retention 配置同样保住。
**教训**：**named volume 挂上去不等于数据落在里面**——必须核对应用的**实际写入路径**与挂载点是否一致。与「`mem_limit` 管不到 swap」（F9 相关）同类：**约束写了 ≠ 约束生效**。

**★ F14 的三条修复（P0 实测，均已验证）**：
1. **Flink 2.2 的配置是 `config.yaml`（YAML）**，不是 `flink-conf.yaml`，**也不吃 `FLINK_PROPERTIES` 那套点号键**；且**不能把空目录 bind 进 `/opt/flink/conf`**（会遮住镜像自带 `config.yaml`）⇒ 改为**挂单文件**。修复后 flink-jm `healthy`、TM 正常、Flink 2.2.1 报 `slots-total: 4`（`numberOfTaskSlots: 4` 生效）。
2. **`apache/superset:6.1.0` 镜像不含任何 DB 驱动**（psycopg2/psycopg/mysqlclient/pymysql 全 MISSING）⇒ 元数据库改 **SQLite**；`ecom_ads` 所需 MySQL 驱动经官方机制 `/app/docker/requirements-local.txt`（`docker-bootstrap.sh:35`）装 **PyMySQL**。
3. **Kafka 4.3.1 的 `kafka-topics.sh` 不再支持 `--alter` 改配置**（报错原文指向 `kafka-configs` 工具）⇒ 改用 **`kafka-configs.sh --alter --entity-type topics`**。修复后两 topic 配置为 `retention.ms=604800000,retention.bytes=2147483648`、6 分区、RF=1，与 §A.2 一致。
| F6 | Kafka 分区空闲致**水印**停滞 | 窗口不关闭、指标不更新 | 空闲超时 `table.exec.source.idle-timeout=30s` 生效 | 手动制造流量 | 停掉部分分区后窗口仍按 60 s 关闭 |
| F7 | offset 过期（retention 生效） | `auto.offset.reset` 触发；消费从头/从尾 | 依赖配置的下游行为 | **离线回放不得从 Kafka 重读**（D3：实际保留窗约 6 小时），应走 `dump` 文件 | 长回放不静默丢数据 |
| F8 | Doris FE/BE 容器重启后数据丢失 | 查询空表 | named volume（**必需**） | 重建初始化 | `down` 再 `up` 数据仍在（`REQ-F-ORCH-05`） |
| F9 | 内存超限 OOM kill | `docker stats` 超预算；容器被杀 | `mem_limit` 硬限 | 按 R3 序列降级（MinIO→文件；Superset→Grafana；**新增：Kafka→原生镜像**） | `docker stats` 总值 ≤8 GB |
| F10 | 磁盘击穿 20 GB | 卷使用率 | 保留策略生效 | 关注 `REQ-NF-CAP-07` 提示 | 分区保留**行为**可证（D1：须显式 `start=-30`） |
| F11 | Paimon 小文件数膨胀 | 文件数/快照数 | compaction（保持官方默认） | 手动 compact | ⚠️ **快照数不是「= 5」**：`snapshot.num-retained.min=5` 是**下限**，实际由 `snapshot.time-retained`(1 h) 与 `num-retained.max`(MAX) 决定（D2 修订）；系统表查询须走 **Flink SQL** |
| F12 | 窗口规则①误判合法历史数据 | 拒绝表 100% 命中 | **相对下界**（按推荐） | 排查规则 | 公开数据集回放**不应** 100% 被判脏（D4） |
| F13 | 漏斗状态无限增长 | 状态大小单调上升 | TTL 清理 32 分钟 | 调参 | 状态大小稳定 |

**DLQ 原因码（机读，本节登记）**：脏三值 `schema_invalid` / `value_out_of_range` / `time_out_of_range`；**版本类** `schema_major_unsupported`——消费端**必须拒绝**更高 major 的事件（铁律 S2；全文见 `canonical-truth.md` §3.1，字段语义见 SRS §3.3.1）。

**编号约定**：本表条目称 **§C F‹n›**；引用「F16」时一律写 **§C F16**——**gdd 不存在 `D16`**（§D 止于 D15），勿再写成「gdd D16」。

---

## D. 对 SRS 的偏离与缺陷

> 本节目的是**暴露**而非覆盖（按 Q8 与 `canonical-truth.md` §0 G1 的分档：**事实类可直接改写上游**，**意图类保留原话并登记**，统一口径见 `canonical-truth.md` §1）。不为「文档说错了」开票；只有需要真正施工动作时才开票。
> **编号约定**：§D 止于 **D15**，**不存在 `D16`**；凡引「F16」者为 **§C 失败路径表 F16**（Kafka named volume 失效），一律写「gdd §C F16」。

### D1 · `REQ-NF-REL-03` 与 `REQ-F-OLAP-02` 把一件事说成两道独立防线
SRS 一边要「end-to-end exactly-once：source offset + checkpoint + sink 2PC 三处对齐」，一边要 unique key 幂等。
**官方原话（两条，互相印证）**：
- *Under the Unique model, you can also disable 2pc to achieve idempotent writes*（[FAQ](https://doris.apache.org/docs/dev/connection-integration/data-integration/flink-doris-connector/faq)）。
- *The Label mechanism guarantees no duplication **for a single transaction**, and two-phase commit provides the ability to coordinate no duplication **across multiple transactions***（[Doris Transaction](https://doris.apache.org/docs/4.x/data-operate/transaction/)）——官方**自己就把二者的分工说清了**：label＝单事务、2PC＝跨事务。
**修正**：写成「一层主防 + 一层兜底」。label＝事务级重放，unique key＝行级重放；UNIQUE MoW 表上后者已使重放成幂等 no-op。
**端点备查**：`PUT /api/{db}/{table}/_stream_load`（普通）与 `PUT /api/{db}/{table}/_stream_load_2pc`（2PC）；2PC 下事务停在 `PRECOMMITTED`，由后续 `txn_operation=commit|abort`（带 `txn_id` 或 `label`）结束。**注意**：4.1.4 起 BE 侧 Stream Load 端点不再做全局 `LOAD` 权限预检（[stream-load-manual](https://doris.apache.org/docs/dev/data-operate/import/import-way/stream-load-manual/)）——不影响本项目，但版本相关行为须记。

### D2 · `REQ-F-VERIFY-04` 把「重复」列为脏，与 `REQ-F-VERIFY-03` 的故障演练互斥
`REQ-F-VERIFY-03` 的故障注入**本身就是合法重放**，重放事件**必然重复**。若重复＝脏，则每次演练都会把合法重放倒进 DLQ，并让「脏数据率」面板飙到假值。
**修正**：重复**摘出为独立通道**（计数、不进 DLQ、不计脏率）；脏通道只保留结构与取值、时间范围两类。**原型已实证**该污染：0% → 50%（真脏 0 条）。

### D3 · `REQ-F-MQ-03`「7 天或 2 GB 取小」的实际生效窗**须按配置层级重算**（2026-10-21 订正）
`retention.bytes` 官方明写**按分区生效** ⇒ 2 GB 是**每分区**上限，本仓 **6 分区** ⇒ 主题级 **≈12 GB**。按 200 ev/s × ~500 B ≈ **8.05 GB/天** 推算，**实际生效窗 ≈36 小时**（**设计值**，算术推算；旧「≈6 小时」把 `retention.bytes` 误当主题级总量，**作废**）。另：段被删后还要等 `file.delete.delay.ms`（默认 60 s）才真正删文件。
⚠️ **逐字出处订正**：旧文所引「A segment will be deleted whenever **either** of these criteria are met」**在 Kafka 4.3 官方站未逐字命中**——官方逐字在 **Log 实现页**（"a segment that is eligible for deletion due to either policy will be deleted"）与 `cleanup.policy` 条目；引用须改指该处，勿再引旧句。
**后果**：`REQ-F-GEN-07` 的「离线回放」**不得**从 Kafka 重读超出保留窗（≈36 h，设计值）的历史（应走 `dump` 文件）；`verify-fault` 的恢复窗口若超出该窗，offset 可能已过期。

### D4 · ★致命★ `REQ-F-LAKE-09` 与质量规则①互斥，公开数据集回放会 100% 被判脏
- `SRS.md` §3.3 规则①：`event_time` **不早于 2020-01-01**。
- [天池 UserBehavior（dataId=649）](https://tianchi.aliyun.com/datalab/dataSet.html?dataId=649)：数据覆盖 **2017-11-25 至 2017-12-03**，100,150,807 条交互。
- **后果**：回放时每一行都早于 2020-01-01 → 全部命中规则① → **100% 进拒绝表**。「抽样回放跑通」实际得到全量拒绝，`REQ-F-LAKE-09` 与 `REQ-F-VERIFY-04` 同时被击穿。
- **性质**：这不是「外部事实变了」——`research-notes.md` 第 138 行**自己就写明**了该数据集的 2017 年区间。**同一套文档内部自相矛盾，且未被任何检查发现。**
- 【按推荐 Q29】**处置**：规则①改为**相对下界**（「不早于回放批次的最早事件时间 − 允许乱序幅度」），使规则对生成器与公开数据集**同时成立**，无需为回放开特例。代价：规则①多一个输入（批次基准时间），须在 §A 写明来源。

### D5 · 数据集行为类型含 `fav`，五类枚举里没有落点
数据集行为为 `pv`/`buy`/`cart`/`fav`；`FSR.md` 称其为 schema 的「子集」——但 **`fav`（收藏）在五类枚举里没有对应**（既非 `add_to_cart` 也非 `view_item`）。
【按推荐 Q30】**处置**：回放时**丢弃 `fav` 行并计数留痕**（符合「丢弃必须有痕迹」），**不扩枚举**（扩枚举会牵动 `REQ-F-GEN-02` 与全部看板面板）。

### D6 · 维表无字段级定义，验收判据空表也能通过
`REQ-F-GEN-08` 要「维度数据」、`REQ-F-LAKE-03` 要「时态维表」，但 §3.3 **只有事件字段表**，没有任何维度表字段定义；判据「可装载出非空维表」「可 join 出结果」**空维表也能通过**。
**处置（GDD 补全）**：见 §D6.1 维表字段定义。

#### D6.1 维表字段定义（GDD 补全；【按推荐 Q31】）
| 表 | 主键 | 字段 | 拉链 |
| --- | --- | --- | --- |
| `dim_item` | `item_id` | `category_id`、`price`、`on_shelf`（上架状态）、`brand` | `start_date` / `end_date` |
| `dim_category` | `category_id` | `category_name`、`parent_id`、`level` | `start_date` / `end_date` |
| `dim_user` | `user_id` | `register_channel`、`province`、`level` | `start_date` / `end_date` |
| `dim_region` | `province` | `region`（华东/华南…）、`city_tier` | 静态（无需拉链） |
| `dim_channel` | `channel` | `channel_name`、`is_paid` | 静态 |
**枚举开放性**：`province` / `channel` 取 **封闭但可扩展**——有默认全集、校验非空、允许新增并记录（理由是这两个维度「可枚举但会增长」）。
**拉链判据**：同一 `item_id` 在不同 `event_time` 下 join 出**不同**属性值，且区间**无重叠无空洞**。

### D7 · `REQ-F-LAKE-08` 快照数 5 与官方默认 10 不符
Paimon `snapshot.num-retained.min` 官方默认 **10**。**处置**：表属性**显式设 5**（见 §A）。

### D8 · `REQ-F-OLAP-03` 的「30 天保留」默认不生效（**2026-10-19 补第三个前置条件**）
官方 `dynamic_partition.start` 默认 **-2147483648** ＝「历史分区不删除」。**处置**：**显式设 `start=-30`**（见 §A）。照默认建表的结果与需求**恰好相反**。
★ **补充（本轮新发现，原 D8 漏了）**：Doris 动态分区另有 **FE 级总开关 `dynamic_partition_enable`，默认 `false`** ⇒ **即使表上写了 `dynamic_partition.enable=true`，默认配置下分区创建/删除调度根本不跑**。故 §1.5 的判据「`SHOW PARTITIONS` 显示历史分区被**实际删除**」在默认配置下**必然不通过**。初始化脚本必须显式执行 `ADMIN SET FRONTEND CONFIG ("dynamic_partition_enable"="true")`，并补齐另三个必填/关键项：`time_unit`、`end`、`prefix`，以及不出历史分区就不生效的 `create_history_partition=true`。

### D9 · `REQ-F-RT-03` 的立论前提不成立（**近似算法**降级）
SRS 称「全量去重状态会顶满天花板」故须近似。**算术核对**：去重基数是**用户池**（10 万）而非事件数（720k/h）；精确去重状态 **MB 级**，前提不成立。
**处置（【已确认 Q23】）**：UV 改 Flink 侧**精确**滚动去重；`REQ-F-RT-03` 的近似算法**降级为可选实验**，理由记为「前提不成立」而非「近似不够好」。**连带**：对账从「误差量化」升级为**一致性校验**。

### D10 · `REQ-F-RT-04` 的迟到通道在 SRS 中无落点
SRS 要求迟到走侧输出、判据是「侧输出确有迟到事件」，但**全篇未定义落点**（无 topic、无表、无标记列）。
**处置**：落点补为「允许延迟内→窗口修正（Doris unique key 覆盖，`revision` 递增）＋超期→迟到计数表＋离线 DWD 加 `is_late` / `late_lag_ms` 标记列」。

### D11 · 作业表达方式受官方能力强制
Flink SQL **没有**迟到侧输出（FLINK-10031 / FLINK-20527 均 Open）。故「迟到走侧输出」这条 MUST **强制**至少一段必须是 DataStream。
【已确认 Q16】**处置**：混合路线（SQL 做窗口聚合与 Doris sink；DataStream 做侧输出与修正段）。

### D12 · 窗口内提前发射的机制约束
`table.exec.emit.early-fire.*` 只对**旧式 Group Window Aggregation** 生效，**Window TVF 不支持**（官方原话：「window table function based aggregate doesn't support early-fire and late-fire configuration」）；且标 `@Experimental`、有移除跟踪 FLINK-29692。
**处置**：若采纳窗口内提前发射（Q13），**必须用 Group Window Agg**，并在文档标注 Experimental 风险；若不可接受则降级为「分钟粒度 + as-of 指示」。

### D13 · 全链路 JDK 基线未记载
connector README 版本表：Flink 2.0–2.3 → **JDK 17**（2.1+ 的 JDK 21 experimental）；Flink 2.x artifact 以 `--release 17` 编译。**SRS 与 FSR 都没写。**
**处置**：写入 §A.3 与 compose 环境变量。

### D14 · R2 的表述可以更精确
connector **支持** Flink 2.3.x——卡住 2.3 的不是 Doris 侧，只是 Kafka connector 5.0.0 那条线。应表述为**单点断链**而非「全栈不支持」。

### D15 · Doris 部署路径与 SRS 假设不符
Doris **无官方 docker-compose**（官方仅 `curl | bash` 启动脚本；仓库内 `docker/` 是开发环境；官方明确 Docker 仅限开发测试、容器销毁即丢数据）。故 `REQ-F-ORCH-01` 的「一条命令」要求**自写 compose**，并自担 FE/BE 注册、root 密码、建库（SRS 未列工作量）；`REQ-F-ORCH-05` 的 named volume 是**必需**。

---

## E. 原型索引

三个原型均为**一次性**（throwaway）、**单文件可双击**、无框架无服务器；已作为证据提交入库；验证脚手架已删除。

| 原型 | 类型 | 结论 | 状态 |
| --- | --- | --- | --- |
| [event-routing.prototype.html](prototypes/event-routing.prototype.html) | 逻辑 | 四关判定顺序、脏/迟到/重复三通道落点；**「重复计脏」会把脏率从 0% 污染到 50%（真脏 0 条）** | 18 条断言全绿 |
| [dashboard.prototype.html](prototypes/dashboard.prototype.html) | UI（`?variant=A\|B\|C`） | 看板三结构范式：大屏镶边 / 左筛选网格 / 旅程时间轴；按用户步进漏斗；速率实测 201.6 与 1001.6 ev/s | 16 条断言全绿 |
| [funnel.prototype.html](prototypes/funnel.prototype.html) | 逻辑 | 步进漏斗记账三策略差异；**抓到并修复「水位无条件抬升致用户永久锁死」的真 bug** | 33 条断言全绿 |

**原型的处置**（按 /prototype 规则）：验证过的逻辑（`dispose` 判定流水线、步进漏斗记账、会话超时）**上提到实现**；HTML 外壳作为**一手证据**留在本分支，不进入实现。

---

## F. 本 GDD 的收敛状态

**已确认**：Q1–Q23（用户逐条裁决）；**Q24–Q31 已于 2026-10-19 全部按推荐裁决**。

**2026-10-19 的修订记录（本文就地修订处，逐条）**：本文以下条目在读一手调研后**已就地修订**（`SRS.md`/`FSR.md` 未动，等 `spec.md` 收口）——
§1.7 G2/D15（Doris 有官方 compose + all-in-one 镜像；**新增 Stream Load 网内 `client` 硬约束**）、§1.7 的 `mem_limit`/`memswap_limit`/`config` 不报缺、§1.4 快照判据（下限 5 非「=5」；系统表查询须走 Flink SQL）、§C F11（同上）、§D D2 修订、§D D8 补 FE 级总开关 `dynamic_partition_enable`、§A.2 内存与磁盘的偏离登记、§A.3 JDK 17 补记本机缺口与 `start_fe.sh` 源码要求、§A.3 补 Kafka connector **完整坐标 `5.0.0-2.2`**、§A.3 补运行面工具链锚点、§1.1 `REQ-F-GEN-06` 补 Faker pin 补丁号前提。
→ 完整依据与逐条出处见 **`HLD.md`**（同址，2026-10-19）§7.2 与 §9。

**下一层文档**：`spec.md`（实施规格）由你另开会话产出；本 GDD 与 `SRS.md` / `FSR.md` / **`HLD.md`** 同址，便于一次性收口。
**推送状态**：本地提交由 agent 完成；**推送由你执行**（2026-10-19 用户明确）。
