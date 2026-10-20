# 05 · 离线分层的范围：P2 与 P1 的先后、门槛、触发面，以及五层内部的“形状”

Type: grilling
Status: resolved
Resolved: 2026-10-20
Claimed by: agent (2026-10-20, frontier session)
Blocked by: 02, 08
Blocked by (note): `02` 指**地图票 D4（已落子）**；`08` 指**Paimon/Flink/Doris 一手事实（已落子）**。
Created: 2026-10-20（原 08 + 10 合并）
Ticket: 05
Source: map.md · SRS §7 / FSR §6 /F §9 / gdd §1.4 / D6.1 / D7 · HLD §2.2 / §4.2

## Question

`SRS §7` 与 `FSR §9` 都把离线分层排在 P2（P1 之后），但**没有任何一条说明两者是强序还是可并行**，而它们在资源与接缝上互相咬：同一批生成器代码、同一套超集 schema、同一个 Flink 集群（4 个槽位）、同一个 Doris（P1 写 `ecom_ads`，而 `REQ-F-LAKE-05` 的 ADS 也要「供看板消费」——两条 ADS 是不是同一个地方）、同一组 `make replay` / `make verify` 判据（`verify` 里就有「Paimon 五层有行」）。

**上半：P2 在路线图里的位置**

1. **强序还是并列**：P2 能否在 P1 未完成前开（先只跑 ODS→DWD）？还是「P1 成立」的定义里已含「Paimon 五层有行」，P2 事实上被拉进 P1？
2. **门槛**：`REQ-NF-CAP-05` 要「≥100 万事件」。这批数据从哪来——`dump` 文件、Kafka 重读（gdd D3 说不能超 6h）、还是 `replay`？谁产出、是否进 git（C5 仓库零原始数据）。
3. **槽位与内存**：P2 批作业启动时，`mem_limit` 与 TM 槽位已按 P1 分配。要不要给 P2 一组独立的 `flink-batch` profile（会改动 HLD §4.2 的预算表 → 须先获批）。
4. **触发面**：`REQ-F-LAKE-07` 冻结的是「`make replay` 可完整跑完五层」。`replay` 是一个目标跑五层，还是 Makefile 依赖链（`ods→dwd→dim→dws→ads`）？后者可观察但新增目标名，而 `REQ-F-ORCH-03` 只列了八个。

**下半：五层内部的形状**（`FSR §6` 定了 append/主键，`gdd D6.1` 定了维表字段，但 DDL 形状从未写出）

5. **分区键**：ODS/DWD 按 `dt`？`hour`？不分区？Paimon 对「分区键与主键的先后关系」有无官方约束（这类约束在 Doris 侧已经踩过一次，见 HLD §7.3 U-ν Bug G）。
6. **bucket**：`bucket=-1`（动态）还是固定数？`file://` 单机、GB 级数据量下官方建议是什么。
7. **拒绝表形态**：`dwd_behavior_event_reject` 用 append 还是主键？若 DWD 是 Paimon 主键表，`event_id` 相同的两次到达会**覆盖**——迟到标记 `is_late` / `late_lag_ms` 就被后到者改写，这正确吗？
8. **拉链时间源**：`dim_item` 的 `start_date`/`end_date` 取事件时间还是处理时间？变更流喂进来时取自生成器的哪个字段（`gdd D6.1` 只说「拉链」，未说时间源）。
9. **判据的可执行形态**：`gdd D6.1` 的「无重叠无空洞」怎么写断言（逐 `item_id` × `event_time` 全对比？抽样？何时修）；`gdd §1.4` 的不变量「ODS = DWD + 拒绝 + 迟到标记差异」里，「迟到标记差异」不是可加项，精确等式待写。
10. **快照与保留**：`snapshot.num-retained.min=5` 是**下限**（gdd D7），与「快照过期才物理删除」的交互是什么；`REQ-F-LAKE-08` 的验收判据措辞该由本票还是 [01] 收口（当前约定：[01] 收口口径，本票给形状）。

## 约束

- HITL。先要**形状**（每层一行的决策表 + 需官方确认的约束清单），不要一次写五层全 DDL。
- 官方约束必须引原文；没给就标「官方未给出」。Paimon 侧拿不到一手证据的，标「待 [08] 确认」。
- **不新增常驻容器**；若提议新 profile，同时给出预算变化与回滚。
- 不改 `ecom_lake` 与 `ods_*/dwd_*/dim_*/dws_*/ads_*` 表名前缀（SRS §3.4 冻结）；新增 Makefile 目标须走冻结项变更并在 Answer 单列「需你批准」。

## Answer

**Resolved 2026-10-20（frontier session）。** 18 问全部按推荐落子（用户逐轮回「全按推荐」：Q1–Q5、Q6–Q12、Q13–Q16、Q17–Q18 各一批）。一手取证由两个只读 agent 完成（Paimon append/bucket/expire/merge-engine 与 Flink SQL Client 断言执行面 + 主键表 batch compaction），引文见各节 `官方逐字`。

### 上半 · P2 在路线图里的位置（Q1–Q4）

| # | 决策 | 内容 |
| --- | --- | --- |
| **Q1** | **强序 + 判据分层** | P2 只在 P1 实时四项（Kafka/Flink/Doris/Superset）通过后开。`REQ-F-VERIFY-01` 第三项「Paimon 五层有行」**在 P1 阶段是显式 `SKIP`（不是 PASS）**，P2 完成后自动变成真断言。依据：SRS.md:209 的五项含 Paimon，而 SRS.md:404 把 `REQ-F-VERIFY-01` 归 P1，但 P1 数据路径（Kafka→Flink→Doris）不碰 Paimon——四锚点矛盾只能用「阶段闸门 + 最终断言」拆开。⚠️ 现状 `make verify` 第三项打 `PENDING` 且不置 `fail=1` ⇒ **今天会假通过**，本决策结构性堵死它 |
| **Q2** | **dump 现产，3 天跨度** | `make replay` 前置 `dump --events 1048576 --logical-span 3d --seed <固定>`，产物落 `data/`（`.gitignore` 已覆盖，C5 满足），**不入 git**。密度 ≈4 ev/s 与 `REQ-NF-PERF-03` 的 200 ev/s **不同源**——「批次密度 ≠ 实时速率」是两个用途不同的参数，不是同一数字的两次声明。3 天跨度使 `dt` 有 3 个分区、拉链有跨日变更对象。Kafka 重读不成立（gdd D3 的 6h 窗，[07] 改判为按配置层级重算） |
| **Q3** | **单目标 `replay` + `LAYER=`** | 不新增 Makefile 目标名，**零冻结项变更**。`LAYER=ods\|dwd\|dim\|dws\|ads`（缺省跑全部），可观察性靠日志行 `layer=dwd rows=…`。`REQ-F-LAKE-07` 冻结的是「可完整跑完五层」的**行为**，不是 Makefile 拓扑 |
| **Q7** | **不新增 profile、不新增容器** | P2 复用同一 JM+TM 全员 4 槽给批作业；`replay` 前置检查「无流作业占槽」（`/jobs/overview` 的 `jobs[]`，与 `verify` 探针同源）。反方案 B（+3 GiB 的 `flink-batch` profile）算不平：10.5 + 3 = 13.5 GiB > 11.68 天花板，与 `REQ-NF-CAP-01` 正面冲突 |

### 下半 · 五层形状（Q4b、Q5、Q6、Q8–Q17）

**五层 + 两张伴生表，一行一层**：

| 层 | 表名 | 类型 | 分区 | 主键 | bucket | 关键属性 |
| --- | --- | --- | --- | --- | --- | --- |
| ODS | `ods_behavior_event` | append | `dt`（**落地日**，Q17） | — | `-1`（默认，**不写**） | `payload` 原文列；**零解析**（gdd Q25） |
| DWD | `dwd_behavior_event` | 主键 `deduplicate` | `dt`（**事件日**，UTC） | `(dt, event_id)` | 固定 `1` | `is_late`/`late_lag_ms` 标记列；`upsert-materialize=NONE`（Q13） |
| 拒绝 | `dwd_behavior_event_reject` | append | `dt`（**摄入日**，处理时间，Q8） | — | `-1`（默认） | `reject_reason` 封闭三值；**重复不进表**（冲突 G） |
| DIM | `dim_item`/`dim_category`/`dim_user` | 主键 `deduplicate` | **不分区** | `(id, valid_from_ms)` | 固定 `1` | 拉链区间 ms（Q10）；区间 join（Q14） |
| DIM 静态 | `dim_region`/`dim_channel` | 主键 `deduplicate` | 不分区 | `(province)`/`(channel)` | 固定 `1` | 无拉链（gdd D6.1） |
| DWS | `dws_subject_window` | 主键 `deduplicate` | `dt`（窗口起点日） | `(dt, subject_type, subject_id, window_start)` | 固定 `1` | `subject_type` ∈ user/item/category；指标列**留白给 [03]** |
| ADS | `ads_window_metric` | 主键 `deduplicate` | `dt`（窗口起点日） | `(dt, metric_name, dim_key, window_start)` | 固定 `1` | **与 Doris `ecom_ads.ads_window_metric` 键同构**（Q6-A 的前提） |
| 迟到计数 | `dwd_late_event_count` | append | `dt` | — | `-1`（默认） | `dt` × `late_bucket`（**60 s 档**）只存计数（§3.3 F-3 的取值落定，F4 收口） |

分区键统一 `dt STRING`（`yyyy-MM-dd`，UTC）。`hour` 被否：分区数 ×24 且保留判据在 Doris 侧是日粒度，Paimon 侧无对应保留需求。

**逐问决策**：

- **Q4a/b · 分区键与主键**：五层事实表统一 `dt`；DWD 主键 `(dt, event_id)`（分区字段进主键＝官方好路径，无跨分区索引代价）。**唯一性域＝分区**要写进不变量：同一 `event_id` 跨 `dt` 会成两行——唯一可能来源是「同批数据用不同 `event_time` 重放」，而 `event_time` 由 seed 决定、重跑必然相同，故代价为空。官方无「主键必须含分区列」禁令（[08] D-8：给的是 cross-partition upsert 代价路径）。⚠️ [08] D-8 的「1 亿 key ≈1 GB 索引内存」**只适用于主键表 dynamic-bucket**，不能平移到 ODS（append 的 `-1` 是 bucket-unaware，物理上是 `bucket-0` 单目录）——见「对 [08] 的订正」R1。
- **Q5 · 快照与保留**：`num-retained.min = 5`（显式，gdd D7）＋ **`num-retained.max = 20`（新增，把「受控」变成有限数）**＋ `time-retained` 保持 1 h 默认。判据 SQL：`SELECT count(*) FROM <t>$snapshots` 落在 **[5, 20]**。`sys.expire_snapshots(retain_max => 20)` 收**上界**；`retain_min` 缺省回落 `snapshot.num-retained.min`（**源码级确认** `ProcedureUtils#fillInSnapshotOptions`，**官方文档未给出**该回落关系，只有 Spark 侧 `expire_changelogs` 有逐字表述）。[08] D-6 的「显式**钉住**」措辞不准——`retain_max` 是上界不是下限（订正 R2）。官方协同语义逐字："When the number of snapshots is less than `snapshot.num-retained.min`, no snapshots will be expired…"（min 优先保护）。
- **Q6 · 两条 ADS**：**两套物化、一个口径**。Paimon `ads_window_metric` 是**离线口径基准物化**，**不直接供看板**；Doris `ecom_ads` 是唯一供数点（`REQ-F-OLAP-01` 冻结）。同一指标名在两处必须由同一公式产出——这件事归 [03] 的对账断言；本票落 GLOSSARY 词条「离线口径基准 / 看板供数点」。「离线重算覆盖实时行」记 COULD 留档，P2 判据不含它。
- **Q8 · 拒绝表分区键**：取**处理时间**（摄入日），不取 `event_time`——拒绝表的行恰恰是事件时间非法的行，拿不可信字段做分区布局等于让坏数据决定布局。字段：`raw_payload` / `reject_reason`（封闭三值 `schema_invalid` / `value_out_of_range` / `time_out_of_range`）/ `reject_detail` / `source_file` / `source_offset` / `ingested_at` / `event_id`（可空）。**重复不进这张表**（冲突 G 已定），三值里没有 `duplicate`。
- **Q9 · `is_late` 覆盖**：两个标记列的公式是**确定性的**（`late_lag_ms = batch_watermark_ms − event_time_ms`，watermark 由 seed 决定）⇒ 同批重放覆盖＝**幂等重写**，不是「被改写」。接受 `deduplicate` 后到覆盖（选 A），不改 merge engine。断言：**同批 dump 重放两遍，DWD `count(*)` 与逐列校验和（`SUM(crc32(concat_ws('|', event_id, is_late, late_lag_ms)))`）两次一致**——这是 `REQ-F-VERIFY-02`（幂等验证）的**离线版**（SRS 那条只覆盖 Doris）。
- **Q10 · 拉链字段**：`gdd D6.1` 的 `start_date`/`end_date` 升精度改名为 **`valid_from_ms` / `valid_to_ms`**（`BIGINT`，UTC epoch 毫秒，与 DWD `event_time_ms` 同单位同源）；`valid_to_ms = 9999999999999` 表示仍有效；新版本到达时旧行 `valid_to_ms = 新 valid_from_ms − 1`。主键 `(item_id, valid_from_ms)`。时间源＝维度变更流的 `change_time`（事件时间语义，满足 `canonical-truth` §3.2 D-3）。对 gdd 是 `覆盖下游`（免签），落笔归 [09]。
- **Q11 · 不变量改写**：「ODS = DWD + 拒绝 + 迟到标记差异」**删除**，换成三条各自可判的断言：
  - **A1 分流完备**：`count(DISTINCT event_id)` 在 ODS、DWD、拒绝表三者上闭合（`ods_distinct = dwd_distinct + reject_distinct`，**全局比对**——Q17-A 使 ODS 无事件日分区，降级为全局粒度，可判性不变）。
  - **A2 重复记账**：`count(*) − count(DISTINCT event_id)` 记为重复率，**只入日志、不作断言**（承认恒真；它是面试里讲「为什么不能可加」的那条）。
  - **A3 迟到对账**：`SELECT count(*) FROM dwd WHERE is_late=1` ＝ `SELECT sum(cnt) FROM dwd_late_event_count`（这正是 §3.3 F-3「只存计数」的唯一用途）。
- **Q12 · DWS/ADS 键与分区**：见形状表。指标定义（漏斗/UV/GMV 口径）全部留白给 [03]——本票只定键与分区，防止两票互相改写。
- **Q13 · `sequence.field`**：**不设**；**必须**设 `table.exec.sink.upsert-materialize = NONE`（写进 `docker/flink/conf/config.yaml`，P1/P2 共用）。官方逐字："The sink materializer can reorder records before Paimon receives them. Use sequence fields to express the required update order explicitly."——我们靠「重放确定性」而非 sequence 保证结果；`sequence.field='event_time_ms'` 无效（同 `event_id` 重复行该值相同 ⇒ 回落内部序，白设）。
- **Q14 · 时态维 join**：**batch 区间 join**（`ON d.item_id = e.item_id AND e.event_time_ms BETWEEN d.valid_from_ms AND d.valid_to_ms`）。lookup join 被官方逐字否定（processing-time，回放必错）；event-time temporal join 在批模式无事件时间推进，官方未给出行为。`gdd §1.4`「时态维表」是语义层措辞，**不改**；本票补实现落点。
- **Q15 · bucket**：ODS `-1`（官方 append DDL 示例即此形）；主键表固定 `bucket = 1`、不设 `bucket-key`（官方默认 bucket-key＝主键去掉分区列，正是想要的 `event_id`）。依据：每分区 ≈170 MB 落在官方 "200 MB–1 GB per bucket"（MOR 口径）量级下沿；官方明说 "bucket count is not a hard read-parallelism cap"。dynamic `-1`（主键表）被否：流写启动要读全表建索引，基数小收益为零。**代价**：单 bucket＝单 LSM，compaction 串行；1M 规模可接受。
- **Q16/Q17' · compaction 与过期**：`replay` 末尾显式维护——五层＋拒绝＋迟到计数逐表 `CALL sys.compact(table => '...', compact_strategy => 'full')`（**full 只支持 batch 模式**，官方逐字），随后逐表 `CALL sys.expire_snapshots(retain_max => 20)`。依据：①纯 batch 下 append 表 writer **不自行压缩**（官方逐字 "For a normal unaware-bucket append table, the writer does not compact files itself"——流式 sink 才有 coordinator）；②主键表默认 writer 压缩属**流式 writer** 行为，batch 作业跑完即无 writer；③`sys.compact` 对主键表的适用性官方无逐句声明，但 procedure 示例含主键表且 `buckets` 参数明写 "supported only for fixed-bucket tables"（推断级，非官方承诺）。**sort compact 不可用**于我们的主键表（"Sort Compact currently supports only bucket=-1 and batch mode"）。`sys.compact_database(includingDatabases => 'ecom_lake', mode => 'combined')` 可一次压全库，但其 `divided/combined` 语义按 sink 作业描述、batch 适用性官方未给出 ⇒ 用逐表 `sys.compact`（保守、可判）。
- **Q17 · ODS 的 `dt`**：取**落地日**（`ingested_at`），不取事件日——否则要在 ODS 解析 `event_time`，违反 gdd Q25「一切解析在 DWD 完成」且让 ODS 不再免疫 schema 演进。代价：3 天 dump 全落同一 `dt`，A1 降为全局比对（已写进 Q11）。
- **Q18 · 全 SQL、零 jar**：P2 不新增 jar、不需要 JDK/Maven。`replay` = `sql-client.sh -f sql/replay_*.sql` 串联：

```
sql/replay_00_catalog.sql   -- CREATE CATALOG paimon (warehouse=file:/warehouse)
sql/replay_10_ods.sql       -- dump → ods_behavior_event（append、dt=落地日）
sql/replay_20_dwd.sql       -- ods → dwd_behavior_event + dwd_behavior_event_reject
sql/replay_30_dim.sql       -- 维度变更流 → dim_*（区间）
sql/replay_40_dws.sql       -- dwd × dim 区间 join → dws_subject_window
sql/replay_50_ads.sql       -- dws → ads_window_metric
sql/replay_90_maintain.sql  -- CALL sys.compact(...) ×N + sys.expire_snapshots(retain_max=>20) ×N
sql/assert_lake.sql         -- 五层行数 + A1/A3 + 拉链 + 快照数，全部 SELECT 哨兵
```

### 断言执行面（不依赖 sql-client 退出码）

**官方文档只承诺 "will exit"，从未给退出码**；源码（release-2.2 `CliClient`）`executeInNonInteractiveMode()` 丢弃 `executeFile()` 的 boolean，全程无 `System.exit` ⇒ **`sql-client.sh -f bad.sql` 语句报错后仍以 0 退出**（源码事实，非官方承诺）。⇒ 断言门禁 = `scripts/lake_assert.sh` 包装：每条断言写成 `SELECT '<断言名>' AS assertion, COUNT(*)>0 AS ok FROM …` 哨兵，脚本判定「无 `[ERROR]` **且** 每条哨兵 `ok=true` 字符串出现」，解析 stdout。另两条官方边界（排除替代路径）：`-i` 初始化文件**不允许放 SELECT**（"it's not allowed to add query or insert into the init file"）；`STATEMENT SET` 官方限定为「多条 INSERT 合并优化」，不是断言工具。

批模式要点（均官方逐字）：`SET 'execution.runtime-mode' = 'batch'`（或 `-Dexecution.runtime-mode=batch`）皆合规；批读主键表默认最新快照，普通全量 `COUNT(*)` **不需要** `/*+ OPTIONS(...) */`（hint 只用于时间旅行）；⚠️ **"Batch SQL drops DELETE records from an ordinary table result"** 且 MOW/first-row 批读默认排除 pending Level-0 ⇒ 拉链断言分两条：当前快照的区间校验走普通 SELECT，「变更发生过」走 `t$audit_log`。

### 部署硬约束（两条新增，HLD §5 未写过，落笔归 [09]）

1. **warehouse 必须落具名卷、同路径挂进 JM 与 TM**。官方把 `file:` 背书到 "single-machine tutorial" 并说 "Use a warehouse on shared storage for a distributed cluster"；本仓 JM/TM 是两个容器，`/opt/flink/...` 不互通 ⇒ 新增 named volume（如 `paimon-warehouse:/warehouse`）挂两个 Flink 容器。官方未给出针对此形态的专门条款。
2. **Paimon jar 进每个 Flink 进程**：官方逐字 "Install one matching Paimon bundled connector in **each** Flink process."（JM 和 TM 都要）；`ADD JAR /宿主路径` 不行（"they only work with the local filesystem, which in this case is Docker's overlay filesystem"）⇒ 构建期 `COPY` 进 `/opt/flink/lib`（Flink Docker 页背书该路径）。

### 对 [08] 的两处订正（事实类，落笔归 [09]）

| # | [08] 原措辞 | 一手事实 | 订正 |
| --- | --- | --- | --- |
| **R1** | D-8 把「`bucket=-1`」笼统当「动态 bucket」，并引「1 亿 key ≈1 GB 索引内存」 | append 表的 `-1` 是 **bucket-unaware**："use a `bucket-0` directory without tying writer parallelism to a fixed bucket count"；键→桶索引只属主键表 dynamic-bucket | 该代价**不适用于 ODS**；引用时须分「主键表 dynamic-bucket」与「append unaware」两种 `-1` |
| **R2** | D-6 说 `sys.expire_snapshots(retain_max => N)` 是「显式**钉住**快照数」 | 官方参数分工：`retain_max`＝"the maximum number of completed snapshots to retain"（**上界**）；`retain_min`＝"the minimum…"（下界） | 改为「`retain_max` 收上界、`retain_min`/表属性 min 保下界」；「钉住」仅当 min/max 同时给定时成立 |

### 需你批准（均已于 2026-10-20 按「全按推荐」批准）

| # | 事项 | 性质 | 落笔 |
| --- | --- | --- | --- |
| 1 | `REQ-F-VERIFY-01` 判据句改写（P1 阶段第三项显式 SKIP、P2 起为真断言） | 意图类，**需签字**——已签（全按推荐） | [09] |
| 2 | 新增离线幂等判据（Q9 的 DWD 重放两遍 count+校验和一致） | 设计面新增判据 | [09] 并入 gdd §1.4 |
| 3 | `dim_*` 字段改名 `valid_from_ms`/`valid_to_ms` | `覆盖下游`（gdd D6.1），免签 | [09] |
| 4 | 迟到计数表名落定 `dwd_late_event_count`、粒度 60 s 档 | FC-2 已批的取值填充（§3.3 归本票填） | [09] |
| 5 | 新增 `paimon-warehouse` named volume + jar 构建期 COPY | 设计面（部署事实），不动冻结面 | [09] |

### 落笔分工（[09] 输入）

1. SRS `REQ-F-VERIFY-01` 判据句（签字版：五项烟测中「Paimon 五层有行」在 P1 里程碑判定中显式 SKIP、不计入通过条件；P2 里程碑判定中该项必须为真断言且退出码参与判定）。
2. gdd §1.4：不变量替换为 A1/A2/A3；快照判据补 `[5,20]` 与 `$snapshots` SQL；补 `upsert-materialize=NONE`。
3. gdd D6.1：`start_date/end_date` → `valid_from_ms/valid_to_ms`（精度 BIGINT ms）；`dim_region`/`dim_channel` 标注静态无拉链。
4. `canonical-truth.md` §6 索引三行闭合：冲突 A 的快照取值（min=5/max=20）、迟到计数表名与粒度、不变量缺陷改写。
5. [08] 订正 R1/R2 与本票「官方未给出清单」并入其证据登记。
6. HLD §5 设计面：warehouse 卷、jar COPY、`sql/replay_*.sql` 目录布局。

### 官方未给出清单（本票新增）

| # | 事项 |
| --- | --- |
| 1 | Flink 侧 `sys.expire_snapshots` 未指定参数回落表属性（仅 Spark 侧有逐字；源码 `ProcedureUtils` 确认） |
| 2 | append 表纯 batch 写入后的 compaction 归属（官方四条路径全挂流式或显式作业） |
| 3 | `sys.compact` 对主键表的逐句适用声明（procedure 示例含主键表＋`buckets` 明写 fixed-bucket，推断级） |
| 4 | batch 写入后必须显式 compact 的官方指引（未给出；`compact_strategy='full'` 仅 batch 模式是唯一相关句） |
| 5 | `sys.compact_database` 的 `divided/combined` 在 batch 模式的适用性 |
| 6 | Paimon `file://` warehouse 在双容器（JM/TM）形态的注意事项（仅有 "shared storage" 范围界定） |
| 7 | Paimon「拉链/zipper/SCD」的任何官方能力或指导（全站检索索引命中 0） |
| 8 | Paimon 维表按事件时间历史回看（官方逐字否定：lookup join 是 processing-time） |
| 9 | `compaction.min.file-num` 是否对 bucketed append（`bucket>0`）同样生效 |
| 10 | Flink SQL 断言失败→非零退出码的机制（源码显示不存在） |

### 交给 [03] 的接口

- 对账口径（Q6）：Paimon `ads_window_metric` 与 Doris `ecom_ads` 同名指标必须同公式——对账断言归 [03]，键同构已由本票保证。
- 指标列留白：DWS/ADS 的具体指标列（漏斗/UV/GMV 口径、`subject_type` 枚举外的维度展开）由 [03] 定义后回填形状表。
- `verify` 退出码语义的完整裁定（[03] Q5）须与本票 Q1 的「阶段闸门」一致：P1 阶段五项＝四真断言＋一 SKIP。

### 迷雾收口

- **F4（迟到计数表落点）→ 收口**：原则（§3.3）＋取值（本票：`dwd_late_event_count`、`dt`×60 s 档、append）齐备。
- **F2（公开数据集回放范围）→ 收敛**：P2 门槛批由生成器 dump 现产（Q2）；`REQ-F-LAKE-09` 维持 COULD，公开数据集回放不在 P2 最小集内，回放可用性由冲突 H 的相对下界保障。
