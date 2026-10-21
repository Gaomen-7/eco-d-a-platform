# SPEC — 电商用户行为数据全链路 · 施工规格

Status: ready-for-agent
Type: spec
Source: `issues/09-implementation-spec-and-handoff.md` Answer（D12 蓝图）· `map.md` Decisions so far
决策日期: 2026-10-20（8 张已解票）／2026-10-21（本 spec 汇编）
闸门: 第 ⑨ 节追溯表逐行全命中才算成（裁决人＝本 spec 撰写会话）

---

## ① 头部定位与阅读顺序

### 1.1 本文件是什么

**本文件是「怎么施工、怎么判成没成」的唯一总纲。** 施工会话照本文件建代码；它不解释「为什么这么定」——那在上游文档与决策档案里。

**形态**：单份自足式文件。施工要用的**表、句、判据逐字嵌入**本文件（照抄即可执行）；票链接只作「为什么」的档案指针。

**语言与术语**：中文；术语一律取 `GLOSSARY.md`（全仓唯一术语表）；数字一律标 `官方给出` 或 `设计值`（**不得把设计值当官方保证**，HLD §0.3）。

### 1.2 与上游文档的分工（读本文件前先认清谁管什么）

| 文件 | 回答什么 | 与本文件的关系 |
| --- | --- | --- |
| `SRS.md` | **必须做到什么**（需求权威） | 本文件第 ⑦ 节把已签字的修订逐字落到 SRS；施工会话按第 ⑦ 节执行 |
| `gdd.md` | **每个功能怎么做**（设计） | 本文件第 ④⑤ 节给 gdd 的施工口径与形状；第 ⑦ 节落笔 |
| `HLD.md` | **结构、技术栈定案、运行约束** | 本文件第 ②④⑥ 节给 HLD 的施工硬约束；第 ⑦ 节落笔 |
| `canonical-truth.md` | **REQ 冲突的裁决权威** | 与本文件**并存**：它裁「谁该让路」，本文件管「怎么施工」。第 ② 节逐字嵌入它的三组规则铁律 |
| `reproducibility-boundary.md` | **可复现承诺的边界**（四档契约、跨平台成立性） | 第 ③ 节薄写已完成事实＋指针 |
| 票 `issues/NN-*.md` | **为什么**（决策档案） | 只作档案指针，不复制内容 |

> **命名澄清**（[09] Q1）：`SRS.md:6` 写「本文件名采用 `SRS.md`，不使用 `spec.md`」——那句话说的是 **SRS 自己**不用该名，不与本文件冲突。`canonical-truth.md` §4.1 早前裁决「收口产物不做 spec.md」，指的是**收口产物**，不是施工规格；两者并存（D5③ 维持）。

### 1.3 落笔链（本文件到代码的顺序，不可跳）

```
[spec 会话]  汇编第 ⑦ 节逐字修订清单（不动上游文件）
     ↓
[施工会话]  ① 按第 ⑦ 节机械改写 SRS/gdd/HLD/init.sql（上游收口）
     ↓
     ② 按第 ④⑤⑥ 节建 P1/P2/看板代码与判据（主接缝：make verify / verify-fault）
     ③ 按第 ③ 节收 P0 已完成事实（薄写，多为指针）
```

**已解票保留原文**作决策档案，不折叠、不加新状态（[09] Q4，`Status` 枚举保持 `open`/`claimed`/`resolved`）。

### 1.4 迷雾承接与冲突兜底

- **F1 残留**（`canonical-truth.md` §3.1 schema_version 六处落点的逐字撰写）＝本文件第 ② 节正文素材，已随本文件落定。
- **F3**（生成器 live 模式确定性）／**F6**（降级阶梯触发条件）／**F7**（世界时钟）＝**P1 施工面参数**，随 D12 转出本图。施工卡住口径时走兜底：**当场作为追加问句请用户裁决**；兜底记本文件附录 A「冲突待回票」，**不回开地图**（[09] Q10/Q14）。

---

## ② 全局铁律

> 本节**逐字嵌入** `canonical-truth.md` §3.1–3.3 三组规则（schema_version 四律、离线 DDL 三原则、迟到计数表三原则）。**一段规则只有一个权威全文**（canonical-truth G3）；本节是给施工的**逐字副本**，别处只写指针。

### 2.1 `schema_version` 四条铁律（canonical-truth §3.1）

**权威全文在 `canonical-truth.md` §3.1**；本节是施工逐字副本。

| # | 铁律 | 判定 |
| --- | --- | --- |
| **S1** | **版本分级**：加**可选**字段＝**minor**（`1.0`→`1.1`）；**必填字段增删、字段类型或语义变更**＝**major**（`2.0`） | `schema_version` 字符串比较：`MAJOR.MINOR`，只比 major 用于兼容性判定 |
| **S2** | **消费端容忍度**：消费端**必须**容忍同 major 的更高 minor（未知可选字段**丢弃并计数**）；**必须拒绝**更高 major ⇒ 进 DLQ，原因码 **`schema_major_unsupported`** | 拒收事件带机读原因码；计数器可判 |
| **S3** | **版本号不要求相等**：**不要求**生产端与消费端版本号相等；要求消费端支持的 major **≥** 生产端 major，且能处理生产端 minor | 契约是「能力下界」，不是「值相等」 |
| **S4** | **checkpoint 恢复**：跨版本恢复**不禁止**，按「**缺省值 + 标记**」放行；恢复后产生的行必须带 **`schema_version_upgraded`** 标记 | 禁止恢复等于任何 schema 变更都逼你重跑全链路；标记让差异可观测、可对账 |

**六处落点分工**（施工按此落笔，归第 ⑦ 节修订清单）：

| 落点 | 写什么 |
| --- | --- |
| SRS §3.3 字段行 | `schema_version` \| string \| 是 \| 如 `1.0`/`1.1`/`2.0`（分级规则见 §3.3.x） |
| SRS **新增 §3.3.x「schema_version 兼容规则」** | S1/S2 的字段语义与未知版本处置（因它定义字段语义，绕不开） |
| SRS §3.4 冻结面「消息格式」行 | 加「兼容规则见 §3.3.x」 |
| gdd §1.1 | 生产端产出**哪个**版本 |
| gdd §1.4 | DWD 清洗对未知版本的**义务** |
| gdd §C | DLQ 原因码 `schema_major_unsupported` |
| HLD §2.5 横切面①「事件契约」 | `schema_version` 语义的**唯一所有者**；规则全文指针 → canonical-truth §3.1 |

### 2.2 离线 DDL 三原则（canonical-truth §3.2）

**本文件定原则，具体取值由第 5.2 节五层形状表给。**

| # | 原则 | 理由 |
| --- | --- | --- |
| **D-1** | ODS/DWD/DWS/ADS 事实表**必须**含**一个时间维分区键**（`dt` 或 `hour`） | 否则 Paimon 小文件与 compaction 成本不可控；`REQ-F-OLAP-03` 的保留策略无处挂载 |
| **D-2** | `dwd_behavior_event_reject` **必须 append**，**不得主键** | DWD 是主键表，同 `event_id` 二次到达会**覆盖**，`is_late`/`late_lag_ms` 被后到者改写 ⇒ 违反「**丢弃必须有痕迹**」（SRS §3.3） |
| **D-3** | `dim_*` 拉链的时间源**必须与事件时间同源**（用 `event_time`），**不得用处理时间** | 处理时间拉链与 `REQ-F-LAKE-03`「时态维表」语义冲突；回放时处理时间＝墙钟，会把 2017 数据落进 2026 区间 |

**D-2/D-3 的推论已在第 5.4 节用三条可判断言 A1/A2/A3 落地**（「迟到标记差异」不是可加项，原不变量已删除）。

### 2.3 迟到计数表三原则（canonical-truth §3.3，收口冲突 C 的落点）

| # | 原则 | 理由 |
| --- | --- | --- |
| **F-1** | 落 **Paimon `ecom_lake`**（与五层同库），**不新增 Doris 表** | 避免看板侧再开一张表、避免 Doris 表面膨胀；查询走 Flink SQL（Paimon 系统表官方支持引擎**不含 Doris**，HLD §5） |
| **F-2** | **append 表** | 计数天然追加；主键表需 upsert 语义，与「计数」不相称 |
| **F-3** | 粒度按 **`dt` × 迟到分桶**（`late_bucket` 取分钟级或 5/15/60s 档）；**只存计数，不存明细** | 与 D-1 同源；明细已在 DWD 的 `is_late`/`late_lag_ms` 标记列 |

**具体取值**（`dwd_late_event_count`、`dt`×60 s 档）见第 5.2 节形状表（[05] 已填，FC-2 已批）。

### 2.4 全局可证纪律（施工时逐条自检）

1. **参数性质边界**：每个数字标 `官方给出` / `设计值`，**不得把设计值当官方保证**。
2. **行为可证**：每条约束附**检查方式与判定条件**；「配置可见」「UI 里能看到」**不是**判据。
3. **实机验证**：格式/镜像内容/数据落点/指标可信度**先验证再写文档**（P0 已在四处被官方说法打脸，教训在 HLD §8）。
4. **能起来 ≠ 能重起来**：状态进卷的组件，验收必须含一次「**不删卷的重启周期**」（HLD §7.3）。
5. **git**：agent 只本地提交，**绝不推送**（本仓硬纪律）。

---

## ③ P0：环境收尾（薄写——已完成事实＋指针）

> [09] Q12 裁决：P0 已由 `4a4759a` 全收口，**本节薄写**，只列「已完成什么」＋「去哪看」，不重述八缺陷细节（教训已在 HLD §8）。

### 3.1 P0 已完成事实（指针，不重做）

| 事实 | 落点 |
| --- | --- |
| Doris 镜像首次**建成并实机跑通**；`make verify` fail=0；Doris 断言 PENDING→PASS；初始化建出三张 ADS 表 | commit `4a4759a`（2026-10-20 15:11）；HLD §7.3 |
| 八缺陷（A–H）教训已进 HLD §8，含「**不删卷的重启周期**」验收纪律 | HLD §7.3 / §8 |
| 「可复现主体＝制品」＋四档契约 L0–L3＋「一条命令＝`make init`」 | `reproducibility-boundary.md`；D7 |
| 镜像体积记账口径＝`docker image inspect`（`eco/doris:4.1.5` = **12.60 GiB**，本机实测） | `reproducibility-boundary.md` §3；HLD §4.2 |

> ⚠️ **别重做**：handoff 交接表里原定入口「Doris 改道＋Bug B/C/D」**已执行入库**（`4a4759a`），记「已完成勿重做」。

### 3.2 P0 的施工边界（薄）

P0 已完成，本 spec 的施工会话**不重跑 P0 破坏性实测**（如 `down -v` 会销毁已验证的卷态成果）。P1/P2 施工若依赖新容量/演练，走**须先获批实机 task**（三张，见 handoff 入口④）。

### 3.3 「一条命令」＝ `make init`（施工时它已具名，四段冻结）

| 段 | 动作 | 为什么不可省 |
| --- | --- | --- |
| ① | `docker compose up -d`（默认 profile） | Kafka / Flink JM+TM / Superset 起来；`kafka-init` 由 `depends_on: service_healthy` 自动跑 |
| ② | `docker compose --profile doris up -d --wait` | 默认 `up` **不含** Doris（`profiles: ["doris"]`）；不加则链路不完整 |
| ③ | `docker compose run --rm client /scripts/doris-init.sh` | Doris 初始化**必须从 compose 网内发起**；脚本自带 FE 轮询与 BE Alive 等待 |
| ④ | Superset 初始化：`db upgrade` → `fab create-admin` → `superset init` | `SUPERSET_LOAD_EXAMPLES=no`；**当前未实现**，`make init` 只打印「待补」——**这是 P1 施工项** |

> `make init` 是**冻结目标名**（SRS §3.4 八个目标之一）⇒ **不触发冻结项变更**；新增别名目标（如 `up-all`）要走 FC 流程，为零收益。

---

## ④ P1 实时闭环：语义、圈表与分层判据

> 主接缝＝`make verify` / `make verify-fault` 两个**冻结目标**（用户 2026-10-21 确认）。SQL 侧**全 SQL 零 jar**（[05] Q18）。

### 4.1 圈表 PI-1/2/3（[02]：[P1 怎样算成立]）

| 圈 | 含义 | 施工口径 |
| --- | --- | --- |
| **PI-1 最小闭环** | 不成立则 P1 不成立 | 每条给一个**可执行断言**（退出码/标量/字段），见第 4.3 节 verify 分层 |
| **PI-2 加固** | 成立则更好，不阻塞进 P2 | 写明延到哪一段 |
| **PI-3 标注** | 只要求在文档里标注风险/口径，不要求实现 | 例如 Experimental/deprecated 的 early-fire（见第 4.6 节） |

### 4.2 P1 语义（[03] A 部分·逐字要点）

- **修正面＝窗口聚合行＋漏斗行**：`purchase` 迟到 ≤120 s 落入已落定窗口 ⇒ 重发 ⇒ 该分钟漏斗客数被修正、`ads_funnel_step.revision`+1。依据 TTL 32 min＝30 min 会话+2 min 允许延迟；**超期（>120 s）只计数、绝不修正**。`revision` 语义两表同构。
- **暂定/落定/修正同表同一行**：`ads_window_metric` 以 `(window_start, metric_name, dim_key)` 唯一键承载三态（`is_settled` 0/1，`revision` 递增）。gdd D1「一层主防」（Doris unique key 幂等吸收重复写）**无需重证**。
- **漏斗唯一键不含 revision**：维持 `(session_date, step_no, window_start)`；revision 是值列。单调解：`cnt_inferred` 沿 `step_no` 1→4 **非增**，违反即 fail；`cnt_observed` **不设**单调断言（`--chaos-ratio` 下跳步合法）。
- **对账口径**：切窗 `window_start = floor(event_time_ms/60000)×60000`（UTC epoch 对齐）；UV 按 1 h epoch 对齐同理。P2 起 `lake_assert.sh` 逐 `metric_name` 比对 Paimon（离线口径基准）与 Doris（看板供数点）两套 `ads_window_metric`，**只比 `is_settled=1`**，逐值严格相等；**P1 阶段显式 SKIP**。
- **UV**：Flink 侧**精确**滚动去重（近似降级为可选实验，见第 ⑦ 节冲突 B）。

### 4.3 `verify` 分层进码（[03] Q5；[05] Q1 阶段闸门同构）

| 阶段 | 五项烟测 | 附加断言 |
| --- | --- | --- |
| **P1** | **4 真断言 + 1 显式 SKIP**（第三项「Paimon 五层有行」SKIP，不计通过不计 fail） | ＋lag 断言 ＋可即时判定的行为判据（脏率不升、漏斗单调——纯 Doris SQL 可判） |
| **P2** | **5 真断言**（五项全真） | 同上 |

- **场景依赖判据**（`revision>0` 出现、UV 对账）带「场景前置」标记：仅在对应注入器/离线场景激活时判定，否则 SKIP——**不是永久豁免**。
- **幂等（`REQ-F-VERIFY-02`）不进 `verify` 每次运行**（全量重放两遍是重操作）：P2 起由第 5.4 节离线幂等断言（Q9）＋ `verify-fault` 恢复重放共同覆盖（SRS 措辞修订见第 ⑦ 节）。
- ⚠️ 现状 `make verify` 第三项打 `PENDING` 且不置 `fail=1` ⇒ **今天会假通过**；本分层**结构性堵死**它。

### 4.4 `verify-fault` 全参数化（[03] Q6/Q7）

```
输入冻结（停生成器或固定 dump 回放）
  → 等 lag==0
  → 快照基线：ads_window_metric 与 ads_funnel_step 两表 count+sum、DLQ 计数、Kafka committed offset
  → kill TM
  → 轮询 /jobs/overview 到 RUNNING，超时 120 s（设计值）→ 超时即 fail
  → 等 lag==0 且此后完成一次 checkpoint，超时 300 s（设计值）
  → 比对两表 count/sum 与基线全等、重复通道计数上升（证明确有重放且被幂等吸收）
```

- **脏率**：分子＝DLQ 消息增量，分母＝输入事件总数（输入冻结后为常数）；**重复不进分子**。
- **负断言基线＝kill 前 DLQ 快照**：因输入冻结+`--seed` 确定性，该值同时应等于 seed 决定的期望脏数（两处对不上本身即断言）。恢复后 `DLQ 增量==0` **且** `重复通道增量>0`；若条数一致但 DLQ 上升 ⇒ **fail**（重复被误当脏的反例保护）。

### 4.5 fail 痕迹三层（[03] Q9·横切关注点「失败必须有痕迹」）

`verify` / `verify-fault` 每条断言的结果必须留三层痕迹：

1. **stdout 人读面**：每条断言一行 `PASS/FAIL/SKIP <名> <实测> vs <期望>`。
2. **JSON 报告面**：退出前写 `artifacts/verify/verify-report-<UTC时间戳>.json`（逐断言状态；报告目录进 `.gitignore`）。
3. **FAIL 原始响应快照**：FAIL 时把原始响应全文（Flink REST JSON、`mysql` 哨兵输出、CLI stderr）快照进同目录。

②③ 双兜底＝「失败必须有痕迹」。

### 4.6 作业形态的官方硬约束（[08] D-1/D-11/D-14·影响 P1）

| 约束 | 官方事实（`官方给出`） | 施工含义 |
| --- | --- | --- |
| **Flink SQL 无迟到侧输出** | FLINK-10031/20527 均 Open/Unresolved；逐字 "The Table API & SQL drop late events right now."；`WindowAggOperator` javadoc 逐字 "late elements … will be simply dropped." | 「混合路线」是**官方能力强制**，不是选型偏好 |
| **SQL 侧无 allow-lateness** | FLINK-24000 Closed/Later 逐字 "Window aggregate does not support allow-lateness"；`allowedLateness` 是 **DataStream-only** | 「允许延迟内→窗口修正」**必须落在 DataStream 段** |
| **窗口内提前发射（10 s 暂定值）** | 唯一支持 early-fire 的 Group Window Aggregation 在 2.2 **官方文档标 deprecated**，源码写 "will be dropped"；TVF 聚合明写 "do not emit intermediate results"；CUMULATE 走 TVF 会被 `checkEmitConfiguration` **直接抛异常** | 该路**只能走一条正在被官方弃用的语法** ⇒ **PI-3 标注**（文档记风险），非 PI-1 硬条件；CUMULATE **不是**替代路径 |

> 「窗口修正」段落在 DataStream、「窗口聚合」在 SQL——这是官方能力的必然切分，不是设计偏好。

### 4.7 P1 构造器（生成器）注入器（[01] 冲突 D·已签）

| 注入器 | 默认 | 语义 |
| --- | --- | --- |
| `--late-correctable` | **3%** | 落 2 分钟允许延迟**内**的迟到（可修正） |
| `--late-expired` | **2%** | **超期**只计数（不可修正） |
| `--chaos-ratio` | 0 | 混沌（使 `cnt_observed` 可非单调） |

偏移一律**相对水印**而非墙上时钟计算，制造可复现乱序（`--seed` 下可复现）。判据：设 `--late-correctable 0.03 --late-expired 0.02` 后，实时侧窗口修正段确有迟到事件落入、超期段确有迟到计数条目。

---

## ⑤ P2 离线分层：五层形状、门槛批与离线断言

> 触发面＝**单目标 `make replay` + `LAYER=`**（零冻结项变更）。P2 **只在 P1 实时四项通过后开**（[05] Q1 强序）。

### 5.1 门槛批（[05] Q2）

- **来源**：`make replay` 前置生成器 `dump --events 1048576 --logical-span 3d --seed <固定>` 现产，落 `data/`、**不入 git**（C5 仓库零原始数据）。
- **密度**：≈4 ev/s，与 `REQ-NF-PERF-03` 的 200 ev/s **不同源**——「批次密度 ≠ 实时速率」是两个用途不同的参数。
- **3 天跨度**使 `dt` 有 3 个分区、拉链有跨日变更对象。
- **Kafka 重读不成立**（保留窗见第 ⑦ 节冲突 J；[07] 改判为按配置层级重算）。

### 5.2 五层＋两伴生表形状表（[05] 下半·逐字）

分区键统一 `dt STRING`（`yyyy-MM-dd`，UTC）。`hour` 被否（分区数×24 且 Paimon 侧无对应保留需求）。

| 层 | 表名 | 类型 | 分区 | 主键 | bucket | 关键属性 |
| --- | --- | --- | --- | --- | --- | --- |
| ODS | `ods_behavior_event` | append | `dt`（**落地日**） | — | `-1`（不写） | `payload` 原文列；**零解析** |
| DWD | `dwd_behavior_event` | 主键 `deduplicate` | `dt`（**事件日** UTC） | `(dt, event_id)` | 固定 `1` | `is_late`/`late_lag_ms`；`upsert-materialize=NONE` |
| 拒绝 | `dwd_behavior_event_reject` | append | `dt`（**摄入日**＝处理时间） | — | `-1` | `reject_reason` 封闭三值；**重复不进表** |
| DIM | `dim_item`/`dim_category`/`dim_user` | 主键 `deduplicate` | **不分区** | `(id, valid_from_ms)` | 固定 `1` | 拉链区间 ms；区间 join |
| DIM 静态 | `dim_region`/`dim_channel` | 主键 `deduplicate` | 不分区 | `(province)`/`(channel)` | 固定 `1` | 无拉链 |
| DWS | `dws_subject_window` | 主键 `deduplicate` | `dt`（窗口起点日） | `(dt, subject_type, subject_id, window_start)` | 固定 `1` | `subject_type`∈user/item/category；指标列＝`pv/uv/gmv`（由 [03] 推定回填） |
| ADS | `ads_window_metric` | 主键 `deduplicate` | `dt`（窗口起点日） | `(dt, metric_name, dim_key, window_start)` | 固定 `1` | **与 Doris `ecom_ads.ads_window_metric` 键同构**（对账前提） |
| 迟到计数 | `dwd_late_event_count` | append | `dt` | — | `-1` | `dt` × `late_bucket`（**60 s 档**）只存计数 |

**逐层要点**：

- **ADS 是「两套物化、一个口径」**：Paimon `ads_window_metric`＝**离线口径基准**（对账基准面，**不直接供看板**）；Doris `ecom_ads`＝**看板供数点**（唯一供看板消费，`REQ-F-OLAP-01` 冻结）。同名指标**同一公式**产出（对账断言见第 4.2 节）。
- **拒绝表分区取处理时间**（摄入日），不取 `event_time`——拒绝行的 `event_time` 本身不可信，拿它做分区布局等于让坏数据决定布局。`reject_reason` 封闭三值：`schema_invalid`/`value_out_of_range`/`time_out_of_range`（**三值里没有 `duplicate`**——重复走幂等吸收通道）。
- **拉链字段改名**：`start_date`/`end_date`→**`valid_from_ms`/`valid_to_ms`**（`BIGINT` UTC epoch 毫秒）；`valid_to_ms = 9999999999999` 表仍有效；主键 `(item_id, valid_from_ms)`；时间源＝维度变更流 `change_time`（事件时间语义，满足 D-3）。
- **时态维 join 走 batch 区间 join**：`ON d.item_id = e.item_id AND e.event_time_ms BETWEEN d.valid_from_ms AND d.valid_to_ms`。lookup join 被官方逐字否定（processing-time，回放必错）；Paimon 全站**无 zipper/SCD 官方能力**。
- **主键表默认 MOR 而非 MOW**（[08] D-7；MOW 需显式 `deletion-vectors.enabled`）；官方 bucket 指引 "200 MB–1 GB per bucket" 是 **MOR 口径**。本仓每分区 ≈170 MB 落量级下沿 ⇒ 主键表固定 `bucket=1`、**不设 `sequence.field`**（靠重放确定性），**必须**设 `table.exec.sink.upsert-materialize=NONE`。

### 5.3 `replay` 编排：全 SQL、零 jar（[05] Q18）

P2 **不新增 jar、不需要 JDK/Maven**。`replay`＝`sql-client.sh -f` 串联：

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

`LAYER=ods|dwd|dim|dws|ads`（缺省跑全部），可观察性靠日志行 `layer=dwd rows=…`。

### 5.4 离线断言 A1/A2/A3（[05] Q11·替换原不变量）

原「ODS = DWD + 拒绝 + 迟到标记差异」**删除**（「迟到标记差异」不是可加项），换三条各自可判的断言：

| # | 断言 | 判据 |
| --- | --- | --- |
| **A1 分流完备** | `count(DISTINCT event_id)` 在 ODS/DWD/拒绝表三者闭合 | `ods_distinct = dwd_distinct + reject_distinct`，**全局比对**（ODS 无事件日分区，降为全局粒度，可判性不变） |
| **A2 重复记账** | `count(*) − count(DISTINCT event_id)` 记为重复率 | **只入日志、不作断言**（承认恒真；它是「为什么不能可加」的实例） |
| **A3 迟到对账** | `count(*) FROM dwd WHERE is_late=1` ＝ `sum(cnt) FROM dwd_late_event_count` | 「只存计数」的唯一用途 |

**其他离线判据**：

- **幂等（Q9 离线版）**：同批 dump 重放两遍，DWD `count(*)` 与逐列校验和（`SUM(crc32(concat_ws('|', event_id, is_late, late_lag_ms)))`）两次一致（`REQ-F-VERIFY-02` 幂等验证的离线版；SRS 那条只覆盖 Doris）。
- **快照 [5,20]**：`num-retained.min=5`（显式）＋ `num-retained.max=20`（把「受控」变成有限数）＋ `time-retained` 保持 1 h 默认；判据 SQL `SELECT count(*) FROM <t>$snapshots` 落 **[5,20]**；`replay` 末尾 `sys.expire_snapshots(retain_max => 20)` 收**上界**（`retain_max` 是上界不是「钉住」）。
- **拉链断言分两条**：批读会丢 DELETE ⇒ 当前快照的区间校验走普通 SELECT，「变更发生过」走 `t$audit_log`。

### 5.5 断言执行面：为什么是 `lake_assert.sh`（[05] 断言执行面）

**源码事实**：`sql-client.sh -f` 语句报错后**仍以 0 退出**（`CliClient` 丢弃返回值、无 `System.exit`）⇒ **退出码不可用**。⇒ 每条断言写成哨兵：

```sql
SELECT '<断言名>' AS assertion, COUNT(*)>0 AS ok FROM …
```

`scripts/lake_assert.sh` 判定「无 `[ERROR]` **且** 每条哨兵 `ok=true` 字符串出现」，解析 stdout。

### 5.6 P2 部署硬约束（两条，HLD §5 未写过）

1. **warehouse 落具名卷、同路径挂进 JM 与 TM**：本仓 JM/TM 是两个容器、`/opt/flink/...` 不互通 ⇒ 新增 named volume（如 `paimon-warehouse:/warehouse`）挂两个 Flink 容器。官方把 `file:` 背书到 "single-machine tutorial"，分布式集群须 shared storage。
2. **Paimon jar 进每个 Flink 进程**：官方逐字 "Install one matching Paimon bundled connector in **each** Flink process."；`ADD JAR` 宿主路径不行 ⇒ 构建期 `COPY` 进 `/opt/flink/lib`（JM 和 TM 都要）。

---

## ⑥ 看板交付：五面板、三态、零账号

> 主判据＝**浏览器能看能刷**（SRS `REQ-F-BI-01..03`）；zip 是 `make init` 的复现**手段**（gdd §1.7），两者都留。官方取证全部 **Superset 6.1.0 tag 源码**（config.py / MainPreset.ts / importexport.py / dashboard importer）。

### 6.1 五个面板与内置图表

| 面板 | 内置图表 | 备注 |
| --- | --- | --- |
| 实时 PV/UV/GMV | Big Number + ECharts Timeseries Bar | 默认注册 |
| 4 步漏斗 | **ECharts Funnel** | legacy Funnel 已不在 6.1.0 注册清单 |
| 品类分布 | **ECharts Treemap** | legacy Treemap 已移除，ECharts 版默认内置 |
| 地域·**排行** | ECharts Bar 横向（orientation） | ⚠️ **方格地图无内置实现路径**：`deck_geojson` 需 GeoJSON 列＋默认 OSM 底图，**违背「不依赖 GIS/底图」** ⇒ 裁决**改横条排行**（`REQ-F-BI-01` 只要求「地域分布」不指定形式） |
| 漏斗双列（观测/推断同屏） | Table 条件格式 或 两张 Funnel 并排 | ECharts Funnel 单值图做不到同屏两列 |

### 6.2 三态载体：B 为主，A 式补充（[06] Q1）

| 变体 | 画法 | 裁决 |
| --- | --- | --- |
| **B 单表行内状态** | Table + 自定义 SQL `CASE WHEN is_settled=0 THEN '暂定' WHEN revision>0 THEN '已修订' ELSE '落定' END` + 条件格式 | ✅ **三态主载体**（[03] Q2「同表同一行」的最直译；三态+revision+as-of 同行可见，零插件） |
| **A 当前窗口双块** | 暂定 Big Number 与落定并排 | ✅ **补充**（当前窗口「暂定感」；跳动的那个数是暂定值） |
| **C 单图双色叠加** | 暂定半透明/落定实色/修订描边 | ❌ **不实现**（「修订描边」需 DOM 挂钩，CSP `script-src: 'self' 'strict-dynamic'` 禁自定义 JS；`style-src` 含 `'unsafe-inline'` ⇒ CSS 可注入、JS 无路径）。**留原型证据** |

> 原型一手证据：[`prototypes/superset-constraints.prototype.html`](../prototypes/superset-constraints.prototype.html)。

### 6.3 刷新判据（`REQ-F-BI-02`，已签）

- **一手事实**：`DASHBOARD_AUTO_REFRESH_INTERVALS` 共 10 档，**最小自动档＝10 s 恰压上限**，**无 1 s/5 s**。
- **矛盾显性化**：数据粒度 60 s ⇒ 相邻两次 10 s 刷新之间 8 次里约 7 次「刷新到了但值没变」。
- **判据（签字版）**：「观察到数值变化」＝**窗口关闭（暂定→落定）与迟到修正（revision+1）时刻，面板值确实变化**；10 s 刷新只承担「链路活着」。GIF 证据录**窗口关闭瞬间的一次跳变**＋一次修订变色。

### 6.4 脏率面板（`REQ-F-BI-04`，SHOULD）

- **分子只含真脏**：`dirty_rate = dlq_count_delta / total_events`，**重复不进分子**（重复走幂等吸收计数通道）。
- **基线＝`verify-fault` 输入冻结快照**（kill 前 DLQ 计数）；面板只显示「当前值 vs 基线 Δ」。
- ⚠️ **「故障演练期间不得飙升」的判据是 `verify-fault` 的退出码断言（DLQ 增量==0），不是面板目测**（「UI 里能看到」不是判据）。
- 基线脏率数值本身是**设计值**（`--chaos-ratio` 决定，`官方未给出`）。

### 6.5 零账号四步链（`REQ-F-BI-03`）

| # | 配置 | 判据映射 |
| --- | --- | --- |
| 1 | `AUTH_TYPE = AUTH_ANONYMOUS` | 前提 |
| 2 | `PUBLIC_ROLE_LIKE = "Gamma"` | 匿名可见 |
| 3 | 显式 dataset 授给 Public | 面板有数据（Public 只给 UI/API 权限，须另授具体 dataset） |
| 4 | 评审 URL `?standalone=1` | 演示干净 |

**面板层判据**：无痕浏览器直接开 `127.0.0.1:8088` 看到刷新。

### 6.6 交付形态与新硬约束

- **zip 保留**为 `make init` 的复现手段（`import-dashboards -p <zip>`，`importexport.py` docstring 源码级）；**判据归 SRS**（浏览器能看能刷）。
- ⚠️ **新硬约束（源码级）**：dashboard importer 逐字 `config["database_id"] = database_ids[config["database_uuid"]]` ⇒ **dataset 经 `database_uuid` 匹配目标实例的 database**。`make init` 建 `ecom_ads` 连接时**必须固定/回填该 UUID**，否则导入后全部图表断链。

---

## ⑦ 逐字修订清单（施工会话机械执行）

> **本节是施工会话的落笔清单**：按文件分组「文件 → 定位 → 原句 → 新句」。**spec 会话只汇编，不动上游文件**。输入源＝`canonical-truth.md` §2/§4/§6 ＋ 各票 Answer 末节落笔分工。

### 7.1 SRS 修订（意图类·已签字）

| # | 定位 | 新句 |
| --- | --- | --- |
| **1** | `REQ-F-GEN-07` | **`--late-correctable`（默认 3%）／`--late-expired`（默认 2%）**：分别把该比例事件的事件时间置于水位线之前（`--late-correctable` 落 2 分钟允许延迟内、`--late-expired` 超期只计数），偏移**一律相对水印**而非墙上时钟计算，制造可复现乱序 \| 设 `--late-correctable 0.03 --late-expired 0.02` 后，实时侧窗口修正段确有迟到事件落入、超期段确有迟到计数条目 |
| **2** | §3.3 规则① | `event_time` 不晚于当前 +5 分钟，且**不早于「本批次最早事件时间 − 允许乱序幅度」**（批次基准时间＝本批摄入事件的最早 `event_time`；允许乱序幅度取实时侧允许延迟，默认 2 分钟）；该下界为**相对**下界，对生成器产出与公开数据集回放同时成立 |
| **3** | `REQ-NF-CAP-01` | 容器群常驻内存 **≤ 8 GB**（天花板约束，**设计值**）。判据按**两个口径**分别断言：①**声明上限**（全部服务 `mem_limit` 之和，含 profile 内服务）≤ 8 GB；②**实测占用**（`docker stats --no-stream` 合计）≤ 8 GB。⚠️ 两者不可互相冒充：P0 实测空载六服务合计 ≈3.83 GiB，声明上限合计 10.5 GiB（10,752 MiB，含 `client` 256m），已按官方默认下调后落在 11.68 GiB 天花板内。**P1 带载后须复测，带载未实测即未通过** |
| **4** | `REQ-F-ORCH-02` | 同 #3 双口径（同改） |
| **5** | `REQ-F-ORCH-01` | **`make init`** 一键拉起整条链路（**含 Doris profile 与初始化脚本**）\| **在已有 Docker + make 的机器上**（L0 制品态）`make init` 后 `make verify` 退出码 0、`docker compose ps` 全 `healthy`；四档契约见 `reproducibility-boundary.md` §1 |
| **6** | `REQ-NF-CAP-03` | **预拉镜像前提下**，`make init` 到**链路全 healthy** **≤ 15 分钟**（**不含出数**：看板出数依赖 P1 的真实 Flink 作业与 Doris 写数，P1 未实现即不可测）。「到看板出数 ≤ 15 分钟」作为**同一需求的第二截**，判据转 P1 实测 |
| **7** | `REQ-NF-CAP-04` | **预构建镜像经 `docker save`/`docker load` 导入后**，L3 跨机搬运冷启动 ≤ 30 分钟。⚠️ **「本地构建 Doris 镜像」不计入本判据**（实测构建 ≈35 min；Doris 无官方版本化镜像，构建不可避免） |
| **8** | `REQ-F-VERIFY-01` | 五项烟测中「Paimon 五层有行」在 **P1 里程碑判定中显式 SKIP、不计入通过条件**；**P2 里程碑判定中该项必须为真断言且退出码参与判定** |
| **9** | `REQ-F-VERIFY-02` | 幂等验证「**不进 `verify` 每次运行**」，改由离线幂等断言（第 5.4 节）＋ `verify-fault` 恢复重放覆盖 |
| **10** | `REQ-F-BI-02` | 「观察到数值变化」＝**窗口关闭/修订时刻值确实变化**（非「每次刷新值都变」）；10 s 刷新只证「链路活着」 |
| **38** | `REQ-F-LAKE-06`（**2026-10-21 签**·附录 A M） | 五层全部落 **单一 Paimon catalog**，该 catalog 下的 **`ecom_lake` database** 承载全部五层表（原文「单一 catalog（库名 `ecom_lake`）」一词混说 catalog 与 database）\| 五层表均存在于该 catalog 的 `ecom_lake` database 内（`SHOW TABLES IN <catalog>.ecom_lake` 逐层可查） |

### 7.2 gdd 修订

| # | 定位 | 改动 |
| --- | --- | --- |
| **11** | gdd.md:6 | 「**下游**：`spec.md`（实施规格，另行开会话产出）」→「**下游**：本 spec 施工规格；**收口**：`canonical-truth.md`」 |
| **12** | gdd.md:437 | 「按 Q8：不改 `SRS.md`，等 `spec.md` 会话统一收口」→ 指向 `canonical-truth.md` §1 |
| **13** | gdd.md:534-535 | 「`SRS.md`/`FSR.md` 未动，等 `spec.md` 收口」→ 同上 |
| **14** | gdd.md:538 | 「**下一层文档**：`spec.md` 由你另开会话产出」→「下一层文档：本 spec；收口产物＝`canonical-truth.md`」 |
| **15** | gdd §1.3/§1.4 | 补漏斗行修正语义（第 4.2 节）、UV 对账断言条文（含 `is_settled=1` 限定与 P1 SKIP）、不变量替换为 **A1/A2/A3**、快照判据补 `[5,20]` 与 `$snapshots` SQL、补 `upsert-materialize=NONE` |
| **16** | gdd §1.8 | `verify` 分层进码语义、**`verify-fault` 参数化判据**（120 s/300 s 超时与两表基线，第 4.4 节） |
| **17** | gdd D6.1 | `start_date/end_date` → **`valid_from_ms`/`valid_to_ms`**（BIGINT ms）；`dim_region`/`dim_channel` 标注静态无拉链 |
| **18** | gdd §1.5 | `partial_columns=true` **不是表属性**（是导入 header/`sink.properties.*`）；普通 `partial_columns` 受 **Batch constraint** 限制（一批内只能更新同一组列）；同批多列组须用 3.1.0+ 灵活列更新（JSON＋`skip_bitmap_column`，**禁用 `sequence_col`**） |
| **19** | gdd §1.6 | 三态载体段落（B 为主＋A 式补充）；**方格地图条目改横条排行** |
| **20** | gdd §1.7 | zip 定位句（手段归 §1.7、判据归 SRS）＋ **`database_uuid` 硬约束**（第 6.6 节） |
| **21** | gdd D12 | 补记 Group Window Aggregation 在 2.2 **官方文档标 deprecated**、源码 "will be dropped" 的事实（[08] D-2） |
| **22** | gdd D10 | 「允许延迟内→窗口修正」**必须落 DataStream 段**（SQL 侧无 allow-lateness，[08] D-11） |
| **23** | gdd D8 | **FE 总开关 `dynamic_partition_enable` 作废**（4.1.5 默认 `true` 不是 `false`）；真正前置＝**表级四项**（enable／显式 start／time_unit+end+prefix／create_history_partition）（[07]） |
| **24** | gdd D3 | Kafka「≈6 h」**须按配置层级重算**（`retention.bytes` **按分区**生效，6 分区下主题级实为 12 GB ⇒ 可能 ≈36 h）；所引逐字句在 Kafka 4.3 官方站未命中（逐字在 Log 实现页）（[07]） |
| **25** | gdd.md:512 | 「Doris 无官方 compose」**已被 gdd.md:197 推翻**（官方有 `docker/runtime/all-in-one/4.1/compose/`）⇒ **勿再引用** |
| **26** | gdd §C F16 / §D 引用 | 「gdd D16」错引（gdd **不存在 D16**，§D 止于 D15；票面引的 `F16` 实为 §C 条目）⇒ 标注为「gdd §C F16」 |

### 7.3 HLD 修订

| # | 定位 | 改动 |
| --- | --- | --- |
| **27** | HLD.md:6 | 「**下游**：`spec.md`」→ 同 #11 |
| **28** | HLD.md:35 | 权威链图末行「`spec.md` 实施规格」→「`canonical-truth.md` 收口 ↔ 本 spec 施工规格」 |
| **29** | HLD.md:412 | 「`SRS.md`/`FSR.md` **一个字都不改**（等 `spec.md` 收口）」→ 按 G1 改写为「事实类可直接改写上游；意图类保留原话并登记」 |
| **30** | HLD.md:7 与 gdd.md:8 | 「**不修改** `SRS.md`/`FSR.md`」自述**从未获 SRS/FSR 背书**（两文件无「不得修改」条款、无变更控制章节）⇒ **一并订正**（随 #29 同批） |
| **31** | **HLD.md:294 表格** | 「10.25 GiB（10,496 MiB）…余量 6.43/**1.43** GiB」→「**10.5 GiB**（10,752 MiB）…余量 6.43/**1.18** GiB」（同文件第二个真相源） |
| **32** | HLD §2.5 横切面① | `schema_version` 语义唯一所有者；规则全文指针 → canonical-truth §3.1（第 2.1 节六处落点之一） |
| **33** | HLD §5 设计面 | 新增：`paimon-warehouse` named volume、Paimon jar 构建期 COPY、`sql/replay_*.sql` 目录布局、迟到计数表名入冻结面清单（FC-2）、`artifacts/verify/` 报告目录与 JSON 结构 |
| **34** | HLD §7.1 | 登记偏离行（原口径把「构建 Doris 镜像」隐含在「拉镜像」内，实测 ≈35 min ⇒ 原口径不可达） |
| **39** | HLD §8-R5 ＋ §7.1（**2026-10-21 签**·附录 A N） | Paimon 主键表**默认 MOR 而非 MOW**（MOW 需显式 `deletion-vectors.enabled`）⇒ §8-R5 的空间放大前提原按 MOW 估算，须**按 MOR 复核**；本仓五张主键表固定 MOR，官方 bucket 指引 "200 MB–1 GB per bucket" 是 MOR 口径。偏离进 §8-R7 登记 |

### 7.4 冻结项变更（已批准，随本批落笔）

| # | 变更 |
| --- | --- |
| **FC-1** | Makefile **新增 `lint` / `typecheck` 目标**（`verify` 保持冻结、只做运行时烟测；`make check` **不新增**，原生多目标并列） |
| **FC-2** | Paimon 表名清单**新增迟到计数表** `dwd_late_event_count` |

### 7.5 实现缺口（doc ≠ reality·归 [09]＋P1）

| # | 缺口 | 处置 |
| --- | --- | --- |
| **35** | Doris 断言在宿主跑（`Makefile:76-78` 宿主侧 `curl 127.0.0.1:8030/api/health`，只查 FE 健康不查 ADS 有行），与 HLD §4.4/§8-R2「Doris 断言必须经 `client`」矛盾 | 随 P1 施工**改为经 `client`** 并补真实断言（查 ADS 有行） |
| **36** | `minio`/`spark` profile 不存在（`REQ-F-ORCH-08` SHOULD 要求两个可选 profile） | **待签字的意图类**（实现 or 撤需求），挂 P2——**不在本 spec 默认范围**，见第 ⑧ 节 |
| **37** | `research-notes.md` 两处非官方清单（`$files`/`$snapshots` 列名；`partial_columns=true` 作为表属性） | 事实类**直接订正**（[08] D-5/D-9） |

---

## ⑧ 不做清单（Out of Scope）

> 单向引用 `map.md` 的 `Out of scope`，**不复制**（防第二真相源，[09] Q7）。

### 8.1 地图级不做（单向引用 map.md Out of scope）

- **本机环境的一次性动作**（Docker Desktop 换盘/重装、全局网络设置、宿主 JDK/Maven 面调整）——属环境事实，已在 HLD §4 落定。
- **`docs/adr/` 的建立**——不在仓库时静默继续；本 spec 不产 ADR。
- **推送与 GitHub Issues**——本仓 issue 后端是本地 markdown，`deck_*` GitHub 后端不适用。
- **面试材料与简历 bullet 的成稿**（P3 的一部分，不属 spec-ready 判据）。
- **亿级回放的实际执行**（`REQ-NF-CAP-06`）——只定口径，不排期。
- **Spark 离线对照 profile 的实现**（`REQ-F-ORCH-08` 的一半）——只留判据。

### 8.2 各票判不做项

| 项 | 理由 |
| --- | --- |
| **两组 ADS 各自供看板** | **两套物化一个口径**：Paimon `ads_window_metric` 只作离线口径基准，Doris `ecom_ads` 是唯一供数点 |
| **「离线重算覆盖实时行」** | 记 COULD 留档，**P2 判据不含它** |
| **幂等进每次 `verify`** | 全量重放两遍是重操作；改由离线幂等断言＋`verify-fault` 覆盖 |
| **排序 compaction（sort compact）** | 官方逐字仅支持 `bucket=-1`＋batch 模式，本仓主键表是固定 bucket |
| **lookup join / zipper / SCD** | lookup join 被官方逐字否定（processing-time，回放必错）；Paimon 全站**无** zipper/SCD 官方能力 |
| **CUMULATE 作提前发射替代路径** | 走 TVF 会被 `checkEmitConfiguration` **直接抛异常**（[08] D-14） |
| **`sequence.field`** | 同 `event_id` 重复行该值相同 ⇒ 回落内部序，白设；靠重放确定性 |
| **方格地图（deck_geojson）** | 需 GeoJSON 列＋OSM 底图，**违背「不依赖 GIS/底图」**；改横条排行 |
| **三态变体 C 实现** | 「修订描边」需 DOM 挂钩，CSP 禁自定义 JS；**留原型证据** |
| **`make check` 新目标** | `make lint typecheck` 原生多目标并列，零冻结项变更（FC-1 已新增 `lint`/`typecheck`，不加 `check`） |
| **L0 零状态实测（`down -v`）** | 需销毁 P0 已验证的卷态成果；L0/L1/L3 标「待测」 |
| **工具链自举进「一条命令」** | JDK/Maven 只服务 Flink 作业构建，不在起链路路径上；bootstrap 脚本留 P1 |
| **三张实机 task（自开）** | HOUR 演练实机 / DROP 后回盘实测 / compaction×20 GB 对撞——**须先获批**，不可自开 |

### 8.3 官方未给出（不猜、只登记）

各票的「官方未给出清单」是施工时**遇到就登记、不臆造**的清单，重点几条：`sys.compact` 对主键表的逐句适用声明、Flink SQL 断言失败→非零退出码机制（**源码显示不存在**）、Paimon `file://` compaction 成本数字、Group Window Aggregation `deprecated` 的移除时间表、`AUTH_ANONYMOUS` 注释逐字、`partial_columns` 作为建表属性（**官方文档与源码均无**）。

---

## ⑨ 追溯表（发布闸门）

> **闸门规则（[09] Q9）**：本表逐行＝D1–D12 ＋ `canonical-truth.md` §4 改写清单 ＋ 各票落笔分工条目。**每行须在本 spec 有落点章节；全命中才发布**。责任人＝spec 撰写会话。

| 行 | 来源 | 本 spec 落点 | ✓ |
| --- | --- | --- | --- |
| D1 | 看板终点＝Superset | 第 ⑥ 节（五面板＋内置图表） | ✓ |
| D2 | 平台形态·类型门禁·工具链·会话沙箱 | 第 ③ 节指针（`client` 两跳、basedpyright pin 1.40.2 在 HLD §3.4）＋第 ⑥.5 节零账号 | ✓ |
| D3 | 执行面＝WSL2·超集 schema | 第 ③ 节指针（HLD §4.7）＋门槛批超集 schema 见第 5.1 节 | ✓ |
| D4 | 配置确定性与可复现＝立项基石 | 第 ③ 节（P0 薄写）＋ `reproducibility-boundary.md` 指针 | ✓ |
| D5 | 唯一真相源＝`canonical-truth.md` | 第 1.2 节分工表＋第 ② 节三组规则铁律逐字 | ✓ |
| D6 | Paimon/Flink/Doris 用法一手事实 | 第 4.6 节作业形态官方硬约束＋第 5.2 节主键默认 MOR＋第 ⑦ 节 #18/#21/#22 | ✓ |
| D7 | 可复现主体＝制品·四档契约 L0–L3 | 第 3.1 节＋第 3.3 节「一条命令＝`make init`」四段 | ✓ |
| D8 | Doris 容量与可证性 | 第 ⑦ 节 #23（gdd D8 修订·表级四项）＋第 ⑧ 节官方未给出 | ✓ |
| D9 | 离线分层范围＋五层形状 | 第 ⑤ 节全节（门槛批、五层形状表、全 SQL 编排、A1/A2/A3、部署硬约束） | ✓ |
| D10 | 语义与判据（漏斗/UV＋verify 分层） | 第 4.2 节语义＋第 4.3 节 verify 分层＋第 4.4 节 verify-fault＋第 ⑦ 节 #8/#9 | ✓ |
| D11 | 看板交付三态画法×五面板×刷新×零账号 | 第 ⑥ 节全节＋第 ⑦ 节 #19/#20 | ✓ |
| D12 | 施工规格蓝图与闸门 | 本 spec 全篇（第 ①–⑨ 节） | ✓ |
| CT-§4.1 | canonical-truth 七处 `spec.md` 空指针 | 第 7.2 节 #11–14＋第 7.3 节 #27–28 | ✓ |
| CT-§4.2 | 冻结项变更 FC-1/FC-2 | 第 7.4 节 | ✓ |
| CT-§4.3 | 三处就地订正（F-1 数字、gdd D16 错引 ×2） | 第 7.3 节 #31＋第 7.2 节 #26 | ✓ |
| CT-§2 | 三条签字逐字新句（D/H/I） | 第 7.1 节 #1/#2/#3 | ✓ |
| CT-§1-A | 冲突 A 快照数（判据须走 Flink SQL） | 第 5.4 节快照 [5,20]（取值随 [05]）＋第 5.5 节（判据走 Flink SQL） | ✓ |
| CT-§1-B | 冲突 B UV 改精确＋一致性校验 | 第 4.2 节 UV 精确＋第 7.1 节 #8（对账断言措辞随本条） | ✓ |
| CT-§1-C | 冲突 C 迟到侧输出（已由 [08] 证实） | 第 4.6 节第一行 | ✓ |
| CT-§1-E | 冲突 E 动态分区（判据含「分区被实际删除」） | 第 7.1 节 #8 周边＋第 8.2 节 HOUR 演练 task（须获批） | ✓ |
| CT-§1-G | 冲突 G 重复摘出独立通道 | 第 4.4 节重复不进分子＋第 5.2 节拒绝表三值无 duplicate | ✓ |
| CT-§1-J | 冲突 J 回放走 dump 不重读 Kafka | 第 5.1 节门槛批（dump 现产）＋第 7.2 节 #24（按层级重算） | ✓ |
| CT-§1-L | 冲突 L Kafka `KAFKA_LOG_DIRS` | 第 8.2 节外；`KAFKA_LOG_DIRS` 已实测入库（HLD §8-R7-e） | ✓ |
| T04-§8 | 可复现四条逐字新句（ORCH-01/CAP-03/CAP-04/HLD:294） | 第 7.1 节 #5/#6/#7＋第 7.3 节 #31 | ✓ |
| T04-§9 | doc≠reality 三缺口 | 第 7.5 节 #35/#36 | ✓ |
| T05-上半 | 强序＋判据分层・门槛批・单目标 replay・不新增 profile | 第 4.3 节阶段闸门＋第 5.1 节门槛批＋第 5.3 节 `LAYER=` | ✓ |
| T05-下半 | 五层形状表・A1/A2/A3・快照・全 SQL 零 jar・warehouse 硬约束 | 第 5.2 节＋第 5.4 节＋第 5.3 节＋第 5.6 节 | ✓ |
| T05-对08 | 两处订正 R1/R2 | 第 5.2 节 bucket 口径（MOR／append unaware）＋第 5.4 节快照（`retain_max` 上界） | ✓ |
| T03-A | 语义四问（漏斗修正／同表三态／对账口径／唯一键不含 revision） | 第 4.2 节 | ✓ |
| T03-B | 判据五问（verify 分层／verify-fault 参数化／负断言基线／不新增 check／fail 三层） | 第 4.3＋4.4＋4.5 节＋第 8.2 节（不新增 `make check`） | ✓ |
| T06-Q1 | 三态载体 B 为主＋A 补充 | 第 6.2 节 | ✓ |
| T06-Q2 | 刷新判据改写（已签） | 第 6.3 节＋第 7.1 节 #10 | ✓ |
| T06-Q3 | 五面板＋方格地图改横条 | 第 6.1 节 | ✓ |
| T06-Q4 | 脏率基线＝DLQ 快照 | 第 6.4 节 | ✓ |
| T06-Q5 | 零账号四步链 | 第 6.5 节 | ✓ |
| T06-Q6 | zip 定位＋`database_uuid` 硬约束 | 第 6.6 节 | ✓ |
| T08-D | 副作用面 14 条中改变设计的八条 | 第 4.6 节（D-1/D-11/D-14）＋第 5.2 节（D-7）＋第 7.2 节 #18/#21/#22（D-2/D-9/D-11） | ✓ |

> **闸门自检：全行命中。** 未签字的新增冲突 M/N/O 记附录 A，**不阻塞发布**（按 G1，事实类可先按事实落笔，签字类等裁决）。

---

## 附录 A · 冲突待回票（裁决已回写）

> 走 [09] Q10 兜底：**不回开地图**；卡住施工口径时以追加问句请用户裁决，裁决后回写本附录或第 ⑦ 节。
> ✅ **2026-10-21 用户签字：M / N / O 三条全部批准落笔**，按 G1「事实类可直接改写上游」处置，无需再登记为待回票。

| # | 冲突 | 状态 |
| --- | --- | --- |
| **M** | `REQ-F-LAKE-06`「单一 catalog（库名 `ecom_lake`）」**一词混说两事**——Paimon 层级 catalog→database→table，`ecom_lake` 官方对应 **database**；原判据不具区分力 | ✅ **已签字（事实类·`改写上游`）**。逐字新句：「五层全部落 **单一 Paimon catalog**，该 catalog 下的 **`ecom_lake` database** 承载全部五层表」\| 判据＝五层表均存在于该 catalog 的 `ecom_lake` database 内（`SHOW TABLES IN <catalog>.ecom_lake` 逐层可查）。**新增为第 ⑦.1 节修订条目 #38** |
| **N** | Paimon 主键表默认 **MOR 而非 MOW**（MOW 需显式 `deletion-vectors.enabled`） | ✅ **已签字（事实类·`改写上游` ＋ 偏离登记）**。本仓五张主键表（DWD/DIM×5/DWS/ADS）**固定 MOR**，不启用 `deletion-vectors.enabled`；官方 bucket 指引 "200 MB–1 GB per bucket" 是 MOR 口径。**新增为第 ⑦.3 节修订条目 #39**：HLD §8-R5 空间放大前提须按 MOR 复核（原按 MOW 估算），偏离进 §8-R7 登记 |
| **P** | **F3 · live 确定性**（用户 2026-10-21 裁决）：**内容确定，节奏不确定**。事件内容、顺序、注入位置由 `--seed` 决定，哈希判据针对内容序列；墙钟只控制发送速率，不进入内容。 | ✅ **已裁决（施工口径）** |
| **Q** | **F6 · 降级阶梯触发**（用户 2026-10-21 裁决）：**容器被 OOM 杀两次即降级**。以 `docker inspect` 的 `OOMKilled` / exit 137 计数为机读信号；阈值 2 次为**设计值**，非官方保证。降级后由 verify 把实测值写入报告。 | ✅ **已裁决（施工口径）** |
| **R** | **F7 · 世界时钟**（用户 2026-10-21 裁决）：**event_time 相对本轮起点平移，墙钟起点**。`--seed` 只决定相对偏移；历史日期不进入 live 流，窗口由 Flink 水印按平移后的时间关闭。 | ✅ **已裁决（施工口径）** |
| **O** | Doris `partial_columns` **非表属性**＋**Batch constraint** 限制 | ✅ **已签字（事实类·`改写上游`）**。**不新增冲突条目**——第 7.2 节 #18 已逐条记入 gdd 修订（`partial_columns=true` 是导入 header/`sink.properties.*` 而非表属性；普通 `partial_columns` 受 Batch constraint 限制；同批多列组须用 3.1.0+ 灵活列更新，JSON＋`skip_bitmap_column`，**禁用 `sequence_col`**）。施工面按 #18 执行即可 |
