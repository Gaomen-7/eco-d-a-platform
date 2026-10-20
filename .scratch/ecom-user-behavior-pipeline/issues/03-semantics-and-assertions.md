# 03 · 语义与判据：漏斗/UV 的窗口语义 + 判据强度 + 「行为可证」的统一断言形式

Type: grilling
Status: resolved
Resolved: 2026-10-20
Claimed by: agent (2026-10-20, frontier session)
Blocked by: 02, 05
Blocked by (note): `02` 指**地图票 D4（已落子）**；`05` 是真实票号（[离线分层范围](05-offline-layer-scope.md)）。
Created: 2026-10-20（原 05 + 09 合并）
Ticket: 03
Source: map.md · GLOSSARY「转化漏斗」「滚动 UV」「落定值/暂定值」· gdd §1.3 / §1.8 · SRS §3.1.8 · HLD §6

## Question

这张票回答一个问题：**「这个量算对了没有」在实时链路上怎么写成一个断言。** 两半：同一批数字既要有语义，也要有判据。

### A · 语义：三条时间轴在同一个 Doris 表里

`GLOSSARY.md` 立了硬区分：漏斗的 1 分钟窗口只决定「每分钟统计多少条步进」，漏斗本身按 `user_id` 步进推进、与窗口正交；滚动 UV 是「最近 1 小时去重用户数」。于是 `ecom_ads` 里至少三个时间轴并行：

| 量 | 时间轴 | 状态规模 | 谁在算 |
| --- | --- | --- | --- |
| 每分钟 PV / GMV / 品类·地域分布 | 1 分钟滚动窗口 | 窗口态 | Flink SQL 窗口聚合 |
| 滚动 UV（最近 1h） | 1 小时滚动 | 键控去重 ~MB 级 | Flink SQL / DataStream |
| 四步漏斗 | 会话（30 min 空闲切断）+ 窗口只做客数 | 键控状态 TTL 32 min | DataStream（`autofill` + 双列并陈） |

1. **多行语义**：`revision` 递增的覆盖写只发生在窗口聚合行，还是漏斗行也会被迟到事件修正（一条 `purchase` 迟到 90s，算不算修正了某分钟的漏斗客数）？
2. **提前发射**（gdd §1.3-4）：每 10s 的「暂定值」与 1 分钟落定值，是同一张表的同一行（靠 `revision`/状态列区分），还是两张表？两张表的代价是唯一键不同，gdd D1 的「一层主防」前提要不要重证。
3. **对账口径**：`gdd §1.4` 要「实时滚动 UV 与离线精确 UV 逐窗口相等」；离线是 Paimon 批，窗口切分必须与实时完全一致才算得出相等——口径写在哪、谁维护。
4. **补记与单调**：`--chaos-ratio` 下 `funnel_cnt_observed` 可非单调、`funnel_cnt_inferred` 必须单调；看板同屏展示两列时，唯一键还包不包 `revision`。

### B · 判据：`verify` / `verify-fault` 的退出码写到哪一层

`gdd §1.8` 给了五项烟测的确切断言（`jobs[].state == "RUNNING"`、`/health` 纯文本 `OK`、`count(*)` 而非 `total_record_count`…），但 **`verify-fault` 只有「条数一致」「脏率不上升」两句自然语言**；gdd §1.3 的四条行为可证判据与 §1.4 的不变量断言**是否进入退出码**也没人说。

5. **退出码语义**：`verify` 退出 0 表示「五项全过」，还是「五项 + 幂等 + lag + 不变量 + 脏率」全过？
6. **`verify-fault` 的机器判据**：kill TM → 等 RUNNING → 比对 Doris `count(*)`；「等多久」、比对**哪张表**、「脏率不上升」的分子分母各取什么。
7. **负断言怎么写**：`gdd §1.8` 的「条数一致但脏率飙升 = 不通过」——基线脏率从哪来。
8. **`lint`/`typecheck` 的位置**：HLD §3.3-6 明确不混入 `verify`；将来要不要一个 `make check` 总入口（要，就是冻结项变更）。
9. **fail 的痕迹**：断言结果落 stdout 行、JSON 文件、还是 Flink REST 原始响应快照（横切关注点 2「失败必须有痕迹」）。

## 约束

- HITL。可以直接起一个 **AFK 原型**（单文件 HTML，照 `prototypes/funnel.prototype.html` 的方式跑断言）把「1h UV × 1min 窗口 × 30min 会话」叠加起来看——原型是手段，裁决仍是你。
- 不改 `GLOSSARY.md` 已定义的概念；有冲突就在 Answer 里提议修订词条。
- 每条判据裁定都要给**脚本可执行的形式**（命令 + 解析方式），否则等于没定（HLD §0.4）。
- 不改八个冻结目标名；加 `make check` 要走冻结项变更，并在 Answer 单列「需你批准」。

## Answer

**Resolved 2026-10-20（frontier session）。** 10 问（Q0–Q9）一轮全部按推荐落子（用户：「全按你推荐」）。A 部分先跑 AFK 原型的手段被否（Q0）——票面语义问题全部是「已有决策的记账方式」，非未验证行为假设，且事件处置/漏斗两个原型已实跑背书；裁决后前沿即清空。

### A · 语义（Q1–Q4）

| # | 决策 | 内容 |
| --- | --- | --- |
| **Q1** | **漏斗行同样被迟到修正** | `purchase` 迟到 ≤120 s 落入已落定窗口 ⇒ DataStream `allowedLateness` 重发 ⇒ 该分钟漏斗客数被修正、`ads_funnel_step.revision` +1，与窗口聚合行同机制。依据：TTL 32 min ＝ 30 min 会话 + 2 min 允许延迟（gdd §A.1），这 2 min 就是为修正留的；超期（>120 s）只计数、绝不修正（GLOSSARY「迟到」冻结）。**修正面＝窗口聚合行＋漏斗行，`revision` 语义两表同构** |
| **Q2** | **同表同一行** | 不建两张表。`ads_window_metric` 以 `(window_start, metric_name, dim_key)` 唯一键承载三态：提前发射写暂定行（`is_settled=0`）→ 窗口关闭覆盖落定（`is_settled=1`）→ 迟到修正同 key 覆盖（`revision`+1）。gdd D1 的「一层主防」（Doris unique key 幂等吸收重复写）**无需重证**——三种写入走同一 key，主防前提原样成立；「两张表唯一键不同」的代价不发生 |
| **Q3** | **对账口径不新增文件** | 窗口切分口径已由 gdd §A.1（参数唯一权威）承载：60 s 滚动、事件时间、UTC epoch 对齐（`window_start = floor(event_time_ms/60000)×60000`）；UV 按 1 h epoch 对齐同理。本票新增**断言**：P2 起 `lake_assert.sh` 逐 `metric_name` 比对 Paimon `ads_window_metric`（离线口径基准）与 Doris `ecom_ads.ads_window_metric`（看板供数点），**只比 `is_settled=1` 的行**，相等＝逐值严格相等（非误差量化）；P1 阶段该断言显式 SKIP（与 [05] Q1 阶段闸门同构）。断言条文落 gdd §1.4，落笔归 [09] |
| **Q4** | **唯一键不含 revision** | `ads_funnel_step` 唯一键维持 `(session_date, step_no, window_start)`——revision 是值列，入键反而制造「同一修正多行」。单调断言（可执行）：判定 `cnt_inferred` 沿 `step_no` 1→4 **非增**，违反即 fail；`cnt_observed` **不设单调断言**（`--chaos-ratio` 下真实跳步合法非单调，GLOSSARY 两列语义冻结） |

### B · 判据（Q5–Q9）

| # | 决策 | 内容 |
| --- | --- | --- |
| **Q5** | **分层进码** | `verify` 退出 0 ＝ 五项烟测全过（P1：4 真 + 1 显式 SKIP，SKIP 不计通过不计 fail；P2：5 真）＋ 附带 lag 断言 ＋ 可即时判定的行为判据（脏率不升、漏斗单调——纯 Doris SQL 可判）。**场景依赖判据**（`revision>0` 出现、UV 对账）带「场景前置」标记：仅在对应注入器/离线场景激活时判定，否则 SKIP——不是永久豁免。**幂等（`REQ-F-VERIFY-02`）不进 `verify` 每次运行**（全量重放两遍是重操作）：P2 起由 [05] Q9 离线幂等断言 + `verify-fault` 恢复重放共同覆盖；SRS 措辞修订归 [09] |
| **Q6** | **`verify-fault` 全参数化** | 输入先冻结：停生成器（或固定 dump 回放）→ 等 lag==0 → 快照基线（`ads_window_metric` 与 `ads_funnel_step` **两张表**的 count+sum、DLQ 计数、Kafka committed offset）→ kill TM → 轮询 `/jobs/overview` 到 RUNNING **超时 120 s**（超时即 fail）→ 等 lag==0 且此后完成一次 checkpoint **超时 300 s** → 比对两表 count/sum 与基线**全等**、重复通道计数**上升**（证明确有重放且被幂等吸收）。脏率分子＝DLQ 消息增量，分母＝输入事件总数（输入冻结后为常数；重复不进分子，GLOSSARY 冻结） |
| **Q7** | **负断言基线＝kill 前 DLQ 快照** | 因输入冻结 + `--seed` 确定性，该值同时应等于 seed 决定的期望脏数（两处对不上本身就是断言）。负断言：恢复后 `DLQ 计数 == 基线`（增量 0）**且** `重复通道增量 > 0`；若条数一致但 DLQ 上升 ⇒ fail（gdd §1.8 反例保护：重复被误当脏）。无需历史基线文件 |
| **Q8** | **不新增 `make check`** | `make lint typecheck` 原生支持多目标并列，总入口冗余；`verify` 保持纯运行时烟测（HLD §3.3-6 已定不混）。**零冻结项变更** |
| **Q9** | **fail 痕迹三层** | ① stdout 每条断言一行 `PASS/FAIL/SKIP <名> <实测> vs <期望>`（人读面）；② 退出前写 JSON 报告 `artifacts/verify/verify-report-<UTC时间戳>.json`（设计面命名，逐断言状态）；③ FAIL 时原始响应全文（Flink REST JSON、mysql 哨兵输出、CLI stderr）快照进同目录。横切关注点 2「失败必须有痕迹」＝ ②③ 双兜底 |

### 与 [05] 三条接口的闭合

1. **对账口径**（[05] Q6 移交）→ Q3 收口：断言归本票、键同构由 [05] 保证，两票不重叠。
2. **指标列留白**（[05] Q12 移交）→ 由 Q1/Q2 的语义推定：DWS/ADS 指标列＝`pv/uv/gmv`（`ads_window_metric.metric_name` 枚举）+ 漏斗（`ads_funnel_step` 独立表）+ 品类/地域分布（`dim_key` 承载）；[09] 回填 [05] 形状表。
3. **`verify` 退出码与阶段闸门一致**（[05] Q1 要求）→ Q5 收口：P1 五项＝4 真断言 + 1 显式 SKIP，与 [05] 签字句同构。

### 需你批准（意图类，1 项）

| # | 事项 | 性质 | 落笔 |
| --- | --- | --- | --- |
| 1 | SRS `REQ-F-VERIFY-02` 措辞改写：幂等验证「不进 `verify` 每次运行」，改由离线幂等断言（[05] Q9）+ `verify-fault` 恢复重放覆盖 | 意图类，**需签字** | [09] |

> 签字记录：2026-10-20 用户「全按你推荐」覆盖本票全部推荐答案（含此项）。

### 落笔分工（[09] 输入）

1. SRS `REQ-F-VERIFY-02` 签字句（上表 #1）。
2. gdd §1.3/§1.4：补漏斗行修正语义（Q1）、UV 对账断言条文（Q3，含 `is_settled=1` 限定与 P1 SKIP）。
3. gdd §1.8：`verify` 分层进码语义（Q5）、`verify-fault` 参数化判据（Q6/Q7，含 120 s/300 s 超时与两表基线）。
4. HLD 设计面：`artifacts/verify/` 报告目录与 JSON 结构（Q9）；`make check` 不新增的裁定留痕（Q8）。
5. [05] 形状表指标列回填（上文接口 2）。
6. GLOSSARY：无修订（Q0–Q9 全部落在既有词条框架内；「落定值/暂定值」「窗口修正」「观测值/推断值」原样适用）。

### 官方未给出清单（本票新增）

| # | 事项 |
| --- | --- |
| 1 | Flink `allowedLateness` 重发在 Doris unique key 上的「暂定→落定→修正」三态覆盖序列的官方端到端承诺（各环节官方文档分别给出，组合行为无官方整句；以 `verify-fault`/注入器演练为判据） |
| 2 | Flink 恢复到 RUNNING 后「lag 清零且完成一次 checkpoint」的最长等待时间（120 s / 300 s 均为**设计值**，非官方保证） |

### 迷雾与下游

- F3（live 确定性）/F6（降级触发）/F7（世界时钟）仍挂在 [02]→[03] 链上未收口；F7 与本票 Q3 的 epoch 对齐口径相容（回放时间戳按同一公式切窗），收口仍归 [09]/后续会话。
- 下游 [06]（看板交付）解除阻塞：暂定/落定/已修订三态的画法依据 Q2 同表三态 + `is_settled`/`revision` 两列。
