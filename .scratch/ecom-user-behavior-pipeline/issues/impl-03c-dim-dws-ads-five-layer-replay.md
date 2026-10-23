# impl-03c: DIM 拉链 + DWS 区间 join + ADS 五层贯通

> 父票：impl-03
> 规格落点：spec §5.2（形状表 DIM/DWS/ADS 三行）、§5.3（`replay_30`–`50_ads`）

Type: task
Status: claimed
Blocked by: impl-03b

**What to build:** 维度变更流灌出拉链维表（跨日多版本），DWD 事件按事件时刻做**区间 join** 命中当时有效的维度版本并回填到 DWS 窗口聚合，DWS 再落成 ADS。跑完本票，`make replay`（缺省、不带 `LAYER=`）**五层一次跑通退出 0**。

## 交付内容

1. **三张拉链表** `dim_item`/`dim_category`/`dim_user`：主键 `(id, valid_from_ms)`，**不分区**，拉链区间 `valid_from_ms`/`valid_to_ms`（`BIGINT` UTC epoch 毫秒；`valid_to_ms = 9999999999999` 表仍有效），时间源＝维度变更流的 `change_time`（事件时间语义，满足 D-3）。
2. **两张静态维表** `dim_region`（主键 `(province)`）/`dim_channel`（主键 `(channel)`）：无拉链，同样固定 `bucket=1`、`upsert-materialize=NONE`。
3. **`replay_30_dim.sql`**：维度变更流 → `dim_*`（区间）。
4. **DWS `dws_subject_window`**：主键 `(dt, subject_type, subject_id, window_start)`，`dt`＝窗口起点日，`subject_type`∈{user,item,category}，指标列 `pv/uv/gmv`。
5. **ADS `ads_window_metric`**：主键 `(dt, metric_name, dim_key, window_start)` —— **与 Doris `ecom_ads.ads_window_metric` 键同构**，这是 03f 对账的前提。
6. **迟到计数 `dwd_late_event_count`**：append，`dt` × 60 s 档，**只存计数**。
7. **`replay_40_dws.sql`**（dwd × dim 区间 join）与 **`replay_50_ads.sql`**。

## 时态维 join 的硬形状

```sql
ON d.item_id = e.item_id
AND e.event_time_ms BETWEEN d.valid_from_ms AND d.valid_to_ms
```

> ⚠️ **落地时的列名差异（本票实测）**：票面与 spec §5.2 用 `e.event_time_ms`，
> 而 DWD 的列名是 **`event_time`**（见 `sql/replay_01_ddl.sql` 的 `dwd_behavior_event`），
> 故 `sql/replay_40_dws.sql` 写的是 `e.event_time BETWEEN …`。
> 名字不同、**语义同一**（都是 BIGINT epoch ms，均按事件时刻）；这条差异登记于此，
> 不改 DWD 列名（改列名会动 §5.2 的形状表与 03b 的已交付面）。

走 **batch 区间 join**，**不是 lookup join** —— lookup join 被官方逐字否定（processing-time，回放必错）；Paimon 全站**无 zipper/SCD 官方能力**。

## Acceptance criteria

- [ ] `make replay`（缺省、不带 `LAYER=`）跑完五层**退出 0**，日志逐层有 `layer=<层> rows=…` 且五层**行数均 > 0**
- [ ] 拉链维表能查到**同一 id 的多个版本**，且版本区间**首尾相接不重叠**（`valid_to_ms` 与下一版 `valid_from_ms` 衔接一致）
- [ ] 抽查若干条事件，其回填到的维度值与**事件时刻**的版本一致（不是最新版本，也不是随机版本）—— 时态 join 真的按事件时刻生效
- [ ] `dws_subject_window` 与 `ads_window_metric` 的 `window_start` 语义一致；`subject_type` 取值域恰为三值
- [ ] ADS 表主键列**逐列同名同义**于 Doris `ecom_ads.ads_window_metric`（03f 对账依赖它；不一致现在改最便宜）
- [ ] `dwd_late_event_count` 只有计数列，**没有事件明细**
- [ ] 三天跨度下 `dt` **有 3 个分区**（门槛批跨度兑现）
- [ ] `dwd_late_event_count` 的 `is_late` 侧与 DWD 有对应量（本票只记录，03d 才做 A3 断言）
- [ ] 仍**全 SQL 零 jar**：未新增 jar、未引入 `ADD JAR`

## Notes

- **不实现**（spec §8.2）：lookup join / zipper / SCD · `sequence.field` · 排序 compaction · CUMULATE 作提前发射替代路径（走 TVF 会被 `checkEmitConfiguration` **直接抛异常**，[08] D-14）。
- **Paimon ↔ Doris 是「两套物化、一个口径」**：Paimon 侧 `ads_window_metric` 是**离线口径基准**、**不直接供看板**；Doris 侧是**唯一看板供数点**（`REQ-F-OLAP-01` 冻结）。本票只保证两边**键同构 + 同公式**，逐值相等由 03f 判。
- `pv/uv/gmv` 的指标语义在 gdd 里是「由 [03] 推定回填」——**依据不如其它决策硬**。若施工中发现口径不自洽或与 ADS 表对不上，**停下来登记**（spec §8.3 口径，不猜），不要自行发明定义。
- 本票**不写断言**；`assert_lake.sql` 与 `lake_assert.sh` 从 03d 开始。
- `replay_90_maintain.sql`（`sys.compact` ×N + `sys.expire_snapshots(retain_max=>20)` ×N）也从 03d 开始 —— 本票可先把五层数据落稳。

## Progress

### 实现（2026-10-23）

新落盘 3 个脚本 + 1 个生成器模块 + 2 个测试文件：

| 文件 | 作用 |
| --- | --- |
| `src/ecomgen/dims.py` | 维度变更流（`rng_for("dim")` 派生流；一行一个版本）。初始版本 `change_time = start_ms` ⇒ 批次内每条事件必落某区间 |
| `src/ecomgen/cli.py` | `dump --dim-out`：用事件批次的实际 `[min,max]` 时刻构建维度流 |
| `sql/replay_30_dim.sql` | `dim.jsonl` → 三张拉链表（`LEAD(change_time) OVER (...)-1`）+ 两张静态维 |
| `sql/replay_40_dws.sql` | DWD × 时态维**区间 join**（三条 subject）→ `dws_subject_window` |
| `sql/replay_50_ads.sql` | DWS/DWD → `ads_window_metric`（`ALL` + 渠道切面）+ `dwd_late_event_count` |
| `tests/test_dims.py` | 维度流 18 条判据（初始版本覆盖 / 严格单调 / 跨日多版本 / 换品类必换到别的 / 同 seed 同字节） |
| `tests/test_replay_temporal_sql.py` | 三层 SQL 的静态守卫（13 个用例：区间闭合 / 禁 lookup / 三 subject / ADS 键同构） |
| `src/ecomgen/jsonl.py` | 事件流与维度流**共用**的 JSONL 落盘（键序固定 / 无空格 / LF / 末尾换行） |
| `tests/test_jsonl.py` | 该写手的字节约定（4 个用例） |

另有 **3 处对既有文件的修改**（不在票面 `交付内容` 列出的 6 项里，属必要连带）：

| 文件 | 改什么 | 为什么必须改 |
| --- | --- | --- |
| `src/ecomgen/world.py` | 抽出 `PRICE_CENTS_MIN/_MAX` 常量 | 维度流要产出同口径新价格；两处硬编码会漂移（字节中性：取值不变） |
| `sql/replay_01_ddl.sql` | ① `valid_to_ms` 注释 `开区间`→**闭区间**（与 `− 1` + `BETWEEN` 配对）<br>② 迟到计数段改成「写入已落地 + 语义缺口已登记」 | ① 原注释与实现矛盾（spec 轴发现）<br>② 原文写「本票不写这张表」，与 ③ 段实际写入矛盾 |


`scripts/lake_replay.sh` 的 `LAYERS` 数组在 03b 已列好五层，本票只需脚本落盘即生效；
`GEN_DIM_OUT` 与 `--dim-out` 已接（缺 `dim.jsonl` 会触发重现产）。

### 实机结果（`make replay`，门槛批 1 048 576 事件 / 3 天）

```
layer=ods rows=1048576
layer=dwd rows=1048576
layer=dim rows=105430
layer=dws rows=1550285
layer=ads rows=90348
完成（请求范围全部跑完）        ← exit 0
```

### ★ 施工中发现的三处口径不自洽（按票面 Notes「停下登记，不猜」）

三处都**不会报错**、只会静默产出错数，且都是实测出来的，不是推演：

**① 三条 subject 相加 = 三倍计数（已修，取单条 1:1 subject）**

首版 `dim_key='ALL'` 把 item/category/user 三条 DWS 行 `SUM` 成一行。实测：

```
SUM(pv) over 三 subject = 1409772   ；DWD 的 page_view = 469924   （恰 3.0×）
SUM(gmv) over 三 subject = 184326357.09 ；DWD 的 purchase = 61442119.03（恰 3.0×）
```

根因：三条 subject 的区间 join **都是 1:1 覆盖**（每条事件恰有一个 item 版本、一个
category 版本、一个 user 版本）⇒ 每条 subject 流本身已是**全量事件集**，相加即三倍。
修法：`ALL` 面取 `WHERE subject_type = 'user'`（user_id 是事件必有字段 ⇒ 与事件
一一对应），pv/gmv 因此精确等于 DWD 真值。**这一条改变了首版的合并规则**：
原设计写「pv/gmv 取三 subject 之和、uv 取最大值」，实测不成立。

**② DWS 的 `uv` 在 user-subject 行上恒等于 1（已修，改成 `COUNT(DISTINCT subject_id)`）**

DWS 的 `uv` 是「该 (subject, 窗口) 内的去重用户数」。但 user-subject 的每一行**就是
一个用户**（`subject_id = user_id`）⇒ 该行的去重用户数恒为 1。首版对 `ALL` 面取
`MAX(uv)` 得到 **1**，实测 **4304 个窗口全部与 DWD 对不上**。修法：`uv =
COUNT(DISTINCT subject_id)`（user-subject 行数 = 该窗口去重用户数）。

**③ 渠道切面的 `pv` 用了 `COUNT(*)`（已修，改成 page_view 计数）**

实测渠道合计 pv = 1 048 576（= 事件总数），而全量面 pv = 469 924（= page_view 数）。
两个切面**不可比**，对账当场破。修法：渠道 `pv` 改为
`SUM(CASE WHEN event_type = 'page_view' THEN 1 ELSE 0 END)`，与 DWS 的 pv 定义逐字同源。
修后：渠道合计 == 全量（pv 469 924 / gmv 61 442 119.03），`uv` 逐窗口与 DWD 一致（0 处不符）。

### 仍待 03f 表态的推定口径（本票**不判**，只登记）

1. **`uv` 的两层口径不是同一个数**：实时侧是 **1 h 滚动去重**（`JobConfig.UV_WINDOW_MS`），
   离线这里是 **1 分钟窗口去重**。对账不能默认「同名即同值」。
2. **实时侧 `metric_name` 与离线侧不同名**：`JobConfig` 的 `duplicate_absorbed` / `uv` /
   `provisional_emitted` 里只有 `uv` 与离线同名；`pv`/`gmv` 在实时侧**不存在**（实时侧
   按 `event_<类型>` × `dim_key=category_id` 分桶）。⇒ 03f 的逐 `metric_name` 对账
   需要先定**哪几个 metric_name 可比**，本票不替它决定。
3. **`dim_key` 的全量值是 `'ALL'`**，Doris 侧「无维度」的约定值是空串 `''`。
   只影响**对账怎么配键**，不改键语义 ⇒ 由 03f 配键时处理。
4. **★ 单位不一致（spec 轴审阅发现，**必须由 03f 先裁**）**：
   Paimon 侧 `ads_window_metric.metric_value` 是 `DECIMAL(18, 2)`（**元**，如 gmv 61 442 119.03），
   而 Doris 侧是 `BIGINT` 且 init.sql 注释写明「UV/GMV 亦以**整数**存（**分**）」，
   同一 gmv 在 Doris 应为 6 144 211 903。⇒ spec §5.2 的「同名指标同一公式产出」在
   **单位层面**当前不成立：键同构了，但值的量纲差 100 倍。
   ⚠️ 本票**不改** Doris DDL（那是 03f/看板面的冻结项），也不改 Paimon 的 DECIMAL
   （改它要动 §5.2 的形状表）—— 按 Notes「停下来登记」，挂给 03f 裁。
5. **★ `is_settled` 本票硬编码 1**：离线回放产出的是**已落定**数据（无重算流），
   故全部写 1 合理；但 Doris 的实时侧是 0/1 三态（暂定/落定）。
   对账（03f）只比 `is_settled=1`（spec §4.2），因此这一条**不影响**现状，
   但「离线只会产 settled=1」这个限制要显式记住。
5. **★★ `dwd_late_event_count` 的语义缺口（本票未解决，03d 必须裁）**：
   DDL 说它装**超期事件**（`sideOutputLateData` 丢掉、没进 DWD 的），
   实现装的是**进了 DWD 的 `is_late=1` 行** —— 两个不同群体。超期事件在 DWD 无行，
   故当前写法对它们**零覆盖**。且 `sum(cnt) == count(*) WHERE is_late=1` 是**同义反复**
   （cnt 由同一谓词算出），不能当 A3 证据。二选一（本票不猜）：改本表语义 + 改 DDL 注释，
   或补超期通道写入（需旁路输出，SQL-only 面做不到）。
6. **★★ 缺口：ADS 缺品类/地域分面（不是「形状」）**：`REQ-F-RT-02` 要求看板有
   「品类分布、地域分布」。本票的 ADS 只产 `dim_key='ALL'` 与 `channel=<channel>`
   ——**没有** `category=<id>` / `region=<大区>` 两种 dim_key。
   ⚠️ 首版把这条写成「§5.2 的形状，不是缺陷」，那是**越界**（spec §8.3 不猜）：
   §5.2 只规定 ADS 的**键**长什么样，没说**该产哪些 dim_key 面**。
   本票按票面「DWS 再落成 ADS」的最小解释只落了全量 + 渠道；品类/地域分面**未实现**，
   应按 Notes 挂起待裁（缺它们不阻塞五层贯通，但阻塞看板可用性）。
7. **`subject_type` 三值是 `item`/`category`/`user`**，与 DDL 注释一致；ADS 键里没有它，
   故三个分面在 ADS 上不可区分 —— 这是键形状的**直接结果**，但「因此不产分面」
   属上一条的缺口，不要用形状当理由。

### 与既有约定的一处刻意差异（已在脚本内逐步注释）

`replay_40_dws.sql` 读 DWD **不加落地日过滤**，与 `replay_20_dwd.sql` 读 ODS 时相反：
DWD 是主键表 + 整表 `INSERT OVERWRITE`（表里只有本批次），而 ODS 是 append + 落地日
分区（跨天累积）。给 DWD 加落地日过滤会**读空**（DWD 的 `dt` 是事件日）。
`tests/test_replay_temporal_sql.py::test_dws_does_not_filter_dwd_by_landing_day` 钉住这条。

### 一处环境性修（不是口径问题）

ADS 层首版 4 并行时报 `Insufficient number of network buffers: required 512, but only
471 available`（total 5079 × 32768 B）。该层输入是 DWS 的窗口级小结果（数千行），
4 并行会把每个算子的 buffer 配额摊薄到不够。修法两条：① 三个聚合面各只算一遍
（首版 pv/uv/gmv 各写一个 `GROUP BY` 的 `UNION ALL` ⇒ 三条支路各 shuffle 一次全表）；
② 本层 `parallelism.default = '2'`（`REQ-F-RT-08` 的「并行度 ≤4」约束**实时作业**，
batch 层用 2 不违反，也不影响口径）。

### 验收证据 —— **可复现**（`artifacts/replay_03c_evidence.sql` + 同名 `.log`）

★ 首版把取证写成「一次性查询，跑完已删」，票面只留数字 —— spec 轴审阅判定
「不可复现的证据等于叙述」。现改为：查询集**留档**在 `artifacts/replay_03c_evidence.sql`，
输出留档在 `artifacts/replay_03c_evidence.log`（21 条结果）。复跑：

```
MSYS_NO_PATHCONV=1 docker exec -i eco-flink-jm bash -c "cat > /tmp/ev.sql" \
  < artifacts/replay_03c_evidence.sql
MSYS_NO_PATHCONV=1 docker exec -i eco-flink-jm /opt/flink/bin/sql-client.sh -f /tmp/ev.sql
```

⚠️ 不能 `docker cp` 进 `/sql`：该挂载点是 **`:ro`**（只读）；`/data` 同样只读。

| 判据 | 实测（来自留档日志） |
| --- | --- |
| AC1 五层行数均 > 0 且 exit 0 | ods 2 097 152 / dwd 1 048 576 / dim_item 2 417 / dim_category 17 / dim_user 102 978 / dim_region 12 / dim_channel 6 / dws 1 550 285 / ads 90 348 |
| AC2 同 id 多版本 + 区间衔接 | 398 个商品 ≥2 版；衔接断点 **0**、区间重叠 **0** |
| **AC3a 时态 join（item 品类）** | **29 660** 条事件的品类 ≠ 该商品**最新**版本品类 |
| **AC3b 时态 join（item 价格）** | **105 704** 条事件的价格 ≠ 该商品**最新**版本价格 ⇒ 回填的**值**真按事件时刻取 |
| **AC3c 时态 join（user 等级）** | **11 691** 条事件的等级 ≠ 该用户**最新**版本等级 |
| AC3d 区间 join 未漏行 | 未命中事件 **0** |
| **AC3e 三条 subject 都是 1:1 覆盖** | item / category / user 三条 pv 合计 **均 = 469 924** = DWD page_view 数 ⇒ 区间无扇出（有扇出会 >） |
| AC4 `window_start` 语义一致 | 未对齐 60 000 = 0；ADS 窗口无对应 DWS = 0；`dt` 与窗口日不一致 = 0 |
| AC4 `subject_type` 恰三值 | item 987 696 / category 51 616 / user 510 973（**仅此三值**） |
| AC5a ADS 键唯一 | 键重复 0 |
| AC5b `metric_name` 恰三值 | pv / uv / gmv 各 30 116 行 |
| AC5c `dim_key` 面 | `ALL` + 6 个 `channel=…`（**无品类/地域面，见「仍待裁」第 6 条**） |
| AC6 迟到计数**只有计数列** | 表列 = `(dt, late_bucket, cnt)`，无事件明细列 |
| ⚠️ AC6 **语义未裁** | `sum(cnt)` 131 316 == DWD `is_late=1` 行数 —— 但这是**同义反复**（cnt 由同一谓词算）。且本表按 DDL 设计该装**超期事件**（DWD 里没有行），当前写法对它们零覆盖。⇒ 见下「仍待裁」第 5 条，03d 做 A3 前必须先裁 |
| AC7 3 个 `dt` 分区 | dws / ads / late 三处均 **3**（2024-01-01..03） |
| AC8 全 SQL 零 jar | 三脚本无 `ADD JAR`（文本判据在 tests/test_replay_temporal_sql.py） |

ADS 与 DWD 的横向对账（本票自证口径闭合，非 03f 的 Doris 对账）：

| 指标 | ADS（`dim_key='ALL'`） | DWD 真值 | 判据 id |
| --- | --- | --- | --- |
| pv | 469 924.00 | 469 924 | X1 |
| gmv | 61 442 119.03 | 61 442 119.03 | X1 |
| uv | **0 个窗口不符**（分母 = 全部 `ALL`-uv 窗口，非空） | 逐窗口 `COUNT(DISTINCT user_id)` | X3 |
| 渠道切面合计 pv | 469 924.00 | 与全量面相同 | X2 |

⚠️ gmv 这一行的**单位**两边不同（Paimon 是元、Doris 是分）—— 见上「仍待裁」第 4 条，
此处比对的是 Paimon↔DWD 的**同单位**真值，不涉及 Doris。

---

## ✅ 回退已修复 + 一处部署面缺陷（2026-10-23 续工）

上一节的停点已**全部收口**。下面记的是「怎么修的」与「修的过程中挖出的第二层问题」——
第二层比第一层更值得记住：它的症状**完全不像**根因。

### ① ADS 两切面合并（停点里那处回退）—— 已修，@rows@ 回归 90 348

按停点预想的「正确修法」落盘：两个切面并进**同一条** @INSERT OVERWRITE@。

★ **但没有按预想的写法写**（预想是「把两段 SELECT ... UNION ALL 并成一个源」，实测撞墙）：
并成一条后整条语句的输入从「DWS 小结果」变成「DWS 小结果 ∪ DWD 百万行渠道聚合」，
**预测的 @Insufficient number of network buffers@ 真的发生了**：

```
java.io.IOException: Insufficient number of network buffers: required 512,
  but only 471 available. (total 5079 × 32768 B)
```

停点当时写「若撞，正确做法不是继续降并行度，而是让渠道面**先物化**到临时视图再参与
union —— 待实测决定」。实测结论：**物化到临时视图不够**，真正管用的是**换摊平（unpivot）的写法**：

| 写法 | 结果 |
| --- | --- |
| 三条各读一遍聚合视图的 @UNION ALL@（首版） | ✗ 撞 buffer 上限 |
| 两切面 @UNION ALL@ 成一条大 INSERT（停点预想） | ✗ 撞 buffer 上限 |
| **两切面并成一张同列中间结果 → CROSS JOIN 3 行静态指标名 → @CASE metric_name@ 挑列** | ✓ 通过 |

关键差别：指标名的展开从「**三条支路各 shuffle 一遍**」变成「**3 行静态视图广播**」。
当前实现的落点：
- ⓪ @v_metric_name@：3 行静态视图（@pv@/@uv@/@gmv@）；
- ① @v_all_metric@ / ② @v_channel_metric@：各自**一次**聚合算三列；
- ③ @v_metric_src@（两切面 UNION ALL，列集相同）@CROSS JOIN@ ⓪ → @v_ads_row@
  （用 @CASE metric_name@ 挑 @pv@/@uv@/@gmv@ 列，**不做**三条各扫一遍）；
- ④ **一条** @INSERT OVERWRITE ads_window_metric ... FROM v_ads_row@。

并行度仍是 2（未靠降并行度绕过，理由与首版同：那是「三次全表扫描」的问题，报错只是逼出来）。

★ **新增两条静态守卫**（@tests/test_replay_temporal_sql.py@），因为这次事故的两次
错法都**不会**让任何既有测试变红：
- @test_no_sql_file_writes_a_table_more_than_once@：同一 SQL 文件里一张表**不得**被两条
  INSERT 写（扫 @sql/*.sql@ 全部文件）。这是事故的最小不变式。
- @test_ads_writes_both_slices_in_one_insert_statement@：ADS 主表恰 1 条 INSERT，
  且两个切面都在同一份源里（只钉「一条 INSERT」不够 —— 删掉整个渠道面也能满足它）。

### ② ★★ 第二层问题：@flink-jm@ 的 @mem_limit: 1g@ 装不下自己（部署面缺陷）

修好 ADS 后重跑，**五层仍在不同位置以 exit 137 失败**，且失败点每次不一样（先是 DWS、
后是 ADS）。这个「位置漂移」正是它不是 SQL 问题的线索。

**根因**：@make replay@ **不是**在宿主跑 SQL，而是
@docker exec eco-flink-jm /opt/flink/bin/sql-client.sh -f /sql/<file>@
⇒ **sql-client 的 JVM 与 JobManager 的 JVM 住同一个 cgroup**（同一个 @mem_limit@）。
而 @docker/flink/conf/config.yaml@ 给 JobManager 本体的
@jobmanager.memory.process.size@ 就是 **1024m** ⇒ 1g 的上限连 **Flink 进程一项**都刚好卡满，
客户端 JVM 只能靠内核挤余量。最重的一层（ADS）提交时就被 **cgroup OOM-kill**。

**实测证据（内核 dmesg 逐字）**：

```
oom-kill:constraint=CONSTRAINT_MEMCG,nodemask=(null),cpuset=35ada115b62fbe6f...,
  mems_allowed=0,oom_memcg=/wsl-user/distro-365/docker/35ada115b62fbe6f...,
  task_memcg=/wsl-user/distro-365/docker/35ada115b62fbe6f...,task=java,pid=23998,uid=9999
Memory cgroup out of memory: Killed process 23998 (java) total-vm:1876884kB,
  anon-rss:563428kB,file-rss:12500kB,shmem-rss:0kB,UID:9999 pgtables:2176kB
```

而 @35ada115b62f…@ 正是 @docker inspect eco-flink-jm --format '{{.Id}}'@ 的值 ——
**memcg 的 hash 直接指认了容器**，不是推测。

⚠️ **为什么这个缺陷极难归因**（值得单独记住）：
- @.State.OOMKilled@ 是 **false** —— 看起来不像 OOM；
- 症状是 @docker exec@ 返回 **137** + JM 被 @restart: unless-stopped@ 拉起
  （@RestartCount@ 每次 +1），@make replay@ 打印的是
  「docker exec 退出 137（容器/挂载/守护进程层失败，不是 SQL 错误）」；
- 若 JM 在作业中途死，Flink 侧报的是
  @JobMasterException: TaskManager with id … is no longer reachable@
  ⇒ 现场读起来像「TaskManager 掉线」，与真实根因隔着两层。

**修法**：@docker-compose.yml@ 的 @flink-jm@ 由 @1g@ 上调到 **@2g@**（@memswap_limit@ 同步）。
这是**部署面缺陷修正**而非调参绕开报错：判据是「声明上限 ≥ 该容器实际**必然**占用」，
而 Flink 进程一项就 1024m、客户端与它同 cgroup。

**新增静态守卫** @tests/test_deployment_memory.py@（2 个用例）：
- @mem_limit(flink-jm) >= jobmanager.memory.process.size + 512MiB@
  （512 MiB 是给 sql-client 的**设计值**余量，**不是**官方最小堆）；
- @memswap_limit == mem_limit@（HLD §4.2 的既有契约，顺带补上此前只由脚本断言的一条）。
★ 判据刻意**不**写成 @mem_limit == 2g@：一旦有人把 @process.size@ 上调，硬编码值会再次
变成「装不下」而测试仍绿。已用**证伪**验证：把 compose 改回 @1g@ 跑该测试 ⇒ 2 failed。

**预算同步**：HLD §4.2 的常驻上限合计 @5.25 GiB@ → **@6.25 GiB@**（天花板 11.68，余 5.43），
@map.md@ 与 @reproducibility-boundary.md@ 里的同一数字已一并订正。

### 实机结果（最终验收，@make replay@ 缺省、不带 @LAYER=@）

容器由 compose **重建**（不是 @docker update@ 临时改），@Mem=2147483648 / Restarts=0@：

```
layer=ods rows=1048576
layer=dwd rows=1048576
layer=dim rows=105430
layer=dws rows=1550285
layer=ads rows=90348
[replay] 完成（请求范围全部跑完）        ← exit 0
```

@ads=90 348@ 与事故前的正确态**逐字一致**（事故中两条 OVERWRITE 时是 77 436）。

### 验收判据现状（AC1–AC8）

AC1（五层行数 > 0 + exit 0）在上面日志里逐层兑现；AC2–AC8 的取证仍以
@artifacts/replay_03c_evidence.sql@ 为**可复跑**的查询集（复跑方式见上一节）。
本票**不写断言**（@assert_lake.sql@ / @lake_assert.sh@ 从 03d 开始），故此处不新增 AC 判据，
只把「五层贯通」这一件事从**红**变**绿**。

### 未完成 / 移交 03d 的（与上一节一致，无变化）

「仍待 03f 表态的推定口径」1–7 条、以及 @dwd_late_event_count@ 的语义缺口，**均未裁、未改**。
本票按 Notes「不猜」，只修了自己引入的回退与挖出的部署面缺陷。
